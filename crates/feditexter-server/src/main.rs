use feditexter_server::{bot, build_app, db::AppState, federation::Federation, tui};
use std::collections::VecDeque;
use std::env;
use std::io::IsTerminal;
use std::sync::{Arc, Mutex};
use sqlx::MySqlPool;
use tracing::info;
use tracing_subscriber::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let mut env_file: Option<String> = None;
    let mut enable_tui = false;
    let mut subcommand: Option<String> = None;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--env" => {
                if i + 1 < args.len() {
                    env_file = Some(args[i + 1].clone());
                    i += 1;
                }
            }
            "--tui" => enable_tui = true,
            other if !other.starts_with('-') && subcommand.is_none() => {
                subcommand = Some(other.to_string());
            }
            _ => {}
        }
        i += 1;
    }
    load_env(env_file.as_deref());

    // `migrate` only touches the database (applies pending migrations and
    // verifies the schema), then exits — used by the update script to make sure
    // the DB is set up for the next version before the server restarts.
    if subcommand.as_deref() == Some("migrate") {
        return run_migrate().await;
    }

    // `--tui` launches the btop++-style dashboard. When active, server logs are
    // routed into the dashboard's log ring instead of stdout so they don't
    // corrupt the alternate screen.
    let enable_tui = enable_tui && std::io::stdout().is_terminal();
    let log_ring: Arc<Mutex<VecDeque<String>>> = Arc::new(Mutex::new(VecDeque::new()));

    if enable_tui {
        let filter = tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
        let ring = log_ring.clone();
        tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(move || tui::RingLog::new(ring.clone(), 2000))
                    .with_filter(filter),
            )
            .init();
    } else {
        tracing_subscriber::fmt::init();
    }

    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set in .env");
    let bind_addr = env::var("BIND_ADDR").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port = env::var("PORT").unwrap_or_else(|_| "3000".to_string());
    let public_domain = env::var("PUBLIC_DOMAIN").unwrap_or_else(|_| "localhost".to_string());
    // Email verification is on by default; set REQUIRE_EMAIL_VERIFICATION=0 to disable.
    let verify_emails = env::var("REQUIRE_EMAIL_VERIFICATION").map(|v| v != "0").unwrap_or(true);
    let mailer = if verify_emails {
        let m = feditexter_server::mail::Mailer::from_env();
        if m.is_none() {
            info!("email verification enabled but SMTP not configured — codes will be logged to the server output");
        }
        m
    } else {
        None
    };

    let pool = MySqlPool::connect(&database_url).await?;
    sqlx::migrate!("../../migrations").run(&pool).await?;
    info!("connected to db, migrations applied");

    let federation = Federation::init(&pool, &public_domain).await?;
    info!("federation identity: {} key {}", public_domain, federation.public_key_hex());

    let state = AppState {
        pool,
        hub: Default::default(),
        federation,
        verify_emails,
        mailer,
        http: reqwest::Client::builder()
            .user_agent("FediTexterServer/1.0")
            .connect_timeout(std::time::Duration::from_secs(10))
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .expect("failed to build http client"),
        presence: Default::default(),
        voice: Default::default(),
    };

    // Clone the state for axum; keep an Arc for the background bot task.
    let shared = Arc::new(state);
    tokio::spawn(bot::bot_loop(Arc::clone(&shared)));
    let app = build_app(shared.as_ref().clone());

    let addr = format!("{bind_addr}:{port}");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    info!("listening on http://{addr}");

    let server = async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
    };

    if enable_tui {
        let (quit_tx, quit_rx) = tokio::sync::oneshot::channel();
        let state_for_tui = shared.as_ref().clone();
        let ring = log_ring.clone();
        let rt = tokio::runtime::Handle::current();
        std::thread::spawn(move || {
            if let Err(e) = tui::run_tui(state_for_tui, ring, rt, quit_tx) {
                eprintln!("[tui] dashboard error: {e}");
            }
        });
        tokio::pin!(server);
        tokio::select! {
            result = &mut server => {
                result?;
            }
            _ = quit_rx => {
                info!("dashboard quit — shutting down");
            }
        }
    } else {
        server.await?;
    }
    Ok(())
}

/// Load the environment from `--env <path>` if given, otherwise the first
/// existing file among: `.env` in the working directory, the per-user config
/// file (`~/.config/feditexter/server.env`), and `.env` next to the executable.
/// This lets `feditexter-server --tui` (and the plain server) be launched from
/// anywhere on the system.
fn load_env(env_file: Option<&str>) {
    if let Some(path) = env_file {
        if let Err(e) = dotenvy::from_path(path) {
            eprintln!("[env] could not load {path}: {e}");
        }
        return;
    }
    let mut candidates = vec![".env".to_string()]; // cwd .env
    if let Some(home) = env::var("HOME").ok() {
        candidates.push(
            std::path::Path::new(&home).join(".config").join("feditexter").join("server.env").to_string_lossy().into_owned(),
        );
    }
    if let Ok(exe) = env::current_exe() {
        if let Some(dir) = exe.parent() {
            candidates.push(dir.join(".env").to_string_lossy().into_owned());
        }
    }
    for candidate in candidates {
        if std::path::Path::new(&candidate).is_file() {
            let _ = dotenvy::from_path(&candidate);
            break;
        }
    }
}

/// Apply pending DB migrations and report the resulting schema state. Exits
/// with a non-zero code if the database is unreachable or a migration fails.
async fn run_migrate() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::level_filters::LevelFilter::INFO)
        .init();
    let url = env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set (in .env, ~/.config/feditexter/server.env, or via --env)");
    let pool = MySqlPool::connect(&url).await?;
    println!("[migrate] connected to database");

    let migrator = sqlx::migrate!("../../migrations");
    println!(
        "[migrate] {} migrations registered; applying any pending…",
        migrator.migrations.len()
    );
    migrator.run(&pool).await?;
    println!("[migrate] migrations applied OK");

    let applied: Vec<(String,)> = sqlx::query_as(
        "SELECT description FROM _sqlx_migrations ORDER BY version",
    )
    .fetch_all(&pool)
    .await?;
    println!("[migrate] applied migrations:");
    for (desc,) in applied {
        println!("[migrate]   - {desc}");
    }
    Ok(())
}
