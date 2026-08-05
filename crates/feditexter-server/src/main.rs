use feditexter_server::{build_app, db::AppState, federation::Federation};
use std::env;
use sqlx::MySqlPool;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt::init();

    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set in .env");
    let bind_addr = env::var("BIND_ADDR").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port = env::var("PORT").unwrap_or_else(|_| "3000".to_string());
    let public_domain = env::var("PUBLIC_DOMAIN").unwrap_or_else(|_| "localhost".to_string());
    let verify_emails = env::var("REQUIRE_EMAIL_VERIFICATION").map(|v| v == "1").unwrap_or(false);

    let pool = MySqlPool::connect(&database_url).await?;
    sqlx::migrate!("../../migrations").run(&pool).await?;
    info!("connected to db, migrations applied");

    let federation = Federation::init(&pool, &public_domain).await?;
    info!("federation identity: {} key {}", public_domain, federation.public_key_hex());

    let app = build_app(AppState {
        pool,
        hub: Default::default(),
        federation,
        verify_emails,
    });

    let addr = format!("{bind_addr}:{port}");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    info!("listening on http://{addr}");
    axum::serve(listener, app).await?;
    Ok(())
}