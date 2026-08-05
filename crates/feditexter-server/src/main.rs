use feditexter_server::{build_app, db::AppState};
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

    let pool = MySqlPool::connect(&database_url).await?;
    sqlx::migrate!("../../migrations").run(&pool).await?;
    info!("connected to db, migrations applied");

    let app = build_app(AppState { pool });

    let addr = format!("{bind_addr}:{port}");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    info!("listening on http://{addr}");
    axum::serve(listener, app).await?;
    Ok(())
}