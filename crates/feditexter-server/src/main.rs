use std::env;

use axum::{routing::get, Router};
use sqlx::{MySqlPool, Pool, MySql};
use tracing::info;

async fn healthz() -> &'static str {
    "ok"
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt::init();

    let database_url = env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set in .env");
    let port = env::var("PORT").unwrap_or_else(|_| "3000".to_string());

    let pool: Pool<MySql> = MySqlPool::connect(&database_url).await?;
    sqlx::migrate!("../../migrations").run(&pool).await?;
    info!("connected to db, migrations applied");

    let app = Router::new()
        .route("/healthz", get(healthz))
        .with_state(pool);

    let addr = format!("0.0.0.0:{port}");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    info!("listening on http://{addr}");

    axum::serve(listener, app).await?;
    Ok(())
}