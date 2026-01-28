//! Helios FHIR Server (HFS)
//!
//! A high-performance FHIR R4/R4B/R5/R6 server.

use clap::Parser;
use helios_rest::{ServerConfig, create_app_with_config, init_logging};
use tracing::info;

#[cfg(feature = "sqlite")]
use helios_persistence::backends::sqlite::SqliteBackend;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = ServerConfig::parse();
    init_logging(&config.log_level);

    if let Err(errors) = config.validate() {
        for error in &errors {
            eprintln!("Configuration error: {}", error);
        }
        std::process::exit(1);
    }

    info!(
        port = config.port,
        host = %config.host,
        "Starting Helios FHIR Server"
    );

    #[cfg(feature = "sqlite")]
    let backend = {
        let db_path = config.database_url.as_deref().unwrap_or("fhir.db");
        info!(database = %db_path, "Initializing SQLite backend");

        let backend = if db_path == ":memory:" {
            SqliteBackend::in_memory()?
        } else {
            SqliteBackend::open(db_path)?
        };
        backend.init_schema()?;
        backend
    };

    #[cfg(not(any(feature = "sqlite", feature = "postgres", feature = "mongodb")))]
    compile_error!("At least one database backend feature must be enabled");

    let app = create_app_with_config(backend, config.clone());
    let addr = config.socket_addr();

    info!(address = %addr, "Server listening");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
