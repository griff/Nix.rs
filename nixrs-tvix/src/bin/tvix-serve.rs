use std::io;

use clap::Parser;
use nixrs::store_path::StoreDir;
use tracing::Level;
use tracing_subscriber::prelude::*;
use tvix_castore::blobservice;
use tvix_castore::directoryservice;
use tvix_store::pathinfoservice;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Whether to log in JSON
    #[arg(long)]
    json: bool,

    #[arg(long)]
    log_level: Option<Level>,

    #[arg(long, env, default_value = "grpc+http://[::1]:8000")]
    blob_service_addr: String,

    #[arg(long, env, default_value = "grpc+http://[::1]:8000")]
    directory_service_addr: String,

    #[arg(long, env, default_value = "grpc+http://[::1]:8000")]
    path_info_service_addr: String,

    #[arg(long)]
    write: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    // configure log settings
    let level = cli.log_level.unwrap_or(Level::INFO);

    let subscriber = tracing_subscriber::registry()
        .with(if cli.json {
            Some(
                tracing_subscriber::fmt::Layer::new()
                    .with_writer(io::stderr.with_max_level(level))
                    .json(),
            )
        } else {
            None
        })
        .with(if !cli.json {
            Some(
                tracing_subscriber::fmt::Layer::new()
                    .with_writer(io::stderr.with_max_level(level))
                    .pretty(),
            )
        } else {
            None
        });

    tracing::subscriber::set_global_default(subscriber).expect("Unable to set global subscriber");

    let blob_service = blobservice::from_addr(&cli.blob_service_addr)?;
    let directory_service = directoryservice::from_addr(&cli.directory_service_addr)?;
    let path_info_service = pathinfoservice::from_addr(
        &cli.path_info_service_addr,
        blob_service.clone(),
        directory_service.clone(),
    )?;
    let tvix_store = nixrs_tvix::store::TvixStore {
        store_dir: StoreDir::default(),
        blob_service,
        directory_service,
        path_info_service,
    };
    let store = nixrs::store::legacy_worker::LegacyWrapStore::new(tvix_store);
    let source = tokio::io::stdin();
    let out = tokio::io::stdout();
    let build_log = tokio::io::stderr();

    nixrs::store::legacy_worker::run_server_with_log(source, out, store, build_log, cli.write)
        .await?;
    Ok(())
}
