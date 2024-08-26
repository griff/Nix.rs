use std::future::{ready, Ready};
use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use nixrs_legacy::store::legacy_worker::LegacyWrapStore;
use nixrs_legacy::store::FailStore;
use nixrs_legacy::store_path::StoreDir;
use nixrs_ssh_store::server::{Server, ServerConfig};
use nixrs_ssh_store::StoreProvider;
use nixrs_tvix::store::TvixStore;
use tracing::{info, Level};
use tracing_subscriber::prelude::*;
use tvix_castore::blobservice;
use tvix_castore::directoryservice;
use tvix_store::pathinfoservice;

#[derive(Clone)]
struct TvixStoreProvider {
    pub store_dir: StoreDir,
    pub blob_service: Arc<dyn blobservice::BlobService>,
    pub directory_service: Arc<dyn directoryservice::DirectoryService>,
    pub path_info_service: Arc<dyn pathinfoservice::PathInfoService>,
}

impl StoreProvider for TvixStoreProvider {
    type Error = nixrs_legacy::store::Error;
    type LegacyStore = LegacyWrapStore<TvixStore>;
    type LegacyFuture = Ready<Result<Option<Self::LegacyStore>, Self::Error>>;
    type DaemonStore = FailStore;
    type DaemonFuture = Ready<Result<Option<Self::DaemonStore>, Self::Error>>;

    fn get_legacy_store(
        &self,
        _stderr: nixrs_ssh_store::io::ExtendedDataWrite,
    ) -> Self::LegacyFuture {
        let tvix_store = TvixStore {
            store_dir: self.store_dir.clone(),
            blob_service: self.blob_service.clone(),
            directory_service: self.directory_service.clone(),
            path_info_service: self.path_info_service.clone(),
        };
        let store = LegacyWrapStore::new(tvix_store);
        ready(Ok(Some(store)))
    }

    fn get_daemon_store(&self) -> Self::DaemonFuture {
        ready(Ok(None))
    }
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Whether to log in JSON
    #[arg(long)]
    json: bool,

    #[arg(long)]
    log_level: Option<Level>,

    #[arg(long, short = 'l')]
    listen_address: Option<String>,

    #[arg(long, env, default_value = "grpc+http://[::1]:8000")]
    blob_service_addr: String,

    #[arg(long, env, default_value = "grpc+http://[::1]:8000")]
    directory_service_addr: String,

    #[arg(long, env, default_value = "grpc+http://[::1]:8000")]
    path_info_service_addr: String,

    #[arg(long)]
    write: bool,

    #[arg(long)]
    host_key: Vec<PathBuf>,

    #[arg(long)]
    user_key: Vec<PathBuf>,

    #[arg(long, default_value = ".")]
    config_root: PathBuf,
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
    let store_provider = TvixStoreProvider {
        store_dir: StoreDir::default(),
        blob_service,
        directory_service,
        path_info_service,
    };
    let mut config = ServerConfig::with_store(store_provider);
    if cli.host_key.is_empty() {
        config.load_host_keys(&cli.config_root).await;
    } else {
        for path in cli.host_key.iter() {
            config.load_host_key(path).await?;
        }
    }
    if cli.user_key.is_empty() {
        config.load_user_keys(&cli.config_root).await;
    } else {
        for path in cli.user_key.iter() {
            config.load_user_key(path, cli.write).await?;
        }
    }
    let server = Server::with_config(config)?;
    let addr = cli.listen_address.unwrap_or("localhost:12222".to_string());
    info!("Listening on {}", addr);
    server.run(&addr).await?;
    Ok(())
}
