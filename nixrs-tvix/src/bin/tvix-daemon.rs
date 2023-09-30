use std::io;

use clap::Parser;
use nixrs_tvix::pathinfoservice;
use tonic::transport::Server;
use tracing::info;
use tracing::Level;
use tracing_subscriber::filter;
use tracing_subscriber::prelude::*;
use tvix_castore::blobservice;
use tvix_castore::directoryservice;
use tvix_castore::proto::blob_service_server::BlobServiceServer;
use tvix_castore::proto::directory_service_server::DirectoryServiceServer;
use tvix_castore::proto::GRPCBlobServiceWrapper;
use tvix_castore::proto::GRPCDirectoryServiceWrapper;
use tvix_store::listener::ListenerStream;
use tvix_store::proto::path_info_service_server::PathInfoServiceServer;
use tvix_store::proto::GRPCPathInfoServiceWrapper;

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

    #[arg(long, env, default_value = "sled:///var/lib/tvix-store/blobs.sled")]
    blob_service_addr: String,

    #[arg(
        long,
        env,
        default_value = "sled:///var/lib/tvix-store/directories.sled"
    )]
    directory_service_addr: String,

    #[arg(
        long,
        env,
        default_value = "sub+sled:///var/lib/tvix-store/pathinfo.sled?recursive="
    )]
    path_info_service_addr: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    // configure log settings
    let level = cli.log_level.unwrap_or(Level::INFO);

    let filter = filter::Targets::new()
        .with_target("sled", Level::INFO)
        .with_target("hyper", Level::INFO)
        .with_target("h2", Level::INFO)
        .with_target("tokio_util", Level::INFO)
        .with_target("nixrs_util::archive::parser", Level::INFO)
        .with_target("my_crate::interesting_module", Level::DEBUG)
        .with_default(level);

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
        })
        .with(filter);

    tracing::subscriber::set_global_default(subscriber).expect("Unable to set global subscriber");
    tracing_log::LogTracer::init()?;

    let blob_service = blobservice::from_addr(&cli.blob_service_addr)?;
    let directory_service = directoryservice::from_addr(&cli.directory_service_addr)?;
    let path_info_service = pathinfoservice::from_addr(
        &cli.path_info_service_addr,
        blob_service.clone(),
        directory_service.clone(),
    )?;

    let listen_address = cli
        .listen_address
        .unwrap_or_else(|| "[::]:8000".to_string())
        .parse()
        .unwrap();

    let mut server = Server::builder();

    #[allow(unused_mut)]
    let mut router = server
        .add_service(BlobServiceServer::new(GRPCBlobServiceWrapper::from(
            blob_service,
        )))
        .add_service(DirectoryServiceServer::new(
            GRPCDirectoryServiceWrapper::from(directory_service),
        ))
        .add_service(PathInfoServiceServer::new(
            GRPCPathInfoServiceWrapper::from(path_info_service),
        ));

    #[cfg(feature = "tonic-reflection")]
    {
        let reflection_svc = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(CASTORE_FILE_DESCRIPTOR_SET)
            .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
            .build()?;
        router = router.add_service(reflection_svc);
    }

    info!("tvix-store listening on {}", listen_address);

    let listener = ListenerStream::bind(&listen_address).await?;

    router.serve_with_incoming(listener).await?;

    Ok(())
}
