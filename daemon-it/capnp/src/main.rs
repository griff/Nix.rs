use std::future::Future;
use std::path::Path;

use capnp_rpc::rpc_twoparty_capnp::Side;
use capnp_rpc::{
    Disconnector, RpcSystem, new_client, new_future_client, rpc_twoparty_capnp, twoparty,
};
use clap::Parser;
use futures::io as fio;
use futures::{AsyncReadExt, TryFutureExt as _, try_join};
use nixrs::daemon::client::DaemonClient;
use nixrs::daemon::{DaemonError, DaemonResult, MutexHandshakeStore, server};
use nixrs_capnp::nix_daemon::{HandshakeLoggedCapnpServer, LoggedCapnpStore};
use nixrs_capnp::{DEFAULT_BUF_SIZE, from_error};
use tokio::io::{AsyncRead, AsyncWrite, duplex};
use tokio::task::LocalSet;
use tracing::info;
use tracing_subscriber::layer::SubscriberExt as _;

fn make_server<S>(client_stream: S) -> (impl Future<Output = DaemonResult<()>>, Disconnector<Side>)
where
    S: AsyncRead + AsyncWrite + 'static,
{
    let (reader, writer) =
        tokio_util::compat::TokioAsyncReadCompatExt::compat(client_stream).split();

    let network = Box::new(twoparty::VatNetwork::new(
        fio::BufReader::new(reader),
        fio::BufWriter::new(writer),
        rpc_twoparty_capnp::Side::Client,
        Default::default(),
    ));
    let mut rpc_system = RpcSystem::new(network, None);
    let disconnector = rpc_system.get_disconnector();
    let client: nixrs_capnp::capnp::nix_daemon_capnp::logged_nix_daemon::Client =
        rpc_system.bootstrap(rpc_twoparty_capnp::Side::Server);
    tokio::task::spawn_local(rpc_system);
    let ret = async move {
        let store = LoggedCapnpStore::load(client)
            .await
            .map_err(DaemonError::custom)?;
        let b = server::Builder::new();
        b.local_serve_connection(tokio::io::stdin(), tokio::io::stdout(), store)
            .await
    };
    (ret, disconnector)
}

fn make_client<S, P>(
    server_stream: S,
    socket: P,
) -> (impl Future<Output = DaemonResult<()>>, Disconnector<Side>)
where
    S: AsyncRead + AsyncWrite + 'static,
    P: AsRef<Path> + Send + 'static,
{
    let (reader, writer) =
        tokio_util::compat::TokioAsyncReadCompatExt::compat(server_stream).split();
    let network = twoparty::VatNetwork::new(
        futures::io::BufReader::new(reader),
        futures::io::BufWriter::new(writer),
        rpc_twoparty_capnp::Side::Server,
        Default::default(),
    );

    let client: nixrs_capnp::capnp::nix_daemon_capnp::logged_nix_daemon::Client =
        new_future_client(async move {
            let store = DaemonClient::builder()
                .build_unix(socket)
                .await
                .map_err(from_error)?;
            let store = MutexHandshakeStore::new(store);

            let rpc_server = HandshakeLoggedCapnpServer::new(store);
            let client: nixrs_capnp::capnp::nix_daemon_capnp::logged_nix_daemon::Client =
                new_client(rpc_server);
            Ok(client)
        });
    let rpc_system = RpcSystem::new(Box::new(network), Some(client.client));
    let disconnector = rpc_system.get_disconnector();
    (rpc_system.map_err(DaemonError::custom), disconnector)
}
pub fn init_logging(verbosity: Option<tracing::Level>) {
    use tracing_subscriber::Layer as _;
    use tracing_subscriber::util::SubscriberInitExt as _;

    let layered = tracing_subscriber::fmt::layer()
        .with_file(false)
        .with_line_number(false)
        .with_writer(std::io::stderr);

    let layered = layered.with_filter({
        let b = tracing_subscriber::EnvFilter::builder()
            .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
            .from_env()
            .expect("invalid RUST_LOG");
        if let Some(level) = verbosity {
            b.add_directive(level.into())
        } else {
            b
        }
    });

    tracing_subscriber::registry().with(layered).init();
}

#[derive(Debug, Parser)]
#[command(version)]
struct Args {
    #[clap(flatten)]
    verbosity: clap_verbosity_flag::Verbosity<clap_verbosity_flag::InfoLevel>,
}

#[tokio::main(flavor = "current_thread")]
pub async fn main() {
    let args = Args::parse();
    init_logging(
        args.verbosity
            .is_present()
            .then_some(())
            .and_then(|_| args.verbosity.tracing_level()),
    );
    info!("Running test");
    let local = LocalSet::new();
    local
        .run_until(async move {
            let (client_stream, server_stream) = duplex(DEFAULT_BUF_SIZE);

            let (server_fut, _server_disconnect) = make_server(client_stream);
            let socket = std::env::var_os("NIXRS_SOCKET").unwrap();
            let (client_fut, _client_disconnect) = make_client(server_stream, socket);
            try_join!(client_fut, server_fut)
        })
        .await
        .unwrap();
}
