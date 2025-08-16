use std::future::Future;
use std::path::Path;

use capnp_rpc::rpc_twoparty_capnp::Side;
use capnp_rpc::{
    Disconnector, RpcSystem, new_client, new_future_client, rpc_twoparty_capnp, twoparty,
};
use futures::io as fio;
use futures::{AsyncReadExt, TryFutureExt as _, try_join};
use nixrs::daemon::client::DaemonClient;
use nixrs::daemon::{DaemonError, DaemonResult, MutexHandshakeStore, server};
use nixrs_capnp::nix_daemon::{HandshakeLoggedCapnpServer, LoggedCapnpStore};
use nixrs_capnp::{DEFAULT_BUF_SIZE, from_error};
use tokio::io::{AsyncRead, AsyncWrite, duplex};
use tokio::task::LocalSet;

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
    let store = LoggedCapnpStore::new(client);
    let ret = async move {
        let b = server::Builder::new();
        let server = b.local_serve_connection(tokio::io::stdin(), tokio::io::stdout(), store);
        server.await
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

#[tokio::main(flavor = "current_thread")]
pub async fn main() {
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
