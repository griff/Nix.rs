use std::{fs::Permissions, os::unix::fs::PermissionsExt as _, path::PathBuf};

use capnp_rpc::{new_client, new_future_client};
use capnp_rpc_tokio::builder::RpcSystemBuilder;
use capnp_rpc_tokio::serve::{make_client, serve};
use clap::Parser;
use nixrs::daemon::{MutexHandshakeStore, client::DaemonClient};
use nixrs_capnp::{from_error, nix_daemon::HandshakeLoggedCapnpServer};
use tokio::io::join;
use tokio::net::UnixListener;
use tokio::task::LocalSet;
use tracing::{error, level_filters::LevelFilter};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt as _, util::SubscriberInitExt as _};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Nix Daemon Store to connect to
    #[arg(short, long, default_value = "daemon")]
    store: String,

    /// Socket to bind to
    #[arg(
        short,
        long,
        default_value = "/nix/var/nixrs-capnp/capnp-socket/socket"
    )]
    bind: PathBuf,

    /// Mode to set on socket
    #[arg(short = 'm', long, default_value_t = 0o666)]
    bind_mode: u32,

    #[arg(long, default_value_t = false)]
    stdio: bool,
}

#[tokio::main(flavor = "current_thread")]
pub async fn main() {
    // Start logging to console
    let env_filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();
    tracing_subscriber::registry()
        .with(env_filter)
        .with(tracing_subscriber::fmt::Layer::default().compact())
        .init();

    let args = Args::parse();
    LocalSet::new().run_until(run_main(args)).await
}

async fn shutdown_signal() {
    // Wait for the CTRL+C signal
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install CTRL+C signal handler");
}

async fn run_main(args: Args) {
    let socket = if args.store == "daemon" {
        "/nix/var/nix/daemon-socket/socket".to_string()
    } else if let Some(socket) = args.store.strip_prefix("unix://") {
        socket.into()
    } else {
        panic!("Unknown store '{}'", args.store);
    };

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

    if args.stdio {
        let io = join(tokio::io::stdin(), tokio::io::stdout());
        RpcSystemBuilder::new()
            .bootstrap(client)
            .serve_connection(io)
            .with_graceful_shutdown(shutdown_signal())
            .await;
    } else {
        if let Some(path) = args.bind.parent() {
            let _ = tokio::fs::create_dir_all(path).await;
        }
        let _ = tokio::fs::remove_file(&args.bind).await;
        let listener = UnixListener::bind(&args.bind)
            .map_err(|err| {
                capnp::Error::failed(format!("Could not bind unix socket {:?}: {err}", args.bind))
            })
            .unwrap();
        let perm = Permissions::from_mode(args.bind_mode);
        tokio::fs::set_permissions(&args.bind, perm)
            .await
            .map_err(|err| {
                capnp::Error::failed(format!(
                    "Could not set permissions on unix socket {:?}: {err}",
                    args.bind
                ))
            })
            .unwrap();

        let res = serve(listener, make_client(client))
            .with_graceful_shutdown(shutdown_signal())
            .await;
        if let Err(err) = res {
            error!("Failed to run RPC system: {err:#}");
        }
    }
}
