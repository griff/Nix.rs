use std::fs::Permissions;
use std::os::unix::fs::PermissionsExt as _;
use std::path::PathBuf;

use capnp_rpc_tokio::client::ClientBuilder;
use clap::Parser;
use nixrs::daemon::server;
use nixrs_capnp::nix_daemon::LoggedCapnpStore;
use tokio::net::UnixListener;
use tokio::task::LocalSet;
use tracing::level_filters::LevelFilter;
use tracing::{error, info};
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Path to Cap'n'proto unix socket
    #[arg(
        short,
        long,
        default_value = "/nix/var/nixrs-capnp/capnp-socket/socket"
    )]
    socket: PathBuf,

    /// Socket to bind to
    #[arg(
        short,
        long,
        default_value = "/nix/var/nixrs-capnp/daemon-socket/socket"
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
    let local_set = LocalSet::new();
    local_set.run_until(run_main(args)).await;
    tokio::select! {
        _ = local_set => {
            info!("all connections gracefully closed");
        },
        _ = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
            error!("timed out wait for all connections to close");
        }
    }
}

async fn shutdown_signal() {
    // Wait for the CTRL+C signal
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install CTRL+C signal handler");
}

async fn run_main(args: Args) {
    let client: nixrs_capnp::capnp::nix_daemon_capnp::logged_nix_daemon::Client =
        ClientBuilder::default()
            .connect_unix(args.socket)
            .await
            .unwrap();
    let mut signal = std::pin::pin!(shutdown_signal());

    if args.stdio {
        let store = LoggedCapnpStore::new(client);
        let b = server::Builder::new();
        tokio::select! {
            res = b.local_serve_connection(tokio::io::stdin(), tokio::io::stdout(), store) => {
                if let Err(err) = res {
                    error!("Error while running connection: {err:#}");
                }
            },
            _ = &mut signal => {
                info!("signal received, shutting down");
            }
        }
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

        loop {
            let (mut io, _addr) = tokio::select! {
                result = listener.accept() => match result {
                    Ok(conn) => conn,
                    Err(err) => {
                        error!("Failed to accept connection: {err:#}");
                        break;
                    }
                },
                _ = &mut signal => {
                    drop(listener);
                    info!("signal received, not accepting new connections");
                    break;
                }
            };
            /*
            let server = if let Ok(cred) = io.peer_cred() {
                trace!(pid=cred.pid(), uid=cred.uid(), gid=cred.gid(), "Got unix connection");
                server.authorize(Principal::Uid(cred.uid())).client
            } else {
                trace!("Got anonymous unix connection");
                server.authorize(Principal::Unauthenticated).client
            };
            */
            let client = client.clone();
            tokio::task::spawn_local(async move {
                let store = LoggedCapnpStore::new(client);
                let b = server::Builder::new();
                let (reader, writer) = io.split();
                if let Err(err) = b.local_serve_connection(reader, writer, store).await {
                    error!("Error while running connection: {err:#}");
                }
            });
        }
    }
}
