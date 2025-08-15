use std::fs::Permissions;
use std::future::ready;
use std::io;
use std::num::ParseIntError;
use std::os::fd::AsRawFd;
use std::os::fd::RawFd;
use std::os::unix::fs::PermissionsExt as _;
use std::path::PathBuf;
use std::sync::Arc;
use std::task::{ready, Poll};
use std::time::Duration;

use capnp_rpc::new_client;
use capnp_rpc_tokio::builder::RpcSystemBuilder;
use capnp_rpc_tokio::client::ClientBuilder;
use clap::Parser;
use nixrs::daemon::DaemonStore;
use nixrs::daemon::HandshakeDaemonStore;
use nixrs::daemon::{wire::types2::BuildMode, FutureResultExt, LocalDaemonStore};
use nixrs_capnp::capnp::nix_daemon_capnp;
use nixrs_capnp::nix_daemon::HandshakeLoggedCapnpServer;
use nixrs_capnp::nix_daemon::LoggedCapnpStore;
use pin_project_lite::pin_project;
use tokio::io::join;
use tokio::io::{Interest, Ready};
use tokio::sync::{mpsc, watch};
use tokio::{
    io::AsyncRead,
    net::{unix::OwnedReadHalf, UnixListener},
    task::LocalSet,
    time::sleep,
};
use tracing::{error, info, level_filters::LevelFilter};
use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt as _, EnvFilter};

#[derive(Debug)]
struct SleepStore(Duration);
impl HandshakeDaemonStore for SleepStore {
    type Store = Self;

    fn handshake(
        self,
    ) -> impl nixrs::daemon::ResultLog<Output = nixrs::daemon::DaemonResult<Self::Store>> {
        ready(Ok(self)).empty_logs()
    }
}
impl DaemonStore for SleepStore {
    fn trust_level(&self) -> nixrs::daemon::TrustLevel {
        nixrs::daemon::TrustLevel::Trusted
    }

    async fn shutdown(&mut self) -> nixrs::daemon::DaemonResult<()> {
        info!("shutting down sleep store");
        Ok(())
    }

    fn build_paths<'a>(
        &'a mut self,
        _drvs: &'a [nixrs::derived_path::DerivedPath],
        _mode: nixrs::daemon::wire::types2::BuildMode,
    ) -> impl nixrs::daemon::ResultLog<Output = nixrs::daemon::DaemonResult<()>> + 'a {
        let duration = self.0;
        async move {
            info!(?duration, "Sleeping build");
            sleep(duration).await;
            info!("Completed build");
            Ok(())
        }
        .empty_logs()
    }
}

impl Clone for SleepStore {
    fn clone(&self) -> Self {
        eprintln!("Cloning SleepStore");
        Self(self.0)
    }
}

impl Drop for SleepStore {
    fn drop(&mut self) {
        eprintln!("SleepStore dropped");
    }
}

fn monitor_hup(fd: RawFd) -> io::Result<()> {
    let mut poll = mio::Poll::new()?;
    let mut events = mio::Events::with_capacity(2);
    const SERVER: mio::Token = mio::Token(0);
    poll.registry().register(
        &mut mio::unix::SourceFd(&fd),
        SERVER,
        mio::Interest::READABLE,
    )?;
    loop {
        // Poll the OS for events, waiting at most 100 milliseconds.
        poll.poll(&mut events, None)?;

        // Process each event.
        for event in events.iter() {
            info!(?event, "Got event");
        }
    }
}

async fn readines(
    reader: Arc<OwnedReadHalf>,
    sender: mpsc::Sender<Ready>,
    signal_tx: watch::Sender<()>,
) {
    loop {
        let p = match sender.reserve().await {
            Ok(p) => p,
            Err(err) => {
                error!(?err, "Reserve Error in readines");
                return;
            }
        };
        info!("Waiting for readiness");
        match reader.ready(Interest::READABLE).await {
            Ok(ready) => {
                if !ready.is_empty() {
                    p.send(ready);
                }
                if ready.is_read_closed() {
                    info!("Reader is closed");
                    let _ = signal_tx.send(());
                    return;
                }
            }
            Err(err) => {
                error!(?err, "IO Error in readines");
                return;
            }
        }
    }
}

pin_project! {
    #[derive(Debug)]
    pub struct InterruptedReader {
        reader: Arc<OwnedReadHalf>,
        receiver: mpsc::Receiver<Ready>,
    }
}

impl InterruptedReader {
    pub fn new(reader: OwnedReadHalf, signal_tx: watch::Sender<()>) -> Self {
        let (sender, receiver) = mpsc::channel(1);
        let reader = Arc::new(reader);
        tokio::spawn(readines(reader.clone(), sender, signal_tx));
        Self { reader, receiver }
    }
}

impl AsyncRead for InterruptedReader {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.project();
        info!("Polling readiness");
        while let Some(ready) = ready!(this.receiver.poll_recv(cx)) {
            info!(?ready, "Reader is ready");
            match this.reader.try_read_buf(buf) {
                Ok(read) => {
                    info!(read, "Read data");
                    return Poll::Ready(Ok(()));
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    info!(?ready, "Reader would block");
                }
                Err(err) => return Poll::Ready(Err(err)),
            }
            info!("Looping");
        }
        Poll::Ready(Ok(()))
    }
}

async fn run_server(listener: UnixListener, sleep: Duration) {
    let (io, _addr) = match listener.accept().await {
        Ok(conn) => conn,
        Err(err) => {
            error!("Failed to accept connection: {err:#}");
            return;
        }
    };
    if let Ok(cred) = io.peer_cred() {
        info!(
            pid = cred.pid(),
            uid = cred.uid(),
            gid = cred.gid(),
            "Got unix connection"
        );
    } else {
        info!("Got anonymous unix connection");
    };

    let fd = io.as_raw_fd();
    let (reader, writer) = io.into_split();
    let (signal_tx, mut signal_rx) = watch::channel(());
    std::thread::spawn(move || {
        monitor_hup(fd).unwrap();
    });
    let reader = InterruptedReader::new(reader, signal_tx);
    let client: nix_daemon_capnp::logged_nix_daemon::Client =
        new_client(HandshakeLoggedCapnpServer::new(SleepStore(sleep)));
    let mut conn = RpcSystemBuilder::new()
        .bootstrap(client)
        .serve_connection(join(reader, writer));
    //let b = server::Builder::new();
    tokio::select! {
        res = &mut conn => {
            if let Err(err) = res {
                error!("Error while running connection: {err:#}");
            }
        }
        /*
        res = b.serve_connection(reader, writer, SleepStore(sleep)) => {
            if let Err(err) = res {
                error!("Error while running connection: {err:#}");
            }
        }
        */
        res = signal_rx.changed() => {
            if let Err(err) = res {
                error!("Error while listening for connection signal: {err:#}");
            }
        }
    }
    info!("Done with select");
    if let Err(err) = conn.await {
        error!("Error while running connection: {err:#}");
    }
    info!("Completed connection");
}

/*
async fn shutdown_signal() {
    // Wait for the CTRL+C signal
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install CTRL+C signal handler");
}
*/

pub fn parse_duration(s: &str) -> Result<Duration, ParseIntError> {
    if let Some(vs) = s.strip_suffix("ms") {
        Ok(Duration::from_millis(vs.parse()?))
    } else if let Some(vs) = s.strip_suffix('s') {
        Ok(Duration::from_secs(vs.parse()?))
    } else if let Some(vs) = s.strip_suffix('m') {
        Ok(Duration::from_secs(vs.parse::<u64>()? * 60))
    } else if let Some(vs) = s.strip_suffix('h') {
        Ok(Duration::from_secs(vs.parse::<u64>()? * 3600))
    } else {
        Ok(Duration::from_secs(s.parse()?))
    }
}

#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(
        short,
        long,
        default_value = "/nix/var/nixrs-capnp/daemon-socket/socket"
    )]
    bind: PathBuf,

    /// Mode to set on socket
    #[arg(short = 'm', long, default_value_t = 0o666)]
    bind_mode: u32,

    /// How long for server to sleep before returning reply
    #[arg(long, default_value = "1m", value_parser = parse_duration)]
    server_sleep: Duration,

    /// How client waits for reply before closing connection
    #[arg(long, default_value = "10s", value_parser = parse_duration)]
    client_timeout: Duration,
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
    let set = LocalSet::new();
    set.spawn_local(run_main(args));
    set.await
}

async fn run_main(args: Args) {
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
    tokio::task::spawn_local(async move {
        run_server(listener, args.server_sleep).await;
    });

    let client: nixrs_capnp::capnp::nix_daemon_capnp::logged_nix_daemon::Client =
        ClientBuilder::default()
            .connect_unix(args.bind)
            .await
            .unwrap();
    let mut store = LoggedCapnpStore::new(client);

    /*
    let mut store = DaemonClient::builder()
        .connect_unix(args.bind)
        .await
        .unwrap();
     */
    tokio::select! {
        result = store.build_paths(&[], BuildMode::Normal) => {
            info!(?result, "Completed build");
        }
        _ = sleep(args.client_timeout) => {
            info!("Timeout build");
        }
    }
    info!("Shutting down");
    store.shutdown().await.unwrap();
    info!("Shutdown");
}
