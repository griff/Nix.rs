use std::fmt;
use std::future::{Future, Ready, ready};
use std::process::Stdio;
use std::sync::{Arc, RwLock};

use futures::TryFutureExt as _;
use nixrs_ssh_store::StoreProvider;
use nixrs_ssh_store::server::ServerConfig;
use rstest::rstest;
use serial_test::serial;
use tokio::process::{ChildStdin, ChildStdout, Command};
use tokio::try_join;

use nixrs::daemon::client::DaemonClient;
use nixrs::daemon::{DaemonError, DaemonResult, DaemonStore as _};
use nixrs::store_path::StorePath;
use nixrs::test::daemon::{MockReporter, MockStore};

struct Provider<R: MockReporter>(Arc<RwLock<Option<MockStore<R>>>>);
impl<R: MockReporter> Clone for Provider<R> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
impl<R> StoreProvider for Provider<R>
where
    R: MockReporter + fmt::Debug + Send + 'static,
{
    type Error = DaemonError;

    #[cfg(feature = "legacy")]
    type LegacyStore = nixrs_legacy::store::FailStore;

    #[cfg(feature = "legacy")]
    type LegacyFuture = Ready<Result<Option<Self::LegacyStore>, Self::Error>>;

    type DaemonStore = MockStore<R>;

    type DaemonFuture = Ready<Result<Option<Self::DaemonStore>, Self::Error>>;

    #[cfg(feature = "legacy")]
    fn get_legacy_store(
        &self,
        _stderr: nixrs_ssh_store::io::ExtendedDataWrite,
    ) -> Self::LegacyFuture {
        ready(Ok(Some(nixrs_legacy::store::FailStore)))
    }

    fn get_daemon_store(&self) -> Self::DaemonFuture {
        let ret = self.0.write().unwrap().take();
        ready(Ok(ret))
    }
}

async fn run_store_test<R, T, F, E>(mock: MockStore<R>, test: T) -> Result<(), E>
where
    R: MockReporter + fmt::Debug + Send + Sync + 'static,
    T: FnOnce(DaemonClient<ChildStdout, ChildStdin>) -> F,
    F: Future<Output = Result<DaemonClient<ChildStdout, ChildStdin>, E>>,
    E: From<DaemonError>,
{
    let mut config = ServerConfig::with_store(Provider(Arc::new(RwLock::new(Some(mock)))));
    config.load_default_keys("./tests").await;
    let server = nixrs_ssh_store::server::Server::with_config(config).map_err(DaemonError::from)?;
    let state = server.state();
    let server = server
        .run("localhost:8222")
        .map_err(DaemonError::from)
        .map_err(From::from);

    let mut child = Command::new("ssh")
        .args([
            "-p",
            "8222",
            "-i",
            "./tests/id_ed25519",
            "-oUserKnownHostsFile=./tests/ssh_known_hosts",
        ])
        .arg("localhost")
        .arg("nix-daemon")
        .arg("--stdio")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    /*
    let uri = "ssh-ng://localhost?ssh-key=./tests/id_ed25519".to_string();
    let mut cmd = Command::new("../../nix/result/bin/nix-daemon");
    cmd.arg("--stdio");
    cmd.arg("--store");
    cmd.arg(&uri);
    cmd.env("NIX_SSHOPTS", "-p 8222 -oUserKnownHostsFile=./tests/ssh_known_hosts");
    cmd.stdin(Stdio::piped());
    cmd.stdout(Stdio::piped());
    let mut child = cmd.spawn().unwrap();
     */
    let stdin = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();

    let client = async move {
        let logs = DaemonClient::builder().connect(stdout, stdin);
        let client = logs.await?;
        let mut client = (test)(client).await?;
        eprintln!("Closing client");
        client.shutdown().await?;
        eprintln!("Shutting down server");
        state.shutdown();
        eprintln!("Killing child");
        child.kill().await.map_err(DaemonError::from)?;
        eprintln!("Waiting for child");
        child.wait().await.map_err(DaemonError::from)?;
        eprintln!("Client done");
        Ok(())
    };
    try_join!(client, server,).map(|_| ())
}

#[tokio::test]
#[serial]
async fn handshake() {
    let mock = MockStore::builder().build();
    run_store_test(mock, |client| ready(Ok(client) as DaemonResult<_>))
        .await
        .unwrap();
}

#[rstest]
#[case("00000000000000000000000000000000-_", Ok(true), Ok(true))]
#[case("00000000000000000000000000000000-_", Ok(false), Ok(false))]
#[case("00000000000000000000000000000000-_", Err(DaemonError::custom("bad input path")), Err("IsValidPath: remote error: IsValidPath: bad input path".into()))]
#[serial]
#[tokio::test]
async fn is_valid_path(
    #[case] store_path: StorePath,
    #[case] response: DaemonResult<bool>,
    #[case] expected: Result<bool, String>,
) {
    let mock = MockStore::builder()
        .is_valid_path(&store_path, response)
        .build()
        .build();
    run_store_test(mock, |mut client| async move {
        assert_eq!(
            expected,
            client
                .is_valid_path(&store_path)
                .await
                .map_err(|err| err.to_string())
        );
        Ok(client) as DaemonResult<_>
    })
    .await
    .unwrap();
}
