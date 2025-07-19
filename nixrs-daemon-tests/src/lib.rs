use std::collections::BTreeSet;
use std::fs::File;
use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::process::Stdio;
use std::sync::LazyLock;

use bstr::ByteSlice;
use bytes::BytesMut;
use futures::{FutureExt as _, StreamExt as _, TryFutureExt as _};
use nixrs::daemon::client::DaemonClient;
use nixrs::daemon::mock::{self, MockReporter, MockStore, ReporterError};
use nixrs::daemon::wire::types::Operation;
use nixrs::daemon::{server, LogMessage, ProtocolRange, ProtocolVersion, ResultLog};
use nixrs::daemon::{DaemonError, DaemonResult, DaemonStore as _};
use serde::de::Error;
use serde::Deserialize;
use tempfile::Builder;
use tokio::io::{split, AsyncBufReadExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::try_join;
use tracing::warn;

#[cfg(test)]
mod proptests;
#[cfg(test)]
mod unittests;

pub trait NixImpl: std::fmt::Debug {
    fn name(&self) -> &str;
    fn program_path(&self) -> PathBuf {
        if let Some(all_nix) = std::env::var_os("ALL_NIX") {
            Path::new(&all_nix).join(self.name()).join("bin/nix-daemon")
        } else {
            warn!("Missing location of ALL_NIX");
            Path::new(".").join(self.name()).join("bin/nix-daemon")
        }
    }
    fn conf_path(&self) -> PathBuf {
        if let Some(all_nix) = std::env::var_os("ALL_NIX") {
            Path::new(&all_nix)
                .join(self.name())
                .join("conf/nix_2_3.conf")
        } else {
            warn!("Missing location of ALL_NIX");
            Path::new("../nix/all-nix/").join("conf/nix_2_3.conf")
        }
    }
    fn prepare_mock(&self, _mock: &mut mock::Builder<()>) {}
    fn prepare_program<'c>(&self, cmd: &'c mut Command) -> &'c mut Command;
    fn prepare_op_logs(&self, op: Operation, logs: &mut Vec<LogMessage>);
    //fn prepare_op_logs2(&self, op: Operation, logs: &mut VecDeque<LogMessage>);
    fn protocol_range(&self) -> ProtocolRange;
    //fn handshake_logs_range(&self) -> SizeRange;
    fn collect_log(&self, log: LogMessage) -> LogMessage {
        log
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct JsonNixImpl {
    name: String,
    conf_path: PathBuf,
    program_path: PathBuf,
    cmd_args: Vec<String>,
    range: ProtocolRange,
    op_log_prefix: bool,
    chomp_log: bool,
    #[serde(default)]
    skip_all: bool,
    skipped: BTreeSet<String>,
}

impl JsonNixImpl {
    pub fn load_file<P: AsRef<Path>>(path: P) -> serde_json::Result<Self> {
        let path = path.as_ref();
        let rdr = File::open(path).map_err(serde_json::Error::custom)?;
        let mut ret: JsonNixImpl = serde_json::from_reader(rdr)?;
        let base_dir = path.parent().unwrap_or(Path::new("."));
        if ret.conf_path.is_relative() {
            ret.conf_path = base_dir.join(ret.conf_path); //.canonicalize().map_err(serde_json::Error::custom)?;
        }
        if ret.program_path.is_relative() {
            ret.program_path = base_dir.join(ret.program_path); //.canonicalize().map_err(serde_json::Error::custom)?;
        }
        eprintln!("nix config: {ret:#?}");
        eprintln!("daemon protocol range: {}", ret.protocol_range());
        Ok(ret)
    }
    pub fn is_skipped(&self, test: &str) -> bool {
        self.skip_all || self.skipped.contains(test)
    }
}
pub static ENV_NIX_IMPL: LazyLock<JsonNixImpl> = LazyLock::new(|| {
    let nix_impl = std::env::var("NIX_IMPL").expect("NIX_IMPL env var is not set");
    JsonNixImpl::load_file(nix_impl).expect("NIX_IMPL env var does not point to json document")
});
pub fn nix_protocol_range() -> ProtocolRange {
    ENV_NIX_IMPL
        .range
        .intersect(&ProtocolRange::default())
        .unwrap()
}

impl NixImpl for JsonNixImpl {
    fn name(&self) -> &str {
        &self.name
    }
    fn program_path(&self) -> PathBuf {
        self.program_path.clone()
    }
    fn conf_path(&self) -> PathBuf {
        self.conf_path.clone()
    }

    fn prepare_program<'c>(&self, cmd: &'c mut Command) -> &'c mut Command {
        cmd.args(self.cmd_args.iter())
    }

    fn prepare_op_logs(&self, op: Operation, logs: &mut Vec<LogMessage>) {
        if self.op_log_prefix {
            let id: u64 = op.into();
            logs.insert(
                0,
                LogMessage::Next(format!("performing daemon worker op: {id}\n").into()),
            )
        }
    }
    fn collect_log(&self, log: LogMessage) -> LogMessage {
        if self.chomp_log {
            chomp_log(log)
        } else {
            log
        }
    }
    fn protocol_range(&self) -> ProtocolRange {
        self.range
            .intersect(&ProtocolRange::default())
            .expect("No overlap between supported range and NIX_IMPL range")
    }
}

#[derive(Debug, Clone, Copy)]
pub struct StdNixImpl {
    name: &'static str,
    cmd_args: &'static [&'static str],
    range: ProtocolRange,
    op_log_prefix: bool,
    //handshake_logs: bool,
}

impl NixImpl for StdNixImpl {
    fn name(&self) -> &str {
        self.name
    }

    fn prepare_mock(&self, _mock: &mut mock::Builder<()>) {
        /*
        let mut options = ClientOptions::default();
        options.build_cores = 12;
        options.max_build_jobs = 12;
        options.verbosity = self.verbosity;
        mock.set_options(&options, Ok(())).build();
         */
    }

    fn prepare_program<'c>(&self, cmd: &'c mut Command) -> &'c mut Command {
        cmd.args(self.cmd_args.iter())
    }

    fn prepare_op_logs(&self, op: Operation, logs: &mut Vec<LogMessage>) {
        if self.op_log_prefix {
            let id: u64 = op.into();
            logs.insert(
                0,
                LogMessage::Next(format!("performing daemon worker op: {id}\n").into()),
            )
        }
    }
    /*
    fn prepare_op_logs2(&self, op: Operation, logs: &mut VecDeque<LogMessage>) {
        if self.op_log_prefix {
            let id: u64 = op.into();
            logs.push_front(LogMessage::Next(
                format!("performing daemon worker op: {}\n", id).into(),
            ))
        }
    }
    */
    fn collect_log(&self, log: LogMessage) -> LogMessage {
        chomp_log(log)
    }

    fn protocol_range(&self) -> ProtocolRange {
        self.range.intersect(&ProtocolRange::default()).unwrap()
    }

    /*
    fn handshake_logs_range(&self) -> SizeRange {
        if self.handshake_logs {
            size_range(0..10)
        } else {
            size_range(0..=0)
        }
    }
    */
}

pub const NIX_2_3: StdNixImpl = StdNixImpl {
    name: "nix_2_3",
    //verbosity: Verbosity::Error,
    cmd_args: &["--process-ops", "--debug", "-vvvvvv", "--stdio"],
    range: ProtocolRange::from_minor(10, 21),
    op_log_prefix: false,
    //handshake_logs: false,
};

pub const NIX_2_24: StdNixImpl = StdNixImpl {
    name: "nix_2_24",
    //verbosity: Verbosity::Error,
    cmd_args: &[
        "--extra-experimental-features",
        "mounted-ssh-store",
        "--process-ops",
        "--debug",
        "-vvvvvv",
        "--stdio",
    ],
    range: ProtocolRange::from_minor(10, 37),
    op_log_prefix: true,
    //handshake_logs: true,
};

pub const LIX_2_91: StdNixImpl = StdNixImpl {
    name: "lix_2_91",
    //verbosity: Verbosity::Vomit,
    cmd_args: &["--process-ops", "--debug", "-vvvvvv", "--stdio"],
    range: ProtocolRange::from_minor(10, 35),
    op_log_prefix: true,
    //handshake_logs: true,
};

pub async fn process_logs<R, L>(logs: L) -> DaemonResult<R>
where
    L: ResultLog<Output = DaemonResult<R>>,
{
    let mut logs = pin!(logs);
    while let Some(log) = logs.next().await {
        eprintln!("Msg: {log:?}");
    }
    logs.await
}

struct KillOnDrop(Child);
impl Drop for KillOnDrop {
    fn drop(&mut self) {
        if std::thread::panicking() {
            match self.0.start_kill() {
                Err(err) => eprintln!("Could not kill child process: {err}"),
                Ok(_) => eprintln!("Successfully killed child process"),
            }
        }
    }
}

impl Deref for KillOnDrop {
    type Target = Child;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for KillOnDrop {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

pub async fn run_store_test<R, T, F, E>(
    nix: &dyn NixImpl,
    version: ProtocolVersion,
    mock: mock::Builder<R>,
    test: T,
) -> Result<(), E>
where
    R: MockReporter,
    T: FnOnce(DaemonClient<ChildStdout, ChildStdin>, Vec<LogMessage>) -> F,
    F: Future<Output = Result<DaemonClient<ChildStdout, ChildStdin>, E>>,
    //    T: FnOnce(DaemonClient<OwnedReadHalf, OwnedWriteHalf>) -> F,
    //    F: Future<Output = Result<DaemonClient<OwnedReadHalf, OwnedWriteHalf>, E>>,
    E: From<DaemonError> + From<std::io::Error>,
{
    use tokio::net::UnixListener;
    let (mock, reporter) = mock.channel_reporter();
    let mock = mock.build();
    let reports = reporter.collect::<Vec<ReporterError>>().map(|r| Ok(r));

    let dir = Builder::new().prefix("test_restore_dir").tempdir().unwrap();
    let remote_program = dir.path().join("local");
    if let Some(unix_proxy) = std::env::var_os("UNIX_PROXY") {
        tokio::fs::symlink(unix_proxy, &remote_program)
            .await
            .unwrap();
    } else {
        warn!("Missing location of UNIX_PROXY");
    }
    let socket = dir.path().join("local.socket");
    let uri = format!(
        "ssh-ng://localhost?remote-program={}&path-info-cache-size=0",
        remote_program.to_str().unwrap()
    );

    let listener = UnixListener::bind(socket.clone()).unwrap();
    let server = async move {
        let (stream, _addr) = listener.accept().await?;
        let (reader, writer) = split(stream);
        let mut b = server::Builder::new();
        b.set_max_version(version);
        eprintln!("Running connnection");
        b.serve_connection(reader, writer, mock).await?;
        eprintln!("Closing connnection");
        Ok(()) as DaemonResult<()>
    }
    .map_err(From::from);

    let conf = nix.conf_path();
    let program = nix.program_path();
    //let program = "../../lix/outputs/out/bin/nix-daemon";
    let mut cmd = Command::new(program);
    nix.prepare_program(&mut cmd)
        .env("NIX_REMOTE", uri)
        .env("NIX_CONF", conf)
        .env("NIXRS_SOCKET", socket)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let child = cmd.spawn().unwrap();
    let mut child = KillOnDrop(child);
    let stdin = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();
    let stderr = child.stderr.take().unwrap();
    let stderr_copy = async move {
        let mut lines = BufReader::new(stderr).lines();
        while let Some(line) = lines.next_line().await? {
            eprintln!("{line}");
        }
        Ok(()) as Result<(), E>
    };

    let client = async move {
        let result = DaemonClient::builder().connect(stdout, stdin);
        let mut r = pin!(result);
        let logs: Vec<_> = r.by_ref().collect().await;
        let client = r.await?;
        let mut client = (test)(client, logs).await?;
        eprintln!("Closing");
        client.shutdown().await?;
        /*

        */
        eprintln!("Killing");
        child.kill().await?;
        eprintln!("Waiting");
        child.wait().await?;
        eprintln!("Done");
        Ok(())
    };
    let reports = (try_join!(stderr_copy, client, server, reports,)
        .map(|(_, _, _, reports)| reports) as Result<Vec<ReporterError>, E>)?;
    if let Some(report) = reports.first() {
        Err(DaemonError::custom(report))?;
    }
    Ok(())
}

pub fn prepare_mock(nix: &dyn NixImpl) -> mock::Builder<()> {
    let mut mock = MockStore::builder();
    nix.prepare_mock(&mut mock);
    mock
}

pub fn chomp_log(log: LogMessage) -> LogMessage {
    match log {
        LogMessage::Next(msg) => {
            let chomped = msg.trim_end_with(|ch| matches!(ch, ' ' | '\n' | '\r' | '\t'));
            let mut new_msg = BytesMut::from(chomped);
            new_msg.extend_from_slice(b"\n");
            LogMessage::Next(new_msg.freeze())
        }
        m => m,
    }
}

#[macro_export]
macro_rules! assert_result {
    ($expected:expr, $actual:expr) => {{
        match ($expected, $actual) {
            (Err(expected), Err(actual)) => {
                assert!(
                    actual.contains(&expected),
                    r#"error did not contain expected string
      error message: {actual:?}
 expected substring: {expected:?}"#
                )
            }
            (expected, actual) => assert_eq!(expected, actual),
        }
    }};
}
