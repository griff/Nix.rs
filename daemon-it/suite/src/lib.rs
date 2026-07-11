use std::collections::{BTreeMap, BTreeSet};
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
use nixrs::daemon::server;
use nixrs::daemon::{
    DaemonError, DaemonResult, DaemonStore as _, Operation, ProtocolRange, ProtocolVersion,
    ResultLog,
};
use nixrs::log::{Message, ParsedLogMessage, Verbosity};
use nixrs::test::daemon::{Builder, MockReporter, MockStore, ReporterError};
use serde::de::Error;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, BufReader, split};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::try_join;
use tracing::warn;

#[cfg(test)]
mod proptests;
#[cfg(test)]
mod unittests;

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Deserialize,
    Serialize,
    enum_bitset::EnumBitset,
)]
#[bitset(name = Quirks)]
pub enum Quirk {
    ChompLog,
    LogPrefix,
    SkipAll,
}

pub trait NixImpl: std::fmt::Debug {
    fn name(&self) -> &str;
    fn program_path(&self) -> PathBuf;
    fn prepare_mock(&self, mock: &mut Builder<()>) {
        let _ = mock;
    }
    fn prepare_program<'c>(&self, cmd: &'c mut Command) -> &'c mut Command;
    fn prepare_op_logs(&self, op: Operation, logs: &mut Vec<ParsedLogMessage>);
    //fn prepare_op_logs2(&self, op: Operation, logs: &mut VecDeque<LogMessage>);
    fn protocol_range(&self) -> ProtocolRange;
    //fn handshake_logs_range(&self) -> SizeRange;
    fn collect_log(&self, log: ParsedLogMessage) -> ParsedLogMessage {
        log
    }
    fn is_test_skipped(&self, test: &str) -> bool {
        let _ = test;
        false
    }
    fn is_operation_skipped(&self, op: Operation) -> bool {
        self.protocol_range().intersect(&op.versions()).is_none()
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct JsonNixImpl {
    name: String,
    program: PathBuf,
    #[serde(rename = "args", default)]
    arguments: Vec<String>,
    #[serde(rename = "env", default)]
    environment: BTreeMap<String, String>,
    protocol_range: ProtocolRange,
    #[serde(default)]
    quirks: Quirks,
    #[serde(default)]
    skipped: BTreeSet<String>,
}

impl JsonNixImpl {
    pub fn load_file<P: AsRef<Path>>(path: P) -> serde_json::Result<Self> {
        let path = path.as_ref();
        let rdr = File::open(path).map_err(serde_json::Error::custom)?;
        let mut ret: JsonNixImpl = serde_json::from_reader(rdr)?;
        let base_dir = path.parent().unwrap_or(Path::new("."));
        if ret.program.is_relative() {
            ret.program = base_dir.join(ret.program); //.canonicalize().map_err(serde_json::Error::custom)?;
        }
        eprintln!("nix config: {ret:#?}");
        eprintln!("daemon protocol range: {}", ret.protocol_range());
        Ok(ret)
    }
    pub fn is_skipped(&self, test: &str) -> bool {
        self.quirks.contains(Quirk::SkipAll) || self.skipped.contains(test)
    }
}
pub static ENV_NIX_IMPL: LazyLock<JsonNixImpl> = LazyLock::new(|| {
    if let Some(nix_impl) = std::env::var_os("NIX_IMPL") {
        JsonNixImpl::load_file(nix_impl).expect("NIX_IMPL env var does not point to json document")
    } else {
        eprintln!("NIX_IMPL was not set. Ignoring ALL tests");
        JsonNixImpl {
            name: "ignored".into(),
            program: "bin/ignored".into(),
            arguments: vec![],
            environment: Default::default(),
            protocol_range: ProtocolRange::default(),
            quirks: Quirk::LogPrefix + Quirk::SkipAll,
            skipped: Default::default(),
        }
    }
});
pub fn nix_protocol_range() -> ProtocolRange {
    ENV_NIX_IMPL
        .protocol_range
        .intersect(&ProtocolRange::default())
        .unwrap()
}

impl NixImpl for JsonNixImpl {
    fn name(&self) -> &str {
        &self.name
    }
    fn program_path(&self) -> PathBuf {
        self.program.clone()
    }

    fn prepare_program<'c>(&self, cmd: &'c mut Command) -> &'c mut Command {
        cmd.args(self.arguments.iter())
            .envs(self.environment.iter())
    }

    fn prepare_op_logs(&self, op: Operation, logs: &mut Vec<ParsedLogMessage>) {
        if self.quirks.contains(Quirk::LogPrefix) {
            let id: u64 = op.into();
            logs.insert(
                0,
                ParsedLogMessage::Message(Message {
                    text: format!("performing daemon worker op: {id}\n").into(),
                    level: Verbosity::Error,
                }),
            )
        }
        for log in logs.iter_mut() {
            match log {
                ParsedLogMessage::Message(Message { level, text: _ })
                    if *level != Verbosity::Error =>
                {
                    *level = Verbosity::Error
                }
                _ => {}
            }
        }
    }

    fn collect_log(&self, log: ParsedLogMessage) -> ParsedLogMessage {
        if self.quirks.contains(Quirk::ChompLog) {
            chomp_log(log)
        } else {
            log
        }
    }
    fn protocol_range(&self) -> ProtocolRange {
        self.protocol_range
            .intersect(&ProtocolRange::default())
            .expect("No overlap between supported range and NIX_IMPL range")
    }
}

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
    mock: Builder<R>,
    test: T,
) -> Result<(), E>
where
    R: MockReporter,
    T: FnOnce(DaemonClient<ChildStdout, ChildStdin>, Vec<ParsedLogMessage>) -> F,
    F: Future<Output = Result<DaemonClient<ChildStdout, ChildStdin>, E>>,
    //    T: FnOnce(DaemonClient<OwnedReadHalf, OwnedWriteHalf>) -> F,
    //    F: Future<Output = Result<DaemonClient<OwnedReadHalf, OwnedWriteHalf>, E>>,
    E: From<DaemonError> + From<std::io::Error>,
{
    use tokio::net::UnixListener;
    let (mock, reporter) = mock.channel_reporter();
    let mock = mock.build();
    let reports = reporter.collect::<Vec<ReporterError>>().map(|r| Ok(r));

    let dir = tempfile::Builder::new()
        .prefix("test_restore_dir")
        .tempdir()
        .unwrap();
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

    let program = nix.program_path();
    //let program = "../../lix/outputs/out/bin/nix-daemon";
    let mut cmd = Command::new(program);
    nix.prepare_program(&mut cmd)
        .env("NIX_REMOTE", uri)
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
        let logs: Vec<_> = futures::StreamExt::map(r.by_ref(), ParsedLogMessage::from)
            .collect()
            .await;
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

pub fn prepare_mock(nix: &dyn NixImpl) -> Builder<()> {
    let mut mock = MockStore::builder();
    nix.prepare_mock(&mut mock);
    mock
}

pub fn chomp_log(log: ParsedLogMessage) -> ParsedLogMessage {
    match log {
        ParsedLogMessage::Message(mut msg) => {
            let chomped = msg
                .text
                .trim_end_with(|ch| matches!(ch, ' ' | '\n' | '\r' | '\t'));
            let mut new_msg = BytesMut::from(chomped);
            new_msg.extend_from_slice(b"\n");
            msg.text = new_msg.freeze();
            ParsedLogMessage::Message(msg)
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
