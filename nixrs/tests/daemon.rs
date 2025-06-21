#![cfg(all(feature = "test", feature = "daemon"))]

use std::collections::BTreeMap;
use std::future::Future;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::process::Stdio;
use std::time::Instant;

use bstr::ByteSlice;
use bytes::{Bytes, BytesMut};
use futures::stream::iter;
use futures::{FutureExt as _, StreamExt as _, TryFutureExt as _};
use proptest::sample::size_range;
use tempfile::Builder;
use tokio::io::{copy_buf, split, AsyncBufReadExt, BufReader};
use tokio::process::{ChildStdin, ChildStdout, Command};
use tokio::try_join;

use nixrs::archive::test_data;
use nixrs::daemon::client::DaemonClient;
use nixrs::daemon::mock::{self, MockReporter, MockStore, ReporterError};
use nixrs::daemon::wire::types::Operation;
use nixrs::daemon::wire::types2::{
    BuildMode, BuildResult, BuildStatus, QueryMissingResult, ValidPathInfo,
};
use nixrs::daemon::{server, LogMessage, ProtocolRange, ProtocolVersion, ResultLog};
use nixrs::daemon::{
    AddToStoreItem, DaemonError, DaemonResult, DaemonStore as _, DaemonString, UnkeyedValidPathInfo,
};
use nixrs::derivation::{BasicDerivation, DerivationOutput};
use nixrs::derived_path::{DerivedPath, OutputName};
use nixrs::hash::NarHash;
use nixrs::store_path::{StoreDir, StorePath, StorePathSet};
use nixrs::ByteString;

trait NixImpl: std::fmt::Debug {
    fn name(&self) -> &str;
    fn program_path(&self) -> PathBuf {
        Path::new(env!("ALL_NIX"))
            .join(self.name())
            .join("bin/nix-daemon")
    }
    fn conf_path(&self) -> PathBuf {
        Path::new(env!("ALL_NIX"))
            .join(self.name())
            .join("conf/nix_2_3.conf")
    }
    fn prepare_mock(&self, mock: &mut mock::Builder<()>);
    fn prepare_program<'c>(&self, cmd: &'c mut Command) -> &'c mut Command;
    fn prepare_op_logs(&self, op: Operation, logs: &mut Vec<LogMessage>);
    //fn prepare_op_logs2(&self, op: Operation, logs: &mut VecDeque<LogMessage>);
    fn protocol_range(&self) -> ProtocolRange;
    //fn handshake_logs_range(&self) -> SizeRange;
}

#[derive(Debug, Clone, Copy)]
struct StdNixImpl {
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
                LogMessage::Next(format!("performing daemon worker op: {}\n", id).into()),
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

const NIX_2_3: StdNixImpl = StdNixImpl {
    name: "nix_2_3",
    //verbosity: Verbosity::Error,
    cmd_args: &[],
    range: ProtocolRange::from_minor(10, 21),
    op_log_prefix: false,
    //handshake_logs: false,
};

const NIX_2_24: StdNixImpl = StdNixImpl {
    name: "nix_2_24",
    //verbosity: Verbosity::Error,
    cmd_args: &["--extra-experimental-features", "mounted-ssh-store"],
    range: ProtocolRange::from_minor(10, 37),
    op_log_prefix: true,
    //handshake_logs: true,
};

const LIX_2_91: StdNixImpl = StdNixImpl {
    name: "lix_2_91",
    //verbosity: Verbosity::Vomit,
    cmd_args: &[],
    range: ProtocolRange::from_minor(10, 35),
    op_log_prefix: true,
    //handshake_logs: true,
};

async fn process_logs<R, L>(logs: L) -> DaemonResult<R>
where
    L: ResultLog<Output = DaemonResult<R>>,
{
    let mut logs = pin!(logs);
    while let Some(log) = logs.next().await {
        eprintln!("Msg: {:?}", log);
    }
    logs.await
}

async fn run_store_test<R, T, F, E>(
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
    let unix_proxy = env!("UNIX_PROXY");
    let remote_program = dir.path().join("local");
    tokio::fs::symlink(unix_proxy, &remote_program)
        .await
        .unwrap();
    let socket = dir.path().join("local.socket");
    /*
    let socket = Path::new("./daemon.socket");
    if socket.exists() {
        remove_file(socket).unwrap();
    }
     */
    let uri = format!(
        "ssh-ng://localhost?remote-program={}&path-info-cache-size=0",
        remote_program.to_str().unwrap()
    );

    let listener = UnixListener::bind(socket).unwrap();
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
        .arg("--process-ops")
        .arg("--debug")
        .arg("-vvvvvv")
        .arg("--stdio")
        .arg("--store")
        .arg(&uri)
        .env("NIX_REMOTE", uri)
        .env("NIX_CONF", conf)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = cmd.spawn().unwrap();
    let stdin = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();
    let stderr = child.stderr.take().unwrap();
    let stderr_copy = async move {
        let mut lines = BufReader::new(stderr).lines();
        while let Some(line) = lines.next_line().await? {
            println!("{}", line);
        }
        Ok(()) as Result<(), E>
    };
    /*
    let s = UnixStream::connect("../../lix/daemon.socket").await.unwrap();
    let (stdout, stdin) = s.into_split();
     */

    let client = async move {
        let mut result = DaemonClient::builder().connect(stdout, stdin);
        let mut r = pin!(result);
        let logs: Vec<_> = r.by_ref().collect().await;
        let client = r.await?;
        let mut client = (test)(client, logs).await?;
        println!("Closing");
        client.shutdown().await?;
        /*

        */
        println!("Killing");
        child.kill().await?;
        println!("Waiting");
        child.wait().await?;
        println!("Done");
        Ok(())
    };
    let reports = (try_join!(stderr_copy, client, server, reports,)
        .map(|(_, _, _, reports)| reports) as Result<Vec<ReporterError>, E>)?;
    if let Some(report) = reports.first() {
        Err(DaemonError::custom(report))?;
    }
    Ok(())
}

fn prepare_mock(nix: &dyn NixImpl) -> mock::Builder<()> {
    let mut mock = MockStore::builder();
    nix.prepare_mock(&mut mock);
    mock
}

fn chomp_log(log: LogMessage) -> LogMessage {
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

mod unittests {
    use std::collections::BTreeSet;

    use super::*;
    use nixrs::archive::read_nar;
    use nixrs::archive::write_nar;
    use nixrs::btree_set;
    use pretty_assertions::assert_eq;
    use rstest::rstest;

    #[tokio::test]
    #[rstest]
    #[should_panic(
        expected = "store dropped with LogOperation { operation: IsValidPath(StorePath(00000000000000000000000000000000-_), Ok(true)), logs: [] } operation still unread"
    )]
    async fn check_unread_fails(#[values(&NIX_2_3, &NIX_2_24, &LIX_2_91)] nix: &dyn NixImpl) {
        let mut mock = prepare_mock(nix);
        let store_path = "00230000000000000000000000000000-_".parse().unwrap();
        mock.is_valid_path(&store_path, Ok(true)).build();
        mock.is_valid_path(
            &"00000000000000000000000000000000-_".parse().unwrap(),
            Ok(true),
        )
        .build();
        let version = nix.protocol_range().max();
        run_store_test(nix, version, mock, |mut client, _| async move {
            process_logs(client.is_valid_path(&store_path)).await?;
            Ok(client) as DaemonResult<_>
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    #[rstest]
    #[case(vec![LogMessage::Next("Hello".into())])]
    #[case(vec![LogMessage::Next("Hello\r".into())])]
    #[case(vec![LogMessage::Next("Hello\n\r".into())])]
    #[case(vec![LogMessage::Next("Hello\r\n".into())])]
    #[case(vec![LogMessage::Next("Lines\n  More\n   ".into())])]
    #[case(vec![LogMessage::Next("Hello".into()), LogMessage::Next("World".into())])]
    async fn op_logs(
        #[values(&NIX_2_3, &NIX_2_24, &LIX_2_91)] nix: &dyn NixImpl,
        #[case] mut logs: Vec<LogMessage>,
    ) {
        let mut mock = prepare_mock(nix);
        let store_path = "00230000000000000000000000000000-_".parse().unwrap();
        let mut op = mock.is_valid_path(&store_path, Ok(true));
        for log in logs.iter() {
            op = op.add_log(log.clone());
        }
        op.build();
        nix.prepare_op_logs(Operation::IsValidPath, &mut logs);
        let version = nix.protocol_range().max();
        run_store_test(nix, version, mock, |mut client, _| async move {
            {
                let res = client.is_valid_path(&store_path);
                let mut r = pin!(res);
                let actual_logs: Vec<_> = r.by_ref().collect().await;
                assert_eq!(
                    actual_logs,
                    logs.into_iter().map(chomp_log).collect::<Vec<_>>()
                );
                r.await?;
            }
            Ok(client) as DaemonResult<_>
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    #[rstest]
    #[case::single(vec![LogMessage::Next("Hello".into())])]
    #[case::empty(vec![LogMessage::Next("".into())])]
    #[case::whitespace(vec![LogMessage::Next("Lines\n  More\n   ".into())])]
    #[case::multiple(vec![LogMessage::Next("Hello".into()), LogMessage::Next("World".into())])]
    async fn handshake_logs(
        #[values(&NIX_2_24, &LIX_2_91)] nix: &dyn NixImpl,
        #[case] logs: Vec<LogMessage>,
    ) {
        let mut mock = prepare_mock(nix);
        for log in logs.iter() {
            mock.add_handshake_log(log.clone());
        }
        let version = nix.protocol_range().max();
        run_store_test(nix, version, mock, |client, actual_logs| async move {
            assert_eq!(
                actual_logs,
                logs.into_iter().map(chomp_log).collect::<Vec<_>>()
            );
            Ok(client) as DaemonResult<_>
        })
        .await
        .unwrap();
    }

    /*
    #[tokio::test]
    #[rstest]
    #[case(ClientOptions::default(), Ok(()), Ok(()))]
    #[case(ClientOptions::default(), Err(DaemonError::custom("bad input path")), Err("remote error: bad input path".into()))]
    async fn set_options(
        #[values("nix_2_3", "nix_2_24", "lix_2_91")] nix: &str,
        #[case] options: ClientOptions,
        #[case] response: DaemonResult<()>,
        #[case] expected: Result<(), String>,
    ) {
        let mut mock = MockStore::builder();
        mock.set_options(&options, response).build();
        run_store_test(nix, mock, |mut client| async move {
            assert_eq!(
                expected,
                client
                    .set_options(&options)
                    .result()
                    .await
                    .map_err(|err| err.to_string())
            );
            Ok(client) as DaemonResult<_>
        })
        .await
        .unwrap();
    }
    */

    #[tokio::test]
    #[rstest]
    #[case::valid("00000000000000000000000000000000-_", Ok(true), Ok(true))]
    #[case::invalid("00000000000000000000000000000000-_", Ok(false), Ok(false))]
    #[case::error("00000000000000000000000000000000-_", Err(DaemonError::custom("bad input path")), Err("IsValidPath: remote error: IsValidPath: bad input path".into()))]
    async fn is_valid_path(
        #[values(&NIX_2_3, &NIX_2_24, &LIX_2_91)] nix: &dyn NixImpl,
        #[case] store_path: StorePath,
        #[case] response: DaemonResult<bool>,
        #[case] expected: Result<bool, String>,
    ) {
        let mut mock = prepare_mock(nix);
        mock.is_valid_path(&store_path, response).build();
        let version = nix.protocol_range().max();
        run_store_test(nix, version, mock, |mut client, _| async move {
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

    #[tokio::test]
    #[rstest]
    //#[case::substitute_all_valid(&["00000000000000000000000000000000-_"][..], true, Ok(&["10000000000000000000000000000000-_"][..]), Ok(&["10000000000000000000000000000000-_"][..]))]
    //#[case::substilute_empty_return(&["00000000000000000000000000000000-_"][..], true, Ok(&[][..]), Ok(&[][..]))]
    #[case::all_valid(&["00000000000000000000000000000000-_"][..], false, Ok(&["10000000000000000000000000000000-_"][..]), Ok(&["10000000000000000000000000000000-_"][..]))]
    #[case::empty_return(&["00000000000000000000000000000000-_"][..], false, Ok(&[][..]), Ok(&[][..]))]
    //#[case::substitute_error(&["00000000000000000000000000000000-_"][..], true, Err(DaemonError::custom("bad input path")), Err("QueryValidPaths: remote error: QueryValidPaths: bad input path".into()))]
    #[case::error(&["00000000000000000000000000000000-_"][..], false, Err(DaemonError::custom("bad input path")), Err("QueryValidPaths: remote error: QueryValidPaths: bad input path".into()))]
    async fn query_valid_paths(
        #[values(&NIX_2_3, &NIX_2_24, &LIX_2_91)] nix: &dyn NixImpl,
        #[case] store_paths: &[&str],
        #[case] substitute: bool,
        #[case] response: DaemonResult<&[&str]>,
        #[case] expected: Result<&[&str], String>,
    ) {
        let store_paths = store_paths.iter().map(|p| p.parse().unwrap()).collect();
        let response = response.map(|r| r.iter().map(|p| p.parse().unwrap()).collect());
        let expected: Result<StorePathSet, String> =
            expected.map(|r| r.iter().map(|p| p.parse().unwrap()).collect());
        let mut mock = prepare_mock(nix);
        mock.query_valid_paths(&store_paths, substitute, response)
            .build();
        let version = nix.protocol_range().max();
        run_store_test(nix, version, mock, |mut client, _| async move {
            assert_eq!(
                expected,
                client
                    .query_valid_paths(&store_paths, substitute)
                    .await
                    .map_err(|err| err.to_string())
            );
            Ok(client) as DaemonResult<_>
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    #[rstest]
    #[case::found_info("00000000000000000000000000000000-_", Ok(Some(UnkeyedValidPathInfo {
        deriver: Some("00000000000000000000000000000000-_.drv".parse().unwrap()),
        nar_hash: NarHash::new(&[0u8; 32]),
        references: btree_set!["00000000000000000000000000000000-_"],
        registration_time: 0,
        nar_size: 0,
        ultimate: true,
        signatures: BTreeSet::new(),
        ca: None,
    })), Ok(Some(UnkeyedValidPathInfo {
        deriver: Some("00000000000000000000000000000000-_.drv".parse().unwrap()),
        nar_hash: NarHash::new(&[0u8; 32]),
        references: btree_set!["00000000000000000000000000000000-_"],
        registration_time: 0,
        nar_size: 0,
        ultimate: true,
        signatures: BTreeSet::new(),
        ca: None,
    })))]
    #[case::no_info("00000000000000000000000000000000-_", Ok(None), Ok(None))]
    #[case::error("00000000000000000000000000000000-_", Err(DaemonError::custom("bad input path")), Err("QueryPathInfo: remote error: QueryPathInfo: bad input path".into()))]
    async fn query_path_info(
        #[values(&NIX_2_3, &NIX_2_24, &LIX_2_91)] nix: &dyn NixImpl,
        #[case] store_path: StorePath,
        #[case] response: DaemonResult<Option<UnkeyedValidPathInfo>>,
        #[case] expected: Result<Option<UnkeyedValidPathInfo>, String>,
    ) {
        let mut mock = prepare_mock(nix);
        mock.query_path_info(&store_path, response).build();
        let version = nix.protocol_range().max();
        run_store_test(nix, version, mock, |mut client, _| async move {
            assert_eq!(
                expected,
                client
                    .query_path_info(&store_path)
                    .await
                    .map_err(|err| err.to_string())
            );
            Ok(client) as DaemonResult<_>
        })
        .await
        .unwrap();
    }

    #[test_log::test(tokio::test)]
    #[rstest]
    #[case::text_file("00000000000000000000000000000000-_", test_data::text_file())]
    #[case::exec_file("00000000000000000000000000000000-_", test_data::exec_file())]
    #[case::empty_file("00000000000000000000000000000000-_", test_data::empty_file())]
    #[case::empty_file_in_dir("00000000000000000000000000000000-_", test_data::empty_file_in_dir())]
    #[case::empty_dir("00000000000000000000000000000000-_", test_data::empty_dir())]
    #[case::empty_dir_in_dir("00000000000000000000000000000000-_", test_data::empty_dir_in_dir())]
    #[case::symlink("00000000000000000000000000000000-_", test_data::symlink())]
    #[case::dir_example("00000000000000000000000000000000-_", test_data::dir_example())]
    async fn nar_from_path(
        #[values(&NIX_2_3, &NIX_2_24, &LIX_2_91)] nix: &dyn NixImpl,
        #[case] store_path: StorePath,
        #[case] events: test_data::TestNarEvents,
    ) {
        use bytes::Bytes;

        let content = write_nar(events.iter());
        let size = content.len();
        let hash = NarHash::digest(&content);
        let mut mock = prepare_mock(nix);
        mock.nar_from_path(&store_path, Ok(content)).build();
        let version = nix.protocol_range().max();
        run_store_test(nix, version, mock, |mut client, _| async move {
            {
                let logs = client.nar_from_path(&store_path);
                let mut reader = process_logs(logs).await.unwrap();
                let mut out = Vec::new();
                copy_buf(&mut reader, &mut out).await?;
                let nar: test_data::TestNarEvents =
                    read_nar(Cursor::new(Bytes::copy_from_slice(&out))).await?;
                assert_eq!(events, nar);
                assert_eq!(size, out.len());
                assert_eq!(NarHash::digest(&out), hash);
            }
            Ok(client) as DaemonResult<_>
        })
        .await
        .unwrap();
    }

    /*
    #[tokio::test]
    #[rstest]
    #[case("00000000000000000000000000000000-_", DaemonError::custom("bad input path"), "remote error: bad input path".into())]
    async fn nar_from_path_err(#[case] store_path: StorePath, #[case] response: DaemonError, #[case] expected: String) {

        let mock = MockStore::builder()
            .nar_from_path(&store_path, Err(response)).build()
            .build();
        run_store_test(mock, |mut client, _| async move {
            let mut out = Vec::new();
            assert_eq!(expected, client.nar_from_path(&store_path, Cursor::new(&mut out)).result().await.unwrap_err().to_string());
            assert_eq!(out.len(), 0);
            Ok(client)
        }).await;
    }
    */

    // BuildPaths
    #[test_log::test(tokio::test)]
    #[rstest]
    #[case::normal(&["/nix/store/00000000000000000000000000000000-_"][..], BuildMode::Normal, Ok(()), Ok(()))]
    #[case::repair(&["/nix/store/00000000000000000000000000000000-_"][..], BuildMode::Repair, Ok(()), Ok(()))]
    #[case::empty(&[][..], BuildMode::Check, Ok(()), Ok(()))]
    #[case::error(&["/nix/store/00000000000000000000000000000000-_"][..], BuildMode::Normal, Err(DaemonError::custom("bad input path")), Err("BuildPaths: remote error: BuildPaths: bad input path".into()))]
    async fn build_paths(
        #[values(&NIX_2_3, &NIX_2_24, &LIX_2_91)] nix: &dyn NixImpl,
        #[case] paths: &[&str],
        #[case] mode: BuildMode,
        #[case] response: DaemonResult<()>,
        #[case] expected: Result<(), String>,
    ) {
        let store_dir = StoreDir::default();
        let paths: Vec<DerivedPath> = paths.iter().map(|p| store_dir.parse(p).unwrap()).collect();
        let mut mock = MockStore::builder();
        mock.build_paths(&paths, mode, response).build();
        let version = nix.protocol_range().max();
        run_store_test(nix, version, mock, |mut client, _| async move {
            assert_eq!(
                expected,
                client
                    .build_paths(&paths, mode)
                    .await
                    .map_err(|err| err.to_string())
            );
            Ok(client) as DaemonResult<_>
        })
        .await
        .unwrap();
    }

    macro_rules! store_path_set {
        () => { StorePathSet::new() };
        ($p:expr $(, $pr:expr)*$(,)?) => {{
            let mut ret = StorePathSet::new();
            let p = $p.parse::<StorePath>().unwrap();
            ret.insert(p);
            $(
                ret.insert($pr.parse::<StorePath>().unwrap());
            )*
            ret
        }}
    }
    macro_rules! btree_map {
        () => {std::collections::BTreeMap::new()};
        ($k:expr => $v:expr
         $(, $kr:expr => $vr:expr )*$(,)?) => {{
            let mut ret = std::collections::BTreeMap::new();
            ret.insert($k, $v);
            $(
                ret.insert($kr, $vr);
            )*
            ret
         }}
    }

    // BuildDerivation
    #[test_log::test(tokio::test)]
    #[rstest]
    #[case::normal(BasicDerivation {
        drv_path: "00000000000000000000000000000000-_.drv".parse().unwrap(),
        outputs: btree_map!(
            OutputName::default() => DerivationOutput::InputAddressed("00000000000000000000000000000000-_".parse().unwrap()),
        ),
        input_srcs: store_path_set!(),
        platform: ByteString::from_static(b"x86_64-linux"),
        builder: ByteString::from_static(b"/bin/sh"),
        args: vec![ByteString::from_static(b"-c"), ByteString::from_static(b"echo Hello")],
        env: btree_map!(),
    }, BuildMode::Normal, Ok(BuildResult {
        status: BuildStatus::Built,
        error_msg: DaemonString::from_static(b""),
        times_built: 1,
        is_non_deterministic: false,
        start_time: 0,
        stop_time: 0,
        cpu_user: None,
        cpu_system: None,
        built_outputs: btree_map!(),
    }), Ok(BuildResult {
        status: BuildStatus::Built,
        error_msg: DaemonString::from_static(b""),
        times_built: 1,
        is_non_deterministic: false,
        start_time: 0,
        stop_time: 0,
        cpu_user: None,
        cpu_system: None,
        built_outputs: btree_map!(),
    }))]
    #[case::error(BasicDerivation {
        drv_path: "00000000000000000000000000000000-_.drv".parse().unwrap(),
        outputs: btree_map!(
            OutputName::default() => DerivationOutput::InputAddressed("00000000000000000000000000000000-_".parse().unwrap()),
        ),
        input_srcs: store_path_set!(),
        platform: ByteString::from_static(b"x86_64-linux"),
        builder: ByteString::from_static(b"/bin/sh"),
        args: vec![ByteString::from_static(b"-c"), ByteString::from_static(b"echo Hello")],
        env: btree_map!(),
    }, BuildMode::Normal, Err(DaemonError::custom("bad input path")), Err("BuildDerivation: remote error: BuildDerivation: bad input path".into()))]
    async fn build_derivation(
        #[values(&NIX_2_3, &NIX_2_24, &LIX_2_91)] nix: &dyn NixImpl,
        #[case] drv: BasicDerivation,
        #[case] mode: BuildMode,
        #[case] response: DaemonResult<BuildResult>,
        #[case] mut expected: Result<BuildResult, String>,
    ) {
        let mut mock = MockStore::builder();
        mock.build_derivation(&drv, mode, response).build();
        let version = nix.protocol_range().max();
        run_store_test(nix, version, mock, |mut client, _| async move {
            let version = client.version();
            if let Ok(expected) = expected.as_mut() {
                if version.minor() < 28 {
                    expected.built_outputs = BTreeMap::new();
                }
                if version.minor() < 29 {
                    expected.times_built = 0;
                    expected.is_non_deterministic = false;
                    expected.start_time = 0;
                    expected.stop_time = 0;
                }
                if version.minor() < 37 {
                    expected.cpu_user = None;
                    expected.cpu_system = None;
                }
            }
            let actual = client
                .build_derivation(&drv, mode)
                .await
                .map_err(|err| err.to_string());
            assert_eq!(expected, actual,);
            Ok(client) as DaemonResult<_>
        })
        .await
        .unwrap();
    }

    // QueryMissing
    #[test_log::test(tokio::test)]
    #[rstest]
    #[case::substitute(&["/nix/store/00000000000000000000000000000000-_"][..],
        Ok(QueryMissingResult {
            will_build: store_path_set!(),
            will_substitute: store_path_set!("00000000000000000000000000000000-_"),
            unknown: store_path_set!(),
            download_size: 200,
            nar_size: 500,
        }), Ok(QueryMissingResult {
            will_build: store_path_set!(),
            will_substitute: store_path_set!("00000000000000000000000000000000-_"),
            unknown: store_path_set!(),
            download_size: 200,
            nar_size: 500,
        }))]
    #[case::empty(&[][..], Ok(QueryMissingResult {
        will_build: store_path_set!(),
        will_substitute: store_path_set!(),
        unknown: store_path_set!(),
        download_size: 0,
        nar_size: 0,
    }), Ok(QueryMissingResult {
        will_build: store_path_set!(),
        will_substitute: store_path_set!(),
        unknown: store_path_set!(),
        download_size: 0,
        nar_size: 0,
    }))]
    #[case::error(&["/nix/store/00000000000000000000000000000000-_"][..], Err(DaemonError::custom("bad input path")), Err("QueryMissing: remote error: QueryMissing: bad input path".into()))]
    async fn query_missing(
        #[values(&NIX_2_3, &NIX_2_24, &LIX_2_91)] nix: &dyn NixImpl,
        #[case] paths: &[&str],
        #[case] response: DaemonResult<QueryMissingResult>,
        #[case] expected: Result<QueryMissingResult, String>,
    ) {
        let store_dir = StoreDir::default();
        let paths: Vec<DerivedPath> = paths.iter().map(|p| store_dir.parse(p).unwrap()).collect();
        let mut mock = MockStore::builder();
        mock.query_missing(&paths, response).build();
        let version = nix.protocol_range().max();
        run_store_test(nix, version, mock, |mut client, _| async move {
            assert_eq!(
                expected,
                client
                    .query_missing(&paths)
                    .await
                    .map_err(|err| err.to_string())
            );
            Ok(client) as DaemonResult<_>
        })
        .await
        .unwrap();
    }

    #[test_log::test(tokio::test)]
    #[rstest]
    #[case::ok(
        ValidPathInfo {
            path: "00000000000000000000000000000000-_".parse().unwrap(),
            info: UnkeyedValidPathInfo {
                deriver: Some("00000000000000000000000000000000-_.drv".parse().unwrap()),
                nar_hash: NarHash::new(&[0u8; 32]),
                references: btree_set!["00000000000000000000000000000000-_"],
                registration_time: 0,
                nar_size: 0,
                ultimate: true,
                signatures: BTreeSet::new(),
                ca: None,
            }
        },
        true,
        true,
        test_data::text_file(),
        Ok(()),
        Ok(())
    )]
    #[case::error(ValidPathInfo {
            path: "00000000000000000000000000000000-_".parse().unwrap(),
            info: UnkeyedValidPathInfo {
                deriver: Some("00000000000000000000000000000000-_.drv".parse().unwrap()),
                nar_hash: NarHash::new(&[0u8; 32]),
                references: btree_set!["00000000000000000000000000000000-_"],
                registration_time: 0,
                nar_size: 0,
                ultimate: true,
                signatures: BTreeSet::new(),
                ca: None,
            }
        }, true, true, test_data::text_file(),
        Err(DaemonError::custom("bad input path")), Err("AddToStoreNar: remote error: AddToStoreNar: bad input path".into())
    )]
    async fn add_to_store_nar(
        #[values(&NIX_2_3, &NIX_2_24, &LIX_2_91)] nix: &dyn NixImpl,
        #[case] info: ValidPathInfo,
        #[case] repair: bool,
        #[case] dont_check_sigs: bool,
        #[case] events: test_data::TestNarEvents,
        #[case] response: DaemonResult<()>,
        #[case] expected: Result<(), String>,
    ) {
        let content = write_nar(events.iter());
        let mut mock = MockStore::builder();
        mock.add_to_store_nar(&info, repair, dont_check_sigs, content.clone(), response)
            .build();
        let version = nix.protocol_range().max();
        run_store_test(nix, version, mock, |mut client, _| async move {
            assert_eq!(
                expected,
                client
                    .add_to_store_nar(&info, Cursor::new(content), repair, dont_check_sigs)
                    .await
                    .map_err(|err| err.to_string())
            );
            Ok(client) as DaemonResult<_>
        })
        .await
        .unwrap();
    }

    // AddMultipleToStore
    #[test_log::test(tokio::test)]
    #[rstest]
    #[case(
        true,
        true,
        vec![
            (
                ValidPathInfo {
                    path: "00000000000000000000000000000000-_".parse().unwrap(),
                    info: UnkeyedValidPathInfo {
                        deriver: Some("00000000000000000000000000000000-_.drv".parse().unwrap()),
                        nar_hash: NarHash::new(&[0u8; 32]),
                        references: btree_set!["00000000000000000000000000000000-_"],
                        registration_time: 0,
                        nar_size: 0,
                        ultimate: true,
                        signatures: BTreeSet::new(),
                        ca: None,
                    }
                },
                test_data::text_file(),
            ),
            (
                ValidPathInfo {
                    path: "00000000000000000000000000000011-_".parse().unwrap(),
                    info: UnkeyedValidPathInfo {
                        deriver: Some("00000000000000000000000000000022-_.drv".parse().unwrap()),
                        nar_hash: NarHash::new(&[1u8; 32]),
                        references: btree_set!["00000000000000000000000000000000-_"],
                        registration_time: 0,
                        nar_size: 200,
                        ultimate: true,
                        signatures: BTreeSet::new(),
                        ca: None,
                    }
                },
                test_data::dir_example()
            ),
        ],
        Ok(()),
        Ok(())
    )]
    #[case(
        true,
        true,
        vec![
            (
                ValidPathInfo {
                    path: "00000000000000000000000000000000-_".parse().unwrap(),
                    info: UnkeyedValidPathInfo {
                        deriver: Some("00000000000000000000000000000000-_.drv".parse().unwrap()),
                        nar_hash: NarHash::new(&[0u8; 32]),
                        references: btree_set!["00000000000000000000000000000000-_"],
                        registration_time: 0,
                        nar_size: 0,
                        ultimate: true,
                        signatures: BTreeSet::new(),
                        ca: None,
                    }
                },
                test_data::text_file(),
            ),
            (
                ValidPathInfo {
                    path: "00000000000000000000000000000011-_".parse().unwrap(),
                    info: UnkeyedValidPathInfo {
                        deriver: Some("00000000000000000000000000000022-_.drv".parse().unwrap()),
                        nar_hash: NarHash::new(&[1u8; 32]),
                        references: btree_set!["00000000000000000000000000000000-_"],
                        registration_time: 0,
                        nar_size: 200,
                        ultimate: true,
                        signatures: BTreeSet::new(),
                        ca: None,
                    }
                },
                test_data::dir_example()
            ),
        ],
        Err(DaemonError::custom("bad input path")), Err("AddMultipleToStore: remote error: AddMultipleToStore: bad input path".into())
    )]
    async fn add_multiple_to_store(
        #[values(&NIX_2_24, &LIX_2_91)] nix: &dyn NixImpl,
        #[case] repair: bool,
        #[case] dont_check_sigs: bool,
        #[case] infos: Vec<(ValidPathInfo, test_data::TestNarEvents)>,
        #[case] response: DaemonResult<()>,
        #[case] expected: Result<(), String>,
    ) {
        let infos_content: Vec<_> = infos
            .iter()
            .map(|(info, events)| {
                let content = write_nar(events.iter());
                (info.clone(), content)
            })
            .collect();
        let infos_stream = iter(infos_content.clone().into_iter().map(|(info, content)| {
            Ok(AddToStoreItem {
                info: info.clone(),
                reader: Cursor::new(content.clone()),
            })
        }));

        let mut mock = MockStore::builder();
        mock.add_multiple_to_store(repair, dont_check_sigs, infos_content, response)
            .build();
        let version = nix.protocol_range().max();
        run_store_test(nix, version, mock, |mut client, _| async move {
            assert_eq!(
                expected,
                client
                    .add_multiple_to_store(repair, dont_check_sigs, infos_stream)
                    .await
                    .map_err(|err| err.to_string())
            );
            Ok(client) as DaemonResult<_>
        })
        .await
        .unwrap();
    }

    #[test_log::test(tokio::test)]
    async fn sesennst() {
        let nix = &NIX_2_24;
        let handshake_logs = vec![LogMessage::Next(Bytes::new())];
        let path_info = ValidPathInfo {
            path: "00000000000000000000000000000000--".parse().unwrap(),
            info: UnkeyedValidPathInfo {
                deriver: None,
                nar_hash: NarHash::new(&[0u8; 32]),
                references: BTreeSet::new(),
                registration_time: 0,
                nar_size: 0,
                ultimate: false,
                signatures: BTreeSet::new(),
                ca: None,
            },
        };
        let nar = b"\r\0\0\0\0\0\0\0nix-archive-1\0\0\0\x01\0\0\0\0\0\0\0(\0\0\0\0\0\0\0\x04\0\0\0\0\0\0\0type\0\0\0\0\x07\0\0\0\0\0\0\0symlink\0\x06\0\0\0\0\0\0\0target\0\0a\0\0\0\0\0\0\0a6 ++Et?+C+= = ABYL+D7C=qEIc?nk/967?//747/0H?by=C+= 3=+?=3+4+e= B+j=i+5v+pW=e+?ht e79?U1/f =P+.KX\0\0\0\0\0\0\0\x01\0\0\0\0\0\0\0)\0\0\0\0\0\0\0";
        let handshake_logs = handshake_logs
            .into_iter()
            .map(chomp_log)
            .collect::<Vec<_>>();
        let mut mock = MockStore::builder();
        for log in handshake_logs.iter() {
            mock.add_handshake_log(log.clone());
        }
        mock.add_to_store_nar(&path_info, false, false, Bytes::from_static(nar), Ok(()))
            .build();
        let version = nix.protocol_range().max();
        run_store_test(nix, version, mock, |mut client, actual_logs| async move {
            assert_eq!(actual_logs, handshake_logs);
            assert_eq!(
                Ok(()),
                client
                    .add_to_store_nar(&path_info, Cursor::new(nar), false, false)
                    .await
                    .map_err(|err| err.to_string())
            );
            Ok(client) as DaemonResult<_>
        })
        .await
        .unwrap();
    }
}

mod proptests {
    use std::time::Duration;

    use super::*;
    use nixrs::pretty_prop_assert_eq;
    use proptest::prelude::*;
    use proptest::test_runner::TestCaseResult;
    use tokio::time::timeout;
    use tracing::error;

    // TODO: proptest handshake
    const ALL_NIX: &[&dyn NixImpl] = &[&NIX_2_3, &NIX_2_24, &LIX_2_91];
    fn arb_nix() -> impl Strategy<Value = &'static dyn NixImpl> {
        any::<proptest::sample::Index>()
            .prop_map(|idx| *idx.get(ALL_NIX))
            .no_shrink()
    }

    /*
    proptest! {
        #[test]
        fn proptest_set_options(
            nix in arb_nix(),
            options in any::<ClientOptions>(),
        )
        {
            let now = Instant::now();
            let r = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            r.block_on(async {
                let mut mock = MockStore::builder();
                mock.set_options(&options, Ok(())).build();
                run_store_test(nix, mock, |mut client| async move {
                    let res = client.set_options(&options).result().await;
                    prop_assert!(res.is_ok(), "invalid result {:?}", res);
                    Ok(client)
                }).await?;
                Ok(()) as Result<_, TestCaseError>
            })?;
            eprintln!("Completed test {}", now.elapsed().as_secs_f64());
        }
    }
    */
    proptest! {
        #[test]
        fn proptest_is_valid_path(
            nix in arb_nix(),
            path in any::<StorePath>(),
            result in any::<bool>(),
        )
        {
            let now = Instant::now();
            let r = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            r.block_on(async {
                let mut mock = prepare_mock(nix);
                mock.is_valid_path(&path, Ok(result)).build();
                let version = nix.protocol_range().max();
                run_store_test(nix, version, mock, |mut client, _| async move {
                    let res = client.is_valid_path(&path).await;
                    pretty_prop_assert_eq!(res.unwrap(), result);
                    Ok(client)
                }).await?;
                Ok(()) as Result<_, TestCaseError>
            })?;
            eprintln!("Completed test {}", now.elapsed().as_secs_f64());
        }
    }

    proptest! {
        #[test]
        fn proptest_query_valid_paths(
            nix in arb_nix(),
            paths in any::<StorePathSet>(),
            result in any::<StorePathSet>(),
        )
        {
            let now = Instant::now();
            let r = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            r.block_on(async {
                let mut mock = prepare_mock(nix);
                mock.query_valid_paths(&paths, false, Ok(result.clone())).build();
                let version = nix.protocol_range().max();
                run_store_test(nix, version, mock, |mut client, _| async move {
                    let res = client.query_valid_paths(&paths, false).await;
                    pretty_prop_assert_eq!(res.unwrap(), result);
                    Ok(client)
                }).await?;
                Ok(()) as Result<_, TestCaseError>
            })?;
            eprintln!("Completed test {}", now.elapsed().as_secs_f64());
        }
    }

    // TODO: proptest query_valid_paths
    // TODO: proptest query_path_info
    // TODO: proptest nar_from_path

    // TODO: proptest all messages
    /*
    #[test_log::test(test_strategy::proptest(
        async = "tokio",
        ProptestConfig::default(),
        max_shrink_iters = 30_000,
    ))]
    async fn proptest_operations(
        #[strategy(arb_nix())]
        nix: &'static dyn NixImpl,
        #[any(#nix.protocol_range())]
        version: ProtocolVersion,
        #[any((#nix.handshake_logs_range(), #version))]
        handshake_logs: Vec<LogMessage>,
        #[any((size_range(0..10), MockOperationParams { version: #version, allow_options: false }))]
        ops: Vec<LogOperation>,
    ) -> TestCaseResult {
        let mut mock = MockStore::builder();
        for op in ops.iter() {
            mock.add_operation(op.clone());
        }
        let handshake_logs = handshake_logs.into_iter().map(chomp_log).collect::<Vec<_>>();
        let op_types : Vec<_> = ops.iter().map(|o| o.operation.operation()).collect();
        info!(?op_types, "Running {} operations", ops.len());
        let res = timeout(Duration::from_secs(60),
            run_store_test(nix, version, mock, |mut client, actual_logs| async move {
                pretty_prop_assert_eq!(actual_logs, handshake_logs);
                for mut op in ops.into_iter() {
                    op.logs = op.logs.into_iter().map(chomp_log).collect();
                    nix.prepare_op_logs2(op.operation.operation(), &mut op.logs);
                    op.check_operation(&mut client).await?;
                }
                Ok(client) as Result<_, TestCaseError>
            })
        ).await;
        match res {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(err)) => {
                //error!("Test failed {}", err);
                Err(err)
            }
            Err(_) => {
                error!("Timeout waiting for test to complete");
                Err(TestCaseError::fail("Timeout waiting for test to complete"))
            }
        }
    }
    */

    #[test_log::test(test_strategy::proptest(
        async = "tokio",
        ProptestConfig::default(),
        max_shrink_iters = 30_000,
    ))]
    async fn proptest_op_logs(
        #[strategy(arb_nix())] nix: &'static dyn NixImpl,
        #[any(#nix.protocol_range())] version: ProtocolVersion,
        #[any((size_range(0..100), #version))] mut op_logs: Vec<LogMessage>,
    ) -> TestCaseResult {
        let mut mock = MockStore::builder();
        let store_path = "00000000000000000000000000000000-_".parse().unwrap();
        let mut op = mock.is_valid_path(&store_path, Ok(true));
        for log in op_logs.iter() {
            op = op.add_log(log.clone());
        }
        op.build();
        nix.prepare_op_logs(Operation::IsValidPath, &mut op_logs);
        let res = timeout(
            Duration::from_secs(60),
            run_store_test(nix, version, mock, |mut client, _| async move {
                {
                    let ret = client.is_valid_path(&store_path);
                    let mut r = pin!(ret);
                    let actual_logs = r.by_ref().collect::<Vec<_>>().await;
                    pretty_prop_assert_eq!(
                        actual_logs,
                        op_logs.into_iter().map(chomp_log).collect::<Vec<_>>()
                    );
                    r.await?;
                }
                Ok(client) as Result<_, TestCaseError>
            }),
        )
        .await;
        match res {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(err)) => {
                //error!("Test failed {}", err);
                Err(err)
            }
            Err(_) => {
                error!("Timeout waiting for test to complete");
                Err(TestCaseError::fail("Timeout waiting for test to complete"))
            }
        }
    }
}
