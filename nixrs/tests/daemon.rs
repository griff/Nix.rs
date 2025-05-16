#![cfg(all(feature = "test", feature = "daemon"))]

use std::collections::BTreeMap;
use std::future::Future;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::process::Stdio;
use std::time::Instant;

use futures::stream::iter;
use futures::{FutureExt as _, StreamExt, TryFutureExt as _};
use tempfile::Builder;
use tokio::io::{copy_buf, split, AsyncBufReadExt, BufReader};
use tokio::process::{ChildStdin, ChildStdout, Command};
use tokio::try_join;
use tracing_test::traced_test;

use nixrs::archive::test_data;
use nixrs::daemon::client::DaemonClient;
use nixrs::daemon::mock::{self, MockReporter, MockStore, ReporterError};
use nixrs::daemon::wire::types2::{
    BuildMode, BuildResult, BuildStatus, QueryMissingResult, ValidPathInfo,
};
use nixrs::daemon::{server, ResultLog};
use nixrs::daemon::{
    AddToStoreItem, DaemonError, DaemonErrorKind, DaemonResult, DaemonStore as _, DaemonString,
    UnkeyedValidPathInfo,
};
use nixrs::derivation::{BasicDerivation, DerivationOutput};
use nixrs::derived_path::DerivedPath;
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
}

#[derive(Debug, Clone, Copy)]
struct StdNixImpl {
    name: &'static str,
    cmd_args: &'static [&'static str],
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
}

const NIX_2_3: StdNixImpl = StdNixImpl {
    name: "nix_2_3",
    //verbosity: Verbosity::Error,
    cmd_args: &[],
};

const NIX_2_24: StdNixImpl = StdNixImpl {
    name: "nix_2_24",
    //verbosity: Verbosity::Error,
    cmd_args: &["--extra-experimental-features", "mounted-ssh-store"],
};

const LIX_2_91: StdNixImpl = StdNixImpl {
    name: "lix_2_91",
    //verbosity: Verbosity::Vomit,
    cmd_args: &[],
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
    mock: mock::Builder<R>,
    test: T,
) -> Result<(), E>
where
    R: MockReporter,
    T: FnOnce(DaemonClient<ChildStdout, ChildStdin>) -> F,
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
        let b = server::Builder::new();
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
        let logs = DaemonClient::builder().connect(stdout, stdin);
        let client = process_logs(logs).await?;
        let mut client = (test)(client).await?;
        println!("Closing");
        client.close().await?;
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
        panic!("{}", report);
    }
    Ok(())
}

fn prepare_mock(nix: &dyn NixImpl) -> mock::Builder<()> {
    let mut mock = MockStore::builder();
    nix.prepare_mock(&mut mock);
    mock
}

mod unittests {
    use super::*;
    use nixrs::archive::read_nar;
    use nixrs::archive::write_nar;
    use rstest::rstest;

    /*
    #[tokio::test]
    #[rstest]
    async fn handshake(#[values("nix_2_24", "lix_2_91")] nix: &str) {
        let mock = MockStore::builder();
        run_store_test(nix, mock, |client| ready(Ok(client) as DaemonResult<_>))
            .await
            .unwrap();
    }
    */
    /*
    #[tokio::test]
    #[rstest]
    #[case(ClientOptions::default(), Ok(()), Ok(()))]
    #[case(ClientOptions::default(), Err(DaemonErrorKind::Custom("bad input path".into()).into()), Err("remote error: bad input path".into()))]
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
    #[case::error("00000000000000000000000000000000-_", Err(DaemonErrorKind::Custom("bad input path".into()).into()), Err("IsValidPath: remote error: IsValidPath: bad input path".into()))]
    async fn is_valid_path(
        #[values(&NIX_2_3, &NIX_2_24, &LIX_2_91)] nix: &dyn NixImpl,
        #[case] store_path: StorePath,
        #[case] response: DaemonResult<bool>,
        #[case] expected: Result<bool, String>,
    ) {
        let mut mock = prepare_mock(nix);
        mock.is_valid_path(&store_path, response).build();
        run_store_test(nix, mock, |mut client| async move {
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
    //#[case::substitute_error(&["00000000000000000000000000000000-_"][..], true, Err(DaemonErrorKind::Custom("bad input path".into()).into()), Err("QueryValidPaths: remote error: QueryValidPaths: bad input path".into()))]
    #[case::error(&["00000000000000000000000000000000-_"][..], false, Err(DaemonErrorKind::Custom("bad input path".into()).into()), Err("QueryValidPaths: remote error: QueryValidPaths: bad input path".into()))]
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
        run_store_test(nix, mock, |mut client| async move {
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
        references: vec!["00000000000000000000000000000000-_".parse().unwrap()],
        registration_time: 0,
        nar_size: 0,
        ultimate: true,
        signatures: vec![],
        ca: None,
    })), Ok(Some(UnkeyedValidPathInfo {
        deriver: Some("00000000000000000000000000000000-_.drv".parse().unwrap()),
        nar_hash: NarHash::new(&[0u8; 32]),
        references: vec!["00000000000000000000000000000000-_".parse().unwrap()],
        registration_time: 0,
        nar_size: 0,
        ultimate: true,
        signatures: vec![],
        ca: None,
    })))]
    #[case::no_info("00000000000000000000000000000000-_", Ok(None), Ok(None))]
    #[case::error("00000000000000000000000000000000-_", Err(DaemonErrorKind::Custom("bad input path".into()).into()), Err("QueryPathInfo: remote error: QueryPathInfo: bad input path".into()))]
    async fn query_path_info(
        #[values(&NIX_2_3, &NIX_2_24, &LIX_2_91)] nix: &dyn NixImpl,
        #[case] store_path: StorePath,
        #[case] response: DaemonResult<Option<UnkeyedValidPathInfo>>,
        #[case] expected: Result<Option<UnkeyedValidPathInfo>, String>,
    ) {
        let mut mock = prepare_mock(nix);
        mock.query_path_info(&store_path, response).build();
        run_store_test(nix, mock, |mut client| async move {
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

    #[traced_test]
    #[tokio::test]
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
        run_store_test(nix, mock, |mut client| async move {
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
    #[case("00000000000000000000000000000000-_", DaemonError::Custom("bad input path".into()), "remote error: bad input path".into())]
    async fn nar_from_path_err(#[case] store_path: StorePath, #[case] response: DaemonError, #[case] expected: String) {

        let mock = MockStore::builder()
            .nar_from_path(&store_path, Err(response)).build()
            .build();
        run_store_test(mock, |mut client| async move {
            let mut out = Vec::new();
            assert_eq!(expected, client.nar_from_path(&store_path, Cursor::new(&mut out)).result().await.unwrap_err().to_string());
            assert_eq!(out.len(), 0);
            Ok(client)
        }).await;
    }
    */

    // BuildPaths
    #[traced_test]
    #[tokio::test]
    #[rstest]
    #[case::normal(&["/nix/store/00000000000000000000000000000000-_"][..], BuildMode::Normal, Ok(()), Ok(()))]
    #[case::repair(&["/nix/store/00000000000000000000000000000000-_"][..], BuildMode::Repair, Ok(()), Ok(()))]
    #[case::empty(&[][..], BuildMode::Check, Ok(()), Ok(()))]
    #[case::error(&["/nix/store/00000000000000000000000000000000-_"][..], BuildMode::Normal, Err(DaemonErrorKind::Custom("bad input path".into()).into()), Err("BuildPaths: remote error: BuildPaths: bad input path".into()))]
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
        run_store_test(nix, mock, |mut client| async move {
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
    #[traced_test]
    #[tokio::test]
    #[rstest]
    #[case::normal(BasicDerivation {
        drv_path: "00000000000000000000000000000000-_.drv".parse().unwrap(),
        outputs: btree_map!(
            "out".into() => DerivationOutput::InputAddressed("00000000000000000000000000000000-_".parse().unwrap()),
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
            "out".into() => DerivationOutput::InputAddressed("00000000000000000000000000000000-_".parse().unwrap()),
        ),
        input_srcs: store_path_set!(),
        platform: ByteString::from_static(b"x86_64-linux"),
        builder: ByteString::from_static(b"/bin/sh"),
        args: vec![ByteString::from_static(b"-c"), ByteString::from_static(b"echo Hello")],
        env: btree_map!(),
    }, BuildMode::Normal, Err(DaemonErrorKind::Custom("bad input path".into()).into()), Err("BuildDerivation: remote error: BuildDerivation: bad input path".into()))]
    async fn build_derivation(
        #[values(&NIX_2_3, &NIX_2_24, &LIX_2_91)] nix: &dyn NixImpl,
        #[case] drv: BasicDerivation,
        #[case] build_mode: BuildMode,
        #[case] response: DaemonResult<BuildResult>,
        #[case] mut expected: Result<BuildResult, String>,
    ) {
        let mut mock = MockStore::builder();
        mock.build_derivation(&drv, build_mode, response).build();
        run_store_test(nix, mock, |mut client| async move {
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
                .build_derivation(&drv, build_mode)
                .await
                .map_err(|err| err.to_string());
            assert_eq!(expected, actual,);
            Ok(client) as DaemonResult<_>
        })
        .await
        .unwrap();
    }

    // QueryMissing
    #[traced_test]
    #[tokio::test]
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
    #[case::error(&["/nix/store/00000000000000000000000000000000-_"][..], Err(DaemonErrorKind::Custom("bad input path".into()).into()), Err("QueryMissing: remote error: QueryMissing: bad input path".into()))]
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
        run_store_test(nix, mock, |mut client| async move {
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

    #[traced_test]
    #[tokio::test]
    #[rstest]
    #[case::ok(
        ValidPathInfo {
            path: "00000000000000000000000000000000-_".parse().unwrap(),
            info: UnkeyedValidPathInfo {
                deriver: Some("00000000000000000000000000000000-_.drv".parse().unwrap()),
                nar_hash: NarHash::new(&[0u8; 32]),
                references: vec!["00000000000000000000000000000000-_".parse().unwrap()],
                registration_time: 0,
                nar_size: 0,
                ultimate: true,
                signatures: vec![],
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
                references: vec!["00000000000000000000000000000000-_".parse().unwrap()],
                registration_time: 0,
                nar_size: 0,
                ultimate: true,
                signatures: vec![],
                ca: None,
            }
        }, true, true, test_data::text_file(),
        Err(DaemonErrorKind::Custom("bad input path".into()).into()), Err("AddToStoreNar: remote error: AddToStoreNar: bad input path".into())
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
        run_store_test(nix, mock, |mut client| async move {
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
    #[traced_test]
    #[tokio::test]
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
                        references: vec!["00000000000000000000000000000000-_".parse().unwrap()],
                        registration_time: 0,
                        nar_size: 0,
                        ultimate: true,
                        signatures: vec![],
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
                        references: vec!["00000000000000000000000000000000-_".parse().unwrap()],
                        registration_time: 0,
                        nar_size: 200,
                        ultimate: true,
                        signatures: vec![],
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
                        references: vec!["00000000000000000000000000000000-_".parse().unwrap()],
                        registration_time: 0,
                        nar_size: 0,
                        ultimate: true,
                        signatures: vec![],
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
                        references: vec!["00000000000000000000000000000000-_".parse().unwrap()],
                        registration_time: 0,
                        nar_size: 200,
                        ultimate: true,
                        signatures: vec![],
                        ca: None,
                    }
                },
                test_data::dir_example()
            ),
        ],
        Err(DaemonErrorKind::Custom("bad input path".into()).into()), Err("AddMultipleToStore: remote error: AddMultipleToStore: bad input path".into())
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
        run_store_test(nix, mock, |mut client| async move {
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
}

mod proptests {
    use super::*;
    use proptest::prelude::*;

    // TODO: proptest handshake
    const ALL_NIX: &[&dyn NixImpl] = &[&NIX_2_3, &NIX_2_24, &LIX_2_91];
    fn arb_nix() -> impl Strategy<Value = &'static dyn NixImpl> {
        any::<proptest::sample::Index>().prop_map(|idx| *idx.get(ALL_NIX))
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
                run_store_test(nix, mock, |mut client| async move {
                    let res = client.is_valid_path(&path).await;
                    prop_assert_eq!(res.unwrap(), result);
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
                run_store_test(nix, mock, |mut client| async move {
                    let res = client.query_valid_paths(&paths, false).await;
                    prop_assert_eq!(res.unwrap(), result);
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
}
