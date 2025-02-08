#![cfg(feature = "test")]

use std::fs::remove_file;
use std::future::{ready, Future};
use std::io::{self, Cursor};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Instant;

use bytes::BytesMut;
use futures::{FutureExt as _, StreamExt, TryFutureExt as _, TryStreamExt as _};
use proptest::prelude::{any, Strategy, TestCaseError};
use proptest::{prop_assert, prop_assert_eq, proptest};
use rstest::rstest;
use tempfile::Builder;
use tokio::io::{split, AsyncBufReadExt, BufReader};
use tokio::net::tcp::ReadHalf;
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{UnixSocket, UnixStream};
use tokio::process::{ChildStdin, ChildStdout, Command};
use tokio::try_join;

use nixrs::archive::{parse_nar, test_data, NAREvent};
use nixrs::daemon::client::DaemonClient;
use nixrs::daemon::mock::{self, ChannelReporter, MockReporter, MockStore, ReporterError};
use nixrs::daemon::{server, Verbosity};
use nixrs::daemon::{
    ClientOptions, DaemonError, DaemonErrorKind, DaemonResult, DaemonStore as _, LoggerResult,
    UnkeyedValidPathInfo,
};
use nixrs::hash::{digest, Algorithm, Context, NarHash};
use nixrs::store_path::{StorePath, StorePathSet};

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
    verbosity: Verbosity,
    cmd_args: &'static [&'static str],
}

impl NixImpl for StdNixImpl {
    fn name(&self) -> &str {
        &self.name
    }

    fn prepare_mock(&self, mock: &mut mock::Builder<()>) {
        let mut options = ClientOptions::default();
        options.build_cores = 12;
        options.max_build_jobs = 12;
        options.verbosity = self.verbosity;
        mock.set_options(&options, Ok(())).build();
    }

    fn prepare_program<'c>(&self, cmd: &'c mut Command) -> &'c mut Command {
        cmd.args(self.cmd_args.iter())
    }
}

const NIX_2_3: StdNixImpl = StdNixImpl {
    name: "nix_2_3",
    verbosity: Verbosity::Error,
    cmd_args: &[],
};

const NIX_2_24: StdNixImpl = StdNixImpl {
    name: "nix_2_24",
    verbosity: Verbosity::Error,
    cmd_args: &[
        "--extra-experimental-features",
        "daemon-trust-override",
        "--force-untrusted",
    ],
};

const LIX_2_91: StdNixImpl = StdNixImpl {
    name: "lix_2_91",
    verbosity: Verbosity::Vomit,
    cmd_args: &[
        "--extra-experimental-features",
        "daemon-trust-override",
        "--force-untrusted",
    ],
};

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
    let socket = dir.path().join("local.socket");
    /*
    let socket = Path::new("./daemon.socket");
    if socket.exists() {
        remove_file(socket).unwrap();
    }
     */
    let uri = format!(
        "proxy://{}?path-info-cache-size=0",
        socket.to_str().unwrap()
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
        let client = logs.result().await?;
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
    for report in reports.iter() {
        panic!("{}", report);
    }
    Ok(())
}

fn prepare_mock(nix: &dyn NixImpl) -> mock::Builder<()> {
    let mut mock = MockStore::builder();
    nix.prepare_mock(&mut mock);
    mock
}
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
                .result()
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
    let response = response.map(|r| r.into_iter().map(|p| p.parse().unwrap()).collect());
    let expected: Result<StorePathSet, String> =
        expected.map(|r| r.into_iter().map(|p| p.parse().unwrap()).collect());
    let mut mock = prepare_mock(nix);
    mock.query_valid_paths(&store_paths, substitute, response)
        .build();
    run_store_test(nix, mock, |mut client| async move {
        assert_eq!(
            expected,
            client
                .query_valid_paths(&store_paths, substitute)
                .result()
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
                .result()
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
    #[case] events: Vec<NAREvent>,
) {
    let mut buf = BytesMut::new();
    let mut ctx = Context::new(Algorithm::SHA256);
    let mut size = 0;
    for event in events.iter() {
        let encoded = event.encoded_size();
        size += encoded as u64;
        buf.reserve(encoded);
        let mut temp = buf.split_off(buf.len());
        event.encode_into(&mut temp);
        ctx.update(&temp);
        buf.unsplit(temp);
    }
    let hash = ctx.finish();
    let content = buf.freeze();

    let mut mock = prepare_mock(nix);
    mock.nar_from_path(&store_path, Ok(content)).build();
    run_store_test(nix, mock, |mut client| async move {
        let mut out = Vec::new();
        client
            .nar_from_path(&store_path, Cursor::new(&mut out))
            .result()
            .await
            .unwrap();
        println!("Parsing NAR {}", out.len());
        let nar: Vec<NAREvent> = parse_nar(Cursor::new(&out)).try_collect().await?;
        assert_eq!(events, nar);
        assert_eq!(size, out.len() as u64);
        assert_eq!(digest(Algorithm::SHA256, &out), hash);
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
                let res = client.is_valid_path(&path).result().await;
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
                let res = client.query_valid_paths(&paths, false).result().await;
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
