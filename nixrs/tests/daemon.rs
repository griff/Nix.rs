#![cfg(feature = "test")]

use std::future::{ready, Future};
use std::io::Cursor;
use std::process::Stdio;
use std::time::Instant;

use bytes::BytesMut;
use futures::{TryFutureExt as _, TryStreamExt as _};
use proptest::prelude::{any, TestCaseError};
use proptest::{prop_assert, prop_assert_eq, proptest};
use rstest::rstest;
use tempfile::Builder;
use tokio::process::{ChildStdin, ChildStdout, Command};
use tokio::try_join;

use nixrs::archive::{parse_nar, test_data, NAREvent};
use nixrs::daemon::client::DaemonClient;
use nixrs::daemon::mock::{MockReporter, MockStore};
use nixrs::daemon::server;
use nixrs::daemon::{
    ClientOptions, DaemonError, DaemonResult, DaemonStore as _, LoggerResult, UnkeyedValidPathInfo,
};
use nixrs::hash::{digest, Algorithm, Context, NarHash};
use nixrs::store_path::{StorePath, StorePathSet};

async fn run_store_test<R, T, F, E>(mock: MockStore<R>, test: T) -> Result<(), E>
where
    R: MockReporter,
    T: FnOnce(DaemonClient<ChildStdout, ChildStdin>) -> F,
    F: Future<Output = Result<DaemonClient<ChildStdout, ChildStdin>, E>>,
    E: From<DaemonError>,
{
    use tokio::net::UnixListener;

    let dir = Builder::new().prefix("test_restore_dir").tempdir().unwrap();
    let socket = dir.path().join("local.socket");
    let uri = format!("proxy://{}", socket.to_str().unwrap());

    let listener = UnixListener::bind(socket).unwrap();
    let server = async move {
        let (stream, _addr) = listener.accept().await?;
        let b = server::Builder::new();
        b.serve_connection(stream, mock).await
    }
    .map_err(From::from);
    let mut cmd = Command::new("../../nix/result/bin/nix-daemon");
    cmd.arg("--stdio");
    cmd.arg("--store");
    cmd.arg(&uri);
    cmd.stdin(Stdio::piped());
    cmd.stdout(Stdio::piped());
    let mut child = cmd.spawn().unwrap();
    let stdin = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();

    let client = async move {
        let logs = DaemonClient::builder().connect(stdout, stdin);
        let client = logs.result().await?;
        let mut client = (test)(client).await?;
        client.close().await?;
        Ok(())
    };
    try_join!(
        client,
        server,
        child
            .wait()
            .map_err(|err| From::from(DaemonError::from(err)))
    )
    .map(|_| ())
}

#[tokio::test]
async fn handshake() {
    let mock = MockStore::builder().build();
    run_store_test(mock, |client| ready(Ok(client) as DaemonResult<_>))
        .await
        .unwrap();
}

#[tokio::test]
#[rstest]
#[case(ClientOptions::default(), Ok(()), Ok(()))]
#[case(ClientOptions::default(), Err(DaemonError::Custom("bad input path".into())), Err("remote error: bad input path".into()))]
async fn set_options(
    #[case] options: ClientOptions,
    #[case] response: DaemonResult<()>,
    #[case] expected: Result<(), String>,
) {
    let mock = MockStore::builder()
        .set_options(&options, response)
        .build()
        .build();
    run_store_test(mock, |mut client| async move {
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

#[tokio::test]
#[rstest]
#[case("00000000000000000000000000000000-_", Ok(true), Ok(true))]
#[case("00000000000000000000000000000000-_", Ok(false), Ok(false))]
#[case("00000000000000000000000000000000-_", Err(DaemonError::Custom("bad input path".into())), Err("remote error: bad input path".into()))]
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
#[case(&["00000000000000000000000000000000-_"][..], true, Ok(&["10000000000000000000000000000000-_"][..]), Ok(&["10000000000000000000000000000000-_"][..]))]
#[case(&["00000000000000000000000000000000-_"][..], true, Ok(&[][..]), Ok(&[][..]))]
#[case(&["00000000000000000000000000000000-_"][..], false, Ok(&["10000000000000000000000000000000-_"][..]), Ok(&["10000000000000000000000000000000-_"][..]))]
#[case(&["00000000000000000000000000000000-_"][..], false, Ok(&[][..]), Ok(&[][..]))]
#[case(&["00000000000000000000000000000000-_"][..], true, Err(DaemonError::Custom("bad input path".into())), Err("remote error: bad input path".into()))]
#[case(&["00000000000000000000000000000000-_"][..], false, Err(DaemonError::Custom("bad input path".into())), Err("remote error: bad input path".into()))]
async fn query_valid_paths(
    #[case] store_paths: &[&str],
    #[case] substitute: bool,
    #[case] response: DaemonResult<&[&str]>,
    #[case] expected: Result<&[&str], String>,
) {
    let store_paths = store_paths.iter().map(|p| p.parse().unwrap()).collect();
    let response = response.map(|r| r.into_iter().map(|p| p.parse().unwrap()).collect());
    let expected: Result<StorePathSet, String> =
        expected.map(|r| r.into_iter().map(|p| p.parse().unwrap()).collect());
    let mock = MockStore::builder()
        .query_valid_paths(&store_paths, substitute, response)
        .build()
        .build();
    run_store_test(mock, |mut client| async move {
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
#[case("00000000000000000000000000000000-_", Ok(Some(UnkeyedValidPathInfo {
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
#[case("00000000000000000000000000000000-_", Ok(None), Ok(None))]
#[case("00000000000000000000000000000000-_", Err(DaemonError::Custom("bad input path".into())), Err("remote error: bad input path".into()))]
async fn query_path_info(
    #[case] store_path: StorePath,
    #[case] response: DaemonResult<Option<UnkeyedValidPathInfo>>,
    #[case] expected: Result<Option<UnkeyedValidPathInfo>, String>,
) {
    let mock = MockStore::builder()
        .query_path_info(&store_path, response)
        .build()
        .build();
    run_store_test(mock, |mut client| async move {
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
#[case("00000000000000000000000000000000-_", test_data::text_file())]
#[case("00000000000000000000000000000000-_", test_data::exec_file())]
#[case("00000000000000000000000000000000-_", test_data::empty_file())]
#[case("00000000000000000000000000000000-_", test_data::empty_file_in_dir())]
#[case("00000000000000000000000000000000-_", test_data::empty_dir())]
#[case("00000000000000000000000000000000-_", test_data::empty_dir_in_dir())]
#[case("00000000000000000000000000000000-_", test_data::symlink())]
#[case("00000000000000000000000000000000-_", test_data::dir_example())]
async fn nar_from_path(#[case] store_path: StorePath, #[case] events: Vec<NAREvent>) {
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

    let mock = MockStore::builder()
        .nar_from_path(&store_path, Ok(content))
        .build()
        .build();
    run_store_test(mock, |mut client| async move {
        let mut out = Vec::new();
        client
            .nar_from_path(&store_path, Cursor::new(&mut out))
            .result()
            .await
            .unwrap();
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

proptest! {
    #[test]
    fn proptest_set_options(
        options in any::<ClientOptions>(),
    )
    {
        let now = Instant::now();
        let r = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        r.block_on(async {
            let mock = MockStore::builder()
                .set_options(&options, Ok(())).build()
                .build();
            run_store_test(mock, |mut client| async move {
                let res = client.set_options(&options).result().await;
                prop_assert!(res.is_ok(), "invalid result {:?}", res);
                Ok(client)
            }).await?;
            Ok(()) as Result<_, TestCaseError>
        })?;
        eprintln!("Completed test {}", now.elapsed().as_secs_f64());
    }
}

proptest! {
    #[test]
    fn proptest_is_valid_path(
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
            let mock = MockStore::builder()
                .is_valid_path(&path, Ok(result)).build()
                .build();
            run_store_test(mock, |mut client| async move {
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
        paths in any::<StorePathSet>(),
        substitute in any::<bool>(),
        result in any::<StorePathSet>(),
    )
    {
        let now = Instant::now();
        let r = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        r.block_on(async {
            let mock = MockStore::builder()
                .query_valid_paths(&paths, substitute, Ok(result.clone())).build()
                .build();
            run_store_test(mock, |mut client| async move {
                let res = client.query_valid_paths(&paths, substitute).result().await;
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
