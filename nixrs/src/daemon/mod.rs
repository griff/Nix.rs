#[cfg(feature = "nixrs-derive")]
pub mod client;
pub mod de;
#[cfg(feature = "nixrs-derive")]
mod fail_store;
#[cfg(feature = "nixrs-derive")]
pub mod local;
#[cfg(feature = "nixrs-derive")]
mod logger;
#[cfg(all(feature = "nixrs-derive", any(test, feature = "test")))]
pub mod mock;
#[cfg(feature = "nixrs-derive")]
mod mutex;
pub mod ser;
#[cfg(feature = "nixrs-derive")]
pub mod server;
#[cfg(feature = "nixrs-derive")]
mod types;
mod version;
#[cfg(feature = "nixrs-derive")]
pub mod wire;

#[cfg(feature = "nixrs-derive")]
pub use fail_store::FailStore;
#[cfg(feature = "nixrs-derive")]
pub use local::{LocalDaemonStore, LocalHandshakeDaemonStore};
#[cfg(feature = "nixrs-derive")]
pub use logger::{DriveResult, FutureResultExt, ResultLog, ResultLogExt, ResultProcess};
#[cfg(feature = "nixrs-derive")]
pub use mutex::{MutexHandshakeStore, MutexStore};
#[cfg(feature = "nixrs-derive")]
pub use types::{
    AddToStoreItem, ClientOptions, DaemonError, DaemonErrorContext, DaemonErrorKind, DaemonInt,
    DaemonPath, DaemonResult, DaemonResultExt, DaemonStore, DaemonString, DaemonTime,
    HandshakeDaemonStore, RemoteError, TrustLevel, UnkeyedValidPathInfo,
};
pub use version::{
    NIX_VERSION, PROTOCOL_VERSION, PROTOCOL_VERSION_MIN, ProtocolRange, ProtocolVersion,
};

#[cfg(any(test, feature = "test"))]
pub mod arbitrary {}

#[cfg(all(test, feature = "daemon"))]
pub(crate) mod unittests {
    use std::collections::BTreeSet;
    use std::future::{Future, ready};
    use std::io::Cursor;

    use bytes::Bytes;
    use futures::stream::iter;
    use futures::{FutureExt as _, StreamExt as _, TryFutureExt as _};
    use pretty_assertions::assert_eq;
    use rstest::rstest;
    use tokio::io::{DuplexStream, ReadHalf, WriteHalf, copy_buf, duplex, split};
    use tokio::try_join;
    use tracing::trace;

    use super::client::DaemonClient;
    use super::mock::{MockReporter, MockStore};
    use super::types::AddToStoreItem;
    use super::wire::types2::{
        BuildMode, BuildResult, BuildStatus, KeyedBuildResult, KeyedBuildResults,
        QueryMissingResult, ValidPathInfo,
    };
    use super::{
        ClientOptions, DaemonError, DaemonResult, DaemonStore, DaemonString, ProtocolVersion,
        UnkeyedValidPathInfo,
    };
    use crate::archive::{test_data, write_nar};
    use crate::btree_set;
    use crate::daemon::server;
    use crate::derivation::{BasicDerivation, DerivationOutput};
    use crate::derived_path::{DerivedPath, OutputName};
    use crate::hash::NarHash;
    use crate::store_path::{StoreDir, StorePath, StorePathSet};
    use crate::test::derived_path::parse_path;

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

    async fn run_client<T, F, E>(test: T, client_s: DuplexStream) -> Result<(), E>
    where
        T: FnOnce(DaemonClient<ReadHalf<DuplexStream>, WriteHalf<DuplexStream>>) -> F + Send,
        F: Future<
                Output = Result<DaemonClient<ReadHalf<DuplexStream>, WriteHalf<DuplexStream>>, E>,
            > + Send,
        E: From<DaemonError>,
    {
        let (client_reader, client_writer) = split(client_s);
        let result = DaemonClient::builder().connect(client_reader, client_writer);
        let mut r = std::pin::pin!(result);
        trace!("Client handshake");
        let _logs: Vec<_> = r.by_ref().collect().await;
        let client = r.await?;
        trace!("Client Sending requests");
        let mut client = (test)(client).await?;
        trace!("Client done");
        client.shutdown().await?;
        trace!("Client closed");
        Ok(())
    }

    pub async fn run_store_test_version<R, T, F, E>(
        mock: MockStore<R>,
        version: ProtocolVersion,
        test: T,
    ) -> Result<(), E>
    where
        R: MockReporter + Send + 'static,
        T: FnOnce(DaemonClient<ReadHalf<DuplexStream>, WriteHalf<DuplexStream>>) -> F + Send,
        F: Future<
                Output = Result<DaemonClient<ReadHalf<DuplexStream>, WriteHalf<DuplexStream>>, E>,
            > + Send,
        E: From<DaemonError>,
    {
        let (client_s, server_s) = duplex(10_000);
        let (server_reader, server_writer) = split(server_s);
        let mut b = server::Builder::new();
        let server = b
            .set_max_version(version)
            .serve_connection(server_reader, server_writer, mock)
            .map_err(From::from)
            .boxed();
        let client = run_client(test, client_s).boxed();
        try_join!(client, server).map(|_| ())
    }

    pub async fn run_store_test<R, T, F, E>(mock: MockStore<R>, test: T) -> Result<(), E>
    where
        R: MockReporter + Send + 'static,
        T: FnOnce(DaemonClient<ReadHalf<DuplexStream>, WriteHalf<DuplexStream>>) -> F + Send,
        F: Future<
                Output = Result<DaemonClient<ReadHalf<DuplexStream>, WriteHalf<DuplexStream>>, E>,
            > + Send,
        E: From<DaemonError>,
    {
        run_store_test_version(mock, ProtocolVersion::default(), test).await
    }

    #[test_log::test(tokio::test)]
    #[should_panic(
        expected = "store dropped with LogOperation { operation: IsValidPath(StorePath(00000000000000000000000000000000-_), Ok(true)), logs: [] } operation still unread"
    )]
    async fn check_asserts() {
        let mock = MockStore::builder()
            .is_valid_path(
                &"00000000000000000000000000000000-_".parse().unwrap(),
                Ok(true),
            )
            .build()
            .build();
        run_store_test(mock, |client| async move { Ok(client) as DaemonResult<_> })
            .await
            .unwrap();
    }

    #[test_log::test(tokio::test)]
    async fn handshake() {
        let mock = MockStore::builder().build();
        run_store_test(mock, |client| ready(Ok(client) as DaemonResult<_>))
            .await
            .unwrap();
    }

    #[test_log::test(tokio::test)]
    #[rstest]
    #[case(ClientOptions::default(), Ok(()), Ok(()))]
    #[case(ClientOptions::default(), Err(DaemonError::custom("bad input path")), Err("SetOptions: remote error: SetOptions: bad input path".into()))]
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
    #[case("00000000000000000000000000000000-_", Ok(true), Ok(true))]
    #[case("00000000000000000000000000000000-_", Ok(false), Ok(false))]
    #[case("00000000000000000000000000000000-_", Err(DaemonError::custom("bad input path")), Err("IsValidPath: remote error: IsValidPath: bad input path".into()))]
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

    #[test_log::test(tokio::test)]
    #[rstest]
    #[case(&["00000000000000000000000000000000-_"][..], true, Ok(&["10000000000000000000000000000000-_"][..]), Ok(&["10000000000000000000000000000000-_"][..]))]
    #[case(&["00000000000000000000000000000000-_"][..], true, Ok(&[][..]), Ok(&[][..]))]
    #[case(&["00000000000000000000000000000000-_"][..], false, Ok(&["10000000000000000000000000000000-_"][..]), Ok(&["10000000000000000000000000000000-_"][..]))]
    #[case(&["00000000000000000000000000000000-_"][..], false, Ok(&[][..]), Ok(&[][..]))]
    #[case(&["00000000000000000000000000000000-_"][..], true, Err(DaemonError::custom("bad input path")), Err("QueryValidPaths: remote error: QueryValidPaths: bad input path".into()))]
    #[case(&["00000000000000000000000000000000-_"][..], false, Err(DaemonError::custom("bad input path")), Err("QueryValidPaths: remote error: QueryValidPaths: bad input path".into()))]
    async fn query_valid_paths(
        #[case] store_paths: &[&str],
        #[case] substitute: bool,
        #[case] response: DaemonResult<&[&str]>,
        #[case] expected: Result<&[&str], String>,
    ) {
        let store_paths = store_paths.iter().map(|p| p.parse().unwrap()).collect();
        let response = response.map(|r| r.iter().map(|p| p.parse().unwrap()).collect());
        let expected: Result<StorePathSet, String> =
            expected.map(|r| r.iter().map(|p| p.parse().unwrap()).collect());
        let mock = MockStore::builder()
            .query_valid_paths(&store_paths, substitute, response)
            .build()
            .build();
        run_store_test(mock, |mut client| async move {
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

    #[test_log::test(tokio::test)]
    #[rstest]
    #[case("00000000000000000000000000000000-_", Ok(Some(UnkeyedValidPathInfo {
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
    #[case::proptested(
        "00000000000000000000000000000000-=",
        Ok(Some(UnkeyedValidPathInfo {
                deriver: None,
                nar_hash: NarHash::new(&[0u8; 32]),
                references: btree_set![],
                registration_time: 0,
                nar_size: 0,
                ultimate: false,
                signatures: BTreeSet::new(),
                ca: Some("text:sha256:09q0000000000000000000000000000000000000000000000000".parse().unwrap()),
            },
        )),
        Ok(Some(UnkeyedValidPathInfo {
            deriver: None,
            nar_hash: NarHash::new(&[0u8; 32]),
            references: btree_set![],
            registration_time: 0,
            nar_size: 0,
            ultimate: false,
            signatures: BTreeSet::new(),
            ca: Some("text:sha256:09q0000000000000000000000000000000000000000000000000".parse().unwrap()),
        }))
    )]
    #[case("00000000000000000000000000000000-_", Ok(None), Ok(None))]
    #[case("00000000000000000000000000000000-_", Err(DaemonError::custom("bad input path")), Err("QueryPathInfo: remote error: QueryPathInfo: bad input path".into()))]
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
        #[case] store_path: StorePath,
        #[case] events: test_data::TestNarEvents,
    ) {
        let content = write_nar(events.iter());
        let hash = NarHash::digest(&content);
        let size = content.len();
        let mock = MockStore::builder()
            .nar_from_path(&store_path, Ok(content))
            .build()
            .build();
        run_store_test(mock, |mut client| async move {
            {
                let mut reader = client.nar_from_path(&store_path).await.unwrap();
                let mut out = Vec::new();
                copy_buf(&mut reader, &mut out).await?;
                let nar: test_data::TestNarEvents =
                    crate::archive::read_nar(Cursor::new(Bytes::copy_from_slice(&out))).await?;
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
    #[test_log::test(tokio::test)]
    #[rstest]
    #[case("00000000000000000000000000000000-_", DaemonError::custom("bad input path"), "remote error: bad input path".into())]
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
    #[test_log::test(tokio::test)]
    #[rstest]
    #[case::normal(&["/nix/store/00000000000000000000000000000000-_"][..], BuildMode::Normal, Ok(()), Ok(()))]
    #[case::repair(&["/nix/store/00000000000000000000000000000000-_"][..], BuildMode::Repair, Ok(()), Ok(()))]
    #[case::empty(&[][..], BuildMode::Check, Ok(()), Ok(()))]
    #[case::error(&["/nix/store/00000000000000000000000000000000-_"][..], BuildMode::Normal, Err(DaemonError::custom("bad input path")), Err("BuildPaths: remote error: BuildPaths: bad input path".into()))]
    async fn build_paths(
        #[case] paths: &[&str],
        #[case] mode: BuildMode,
        #[case] response: DaemonResult<()>,
        #[case] expected: Result<(), String>,
    ) {
        let store_dir = StoreDir::default();
        let paths: Vec<DerivedPath> = paths.iter().map(|p| store_dir.parse(p).unwrap()).collect();
        let mock = MockStore::builder()
            .build_paths(&paths, mode, response)
            .build()
            .build();
        run_store_test(mock, |mut client| async move {
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

    // BuildPathsWithResults
    #[test_log::test(tokio::test)]
    #[rstest]
    #[case::normal(&["/nix/store/00000000000000000000000000000000-_"][..], BuildMode::Normal, Ok(
        vec![KeyedBuildResult {
            path: parse_path("/nix/store/00000000000000000000000000000000-_"),
            result: BuildResult {
                status: BuildStatus::Built,
                error_msg: DaemonString::from_static(b""),
                times_built: 1,
                is_non_deterministic: false,
                start_time: 0,
                stop_time: 0,
                cpu_user: None,
                cpu_system: None,
                built_outputs: btree_map!(),
            }
        }]), Ok(vec![KeyedBuildResult {
            path: parse_path("/nix/store/00000000000000000000000000000000-_"),
            result: BuildResult {
                status: BuildStatus::Built,
                error_msg: DaemonString::from_static(b""),
                times_built: 1,
                is_non_deterministic: false,
                start_time: 0,
                stop_time: 0,
                cpu_user: None,
                cpu_system: None,
                built_outputs: btree_map!(),
            }
        }]))]
    #[case::repair(&["/nix/store/00000000000000000000000000000000-_"][..], BuildMode::Repair, Ok(vec![KeyedBuildResult {
        path: parse_path("/nix/store/00000000000000000000000000000000-_"),
        result: BuildResult {
            status: BuildStatus::Built,
            error_msg: DaemonString::from_static(b""),
            times_built: 1,
            is_non_deterministic: false,
            start_time: 0,
            stop_time: 0,
            cpu_user: None,
            cpu_system: None,
            built_outputs: btree_map!(),
        }
    }]), Ok(vec![KeyedBuildResult {
        path: parse_path("/nix/store/00000000000000000000000000000000-_"),
        result: BuildResult {
            status: BuildStatus::Built,
            error_msg: DaemonString::from_static(b""),
            times_built: 1,
            is_non_deterministic: false,
            start_time: 0,
            stop_time: 0,
            cpu_user: None,
            cpu_system: None,
            built_outputs: btree_map!(),
        }
    }]))]
    #[case::empty(&[][..], BuildMode::Check, Ok(vec![]), Ok(vec![]))]
    #[case::error(&["/nix/store/00000000000000000000000000000000-_"][..], BuildMode::Normal, Err(DaemonError::custom("bad input path")), Err("BuildPathsWithResults: remote error: BuildPathsWithResults: bad input path".into()))]
    async fn build_paths_with_results(
        #[case] paths: &[&str],
        #[case] mode: BuildMode,
        #[case] response: DaemonResult<KeyedBuildResults>,
        #[case] expected: Result<KeyedBuildResults, String>,
    ) {
        let store_dir = StoreDir::default();
        let paths: Vec<DerivedPath> = paths.iter().map(|p| store_dir.parse(p).unwrap()).collect();
        let mock = MockStore::builder()
            .build_paths_with_results(&paths, mode, response)
            .build()
            .build();
        run_store_test(mock, |mut client| async move {
            let actual = client
                .build_paths_with_results(&paths, mode)
                .await
                .map_err(|err| err.to_string());
            assert_eq!(expected, actual);
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

    // BuildDerivation
    #[test_log::test(tokio::test)]
    #[rstest]
    #[case::normal(BasicDerivation {
        drv_path: "00000000000000000000000000000000-_.drv".parse().unwrap(),
        outputs: btree_map!(
            OutputName::default() => DerivationOutput::InputAddressed("00000000000000000000000000000000-_".parse().unwrap()),
        ),
        input_srcs: store_path_set!(),
        platform: DaemonString::from_static(b"x86_64-linux"),
        builder: DaemonString::from_static(b"/bin/sh"),
        args: vec![DaemonString::from_static(b"-c"), DaemonString::from_static(b"echo Hello")],
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
        platform: DaemonString::from_static(b"x86_64-linux"),
        builder: DaemonString::from_static(b"/bin/sh"),
        args: vec![DaemonString::from_static(b"-c"), DaemonString::from_static(b"echo Hello")],
        env: btree_map!(),
    }, BuildMode::Normal, Err(DaemonError::custom("bad input path")), Err("BuildDerivation: remote error: BuildDerivation: bad input path".into()))]
    async fn build_derivation(
        #[case] drv: BasicDerivation,
        #[case] mode: BuildMode,
        #[case] response: DaemonResult<BuildResult>,
        #[case] expected: Result<BuildResult, String>,
    ) {
        let mock = MockStore::builder()
            .build_derivation(&drv, mode, response)
            .build()
            .build();
        run_store_test(mock, |mut client| async move {
            assert_eq!(
                expected,
                client
                    .build_derivation(&drv, mode)
                    .await
                    .map_err(|err| err.to_string())
            );
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
        #[case] paths: &[&str],
        #[case] response: DaemonResult<QueryMissingResult>,
        #[case] expected: Result<QueryMissingResult, String>,
    ) {
        let store_dir = StoreDir::default();
        let paths: Vec<DerivedPath> = paths.iter().map(|p| store_dir.parse(p).unwrap()).collect();
        let mock = MockStore::builder()
            .query_missing(&paths, response)
            .build()
            .build();
        run_store_test(mock, |mut client| async move {
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
        #[values((1, 21), (1, 23))] version: (u8, u8),
        #[case] info: ValidPathInfo,
        #[case] repair: bool,
        #[case] dont_check_sigs: bool,
        #[case] events: test_data::TestNarEvents,
        #[case] response: DaemonResult<()>,
        #[case] expected: Result<(), String>,
    ) {
        let version = ProtocolVersion::from(version);
        let content = write_nar(events.iter());

        let mock = MockStore::builder()
            .add_to_store_nar(&info, repair, dont_check_sigs, content.clone(), response)
            .build()
            .build();
        run_store_test_version(mock, version, |mut client| async move {
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

        let mock = MockStore::builder()
            .add_multiple_to_store(repair, dont_check_sigs, infos_content, response)
            .build()
            .build();
        run_store_test(mock, |mut client| async move {
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

#[cfg(all(test, feature = "daemon"))]
mod proptests {
    use std::io::Cursor;
    use std::time::Instant;

    use bytes::Bytes;
    use futures::stream::iter;
    use proptest::prelude::*;
    use proptest::test_runner::TestCaseResult;
    use proptest::{prop_assert, prop_assert_eq, proptest};
    use tokio::io::copy_buf;
    use tracing::info;

    use super::DaemonResult;
    use super::mock::MockStore;
    use super::unittests::run_store_test;
    use super::wire::types2::{BuildMode, BuildResult, KeyedBuildResult, QueryMissingResult};
    use super::{ClientOptions, UnkeyedValidPathInfo};
    use crate::archive::{read_nar, test_data};
    use crate::daemon::wire::types2::ValidPathInfo;
    use crate::daemon::{AddToStoreItem, DaemonStore as _};
    use crate::derivation::BasicDerivation;
    use crate::derived_path::DerivedPath;
    use crate::hash::NarHash;
    use crate::pretty_prop_assert_eq;
    use crate::store_path::{StorePath, StorePathSet};
    use crate::test::arbitrary::archive::arb_nar_contents;
    use crate::test::arbitrary::daemon::arb_nar_contents_items;

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
                    let res = client.set_options(&options).await;
                    prop_assert!(res.is_ok(), "invalid result {:?}", res);
                    Ok(client)
                }).await?;
                Ok(()) as Result<(), TestCaseError>
            })?;
            info!("Completed test {}", now.elapsed().as_secs_f64());
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
                    let res = client.is_valid_path(&path).await;
                    prop_assert_eq!(res.unwrap(), result);
                    Ok(client)
                }).await?;
                Ok(()) as Result<(), TestCaseError>
            })?;
            info!("Completed test {}", now.elapsed().as_secs_f64());
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
                    let res = client.query_valid_paths(&paths, substitute).await;
                    prop_assert_eq!(res.unwrap(), result);
                    Ok(client)
                }).await?;
                Ok(()) as Result<(), TestCaseError>
            })?;
            info!("Completed test {}", now.elapsed().as_secs_f64());
        }
    }

    proptest! {
        #[test]
        fn proptest_query_path_info(
            path in any::<StorePath>(),
            result in any::<Option<UnkeyedValidPathInfo>>(),
        )
        {
            let now = Instant::now();
            let r = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            r.block_on(async {
                let mock = MockStore::builder()
                    .query_path_info(&path, Ok(result.clone())).build()
                    .build();
                run_store_test(mock, |mut client| async move {
                    let res = client.query_path_info(&path).await;
                    prop_assert_eq!(res.unwrap(), result);
                    Ok(client)
                }).await?;
                Ok(()) as Result<(), TestCaseError>
            })?;
            info!("Completed test {}", now.elapsed().as_secs_f64());
        }
    }

    proptest! {
        #[test]
        fn proptest_nar_from_path(
            path in any::<StorePath>(),
            nar_content in arb_nar_contents(20, 20, 5),
        )
        {
            let nar_hash = NarHash::digest(&nar_content);
            let now = Instant::now();
            let r = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            r.block_on(async {
                let mock = MockStore::builder()
                    .nar_from_path(&path, Ok(nar_content.clone())).build()
                    .build();
                run_store_test(mock, |mut client| async move {
                    {
                        let mut reader = client.nar_from_path(&path).await?;
                        let mut out = Vec::new();
                        copy_buf(&mut reader, &mut out).await?;
                        let _ : test_data::TestNarEvents = read_nar(Cursor::new(Bytes::copy_from_slice(&out))).await?;
                        prop_assert_eq!(nar_content.len(), out.len());
                        let hash = NarHash::digest(&out);
                        prop_assert_eq!(hash.as_ref(), nar_hash.as_ref());
                    }
                    Ok(client)
                }).await?;
                Ok(()) as Result<(), TestCaseError>
            })?;
            info!("Completed test {}", now.elapsed().as_secs_f64());
        }
    }

    proptest! {
        #[test]
        fn proptest_add_to_store_nar(
            info in any::<ValidPathInfo>(),
            repair in any::<bool>(),
            dont_check_sigs in any::<bool>(),
            nar_content in arb_nar_contents(20, 20, 5),
        )
        {
            let now = Instant::now();
            let r = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            r.block_on(async {
                let mock = MockStore::builder()
                    .add_to_store_nar(&info, repair, dont_check_sigs, nar_content.clone(), Ok(())).build()
                    .build();
                run_store_test(mock, |mut client| async move {
                    client.add_to_store_nar(&info, Cursor::new(nar_content), repair, dont_check_sigs).await?;
                    Ok(client) as DaemonResult<_>
                }).await?;
                Ok(()) as Result<(), TestCaseError>
            })?;
            info!("Completed test {}", now.elapsed().as_secs_f64());
        }
    }

    #[test_strategy::proptest(async = "tokio")]
    async fn build_paths(paths: Vec<DerivedPath>, mode: BuildMode) -> TestCaseResult {
        let mock = MockStore::builder()
            .build_paths(&paths, mode, Ok(()))
            .build()
            .build();
        run_store_test(mock, |mut client| async move {
            client.build_paths(&paths, mode).await?;
            Ok(client) as Result<_, TestCaseError>
        })
        .await
    }

    #[test_strategy::proptest(async = "tokio")]
    async fn build_paths_with_results(
        result: Vec<KeyedBuildResult>,
        mode: BuildMode,
    ) -> TestCaseResult {
        let paths: Vec<DerivedPath> = result.iter().map(|r| r.path.clone()).collect();
        let mock = MockStore::builder()
            .build_paths_with_results(&paths, mode, Ok(result.clone()))
            .build()
            .build();
        run_store_test(mock, |mut client| async move {
            let actual = client.build_paths_with_results(&paths, mode).await?;
            pretty_prop_assert_eq!(result, actual);
            Ok(client) as Result<_, TestCaseError>
        })
        .await
    }

    #[test_strategy::proptest(async = "tokio")]
    async fn build_derivation(
        drv: BasicDerivation,
        mode: BuildMode,
        result: BuildResult,
    ) -> TestCaseResult {
        let mock = MockStore::builder()
            .build_derivation(&drv, mode, Ok(result.clone()))
            .build()
            .build();
        run_store_test(mock, |mut client| async move {
            let actual = client.build_derivation(&drv, mode).await?;
            pretty_prop_assert_eq!(result, actual);
            Ok(client) as Result<_, TestCaseError>
        })
        .await
    }

    #[test_strategy::proptest(async = "tokio")]
    async fn query_missing(paths: Vec<DerivedPath>, result: QueryMissingResult) -> TestCaseResult {
        let mock = MockStore::builder()
            .query_missing(&paths, Ok(result.clone()))
            .build()
            .build();
        run_store_test(mock, |mut client| async move {
            let actual = client.query_missing(&paths).await?;
            pretty_prop_assert_eq!(result, actual);
            Ok(client) as Result<_, TestCaseError>
        })
        .await
    }

    #[test_strategy::proptest(async = "tokio")]
    async fn add_multiple_to_store(
        repair: bool,
        dont_check_sigs: bool,
        #[strategy(arb_nar_contents_items())] infos: Vec<(ValidPathInfo, Bytes)>,
    ) -> TestCaseResult {
        let infos_stream = iter(infos.clone().into_iter().map(|(info, content)| {
            Ok(AddToStoreItem {
                info: info.clone(),
                reader: Cursor::new(content.clone()),
            })
        }));

        let mock = MockStore::builder()
            .add_multiple_to_store(repair, dont_check_sigs, infos, Ok(()))
            .build()
            .build();
        run_store_test(mock, |mut client| async move {
            client
                .add_multiple_to_store(repair, dont_check_sigs, infos_stream)
                .await?;
            Ok(client) as Result<_, TestCaseError>
        })
        .await
    }

    /*
    #[test_strategy::proptest(
        async = "tokio",
        ProptestConfig::default(),
        max_shrink_iters = 1_000_000
    )]
    async fn proptest_operations(
        version: ProtocolVersion,
        #[any((size_range(1..10), MockOperationParams { version: #version, allow_options: true }))]
        ops: Vec<LogOperation>,
    ) -> TestCaseResult {
        let mut mock = MockStore::builder();
        for op in ops.iter() {
            mock.add_operation(op.clone());
        }
        let mock = mock.build();
        run_store_test_version(mock, version, |mut client| async move {
            for op in ops.into_iter() {
                op.check_operation(&mut client).await?;
            }
            Ok(client) as Result<_, TestCaseError>
        })
        .await
    }
     */
}
