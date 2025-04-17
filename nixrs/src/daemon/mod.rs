use std::fmt;

#[cfg(feature = "nixrs-derive")]
use nixrs_derive::{NixDeserialize, NixSerialize};

#[cfg(feature = "nixrs-derive")]
pub mod client;
pub mod de;
#[cfg(feature = "nixrs-derive")]
mod fail_store;
#[cfg(feature = "nixrs-derive")]
mod logger;
#[cfg(all(feature = "nixrs-derive", any(test, feature = "test")))]
pub mod mock;
pub mod ser;
#[cfg(feature = "nixrs-derive")]
pub mod server;
#[cfg(feature = "nixrs-derive")]
mod types;
#[cfg(feature = "nixrs-derive")]
pub mod wire;

#[cfg(feature = "nixrs-derive")]
pub use fail_store::FailStore;
#[cfg(feature = "nixrs-derive")]
pub use logger::{
    LocalLoggerResult, LogError, LogMessage, ResultLog, ResultLogExt, TraceLine, Verbosity,
};
#[cfg(feature = "nixrs-derive")]
pub use types::{
    AddToStoreItem, ClientOptions, DaemonError, DaemonErrorContext, DaemonErrorKind, DaemonInt,
    DaemonPath, DaemonResult, DaemonResultExt, DaemonStore, DaemonString, DaemonTime,
    HandshakeDaemonStore, LocalDaemonStore, LocalHandshakeDaemonStore, RemoteError, TrustLevel,
    UnkeyedValidPathInfo,
};

pub const NIX_VERSION: &str = "Nix.rs 1.0";
pub const PROTOCOL_VERSION: ProtocolVersion = ProtocolVersion::from_parts(1, 35);
pub const PROTOCOL_VERSION_MIN: ProtocolVersion = ProtocolVersion::from_parts(1, 21);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
#[cfg_attr(feature = "nixrs-derive", nix(from = "u16", into = "u16"))]
pub struct ProtocolVersion(u8, u8);
impl ProtocolVersion {
    pub const fn max() -> Self {
        PROTOCOL_VERSION
    }

    pub const fn min() -> Self {
        PROTOCOL_VERSION_MIN
    }

    pub const fn from_parts(major: u8, minor: u8) -> Self {
        Self(major, minor)
    }

    #[inline]
    pub const fn major(&self) -> u8 {
        self.0
    }

    #[inline]
    pub const fn minor(&self) -> u8 {
        self.1
    }
}

impl Default for ProtocolVersion {
    fn default() -> Self {
        PROTOCOL_VERSION
    }
}

impl From<u16> for ProtocolVersion {
    fn from(value: u16) -> Self {
        ProtocolVersion::from_parts(((value & 0xff00) >> 8) as u8, (value & 0x00ff) as u8)
    }
}

impl From<(u8, u8)> for ProtocolVersion {
    fn from((major, minor): (u8, u8)) -> Self {
        ProtocolVersion::from_parts(major, minor)
    }
}

impl From<ProtocolVersion> for u16 {
    fn from(value: ProtocolVersion) -> Self {
        ((value.major() as u16) << 8) | (value.minor() as u16)
    }
}

impl fmt::Display for ProtocolVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.major(), self.minor())
    }
}

#[cfg(all(test, feature = "daemon"))]
pub(crate) mod tests {
    use std::future::{ready, Future};
    use std::io::Cursor;

    use bytes::BytesMut;
    use futures::stream::iter;
    use futures::{FutureExt as _, TryFutureExt as _, TryStreamExt as _};
    use pretty_assertions::assert_eq;
    use rstest::rstest;
    use tokio::io::{copy_buf, duplex, split, DuplexStream, ReadHalf, WriteHalf};
    use tokio::try_join;
    use tracing::trace;
    use tracing_test::traced_test;

    use super::wire::types2::{BasicDerivation, QueryMissingResult};
    use super::{
        client::DaemonClient,
        mock::{MockReporter, MockStore},
        DaemonResult, DaemonStore,
    };
    use super::{ClientOptions, UnkeyedValidPathInfo};
    use crate::archive::test_data;
    use crate::archive::NAREvent;
    use crate::daemon::types::AddToStoreItem;
    use crate::daemon::wire::types2::{
        BuildMode, BuildResult, BuildStatus, DerivationOutput, DerivedPath, ValidPathInfo,
    };
    use crate::daemon::{server, DaemonString};
    use crate::daemon::{DaemonError, DaemonErrorKind};
    use crate::hash::{digest, Algorithm, NarHash};
    use crate::store_path::StorePath;
    use crate::store_path::StorePathSet;

    pub async fn run_store_test<R, T, F, E>(mock: MockStore<R>, test: T) -> Result<(), E>
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
        let b = server::Builder::new();
        let server = b
            .serve_connection(server_reader, server_writer, mock)
            .map_err(From::from)
            .boxed();
        let (client_reader, client_writer) = split(client_s);
        let client = async move {
            let logs = DaemonClient::builder().connect(client_reader, client_writer);
            trace!("Client handshake");
            let client = logs.await?;
            trace!("Client Sending requests");
            let mut client = (test)(client).await?;
            trace!("Client done");
            client.close().await?;
            trace!("Client closed");
            Ok(())
        }
        .boxed();
        try_join!(client, server).map(|_| ())
    }

    #[traced_test]
    #[tokio::test]
    async fn handshake() {
        let mock = MockStore::builder().build();
        run_store_test(mock, |client| ready(Ok(client) as DaemonResult<_>))
            .await
            .unwrap();
    }

    #[traced_test]
    #[tokio::test]
    #[rstest]
    #[case(ClientOptions::default(), Ok(()), Ok(()))]
    #[case(ClientOptions::default(), Err(DaemonErrorKind::Custom("bad input path".into()).into()), Err("SetOptions: remote error: SetOptions: bad input path".into()))]
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

    #[traced_test]
    #[tokio::test]
    #[rstest]
    #[case("00000000000000000000000000000000-_", Ok(true), Ok(true))]
    #[case("00000000000000000000000000000000-_", Ok(false), Ok(false))]
    #[case("00000000000000000000000000000000-_", Err(DaemonErrorKind::Custom("bad input path".into()).into()), Err("IsValidPath: remote error: IsValidPath: bad input path".into()))]
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

    #[traced_test]
    #[tokio::test]
    #[rstest]
    #[case(&["00000000000000000000000000000000-_"][..], true, Ok(&["10000000000000000000000000000000-_"][..]), Ok(&["10000000000000000000000000000000-_"][..]))]
    #[case(&["00000000000000000000000000000000-_"][..], true, Ok(&[][..]), Ok(&[][..]))]
    #[case(&["00000000000000000000000000000000-_"][..], false, Ok(&["10000000000000000000000000000000-_"][..]), Ok(&["10000000000000000000000000000000-_"][..]))]
    #[case(&["00000000000000000000000000000000-_"][..], false, Ok(&[][..]), Ok(&[][..]))]
    #[case(&["00000000000000000000000000000000-_"][..], true, Err(DaemonErrorKind::Custom("bad input path".into()).into()), Err("QueryValidPaths: remote error: QueryValidPaths: bad input path".into()))]
    #[case(&["00000000000000000000000000000000-_"][..], false, Err(DaemonErrorKind::Custom("bad input path".into()).into()), Err("QueryValidPaths: remote error: QueryValidPaths: bad input path".into()))]
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

    #[traced_test]
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
    #[case::proptested(
        "00000000000000000000000000000000-=",
        Ok(Some(UnkeyedValidPathInfo {
                deriver: None,
                nar_hash: NarHash::new(&[0u8; 32]),
                references: vec![],
                registration_time: 0,
                nar_size: 0,
                ultimate: false,
                signatures: vec![],
                ca: Some("text:sha256:09q0000000000000000000000000000000000000000000000000".parse().unwrap()),
            },
        )),
        Ok(Some(UnkeyedValidPathInfo {
            deriver: None,
            nar_hash: NarHash::new(&[0u8; 32]),
            references: vec![],
            registration_time: 0,
            nar_size: 0,
            ultimate: false,
            signatures: vec![],
            ca: Some("text:sha256:09q0000000000000000000000000000000000000000000000000".parse().unwrap()),
        }))
    )]
    #[case("00000000000000000000000000000000-_", Ok(None), Ok(None))]
    #[case("00000000000000000000000000000000-_", Err(DaemonErrorKind::Custom("bad input path".into()).into()), Err("QueryPathInfo: remote error: QueryPathInfo: bad input path".into()))]
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

    #[traced_test]
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
        let mut ctx = crate::hash::Context::new(Algorithm::SHA256);
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
            {
                let mut reader = client.nar_from_path(&store_path).await.unwrap();
                let mut out = Vec::new();
                copy_buf(&mut reader, &mut out).await?;
                let nar: Vec<NAREvent> = crate::archive::parse_nar(Cursor::new(&out))
                    .try_collect()
                    .await?;
                assert_eq!(events, nar);
                assert_eq!(size, out.len() as u64);
                assert_eq!(digest(Algorithm::SHA256, &out), hash);
            }
            Ok(client) as DaemonResult<_>
        })
        .await
        .unwrap();
    }

    /*
    #[traced_test]
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
    #[case::normal(&["00000000000000000000000000000000-_"][..], BuildMode::Normal, Ok(()), Ok(()))]
    #[case::repair(&["00000000000000000000000000000000-_"][..], BuildMode::Repair, Ok(()), Ok(()))]
    #[case::empty(&[][..], BuildMode::Check, Ok(()), Ok(()))]
    #[case::error(&["00000000000000000000000000000000-_"][..], BuildMode::Normal, Err(DaemonErrorKind::Custom("bad input path".into()).into()), Err("BuildPaths: remote error: BuildPaths: bad input path".into()))]
    async fn build_paths(
        #[case] paths: &[&str],
        #[case] mode: BuildMode,
        #[case] response: DaemonResult<()>,
        #[case] expected: Result<(), String>,
    ) {
        let paths: Vec<DerivedPath> = paths.iter().map(|p| p.parse().unwrap()).collect();
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

    macro_rules! store_path_set {
        () => { StorePathSet::new() };
        ($p:expr $(, $pr:expr)*) => {{
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
         $(, $kr:expr => $vr:expr )*) => {{
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
            "out".into() => DerivationOutput {
                path: Some("00000000000000000000000000000000-_".parse().unwrap()),
                hash_algo: None,
                hash: None,
            }
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
            "out".into() => DerivationOutput {
                path: Some("00000000000000000000000000000000-_".parse().unwrap()),
                hash_algo: None,
                hash: None,
            }
        ),
        input_srcs: store_path_set!(),
        platform: DaemonString::from_static(b"x86_64-linux"),
        builder: DaemonString::from_static(b"/bin/sh"),
        args: vec![DaemonString::from_static(b"-c"), DaemonString::from_static(b"echo Hello")],
        env: btree_map!(),
    }, BuildMode::Normal, Err(DaemonErrorKind::Custom("bad input path".into()).into()), Err("BuildDerivation: remote error: BuildDerivation: bad input path".into()))]
    async fn build_derivation(
        #[case] drv: BasicDerivation,
        #[case] build_mode: BuildMode,
        #[case] response: DaemonResult<BuildResult>,
        #[case] expected: Result<BuildResult, String>,
    ) {
        let mock = MockStore::builder()
            .build_derivation(&drv, build_mode, response)
            .build()
            .build();
        run_store_test(mock, |mut client| async move {
            assert_eq!(
                expected,
                client
                    .build_derivation(&drv, build_mode)
                    .await
                    .map_err(|err| err.to_string())
            );
            Ok(client) as DaemonResult<_>
        })
        .await
        .unwrap();
    }

    // QueryMissing
    #[traced_test]
    #[tokio::test]
    #[rstest]
    #[case::substitute(&["00000000000000000000000000000000-_"][..],
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
    #[case::error(&["00000000000000000000000000000000-_"][..], Err(DaemonErrorKind::Custom("bad input path".into()).into()), Err("QueryMissing: remote error: QueryMissing: bad input path".into()))]
    async fn query_missing(
        #[case] paths: &[&str],
        #[case] response: DaemonResult<QueryMissingResult>,
        #[case] expected: Result<QueryMissingResult, String>,
    ) {
        let paths: Vec<DerivedPath> = paths.iter().map(|p| p.parse().unwrap()).collect();
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
        #[case] info: ValidPathInfo,
        #[case] repair: bool,
        #[case] dont_check_sigs: bool,
        #[case] events: Vec<NAREvent>,
        #[case] response: DaemonResult<()>,
        #[case] expected: Result<(), String>,
    ) {
        let mut buf = BytesMut::new();
        for event in events.iter() {
            let encoded = event.encoded_size();
            buf.reserve(encoded);
            let mut temp = buf.split_off(buf.len());
            event.encode_into(&mut temp);
            buf.unsplit(temp);
        }
        let content = buf.freeze();

        let mock = MockStore::builder()
            .add_to_store_nar(&info, repair, dont_check_sigs, content.clone(), response)
            .build()
            .build();
        run_store_test(mock, |mut client| async move {
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
        #[case] repair: bool,
        #[case] dont_check_sigs: bool,
        #[case] infos: Vec<(ValidPathInfo, Vec<NAREvent>)>,
        #[case] response: DaemonResult<()>,
        #[case] expected: Result<(), String>,
    ) {
        let infos_content: Vec<_> = infos
            .iter()
            .map(|(info, events)| {
                let mut buf = BytesMut::new();
                for event in events.iter() {
                    let encoded = event.encoded_size();
                    buf.reserve(encoded);
                    let mut temp = buf.split_off(buf.len());
                    event.encode_into(&mut temp);
                    buf.unsplit(temp);
                }
                (info.clone(), buf.freeze())
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

    use futures::TryStreamExt as _;
    use nixrs_archive::proptest::arb_nar_contents;
    use proptest::prelude::{any, TestCaseError};
    use proptest::{prop_assert, prop_assert_eq, proptest};
    use tokio::io::copy_buf;
    use tracing::info;
    use tracing_test::traced_test;

    use super::mock::MockStore;
    use super::tests::run_store_test;
    use super::DaemonResult;
    use super::{ClientOptions, UnkeyedValidPathInfo};
    use crate::archive::NAREvent;
    use crate::daemon::wire::types2::ValidPathInfo;
    use crate::daemon::DaemonStore as _;
    use crate::hash::{digest, Algorithm};
    use crate::store_path::StorePath;
    use crate::store_path::StorePathSet;

    // TODO: proptest handshake

    proptest! {
        #[traced_test]
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
                Ok(()) as Result<_, TestCaseError>
            })?;
            info!("Completed test {}", now.elapsed().as_secs_f64());
        }
    }

    proptest! {
        #[traced_test]
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
                Ok(()) as Result<_, TestCaseError>
            })?;
            info!("Completed test {}", now.elapsed().as_secs_f64());
        }
    }

    proptest! {
        #[traced_test]
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
                Ok(()) as Result<_, TestCaseError>
            })?;
            info!("Completed test {}", now.elapsed().as_secs_f64());
        }
    }

    proptest! {
        #[traced_test]
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
                Ok(()) as Result<_, TestCaseError>
            })?;
            info!("Completed test {}", now.elapsed().as_secs_f64());
        }
    }

    proptest! {
        #[traced_test]
        #[test]
        fn proptest_nar_from_path(
            path in any::<StorePath>(),
            (nar_size, nar_hash, nar_content) in arb_nar_contents(20, 20, 5),
        )
        {
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
                        let _ : Vec<NAREvent> = crate::archive::parse_nar(Cursor::new(&out)).try_collect().await?;
                        prop_assert_eq!(nar_size, out.len() as u64);
                        let hash = digest(Algorithm::SHA256, &out);
                        prop_assert_eq!(hash.as_ref(), nar_hash.as_ref());
                    }
                    Ok(client)
                }).await?;
                Ok(()) as Result<_, TestCaseError>
            })?;
            info!("Completed test {}", now.elapsed().as_secs_f64());
        }
    }

    proptest! {
        #[traced_test]
        #[test]
        fn proptest_add_to_store_nar(
            info in any::<ValidPathInfo>(),
            repair in any::<bool>(),
            dont_check_sigs in any::<bool>(),
            (_nar_size, _nar_hash, nar_content) in arb_nar_contents(20, 20, 5),
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
                Ok(()) as Result<_, TestCaseError>
            })?;
            info!("Completed test {}", now.elapsed().as_secs_f64());
        }
    }
    // TODO: proptest all messages

    /*
    proptest! {
        #[traced_test]
        #[test]
        fn proptest_operations(
            ops in any::<Vec<LogOperation>>(),
        )
        {
            let now = Instant::now();
            let r = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            r.block_on(async {
                let mut mock = MockStore::builder();
                for op in ops.iter() {
                    mock.add_operation(op.clone());
                }
                let mock = mock.build();
                run_store_test(mock, |mut client| async move {
                    let mut out = Vec::new();
                    client.nar_from_path(&path, Cursor::new(&mut out)).result().await?;
                    let _ : Vec<NAREvent> = crate::archive::parse_nar(Cursor::new(&out)).try_collect().await?;
                    prop_assert_eq!(nar_size, out.len() as u64);
                    let hash = digest(Algorithm::SHA256, &out);
                    prop_assert_eq!(hash.as_ref(), nar_hash.as_ref());
                    Ok(client)
                }).await?;
                Ok(()) as Result<_, TestCaseError>
            })?;
            info!("Completed test {}", now.elapsed().as_secs_f64());
        }
    }
     */
}
