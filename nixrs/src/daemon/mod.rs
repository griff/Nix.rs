use std::fmt;

#[cfg(feature = "nixrs-derive")]
use nixrs_derive::{NixDeserialize, NixSerialize};

#[cfg(feature = "nixrs-derive")]
pub mod client;
pub mod de;
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
pub use logger::{LogError, LoggerResult, TraceLine, Verbosity};
#[cfg(feature = "nixrs-derive")]
pub use types::{
    ClientOptions, DaemonError, DaemonInt, DaemonPath, DaemonResult, DaemonStore, DaemonString,
    DaemonTime, HandshakeDaemonStore, RemoteError, TrustLevel, UnkeyedValidPathInfo,
};

pub(crate) const ZEROS: [u8; 8] = [0u8; 8];

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
        ProtocolVersion::from_parts((value & 0xff00 >> 8) as u8, (value & 0x00ff) as u8)
    }
}

impl From<(u8, u8)> for ProtocolVersion {
    fn from((major, minor): (u8, u8)) -> Self {
        ProtocolVersion::from_parts(major, minor)
    }
}

impl From<ProtocolVersion> for u16 {
    fn from(value: ProtocolVersion) -> Self {
        (value.major() as u16) << 8 | (value.minor() as u16)
    }
}

impl fmt::Display for ProtocolVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.major(), self.minor())
    }
}

#[cfg(test)]
mod test {
    use std::future::{ready, Future};
    use std::io::Cursor;
    use std::time::Instant;

    use bytes::BytesMut;
    use futures::{TryFutureExt as _, TryStreamExt as _};
    use nixrs_archive::proptest::arb_nar_contents;
    use pretty_assertions::assert_eq;
    use proptest::prelude::{any, TestCaseError};
    use proptest::{prop_assert, prop_assert_eq, proptest};
    use rstest::rstest;
    use tokio::io::{duplex, split, DuplexStream, ReadHalf, WriteHalf};
    use tokio::try_join;

    use super::{
        client::DaemonClient,
        mock::{MockReporter, MockStore},
        DaemonResult, DaemonStore,
    };
    use super::{ClientOptions, UnkeyedValidPathInfo};
    use crate::archive::test_data;
    use crate::archive::NAREvent;
    use crate::daemon::DaemonError;
    use crate::daemon::{logger::LoggerResult as _, server};
    use crate::hash::{digest, Algorithm, NarHash};
    use crate::store_path::StorePath;
    use crate::store_path::StorePathSet;

    async fn run_store_test<R, T, F, E>(mock: MockStore<R>, test: T) -> Result<(), E>
    where
        R: MockReporter,
        T: FnOnce(DaemonClient<ReadHalf<DuplexStream>, WriteHalf<DuplexStream>>) -> F,
        F: Future<
            Output = Result<DaemonClient<ReadHalf<DuplexStream>, WriteHalf<DuplexStream>>, E>,
        >,
        E: From<DaemonError>,
    {
        let (client_s, server_s) = duplex(10_000);
        let b = server::Builder::new();
        let server = b.serve_connection(server_s, mock).map_err(From::from);
        let (client_reader, client_writer) = split(client_s);
        let client = async move {
            let logs = DaemonClient::builder().connect(client_reader, client_writer);
            let client = logs.result().await?;
            let mut client = (test)(client).await?;
            client.close().await?;
            Ok(())
        };
        try_join!(client, server).map(|_| ())
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
            let mut out = Vec::new();
            client
                .nar_from_path(&store_path, Cursor::new(&mut out))
                .result()
                .await
                .unwrap();
            let nar: Vec<NAREvent> = crate::archive::parse_nar(Cursor::new(&out))
                .try_collect()
                .await?;
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
                    let res = client.query_path_info(&path).result().await;
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
            eprintln!("Completed test {}", now.elapsed().as_secs_f64());
        }
    }
    // TODO: proptest all messages

    /*
    proptest! {
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
            eprintln!("Completed test {}", now.elapsed().as_secs_f64());
        }
    }
     */
}
