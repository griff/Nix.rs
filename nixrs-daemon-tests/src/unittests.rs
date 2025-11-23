use std::collections::{BTreeMap, BTreeSet};
use std::io::Cursor;
use std::ops::Deref;
use std::pin::pin;

use bytes::Bytes;
use futures::StreamExt as _;
use futures::stream::iter;
use nixrs::daemon::wire::types::Operation;
use nixrs::daemon::{
    AddToStoreItem, BuildMode, BuildResult, BuildStatus, DaemonError, DaemonResult,
    DaemonStore as _, DaemonString, QueryMissingResult, UnkeyedValidPathInfo, ValidPathInfo,
};
use nixrs::derivation::{BasicDerivation, DerivationOutput};
use nixrs::derived_path::{DerivedPath, OutputName};
use nixrs::hash::NarHash;
use nixrs::log::{Activity, ActivityResult, LogMessage};
use nixrs::store_path::{StoreDir, StorePath, StorePathSet};
use nixrs::test::archive::{read_nar, test_data, write_nar};
use nixrs::{ByteString, btree_set};
use pretty_assertions::assert_eq;
use rstest::rstest;
use tokio::io::copy_buf;

use crate::assert_result;
use crate::{ENV_NIX_IMPL, NixImpl as _, prepare_mock, process_logs, run_store_test};

#[test_log::test(tokio::test)]
#[should_panic(
    expected = "store dropped with LogOperation { operation: IsValidPath(StorePath(00000000000000000000000000000000-_), Ok(true)), logs: [] } operation still unread"
)]
async fn check_unread_fails() {
    let nix = ENV_NIX_IMPL.deref();
    if nix.is_skipped("unittests::check_unread_fails") {
        panic!(
            "store dropped with LogOperation {{ operation: IsValidPath(StorePath(00000000000000000000000000000000-_), Ok(true)), logs: [] }} operation still unread"
        );
    }
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

#[test_log::test(tokio::test)]
#[rstest]
#[case::message(vec![LogMessage::message("Hello")])]
#[case::message_cr(vec![LogMessage::message("Hello\r")])]
#[case::message_lncr(vec![LogMessage::message("Hello\n\r")])]
#[case::message_crln(vec![LogMessage::message("Hello\r\n")])]
#[case::messaage_lines(vec![LogMessage::message("Lines\n  More\n   ")])]
#[case::start_activity(vec![LogMessage::StartActivity(Activity {
    id: 666,
    level: nixrs::log::Verbosity::Chatty,
    text: "Hello World".into(),
    activity_type: nixrs::log::ActivityType::OptimiseStore,
    fields: vec![nixrs::log::Field::Int(44), nixrs::log::Field::String("More path".into())],
    parent: 555,
})])]
#[case::result(vec![LogMessage::Result(ActivityResult {
    id: 666,
    result_type: nixrs::log::ResultType::CorruptedPath,
    fields: vec![nixrs::log::Field::Int(44), nixrs::log::Field::String("More path".into())],
})])]
#[case::multiple(vec![LogMessage::message("Hello"), LogMessage::message("World")])]
async fn op_logs(#[case] mut logs: Vec<LogMessage>) {
    let nix = ENV_NIX_IMPL.deref();
    if nix.is_skipped("unittests::op_logs") {
        return;
    }

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
                logs.into_iter()
                    .map(|log| nix.collect_log(log))
                    .collect::<Vec<_>>()
            );
            r.await?;
        }
        Ok(client) as DaemonResult<_>
    })
    .await
    .unwrap();
}

#[test_log::test(tokio::test)]
#[rstest]
#[case::single(vec![LogMessage::message("Hello")])]
#[case::empty(vec![LogMessage::message("")])]
#[case::whitespace(vec![LogMessage::message("Lines\n  More\n   ")])]
#[case::multiple(vec![LogMessage::message("Hello"), LogMessage::message("World")])]
async fn handshake_logs(#[case] logs: Vec<LogMessage>) {
    let nix = ENV_NIX_IMPL.deref();
    if nix.is_skipped("unittests::handshake_logs") {
        return;
    }

    let mut mock = prepare_mock(nix);
    for log in logs.iter() {
        mock.add_handshake_log(log.clone());
    }
    let version = nix.protocol_range().max();
    run_store_test(nix, version, mock, |client, actual_logs| async move {
        assert_eq!(
            actual_logs,
            logs.into_iter()
                .map(|log| nix.collect_log(log))
                .collect::<Vec<_>>()
        );
        Ok(client) as DaemonResult<_>
    })
    .await
    .unwrap();
}

/*
#[test_log::test(tokio::test)]
#[rstest]
#[case(ClientOptions::default(), Ok(()), Ok(()))]
#[case(ClientOptions::default(), Err(DaemonError::custom("bad input path")), Err("remote error: bad input path".into()))]
async fn set_options(
    #[case] options: ClientOptions,
    #[case] response: DaemonResult<()>,
    #[case] expected: Result<(), String>,
) {
    let binding = ENV_NIX_IMPL;
    let nix = binding.deref();

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

#[test_log::test(tokio::test)]
#[rstest]
#[case::valid("00000000000000000000000000000000-_", Ok(true), Ok(true))]
#[case::invalid("00000000000000000000000000000000-_", Ok(false), Ok(false))]
#[case::error("00000000000000000000000000000000-_", Err(DaemonError::custom("bad input path")), Err("IsValidPath: bad input path".into()))]
async fn is_valid_path(
    #[case] store_path: StorePath,
    #[case] response: DaemonResult<bool>,
    #[case] expected: Result<bool, String>,
) {
    let nix = ENV_NIX_IMPL.deref();
    if nix
        .range
        .intersect(&Operation::IsValidPath.versions())
        .is_none()
        || nix.is_skipped("unittests::is_valid_path")
    {
        return;
    }
    let mut mock = prepare_mock(nix);
    mock.is_valid_path(&store_path, response).build();
    let version = nix.protocol_range().max();
    run_store_test(nix, version, mock, |mut client, _| async move {
        assert_result!(
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
//#[case::substitute_all_valid(&["00000000000000000000000000000000-_"][..], true, Ok(&["10000000000000000000000000000000-_"][..]), Ok(&["10000000000000000000000000000000-_"][..]))]
//#[case::substilute_empty_return(&["00000000000000000000000000000000-_"][..], true, Ok(&[][..]), Ok(&[][..]))]
#[case::all_valid(&["00000000000000000000000000000000-_"][..], false, Ok(&["10000000000000000000000000000000-_"][..]), Ok(&["10000000000000000000000000000000-_"][..]))]
#[case::empty_return(&["00000000000000000000000000000000-_"][..], false, Ok(&[][..]), Ok(&[][..]))]
//#[case::substitute_error(&["00000000000000000000000000000000-_"][..], true, Err(DaemonError::custom("bad input path")), Err("QueryValidPaths: remote error: QueryValidPaths: bad input path".into()))]
#[case::error(&["00000000000000000000000000000000-_"][..], false, Err(DaemonError::custom("bad input path")), Err("QueryValidPaths: remote error: QueryValidPaths: bad input path".into()))]
async fn query_valid_paths(
    #[case] store_paths: &[&str],
    #[case] substitute: bool,
    #[case] response: DaemonResult<&[&str]>,
    #[case] expected: Result<&[&str], String>,
) {
    let nix = ENV_NIX_IMPL.deref();
    if nix
        .range
        .intersect(&Operation::QueryValidPaths.versions())
        .is_none()
        || nix.is_skipped("unittests::query_valid_paths")
    {
        return;
    }
    let store_paths = store_paths.iter().map(|p| p.parse().unwrap()).collect();
    let response = response.map(|r| r.iter().map(|p| p.parse().unwrap()).collect());
    let expected: Result<StorePathSet, String> =
        expected.map(|r| r.iter().map(|p| p.parse().unwrap()).collect());
    let mut mock = prepare_mock(nix);
    mock.query_valid_paths(&store_paths, substitute, response)
        .build();
    let version = nix.protocol_range().max();
    run_store_test(nix, version, mock, |mut client, _| async move {
        assert_result!(
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
    #[case] store_path: StorePath,
    #[case] response: DaemonResult<Option<UnkeyedValidPathInfo>>,
    #[case] expected: Result<Option<UnkeyedValidPathInfo>, String>,
) {
    let nix = ENV_NIX_IMPL.deref();
    if nix
        .range
        .intersect(&Operation::QueryPathInfo.versions())
        .is_none()
        || nix.is_skipped("unittests::query_path_info")
    {
        return;
    }
    let mut mock = prepare_mock(nix);
    mock.query_path_info(&store_path, response).build();
    let version = nix.protocol_range().max();
    run_store_test(nix, version, mock, |mut client, _| async move {
        assert_result!(
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
async fn nar_from_path(#[case] store_path: StorePath, #[case] events: test_data::TestNarEvents) {
    let nix = ENV_NIX_IMPL.deref();
    if nix
        .range
        .intersect(&Operation::NarFromPath.versions())
        .is_none()
        || nix.is_skipped("unittests::nar_from_path")
    {
        return;
    }
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
#[test_log::test(tokio::test)]
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
    #[case] paths: &[&str],
    #[case] mode: BuildMode,
    #[case] response: DaemonResult<()>,
    #[case] expected: Result<(), String>,
) {
    let nix = ENV_NIX_IMPL.deref();
    if nix
        .range
        .intersect(&Operation::BuildPaths.versions())
        .is_none()
        || nix.is_skipped("unittests::build_paths")
    {
        return;
    }
    let store_dir = StoreDir::default();
    let paths: Vec<DerivedPath> = paths.iter().map(|p| store_dir.parse(p).unwrap()).collect();
    let mut mock = prepare_mock(nix);
    mock.build_paths(&paths, mode, response).build();
    let version = nix.protocol_range().max();
    run_store_test(nix, version, mock, |mut client, _| async move {
        assert_result!(
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
    #[case] drv: BasicDerivation,
    #[case] mode: BuildMode,
    #[case] response: DaemonResult<BuildResult>,
    #[case] mut expected: Result<BuildResult, String>,
) {
    let nix = ENV_NIX_IMPL.deref();
    if nix
        .range
        .intersect(&Operation::BuildDerivation.versions())
        .is_none()
        || nix.is_skipped("unittests::build_derivation")
    {
        return;
    }
    let mut mock = prepare_mock(nix);
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
        assert_result!(expected, actual);
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
    let nix = ENV_NIX_IMPL.deref();
    if nix
        .range
        .intersect(&Operation::QueryMissing.versions())
        .is_none()
        || nix.is_skipped("unittests::query_missing")
    {
        return;
    }
    let store_dir = StoreDir::default();
    let paths: Vec<DerivedPath> = paths.iter().map(|p| store_dir.parse(p).unwrap()).collect();
    let mut mock = prepare_mock(nix);
    mock.query_missing(&paths, response).build();
    let version = nix.protocol_range().max();
    run_store_test(nix, version, mock, |mut client, _| async move {
        assert_result!(
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
    #[case] info: ValidPathInfo,
    #[case] repair: bool,
    #[case] dont_check_sigs: bool,
    #[case] events: test_data::TestNarEvents,
    #[case] response: DaemonResult<()>,
    #[case] expected: Result<(), String>,
) {
    let nix = ENV_NIX_IMPL.deref();
    if nix
        .range
        .intersect(&Operation::AddToStoreNar.versions())
        .is_none()
        || nix.is_skipped("unittests::add_to_store_nar")
    {
        return;
    }
    let content = write_nar(events.iter());
    let mut mock = prepare_mock(nix);
    mock.add_to_store_nar(&info, repair, dont_check_sigs, content.clone(), response)
        .build();
    let version = nix.protocol_range().max();
    run_store_test(nix, version, mock, |mut client, _| async move {
        assert_result!(
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
#[case::ok(
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
#[case::empty(true, true, vec![], Ok(()), Ok(()))]
#[case::error(
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
    let nix = ENV_NIX_IMPL.deref();
    if nix
        .range
        .intersect(&Operation::AddMultipleToStore.versions())
        .is_none()
        || nix.is_skipped("unittests::add_multiple_to_store")
    {
        return;
    }
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

    let mut mock = prepare_mock(nix);
    mock.add_multiple_to_store(repair, dont_check_sigs, infos_content, response)
        .build();
    let version = nix.protocol_range().max();
    run_store_test(nix, version, mock, |mut client, _| async move {
        assert_result!(
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
    let nix = ENV_NIX_IMPL.deref();
    if nix
        .range
        .intersect(&Operation::AddToStoreNar.versions())
        .is_none()
        || nix.is_skipped("unittests::sesennst")
    {
        return;
    }
    let handshake_logs = vec![LogMessage::message(Bytes::new())];
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
        .map(|log| nix.collect_log(log))
        .collect::<Vec<_>>();
    let mut mock = prepare_mock(nix);
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
