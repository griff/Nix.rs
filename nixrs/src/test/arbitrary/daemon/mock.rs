use std::collections::VecDeque;
use std::ops::RangeBounds;

use proptest::prelude::*;

use crate::daemon::wire::types::{
    AddMultipleToStoreRequest, AddToStoreNarRequest, BuildDerivationRequest, BuildPathsRequest,
    Operation, QueryValidPathsRequest,
};
use crate::daemon::{
    BuildMode, BuildResult, ClientOptions, KeyedBuildResult, ProtocolVersion, QueryMissingResult,
    UnkeyedValidPathInfo, ValidPathInfo,
};
use crate::derivation::BasicDerivation;
use crate::derived_path::DerivedPath;
use crate::log::LogMessage;
use crate::store_path::{StorePath, StorePathSet};
use crate::test::arbitrary::archive::arb_nar_contents;
use crate::test::arbitrary::daemon::{arb_nar_contents_items, field_after};
use crate::test::arbitrary::helpers::Union;
use crate::test::daemon::{LogOperation, MockOperation};

prop_compose! {
    pub fn arb_mock_set_options()(options in any::<ClientOptions>()) -> MockOperation {
        MockOperation::SetOptions(options, Ok(()))
    }
}
prop_compose! {
    pub fn arb_mock_is_valid_path()(
        path in any::<StorePath>(),
        result in proptest::bool::ANY) -> MockOperation {
        MockOperation::IsValidPath(path, Ok(result))
    }
}

prop_compose! {
    pub fn arb_mock_query_valid_paths(version: ProtocolVersion)(
        paths in any::<StorePathSet>(),
        substitute in field_after(version, 27, proptest::bool::ANY),
        result in any::<StorePathSet>()) -> MockOperation {
        MockOperation::QueryValidPaths(QueryValidPathsRequest {
            paths, substitute
        }, Ok(result))
    }
}

prop_compose! {
    pub fn arb_mock_query_path_info()(
        path in any::<StorePath>(),
        result in any::<Option<UnkeyedValidPathInfo>>()) -> MockOperation {
        MockOperation::QueryPathInfo(path, Ok(result))
    }
}
prop_compose! {
    pub fn arb_mock_nar_from_path()(
        path in any::<StorePath>(),
        result in arb_nar_contents(20, 20, 3)) -> MockOperation {
        MockOperation::NarFromPath(path, Ok(result))
    }
}
prop_compose! {
    pub fn arb_mock_build_paths()(
        paths in any::<Vec<DerivedPath>>(),
        mode in any::<BuildMode>()) -> MockOperation {
        MockOperation::BuildPaths(BuildPathsRequest { paths, mode }, Ok(()))
    }
}
prop_compose! {
    pub fn arb_mock_build_paths_with_results(version: ProtocolVersion)(
        results in any_with::<Vec<KeyedBuildResult>>((Default::default(), version)),
        mode in any::<BuildMode>()) -> MockOperation {
        let paths = results.iter().map(|r| r.path.clone()).collect();
        MockOperation::BuildPathsWithResults(BuildPathsRequest { paths, mode }, Ok(results))
    }
}

prop_compose! {
    pub fn arb_mock_build_derivation(version: ProtocolVersion)(
        drv in any::<BasicDerivation>(),
        mode in any::<BuildMode>(),
        result in any_with::<BuildResult>(version)) -> MockOperation {
        MockOperation::BuildDerivation(BuildDerivationRequest { drv, mode }, Ok(result))
    }
}
prop_compose! {
    pub fn arb_mock_query_missing()(
        paths in any::<Vec<DerivedPath>>(),
        result in any::<QueryMissingResult>()) -> MockOperation {
        MockOperation::QueryMissing(paths, Ok(result))
    }
}
prop_compose! {
    pub fn arb_mock_add_to_store_nar()(
        path_info in any::<ValidPathInfo>(),
        repair in proptest::bool::ANY,
        dont_check_sigs in proptest::bool::ANY,
        content in arb_nar_contents(20, 20, 3)) -> MockOperation {
        MockOperation::AddToStoreNar(AddToStoreNarRequest {
            path_info, repair, dont_check_sigs
        }, content, Ok(()))
    }
}
prop_compose! {
    pub fn arb_mock_add_multiple_to_store()(
        repair in proptest::bool::ANY,
        dont_check_sigs in proptest::bool::ANY,
        infos in arb_nar_contents_items()) -> MockOperation {
        MockOperation::AddMultipleToStore(AddMultipleToStoreRequest {
            repair, dont_check_sigs
        }, infos, Ok(()))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct MockOperationParams {
    pub version: ProtocolVersion,
    pub allow_options: bool,
}

impl Default for MockOperationParams {
    fn default() -> Self {
        Self {
            version: Default::default(),
            allow_options: true,
        }
    }
}

impl Arbitrary for MockOperation {
    type Parameters = MockOperationParams;
    type Strategy = Union<BoxedStrategy<Self>>;

    fn arbitrary_with(args: Self::Parameters) -> Self::Strategy {
        let mut ret = Union::new([
            arb_mock_is_valid_path().boxed(),
            arb_mock_query_valid_paths(args.version).boxed(),
            arb_mock_query_path_info().boxed(),
            arb_mock_nar_from_path().boxed(),
            arb_mock_build_paths().boxed(),
            arb_mock_build_derivation(args.version).boxed(),
            arb_mock_add_to_store_nar().boxed(),
        ]);
        if args.allow_options {
            ret = ret.or(arb_mock_set_options().boxed());
        }
        if Operation::BuildPathsWithResults
            .versions()
            .contains(&args.version)
        {
            ret = ret.or(arb_mock_build_paths_with_results(args.version).boxed());
        }
        if Operation::AddMultipleToStore
            .versions()
            .contains(&args.version)
        {
            ret = ret.or(arb_mock_add_multiple_to_store().boxed());
        }
        ret
    }
}

impl Arbitrary for LogOperation {
    type Parameters = MockOperationParams;
    type Strategy = BoxedStrategy<Self>;
    fn arbitrary_with(args: Self::Parameters) -> Self::Strategy {
        (
            any_with::<MockOperation>(args),
            any::<VecDeque<LogMessage>>(),
        )
            .prop_map(|(operation, logs)| LogOperation { operation, logs })
            .boxed()
    }
}
