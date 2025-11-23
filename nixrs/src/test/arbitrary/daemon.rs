#[cfg(feature = "daemon")]
mod mock;
pub mod ser;

#[cfg(feature = "nixrs-derive")]
use std::collections::BTreeMap;
use std::fmt::Debug;

#[cfg(feature = "nixrs-derive")]
use bytes::Bytes;
#[cfg(feature = "nixrs-derive")]
use proptest::collection::btree_map;
use proptest::prelude::*;
#[cfg(feature = "nixrs-derive")]
use proptest::sample::SizeRange;

use crate::daemon::ProtocolVersion;
#[cfg(feature = "nixrs-derive")]
use crate::daemon::{
    BuildMode, BuildResult, BuildStatus, ClientOptions, DaemonInt, DaemonString, DaemonTime,
    Microseconds, ValidPathInfo,
};
#[cfg(feature = "nixrs-derive")]
use crate::log::Verbosity;
#[cfg(feature = "nixrs-derive")]
use crate::test::arbitrary::arb_byte_string;
#[cfg(feature = "nixrs-derive")]
use crate::test::arbitrary::archive::{arb_nar_contents, arb_nar_events};
#[cfg(feature = "nixrs-derive")]
use crate::test::arbitrary::realisation::arb_drv_outputs;
#[cfg(feature = "nixrs-derive")]
use crate::test::archive::test_data;

#[cfg(feature = "daemon")]
pub use mock::{
    MockOperationParams, arb_mock_add_multiple_to_store, arb_mock_add_to_store_nar,
    arb_mock_build_derivation, arb_mock_build_paths, arb_mock_build_paths_with_results,
    arb_mock_is_valid_path, arb_mock_nar_from_path, arb_mock_query_missing,
    arb_mock_query_path_info, arb_mock_query_valid_paths, arb_mock_set_options,
};

pub fn version_cut_off<B, A, V>(
    version: ProtocolVersion,
    cut_off: u8,
    before: B,
    after: A,
) -> BoxedStrategy<V>
where
    B: Strategy<Value = V> + 'static,
    A: Strategy<Value = V> + 'static,
{
    if version.minor() < cut_off {
        before.boxed()
    } else {
        after.boxed()
    }
}

pub fn field_after<A, V>(version: ProtocolVersion, cut_off: u8, after: A) -> BoxedStrategy<V>
where
    A: Strategy<Value = V> + 'static,
    V: Default + Clone + Debug + 'static,
{
    version_cut_off(version, cut_off, Just(V::default()), after)
}

#[cfg(feature = "nixrs-derive")]
pub fn arb_nar_item() -> impl Strategy<Value = (ValidPathInfo, test_data::TestNarEvents)> {
    (any::<ValidPathInfo>(), arb_nar_events(20, 20, 5))
}

#[cfg(feature = "nixrs-derive")]
pub fn arb_nar_contents_item() -> impl Strategy<Value = (ValidPathInfo, Bytes)> {
    (any::<ValidPathInfo>(), arb_nar_contents(20, 20, 5))
}

#[cfg(feature = "nixrs-derive")]
pub fn arb_nar_items() -> impl Strategy<Value = Vec<(ValidPathInfo, test_data::TestNarEvents)>> {
    proptest::collection::vec(arb_nar_item(), SizeRange::default())
}

#[cfg(feature = "nixrs-derive")]
pub fn arb_nar_contents_items() -> impl Strategy<Value = Vec<(ValidPathInfo, Bytes)>> {
    proptest::collection::vec(arb_nar_contents_item(), SizeRange::default())
}

#[cfg(feature = "nixrs-derive")]
impl Arbitrary for BuildMode {
    type Parameters = ();
    type Strategy = BoxedStrategy<BuildMode>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        use BuildMode::*;
        prop_oneof![
            50 => Just(Normal),
            5 => Just(Repair),
            5 => Just(Check),
        ]
        .boxed()
    }
}

#[cfg(feature = "nixrs-derive")]
impl Arbitrary for BuildStatus {
    type Parameters = ();
    type Strategy = BoxedStrategy<BuildStatus>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        use BuildStatus::*;
        prop_oneof![
            50 => Just(Built),
            5 => Just(Substituted),
            5 => Just(AlreadyValid),
            5 => Just(PermanentFailure),
            5 => Just(InputRejected),
            5 => Just(OutputRejected),
            5 => Just(TransientFailure), // possibly transient
            5 => Just(TimedOut),
            5 => Just(MiscFailure),
            5 => Just(DependencyFailed),
            5 => Just(LogLimitExceeded),
            5 => Just(NotDeterministic)
        ]
        .boxed()
    }
}

#[cfg(feature = "nixrs-derive")]
impl Arbitrary for Microseconds {
    type Parameters = ();
    type Strategy = BoxedStrategy<Microseconds>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        arb_microseconds().boxed()
    }
}

prop_compose! {
    #[cfg(feature = "nixrs-derive")]
    fn arb_microseconds()(ms in 0i64..i64::MAX) -> Microseconds {
        ms.into()
    }
}

#[cfg(feature = "nixrs-derive")]
impl Arbitrary for BuildResult {
    type Parameters = ProtocolVersion;
    type Strategy = BoxedStrategy<BuildResult>;
    fn arbitrary_with(args: Self::Parameters) -> Self::Strategy {
        arb_build_result(args).boxed()
    }
}

prop_compose! {
    #[cfg(feature = "nixrs-derive")]
    pub fn arb_build_result(version: ProtocolVersion)
    (
        status in any::<BuildStatus>(),
        error_msg in arb_byte_string(),
        times_built in field_after(version, 29, 0u32..50u32),
        is_non_deterministic in field_after(version, 29, ::proptest::bool::ANY),
        start_time in field_after(version, 29, ::proptest::num::i64::ANY),
        duration_secs in field_after(version, 29, 0i64..604_800i64),
        cpu_user in field_after(version, 37, any::<Option<Microseconds>>()),
        cpu_system in field_after(version, 37, any::<Option<Microseconds>>()),
        built_outputs in field_after(version, 28, arb_drv_outputs(0..5)),
    ) -> BuildResult
    {
        let stop_time = start_time + duration_secs;
        BuildResult {
            status, error_msg, times_built, is_non_deterministic,
            start_time, stop_time, cpu_user, cpu_system, built_outputs,
        }
    }
}

#[cfg(feature = "nixrs-derive")]
fn arb_client_settings(
    size: impl Into<SizeRange>,
) -> impl Strategy<Value = BTreeMap<String, DaemonString>> {
    let key = any::<String>();
    let value = any::<Vec<u8>>().prop_map(DaemonString::from);
    btree_map(key, value, size)
}

#[cfg(feature = "nixrs-derive")]
impl Arbitrary for ClientOptions {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        (
            any::<bool>(),
            any::<bool>(),
            any::<bool>(),
            any::<Verbosity>(),
            any::<DaemonInt>(),
            any::<DaemonTime>(),
            any::<Verbosity>(),
            any::<DaemonInt>(),
            any::<bool>(),
            arb_client_settings(..30),
        )
            .prop_map(
                |(
                    keep_failed,
                    keep_going,
                    try_fallback,
                    verbosity,
                    max_build_jobs,
                    max_silent_time,
                    verbose_build,
                    build_cores,
                    use_substitutes,
                    other_settings,
                )| {
                    ClientOptions {
                        keep_failed,
                        keep_going,
                        try_fallback,
                        verbosity,
                        max_build_jobs,
                        max_silent_time,
                        verbose_build,
                        build_cores,
                        use_substitutes,
                        other_settings,
                        ..Default::default()
                    }
                },
            )
            .boxed()
    }
}
