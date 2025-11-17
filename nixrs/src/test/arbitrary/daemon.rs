use std::fmt::Debug;

#[cfg(feature = "nixrs-derive")]
use crate::daemon::wire::types2::ValidPathInfo;
#[cfg(feature = "nixrs-derive")]
use bytes::Bytes;
use proptest::prelude::*;
#[cfg(feature = "nixrs-derive")]
use proptest::sample::SizeRange;

use crate::daemon::ProtocolVersion;
#[cfg(feature = "nixrs-derive")]
use crate::test::arbitrary::archive::{arb_nar_contents, arb_nar_events};
#[cfg(feature = "nixrs-derive")]
use crate::test::archive::test_data;

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
