
use std::time::SystemTime;

use crate::StorePath;
use crate::content_address::ContentAddress;
use crate::path::StorePathSet;
use nixrs_util::{StringSet, hash::Hash};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct ValidPathInfo {
    pub path: StorePath,
    pub deriver: Option<StorePath>,
    pub nar_size: u64,
    pub nar_hash: Hash,
    pub references: StorePathSet,
    pub sigs: StringSet,
    pub registration_time: SystemTime,

    /// Whether the path is ultimately trusted, that is, it's a
    /// derivation output that was built locally. */
    pub ultimate: bool,

    /// If non-empty, an assertion that the path is content-addressed,
    ///  i.e., that the store path is computed from a cryptographic hash
    /// of the contents of the path, plus some other bits of data like
    /// the "name" part of the path. Such a path doesn't need
    /// signatures, since we don't have to trust anybody's claim that
    /// the path is the output of a particular derivation. (In the
    /// extensional store model, we have to trust that the *contents*
    /// of an output path of a derivation were actually produced by
    /// that derivation. In the intensional model, we have to trust
    /// that a particular output path was produced by a derivation; the
    /// path then implies the contents.)
    ///
    /// Ideally, the content-addressability assertion would just be a Boolean,
    /// and the store path would be computed from the name component, ‘narHash’
    /// and ‘references’. However, we support many types of content addresses.
    pub ca: Option<ContentAddress>,
}

#[cfg(any(test, feature="test"))]
pub mod proptest {
    use bytes::Bytes;
    use ::proptest::prelude::*;
    use super::*;
    use nixrs_util::archive::proptest::arb_nar_contents;
    use nixrs_util::proptest::arb_system_time;

    prop_compose! {
        pub fn arb_valid_info_and_content(
            depth: u32,
            desired_size: u32,
            expected_branch_size: u32,
        )(
            (nar_size, nar_hash, contents) in arb_nar_contents(depth, desired_size, expected_branch_size),
            path in any::<StorePath>(),
            deriver in any::<Option<StorePath>>(),
            references in any::<StorePathSet>(),
            sigs in any::<StringSet>(),
            registration_time in arb_system_time(),
            ultimate in ::proptest::bool::ANY
        ) -> (ValidPathInfo, Bytes)
        {
            (ValidPathInfo {
                nar_size, nar_hash, path, deriver, references, sigs, registration_time, ultimate,
                ca: None,
             }, contents)
        }
    }
}