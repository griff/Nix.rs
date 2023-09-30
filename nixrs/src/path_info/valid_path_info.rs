use std::fmt;
use std::time::SystemTime;

use thiserror::Error;

use crate::hash::Hash;
use crate::signature::SignatureSet;
use crate::store_path::{ContentAddress, StoreDir, StorePath, StorePathSet};

#[derive(Debug, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct ValidPathInfo {
    pub path: StorePath,
    pub deriver: Option<StorePath>,
    pub nar_size: u64,
    pub nar_hash: Hash,
    pub references: StorePathSet,
    pub sigs: SignatureSet,
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

struct FingerprintDisplay<'a> {
    info: &'a ValidPathInfo,
    store: &'a StoreDir,
}

impl<'a> fmt::Display for FingerprintDisplay<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "1;{};{};{};",
            self.store.display_path(&self.info.path),
            self.info.nar_hash.to_base32(),
            self.info.nar_size
        )?;

        let mut first = true;
        for r in &self.info.references {
            if first {
                first = false
            } else {
                write!(f, ",")?;
            }
            write!(f, "{}", self.store.display_path(r))?;
        }
        Ok(())
    }
}

#[derive(Error, Debug)]
#[error("cannot calculate fingerprint of path '{0}' because its size is not known")]
pub struct InvalidPathInfo(String);

impl ValidPathInfo {
    pub fn new(path: StorePath, nar_hash: Hash) -> ValidPathInfo {
        ValidPathInfo {
            path,
            nar_hash,
            deriver: None,
            nar_size: 0,
            references: StorePathSet::new(),
            sigs: SignatureSet::new(),
            registration_time: SystemTime::UNIX_EPOCH,
            ultimate: false,
            ca: None,
        }
    }

    pub fn fingerprint<'a>(
        &'a self,
        store: &'a StoreDir,
    ) -> Result<impl fmt::Display + 'a, InvalidPathInfo> {
        if self.nar_size == 0 {
            Err(InvalidPathInfo(store.display_path(&self.path).to_string()))
        } else {
            Ok(FingerprintDisplay { store, info: self })
        }
    }
}

impl PartialEq for ValidPathInfo {
    fn eq(&self, other: &Self) -> bool {
        self.path == other.path
            && self.nar_hash == other.nar_hash
            && self.references == other.references
    }
}

#[cfg(any(test, feature = "test"))]
pub mod proptest {
    use super::*;
    use crate::archive::proptest::arb_nar_contents;
    use crate::proptest::arb_system_time;
    use ::proptest::prelude::*;
    use bytes::Bytes;

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
            sigs in any::<SignatureSet>(),
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
