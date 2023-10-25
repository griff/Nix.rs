use std::fmt;
use std::time::SystemTime;

use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tracing::{debug, trace};

use crate::hash::{Algorithm, Hash};
use crate::io::{AsyncSink, AsyncSource};
use crate::signature::{ParseSignatureError, SignatureSet};
use crate::store::Error;
use crate::store_path::{
    ContentAddress, ContentAddressMethod, ContentAddressWithReferences, FixedOutputInfo, StoreDir,
    StorePath, StorePathSet, StorePathSetExt, StoreReferences, TextInfo,
};
use crate::StringSet;

#[derive(Debug, Eq, PartialOrd, Ord, Clone)]
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

    pub fn content_address_with_references(&self) -> Option<ContentAddressWithReferences> {
        if let Some(ca) = self.ca.as_ref() {
            match ca.method {
                ContentAddressMethod::Text => {
                    assert!(!self.references.contains(&self.path));
                    Some(ContentAddressWithReferences::Text(TextInfo {
                        hash: ca.hash,
                        references: self.references.clone(),
                    }))
                }
                ContentAddressMethod::Fixed(method) => {
                    let mut others = self.references.clone();
                    let self_ref = others.remove(&self.path);
                    Some(ContentAddressWithReferences::Fixed(FixedOutputInfo {
                        method,
                        hash: ca.hash,
                        references: StoreReferences { others, self_ref },
                    }))
                }
            }
        } else {
            None
        }
    }

    pub async fn read<R: AsyncRead + Unpin>(
        mut source: R,
        store_dir: &StoreDir,
        format: u64,
    ) -> Result<ValidPathInfo, Error> {
        let path: StorePath = source.read_parsed(store_dir).await?;
        debug!(%path, "Path is {}", path);
        Self::read_path(source, store_dir, format, path).await
    }

    pub async fn read_path<R: AsyncRead + Unpin>(
        mut source: R,
        store_dir: &StoreDir,
        format: u64,
        path: StorePath,
    ) -> Result<ValidPathInfo, Error> {
        let deriver = source.read_string().await?;
        debug!(deriver, "Deriver is {}", deriver);
        let deriver = if !deriver.is_empty() {
            Some(store_dir.parse_path(&deriver)?)
        } else {
            None
        };
        let hash_s = source.read_string().await?;
        debug!(hash = hash_s, "Hash is {}", hash_s);
        let nar_hash = Hash::parse_any(&hash_s, Some(Algorithm::SHA256))?;
        trace!("Reading references");
        let references: StorePathSet = source.read_parsed_coll(&store_dir).await?;
        debug!("References {}", references.join());
        let registration_time = source.read_time().await?;
        let nar_size = source.read_u64_le().await?;
        let mut ultimate = false;
        let mut sigs = SignatureSet::new();
        let mut ca = None;
        if format >= 16 {
            ultimate = source.read_bool().await?;

            let sigs_s: Vec<String> = source.read_string_coll().await?;
            sigs = sigs_s
                .iter()
                .map(|s| s.parse())
                .collect::<Result<SignatureSet, ParseSignatureError>>()?;

            let ca_s = source.read_string().await?;
            debug!(ca = ca_s, "CA is {}", ca_s);
            if !ca_s.is_empty() {
                ca = Some(ca_s.parse()?);
            };
        }

        Ok(ValidPathInfo {
            path,
            deriver,
            nar_size,
            nar_hash,
            references,
            sigs,
            registration_time,
            ultimate,
            ca,
        })
    }

    pub async fn write<W: AsyncWrite + Unpin>(
        &self,
        mut sink: W,
        store_dir: &StoreDir,
        format: u64,
        include_path: bool,
    ) -> Result<(), Error> {
        if include_path {
            sink.write_printed(store_dir, &self.path).await?;
        }
        if let Some(deriver) = self.deriver.as_ref() {
            sink.write_printed(store_dir, deriver).await?;
        } else {
            sink.write_str("").await?;
        }
        sink.write_string(self.nar_hash.encode_base16()).await?;
        sink.write_printed_coll(store_dir, &self.references).await?;
        sink.write_time(self.registration_time).await?;
        sink.write_u64_le(self.nar_size).await?;
        if format >= 16 {
            sink.write_bool(self.ultimate).await?;
            let sigs: StringSet = self.sigs.iter().map(|s| s.to_string()).collect();
            sink.write_string_coll(&sigs).await?;
            if let Some(ca) = self.ca.as_ref() {
                sink.write_string(ca.to_string()).await?;
            } else {
                sink.write_str("").await?;
            }
        }
        Ok(())
    }
}

impl PartialEq for ValidPathInfo {
    fn eq(&self, other: &Self) -> bool {
        self.path == other.path
            && self.nar_hash == other.nar_hash
            && self.references == other.references
    }
}

impl std::hash::Hash for ValidPathInfo {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.path.hash(state);
        self.nar_hash.hash(state);
        self.references.hash(state);
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
