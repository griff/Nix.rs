use derive_more::Display;
#[cfg(feature = "nixrs-derive")]
use nixrs_derive::{NixDeserialize, NixSerialize};
use num_enum::{IntoPrimitive, TryFromPrimitive};

#[cfg(feature = "nixrs-derive")]
use crate::daemon::de::{NixDeserialize, NixRead};
#[cfg(feature = "nixrs-derive")]
use crate::daemon::ser::{NixSerialize, NixWrite};
#[cfg(feature = "nixrs-derive")]
use crate::daemon::types::ContentAddress;
#[cfg(feature = "nixrs-derive")]
use crate::store_path::StorePath;

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    TryFromPrimitive,
    IntoPrimitive,
    Display,
)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
#[cfg_attr(feature = "nixrs-derive", nix(try_from = "u64", into = "u64"))]
#[repr(u64)]
pub enum Operation {
    IsValidPath = 1,
    QueryReferrers = 6,
    AddToStore = 7,
    BuildPaths = 9,
    EnsurePath = 10,
    AddTempRoot = 11,
    AddIndirectRoot = 12,
    FindRoots = 14,
    SetOptions = 19,
    CollectGarbage = 20,
    QueryAllValidPaths = 23,
    QueryPathInfo = 26,
    QueryPathFromHashPart = 29,
    QueryValidPaths = 31,
    QuerySubstitutablePaths = 32,
    QueryValidDerivers = 33,
    OptimiseStore = 34,
    VerifyStore = 35,
    BuildDerivation = 36,
    AddSignatures = 37,
    NarFromPath = 38,
    AddToStoreNar = 39,
    QueryMissing = 40,
    QueryDerivationOutputMap = 41,
    RegisterDrvOutput = 42,
    QueryRealisation = 43,
    AddMultipleToStore = 44,
    AddBuildLog = 45,
    BuildPathsWithResults = 46,
    AddPermRoot = 47,

    /// Obsolete Nix 2.5.0 Protocol 1.32
    SyncWithGC = 13,
    /// Obsolete Nix 2.4 Protocol 1.25
    AddTextToStore = 8,
    /// Obsolete Nix 2.4 Protocol 1.22*
    QueryDerivationOutputs = 22,
    /// Obsolete Nix 2.4 Protocol 1.21
    QueryDerivationOutputNames = 28,
    /// Obsolete Nix 2.0, Protocol 1.19*
    QuerySubstitutablePathInfos = 30,
    /// Obsolete Nix 2.0 Protocol 1.17
    ExportPath = 16,
    /// Obsolete Nix 2.0 Protocol 1.17
    ImportPaths = 27,
    /// Obsolete Nix 2.0 Protocol 1.16
    QueryPathHash = 4,
    /// Obsolete Nix 2.0 Protocol 1.16
    QueryReferences = 5,
    /// Obsolete Nix 2.0 Protocol 1.16
    QueryDeriver = 18,
    /// Obsolete Nix 1.2 Protocol 1.12
    HasSubstitutes = 3,
    /// Obsolete Nix 1.2 Protocol 1.12
    QuerySubstitutablePathInfo = 21,
    // Removed Nix 2.0 Protocol 1.16
    // QueryFailedPaths = 24,
    // Removed Nix 2.0 Protocol 1.16
    // ClearFailedPaths = 25,
    // Removed Nix 1.0 Protocol 1.09
    // ImportPath = 17,
    // Became dead code in Nix 0.11 and removed in Nix 1.8
    // Quit = 0,
    // Removed Nix 0.12 Protocol 1.02
    // RemovedCollectGarbage = 15,
}

#[cfg(feature = "nixrs-derive")]
macro_rules! optional_string {
    ($sub:ty) => {
        impl NixDeserialize for Option<$sub> {
            fn try_deserialize<R>(
                reader: &mut R,
            ) -> impl std::future::Future<Output = Result<Option<Self>, R::Error>> + Send + '_
            where
                R: ?Sized + NixRead + Send,
            {
                async move {
                    use nixrs::daemon::de::Error;
                    use nixrs::store_path::FromStoreDirStr;
                    if let Some(buf) = reader.try_read_bytes().await? {
                        let s = ::std::str::from_utf8(&buf).map_err(Error::invalid_data)?;
                        if s == "" {
                            Ok(Some(None))
                        } else {
                            let dir = reader.store_dir();
                            <$sub as FromStoreDirStr>::from_store_dir_str(dir, s)
                                .map_err(Error::invalid_data)
                                .map(|v| Some(Some(v)))
                        }
                    } else {
                        Ok(None)
                    }
                }
            }
        }
        impl NixSerialize for Option<$sub> {
            async fn serialize<W>(&self, writer: &mut W) -> Result<(), W::Error>
            where
                W: NixWrite,
            {
                if let Some(value) = self.as_ref() {
                    writer.write_value(value).await
                } else {
                    writer.write_slice(b"").await
                }
            }
        }
    };
}
#[cfg(feature = "nixrs-derive")]
optional_string!(StorePath);
#[cfg(feature = "nixrs-derive")]
optional_string!(ContentAddress);
