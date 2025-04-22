use std::fmt;

#[cfg(feature = "nixrs-derive")]
use nixrs_derive::NixDeserialize;

#[cfg(any(feature = "xp-ca-derivations", feature = "xp-impure-derivations"))]
use crate::store_path::ContentAddressMethodAlgorithm;
use crate::store_path::{ContentAddress, StoreDir, StorePath, StorePathError};

struct OutputPathName<'b> {
    drv_name: &'b str,
    output_name: &'b str,
}
impl fmt::Display for OutputPathName<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.output_name != "out" {
            write!(f, "{}-{}", self.drv_name, self.output_name)
        } else {
            write!(f, "{}", self.drv_name)
        }
    }
}
pub(crate) fn output_path_name<'s>(
    drv_name: &'s str,
    output_name: &'s str,
) -> impl fmt::Display + 's {
    OutputPathName {
        drv_name,
        output_name,
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize))]
#[cfg_attr(
    feature = "nixrs-derive",
    nix(try_from = "daemon_serde::DerivationOutputData")
)]
pub enum DerivationOutput {
    InputAddressed(StorePath),
    CAFixed(ContentAddress),
    Deferred,
    #[cfg(feature = "xp-ca-derivations")]
    CAFloating(ContentAddressMethodAlgorithm),
    #[cfg(feature = "xp-impure-derivations")]
    Impure(ContentAddressMethodAlgorithm),
}

impl DerivationOutput {
    pub fn path(
        &self,
        store_dir: &StoreDir,
        drv_name: &str,
        output_name: &str,
    ) -> Result<Option<StorePath>, StorePathError> {
        match self {
            DerivationOutput::InputAddressed(store_path) => Ok(Some(store_path.clone())),
            DerivationOutput::CAFixed(ca) => {
                let name = output_path_name(drv_name, output_name).to_string();
                Ok(Some(store_dir.make_store_path_from_ca(&name, *ca)?))
            }
            _ => Ok(None),
        }
    }
}

#[cfg(feature = "nixrs-derive")]
mod daemon_serde {
    use nixrs_derive::{NixDeserialize, NixSerialize};
    use thiserror::Error;

    use crate::{
        daemon::ser::{Error, NixWrite},
        hash,
        store_path::{
            ContentAddress, ContentAddressMethod, ContentAddressMethodAlgorithm, StorePath,
        },
    };

    use super::{output_path_name, DerivationOutput};

    #[derive(Error, Debug, PartialEq, Clone)]
    pub enum ParseDerivationOutput {
        #[error("{0}")]
        Hash(
            #[from]
            #[source]
            hash::ParseHashError,
        ),
        #[error("{0}")]
        InvalidData(String),
        #[error("Missing experimental feature {0}")]
        MissingExperimentalFeature(String),
    }

    impl From<hash::UnknownAlgorithm> for ParseDerivationOutput {
        fn from(value: hash::UnknownAlgorithm) -> Self {
            Self::Hash(value.into())
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, Hash, NixDeserialize, NixSerialize)]
    pub struct DerivationOutputData {
        pub path: Option<StorePath>,
        pub hash_algo: Option<ContentAddressMethodAlgorithm>,
        pub hash: Option<String>,
    }

    impl TryFrom<DerivationOutputData> for DerivationOutput {
        type Error = ParseDerivationOutput;

        fn try_from(value: DerivationOutputData) -> Result<Self, Self::Error> {
            if let Some(hash_algo) = value.hash_algo {
                #[cfg(not(feature = "xp-dynamic-derivations"))]
                if hash_algo.method() == ContentAddressMethod::Text {
                    return Err(ParseDerivationOutput::MissingExperimentalFeature(
                        "dynamic-derivations".into(),
                    ));
                }
                if let Some(hash) = value.hash {
                    if hash == "impure" {
                        #[cfg(not(feature = "xp-impure-derivations"))]
                        {
                            Err(ParseDerivationOutput::MissingExperimentalFeature(
                                "impure-derivations".into(),
                            ))
                        }
                        #[cfg(feature = "xp-impure-derivations")]
                        {
                            if value.path.is_some() {
                                Err(ParseDerivationOutput::InvalidData(
                                    "expected path to be empty".into(),
                                ))
                            } else {
                                Ok(DerivationOutput::Impure(hash_algo))
                            }
                        }
                    } else if value.path.is_none() {
                        Err(ParseDerivationOutput::InvalidData(
                            "expected path to have StorePath".into(),
                        ))
                    } else {
                        let hash =
                            hash::Hash::parse_non_sri_unprefixed(&hash, hash_algo.algorithm())?;
                        let hash = ContentAddress::from_hash(hash_algo.method(), hash)?;
                        Ok(DerivationOutput::CAFixed(hash))
                    }
                } else if value.path.is_some() {
                    Err(ParseDerivationOutput::InvalidData(
                        "expected path to have StorePath".into(),
                    ))
                } else {
                    #[cfg(not(feature = "xp-ca-derivations"))]
                    {
                        Err(ParseDerivationOutput::MissingExperimentalFeature(
                            "ca-derivations".into(),
                        ))
                    }
                    #[cfg(feature = "xp-ca-derivations")]
                    {
                        Ok(DerivationOutput::CAFloating(hash_algo))
                    }
                }
            } else if let Some(path) = value.path {
                Ok(DerivationOutput::InputAddressed(path))
            } else {
                Ok(DerivationOutput::Deferred)
            }
        }
    }

    impl DerivationOutput {
        pub(crate) async fn write_output<W>(
            &self,
            drv_name: &str,
            output_name: &str,
            mut writer: W,
        ) -> Result<(), W::Error>
        where
            W: NixWrite,
        {
            match self {
                DerivationOutput::InputAddressed(store_path) => {
                    writer.write_value(store_path).await?;
                    writer.write_value("").await?;
                    writer.write_value("").await?;
                }
                DerivationOutput::CAFixed(ca) => {
                    let name = output_path_name(drv_name, output_name).to_string();
                    let path = writer
                        .store_dir()
                        .make_store_path_from_ca(&name, *ca)
                        .map_err(Error::unsupported_data)?;
                    writer.write_value(&path).await?;
                    writer.write_value(&ca.method_algorithm()).await?;
                    writer.write_display(ca.hash().bare()).await?;
                }
                DerivationOutput::Deferred => {
                    writer.write_value("").await?;
                    writer.write_value("").await?;
                    writer.write_value("").await?;
                }
                #[cfg(feature = "xp-ca-derivations")]
                DerivationOutput::CAFloating(algo) => {
                    writer.write_value("").await?;
                    writer.write_value(algo).await?;
                    writer.write_value("").await?;
                }
                #[cfg(feature = "xp-impure-derivations")]
                DerivationOutput::Impure(algo) => {
                    writer.write_value("").await?;
                    writer.write_value(algo).await?;
                    writer.write_value("impure").await?;
                }
            }
            Ok(())
        }
    }
}
