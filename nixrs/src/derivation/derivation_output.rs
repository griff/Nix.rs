use std::{collections::BTreeMap, fmt};

#[cfg(feature = "nixrs-derive")]
use nixrs_derive::NixDeserialize;

#[cfg(any(feature = "xp-ca-derivations", feature = "xp-impure-derivations"))]
use crate::store_path::ContentAddressMethodAlgorithm;
use crate::store_path::{ContentAddress, StoreDir, StorePath, StorePathNameError};

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
    ) -> Result<Option<StorePath>, StorePathNameError> {
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

pub type DerivationOutputs = BTreeMap<String, DerivationOutput>;

#[cfg(any(test, feature = "test"))]
pub mod arbitrary {
    use super::*;
    use crate::hash;
    use crate::store_path::proptest::arb_output_name;
    use crate::test::arbitrary::helpers::Union;
    use ::proptest::prelude::*;
    use ::proptest::sample::SizeRange;

    pub fn arb_derivation_outputs(
        size: impl Into<SizeRange>,
    ) -> impl Strategy<Value = DerivationOutputs> {
        use DerivationOutput::*;
        let size = size.into();
        #[cfg(feature = "xp-ca-derivations")]
        let size2 = size.clone();
        //InputAddressed
        let input = prop::collection::btree_map(
            arb_output_name(),
            arb_derivation_output_input_addressed(),
            size.clone(),
        )
        .boxed();
        // CAFixed
        let fixed = arb_derivation_output_fixed()
            .prop_map(|ca| {
                let mut ret = BTreeMap::new();
                ret.insert("out".to_string(), ca);
                ret
            })
            .boxed();
        // Deferred
        let deferred =
            prop::collection::btree_map(arb_output_name(), Just(Deferred), size.clone()).boxed();

        #[cfg_attr(
            not(any(feature = "xp-ca-derivations", feature = "xp-impure-derivations")),
            allow(unused_mut)
        )]
        let mut ret = Union::new([input, fixed, deferred]);
        #[cfg(feature = "xp-ca-derivations")]
        {
            // CAFloating
            ret = ret.or(any::<hash::Algorithm>()
                .prop_flat_map(move |hash_type| {
                    prop::collection::btree_map(
                        arb_output_name(),
                        arb_derivation_output_floating(Just(hash_type)),
                        size2.clone(),
                    )
                })
                .boxed());
        }
        #[cfg(feature = "xp-impure-derivations")]
        {
            // Impure
            ret = ret.or(prop::collection::btree_map(
                arb_output_name(),
                arb_derivation_output_impure(),
                size.clone(),
            ));
        }
        ret
    }

    impl Arbitrary for DerivationOutput {
        type Parameters = ();
        type Strategy = BoxedStrategy<DerivationOutput>;

        fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
            arb_derivation_output().boxed()
        }
    }

    pub fn arb_derivation_output_input_addressed() -> impl Strategy<Value = DerivationOutput> {
        any::<StorePath>().prop_map(DerivationOutput::InputAddressed)
    }

    #[cfg(feature = "xp-dynamic-derivations")]
    pub fn arb_derivation_output_fixed() -> impl Strategy<Value = DerivationOutput> {
        any::<ContentAddress>().prop_map(DerivationOutput::CAFixed)
    }

    #[cfg(not(feature = "xp-dynamic-derivations"))]
    pub fn arb_derivation_output_fixed() -> impl Strategy<Value = DerivationOutput> {
        prop_oneof![
            any::<hash::Hash>().prop_map(|h| DerivationOutput::CAFixed(ContentAddress::Flat(h))),
            any::<hash::Hash>()
                .prop_map(|h| DerivationOutput::CAFixed(ContentAddress::Recursive(h)))
        ]
    }

    #[cfg(feature = "xp-impure-derivations")]
    pub fn arb_derivation_output_impure() -> impl Strategy<Value = DerivationOutput> {
        any::<ContentAddressMethodAlgorithm>(any::<hash::Algorithm>())
            .prop_map(|ca| DerivationOutput::Impure(ca))
    }

    #[cfg(feature = "xp-ca-derivations")]
    pub fn arb_derivation_output_floating<H>(
        hash_type: H,
    ) -> impl Strategy<Value = DerivationOutput>
    where
        H: Strategy<Value = hash::Algorithm>,
    {
        any::<ContentAddressMethodAlgorithm>(hash_type)
            .prop_map(|ca| DerivationOutput::CAFloating(ca))
    }

    pub fn arb_derivation_output() -> impl Strategy<Value = DerivationOutput> {
        use DerivationOutput::*;
        #[cfg(all(feature = "xp-ca-derivations", feature = "xp-impure-derivations"))]
        {
            prop_oneof![
                arb_derivation_output_input_addressed(),
                arb_derivation_output_fixed(),
                arb_derivation_output_floating(any::<hash::Algorithm>()),
                Just(Deferred),
                arb_derivation_output_impure(),
            ]
        }
        #[cfg(all(not(feature = "xp-ca-derivations"), feature = "xp-impure-derivations"))]
        {
            prop_oneof![
                arb_derivation_output_input_addressed(),
                arb_derivation_output_fixed(),
                Just(Deferred),
                arb_derivation_output_impure(),
            ]
        }
        #[cfg(all(feature = "xp-ca-derivations", not(feature = "xp-impure-derivations")))]
        {
            prop_oneof![
                arb_derivation_output_input_addressed(),
                arb_derivation_output_fixed(),
                arb_derivation_output_floating(any::<hash::Algorithm>()),
                Just(Deferred),
            ]
        }
        #[cfg(not(any(feature = "xp-ca-derivations", feature = "xp-impure-derivations")))]
        {
            prop_oneof![
                arb_derivation_output_input_addressed(),
                arb_derivation_output_fixed(),
                Just(Deferred),
            ]
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

#[cfg(test)]
mod unittests {
    use rstest::rstest;

    use crate::store_path::{StorePath, StorePathNameError};

    use super::DerivationOutput;

    #[rstest]
    #[case::deffered(DerivationOutput::Deferred, "a", "a", Ok(None))]
    #[case::input(DerivationOutput::InputAddressed("00000000000000000000000000000000-_".parse().unwrap()), "a", "a", Ok(Some("00000000000000000000000000000000-_".parse().unwrap())))]
    #[case::fixed_flat(DerivationOutput::CAFixed("fixed:sha256:248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1".parse().unwrap()), "konsole-18.12.3", "out", Ok(Some("g9ngnw4w5vr9y3xkb7k2awl3mp95abrb-konsole-18.12.3".parse().unwrap())))]
    #[case::fixed_sha1(DerivationOutput::CAFixed("fixed:r:sha1:84983e441c3bd26ebaae4aa1f95129e5e54670f1".parse().unwrap()), "konsole-18.12.3", "out", Ok(Some("ag0y7g6rci9zsdz9nxcq5l1qllx3r99x-konsole-18.12.3".parse().unwrap())))]
    #[case::fixed_source(DerivationOutput::CAFixed("fixed:r:sha256:248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1".parse().unwrap()), "konsole-18.12.3", "out", Ok(Some("1w01xxn8f7s9s4n65ry6rwd7x9awf04s-konsole-18.12.3".parse().unwrap())))]
    fn test_path(
        #[case] output: DerivationOutput,
        #[case] drv_name: &str,
        #[case] output_name: &str,
        #[case] path: Result<Option<StorePath>, StorePathNameError>,
    ) {
        let store_dir = Default::default();
        assert_eq!(path, output.path(&store_dir, drv_name, output_name))
    }
}
