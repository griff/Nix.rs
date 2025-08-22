use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;

use derive_more::Display;
#[cfg(feature = "nixrs-derive")]
use nixrs_derive::{NixDeserialize, NixSerialize};
use serde::{Deserialize, Serialize};
use serde_with::{DeserializeFromStr, SerializeDisplay};
use thiserror::Error;

#[cfg(feature = "daemon-serde")]
use crate::daemon::{de::NixDeserialize, ser::NixSerialize};
use crate::derived_path::OutputName;
use crate::hash::fmt::Any;
use crate::hash::{self, Hash};
use crate::signature::Signature;
use crate::store_path::{StorePath, StorePathNameError};

#[derive(
    Debug,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Clone,
    Display,
    SerializeDisplay,
    DeserializeFromStr,
)]
#[display("{drv_hash:x}!{output_name}")]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
#[cfg_attr(feature = "nixrs-derive", nix(from_str, display))]
pub struct DrvOutput {
    pub drv_hash: hash::Hash,
    pub output_name: OutputName,
}

#[derive(Debug, PartialEq, Clone, Error)]
pub enum ParseDrvOutputError {
    #[error("hash error {0}")]
    Hash(
        #[from]
        #[source]
        hash::ParseHashError,
    ),
    #[error("output name error {0}")]
    OutputName(
        #[from]
        #[source]
        StorePathNameError,
    ),
    #[error("invalid derivation output {0}")]
    InvalidDerivationOutputId(String),
}

impl FromStr for DrvOutput {
    type Err = ParseDrvOutputError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut it = s.splitn(2, '!');
        if let (Some(drv_hash_s), Some(output_name_s)) = (it.next(), it.next()) {
            let drv_hash = drv_hash_s.parse::<Any<Hash>>()?.into_hash();
            let output_name = output_name_s.parse()?;
            Ok(DrvOutput {
                drv_hash,
                output_name,
            })
        } else {
            Err(ParseDrvOutputError::InvalidDerivationOutputId(s.into()))
        }
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Realisation {
    pub id: DrvOutput,
    pub out_path: StorePath,
    pub signatures: BTreeSet<Signature>,
    #[serde(default)]
    pub dependent_realisations: BTreeMap<DrvOutput, StorePath>,
}

#[cfg(feature = "daemon-serde")]
impl NixSerialize for Realisation {
    async fn serialize<W>(&self, writer: &mut W) -> Result<(), W::Error>
    where
        W: crate::daemon::ser::NixWrite,
    {
        use crate::daemon::ser::Error;
        let s = serde_json::to_string(&self).map_err(W::Error::custom)?;
        writer.write_slice(s.as_bytes()).await
    }
}

#[cfg(feature = "daemon-serde")]
impl NixDeserialize for Realisation {
    async fn try_deserialize<R>(reader: &mut R) -> Result<Option<Self>, R::Error>
    where
        R: ?Sized + crate::daemon::de::NixRead + Send,
    {
        use crate::daemon::de::Error;
        if let Some(buf) = reader.try_read_bytes().await? {
            Ok(Some(
                serde_json::from_slice(&buf).map_err(R::Error::custom)?,
            ))
        } else {
            Ok(None)
        }
    }
}

pub type DrvOutputs = BTreeMap<DrvOutput, Realisation>;

#[cfg(any(test, feature = "test"))]
pub mod arbitrary {
    use crate::signature::proptests::arb_signatures;

    use super::*;
    use ::proptest::prelude::*;
    use ::proptest::sample::SizeRange;

    impl Arbitrary for DrvOutput {
        type Parameters = ();
        type Strategy = BoxedStrategy<DrvOutput>;

        fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
            arb_drv_output().boxed()
        }
    }

    prop_compose! {
        pub fn arb_drv_output()
        (
            drv_hash in any::<hash::Hash>(),
            output_name in any::<OutputName>(),
        ) -> DrvOutput
        {
            DrvOutput { drv_hash, output_name }
        }
    }

    pub fn arb_drv_outputs(size: impl Into<SizeRange>) -> impl Strategy<Value = DrvOutputs> {
        let size = size.into();
        let min_size = size.start();
        prop::collection::vec(arb_realisation(), size)
            .prop_map(|r| {
                let mut ret = BTreeMap::new();
                for value in r {
                    ret.insert(value.id.clone(), value);
                }
                ret
            })
            .prop_filter("BTreeMap minimum size", move |m| m.len() >= min_size)
    }

    impl Arbitrary for Realisation {
        type Parameters = ();
        type Strategy = BoxedStrategy<Realisation>;

        fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
            arb_realisation().boxed()
        }
    }

    prop_compose! {
        pub fn arb_realisation()
        (
            id in any::<DrvOutput>(),
            out_path in any::<StorePath>(),
            signatures in arb_signatures(),
            dependent_realisations in  prop::collection::btree_map(
                arb_drv_output(),
                any::<StorePath>(),
                0..50),
        ) -> Realisation
        {
            Realisation {
                id, out_path, signatures, dependent_realisations,
            }
        }
    }
}

#[cfg(test)]
mod unittests {
    use rstest::rstest;

    use crate::btree_map;
    use crate::derived_path::OutputName;
    use crate::hash::fmt::Any;
    use crate::hash::{self, Hash};
    use crate::set;
    use crate::store_path::StorePathNameError;

    use super::{DrvOutput, ParseDrvOutputError, Realisation};

    #[rstest]
    #[case("sha256:248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1!out", Ok(DrvOutput {
        drv_hash: "sha256:248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1".parse::<Any<Hash>>().unwrap().into_hash(),
        output_name: OutputName::default(),
    }))]
    #[case("sha256:1h86vccx9vgcyrkj3zv4b7j3r8rrc0z0r4r6q3jvhf06s9hnm394!out_put", Ok(DrvOutput {
        drv_hash: "sha256:1h86vccx9vgcyrkj3zv4b7j3r8rrc0z0r4r6q3jvhf06s9hnm394".parse::<Any<Hash>>().unwrap().into_hash(),
        output_name: "out_put".parse().unwrap(),
    }))]
    #[case("sha256:1h86vccx9vgcyrkj3zv4b7j3r8rrc0z0r4r6q3jvhf06s9hnm394", Err(ParseDrvOutputError::InvalidDerivationOutputId("sha256:1h86vccx9vgcyrkj3zv4b7j3r8rrc0z0r4r6q3jvhf06s9hnm394".into())))]
    #[case("sha256:1h86vccx9vgcyrkj3zv4b7j3r8rrc0z0r4r6q3jvhf06s9hnm39!out", Err(ParseDrvOutputError::Hash(crate::hash::ParseHashError::WrongHashLength(hash::Algorithm::SHA256, "1h86vccx9vgcyrkj3zv4b7j3r8rrc0z0r4r6q3jvhf06s9hnm39".into()))))]
    #[case(
        "sha256:1h86vccx9vgcyrkj3zv4b7j3r8rrc0z0r4r6q3jvhf06s9hnm394!out{put",
        Err(ParseDrvOutputError::OutputName(StorePathNameError::Symbol(3, b'{')))
    )]
    fn parse_drv_output(
        #[case] value: &str,
        #[case] expected: Result<DrvOutput, ParseDrvOutputError>,
    ) {
        let actual: Result<DrvOutput, _> = value.parse();
        assert_eq!(actual, expected);
    }

    #[rstest]
    #[case(DrvOutput {
        drv_hash: "sha256:248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1".parse::<Any<Hash>>().unwrap().into_hash(),
        output_name: OutputName::default(),
    }, "sha256:248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1!out")]
    #[case(DrvOutput {
        drv_hash: "sha256:1h86vccx9vgcyrkj3zv4b7j3r8rrc0z0r4r6q3jvhf06s9hnm394".parse::<Any<Hash>>().unwrap().into_hash(),
        output_name: OutputName::default(),
    }, "sha256:248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1!out")]
    #[case(DrvOutput {
        drv_hash: "sha1:y5q4drg5558zk8aamsx6xliv3i23x644".parse::<Any<Hash>>().unwrap().into_hash(),
        output_name: "out_put".parse().unwrap(),
    }, "sha1:84983e441c3bd26ebaae4aa1f95129e5e54670f1!out_put")]
    fn display_drv_output(#[case] value: DrvOutput, #[case] expected: &str) {
        assert_eq!(value.to_string(), expected);
    }

    #[rstest]
    #[case(
        "{\"dependentRealisations\":{\"sha256:ba7816bf8f01cfea414140de5dae2223b00361a496177a9cf410ff61f20015ad!dev\":\"7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3-dev\",\"sha256:ba7816bf8f01cfea414140de5dae2223b00361a696177a9cf410ff61f20015ad!bin\":\"7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3-bin\"},\"id\":\"sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad!out\",\"outPath\":\"7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3\",\"signatures\":[\"cache.nixos.org-1:0CpHca+06TwFp9VkMyz5OaphT3E8mnS+1SWymYlvFaghKSYPCMQ66TS1XPAr1+y9rfQZPLaHrBjjnIRktE/nAA==\"]}",
        Realisation {
            id: "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad!out".parse().unwrap(),
            out_path: "7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3".parse().unwrap(),
            signatures: set!["cache.nixos.org-1:0CpHca+06TwFp9VkMyz5OaphT3E8mnS+1SWymYlvFaghKSYPCMQ66TS1XPAr1+y9rfQZPLaHrBjjnIRktE/nAA=="],
            dependent_realisations: btree_map![
                "sha256:ba7816bf8f01cfea414140de5dae2223b00361a496177a9cf410ff61f20015ad!dev" => "7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3-dev",
                "sha256:ba7816bf8f01cfea414140de5dae2223b00361a696177a9cf410ff61f20015ad!bin" => "7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3-bin",

            ],
        }
    )]
    fn parse_realisation(#[case] value: &str, #[case] expected: Realisation) {
        let actual: Realisation = serde_json::from_str(value).unwrap();
        pretty_assertions::assert_eq!(actual, expected);
    }

    #[rstest]
    #[case(
        Realisation {
            id: "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad!out".parse().unwrap(),
            out_path: "7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3".parse().unwrap(),
            signatures: set!["cache.nixos.org-1:0CpHca+06TwFp9VkMyz5OaphT3E8mnS+1SWymYlvFaghKSYPCMQ66TS1XPAr1+y9rfQZPLaHrBjjnIRktE/nAA=="],
            dependent_realisations: btree_map![
                "sha256:ba7816bf8f01cfea414140de5dae2223b00361a496177a9cf410ff61f20015ad!dev" => "7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3-dev",
                "sha256:ba7816bf8f01cfea414140de5dae2223b00361a696177a9cf410ff61f20015ad!bin" => "7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3-bin",

            ],
        },
        "{\"id\":\"sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad!out\",\"outPath\":\"7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3\",\"signatures\":[\"cache.nixos.org-1:0CpHca+06TwFp9VkMyz5OaphT3E8mnS+1SWymYlvFaghKSYPCMQ66TS1XPAr1+y9rfQZPLaHrBjjnIRktE/nAA==\"],\"dependentRealisations\":{\"sha256:ba7816bf8f01cfea414140de5dae2223b00361a496177a9cf410ff61f20015ad!dev\":\"7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3-dev\",\"sha256:ba7816bf8f01cfea414140de5dae2223b00361a696177a9cf410ff61f20015ad!bin\":\"7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3-bin\"}}",
    )]
    fn write_realisation(#[case] value: Realisation, #[case] expected: &str) {
        let actual = serde_json::to_string(&value).unwrap();
        pretty_assertions::assert_eq!(actual, expected);
    }

    #[tokio::test]
    #[rstest]
    #[case(
        Realisation {
            id: "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad!out".parse().unwrap(),
            out_path: "7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3".parse().unwrap(),
            signatures: set!["cache.nixos.org-1:0CpHca+06TwFp9VkMyz5OaphT3E8mnS+1SWymYlvFaghKSYPCMQ66TS1XPAr1+y9rfQZPLaHrBjjnIRktE/nAA=="],
            dependent_realisations: btree_map![
                "sha256:ba7816bf8f01cfea414140de5dae2223b00361a496177a9cf410ff61f20015ad!dev" => "7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3-dev",
                "sha256:ba7816bf8f01cfea414140de5dae2223b00361a696177a9cf410ff61f20015ad!bin" => "7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3-bin",

            ],
        },
        "{\"id\":\"sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad!out\",\"outPath\":\"7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3\",\"signatures\":[\"cache.nixos.org-1:0CpHca+06TwFp9VkMyz5OaphT3E8mnS+1SWymYlvFaghKSYPCMQ66TS1XPAr1+y9rfQZPLaHrBjjnIRktE/nAA==\"],\"dependentRealisations\":{\"sha256:ba7816bf8f01cfea414140de5dae2223b00361a496177a9cf410ff61f20015ad!dev\":\"7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3-dev\",\"sha256:ba7816bf8f01cfea414140de5dae2223b00361a696177a9cf410ff61f20015ad!bin\":\"7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3-bin\"}}",
    )]
    async fn nix_write_realisation(#[case] value: Realisation, #[case] expected: &str) {
        use crate::daemon::ser::NixWrite as _;

        let mut mock = crate::daemon::ser::mock::Builder::new()
            .write_slice(expected.as_bytes())
            .build();
        mock.write_value(&value).await.unwrap();
    }

    #[tokio::test]
    #[rstest]
    #[case(
        Realisation {
            id: "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad!out".parse().unwrap(),
            out_path: "7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3".parse().unwrap(),
            signatures: set!["cache.nixos.org-1:0CpHca+06TwFp9VkMyz5OaphT3E8mnS+1SWymYlvFaghKSYPCMQ66TS1XPAr1+y9rfQZPLaHrBjjnIRktE/nAA=="],
            dependent_realisations: btree_map![
                "sha256:ba7816bf8f01cfea414140de5dae2223b00361a496177a9cf410ff61f20015ad!dev" => "7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3-dev",
                "sha256:ba7816bf8f01cfea414140de5dae2223b00361a696177a9cf410ff61f20015ad!bin" => "7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3-bin",

            ],
        },
        "{\"id\":\"sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad!out\",\"outPath\":\"7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3\",\"signatures\":[\"cache.nixos.org-1:0CpHca+06TwFp9VkMyz5OaphT3E8mnS+1SWymYlvFaghKSYPCMQ66TS1XPAr1+y9rfQZPLaHrBjjnIRktE/nAA==\"],\"dependentRealisations\":{\"sha256:ba7816bf8f01cfea414140de5dae2223b00361a496177a9cf410ff61f20015ad!dev\":\"7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3-dev\",\"sha256:ba7816bf8f01cfea414140de5dae2223b00361a696177a9cf410ff61f20015ad!bin\":\"7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3-bin\"}}",
    )]
    async fn nix_read_realisation(#[case] expected: Realisation, #[case] value: &str) {
        use crate::daemon::de::NixRead as _;

        let mut mock = crate::daemon::de::mock::Builder::new()
            .read_slice(value.as_bytes())
            .build();
        let actual: Realisation = mock.read_value().await.unwrap();
        pretty_assertions::assert_eq!(actual, expected);
    }
}
