use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::str::FromStr;

use derive_more::Display;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::hash;
use crate::store_path::StorePath;
use crate::StringSet;

#[derive(Error, Debug, PartialEq, Clone)]
pub enum ParseDrvOutputError {
    #[error("bad hash in derivation: {0}")]
    BadHash(#[from] hash::ParseHashError),
    #[error("Invalid derivation output id {0}")]
    InvalidDerivationOutputId(String),
}

#[derive(Debug, Display, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Deserialize, Serialize)]
#[display(fmt = "{:x}!{}", drv_hash, output_name)]
#[serde(try_from = "String", into = "String")]
pub struct DrvOutput {
    /// The hash modulo of the derivation
    pub drv_hash: hash::Hash,
    pub output_name: String,
}

impl DrvOutput {
    pub fn parse(s: &str) -> Result<DrvOutput, ParseDrvOutputError> {
        if let Some(pos) = s.find('!') {
            let drv_hash = hash::Hash::parse_any_prefixed(&s[..pos])?;
            let output_name = (&s[(pos + 1)..]).into();
            Ok(DrvOutput {
                drv_hash,
                output_name,
            })
        } else {
            Err(ParseDrvOutputError::InvalidDerivationOutputId(s.into()))
        }
    }
}

impl<'a> TryFrom<&'a str> for DrvOutput {
    type Error = ParseDrvOutputError;

    fn try_from(value: &'a str) -> Result<Self, Self::Error> {
        DrvOutput::parse(value)
    }
}
impl TryFrom<String> for DrvOutput {
    type Error = ParseDrvOutputError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        DrvOutput::parse(&value)
    }
}

impl FromStr for DrvOutput {
    type Err = ParseDrvOutputError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        DrvOutput::parse(s)
    }
}

impl From<DrvOutput> for String {
    fn from(v: DrvOutput) -> Self {
        v.to_string()
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Realisation {
    pub id: DrvOutput,
    pub out_path: StorePath,
    pub signatures: StringSet,

    /// The realisations that are required for the current one to be valid.
    ///
    /// When importing this realisation, the store will first check that all its
    /// dependencies exist, and map to the correct output path
    #[serde(default)]
    pub dependent_realisations: BTreeMap<DrvOutput, StorePath>,
}

impl Realisation {
    pub fn from_json(json: &str) -> serde_json::Result<Realisation> {
        serde_json::from_str(json)
    }
    pub fn to_json(&self) -> serde_json::Result<serde_json::Value> {
        serde_json::to_value(self)
    }

    pub fn to_json_string(&self) -> serde_json::Result<String> {
        let value = self.to_json()?;
        serde_json::to_string(&value)
    }
}

impl FromStr for Realisation {
    type Err = serde_json::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Realisation::from_json(s)
    }
}

pub type DrvOutputs = BTreeMap<DrvOutput, Realisation>;

#[cfg(any(test, feature = "test"))]
pub mod proptest {
    use super::*;
    use crate::store_path::proptest::arb_output_name;
    use proptest::prelude::*;
    use proptest::sample::SizeRange;

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
            output_name in arb_output_name(),
        ) -> DrvOutput
        {
            DrvOutput { drv_hash, output_name }
        }
    }

    pub fn arb_drv_outputs(size: impl Into<SizeRange>) -> impl Strategy<Value = DrvOutputs> {
        prop::collection::btree_map(arb_drv_output(), arb_realisation(), size).prop_map(
            |mut map| {
                for (key, value) in map.iter_mut() {
                    value.id = key.clone();
                }
                map
            },
        )
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
            signatures in any::<StringSet>(),
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
mod tests {
    use crate::string_set;

    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_drv_output_parse() {
        let p = DrvOutput::parse(
            "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad!out",
        )
        .unwrap();
        let drv_hash = "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
            .parse::<hash::Hash>()
            .unwrap();
        let id = DrvOutput {
            drv_hash,
            output_name: "out".into(),
        };
        assert_eq!(p, id);
        let p = DrvOutput::try_from(
            "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad!out",
        )
        .unwrap();
        assert_eq!(p, id);
        let p = DrvOutput::try_from(
            "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad!out"
                .to_owned(),
        )
        .unwrap();
        assert_eq!(p, id);
        let p = "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad!out"
            .parse::<DrvOutput>()
            .unwrap();
        assert_eq!(p, id);
    }

    #[test]
    fn test_drv_output_errors() {
        let s = "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
        let p = DrvOutput::parse(s);
        assert_eq!(
            p,
            Err(ParseDrvOutputError::InvalidDerivationOutputId(s.into()))
        );

        let s2 = "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015a!bin";
        let p = DrvOutput::parse(s2);
        assert_eq!(
            p,
            Err(ParseDrvOutputError::BadHash(
                hash::ParseHashError::WrongHashLength(
                    hash::Algorithm::SHA256,
                    "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015a".into()
                )
            ))
        );
    }

    #[test]
    fn test_drv_output_display() {
        let drv_hash = "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
            .parse::<hash::Hash>()
            .unwrap();
        let id = DrvOutput {
            drv_hash,
            output_name: "out".into(),
        };
        assert_eq!(
            id.to_string(),
            "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad!out"
        );
        let s = String::try_from(id).unwrap();
        assert_eq!(
            s,
            "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad!out"
        );
    }

    #[test]
    fn test_realisation_json() {
        let s = "{\"dependentRealisations\":{\"sha256:ba7816bf8f01cfea414140de5dae2223b00361a496177a9cf410ff61f20015ad!dev\":\"7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3-dev\",\"sha256:ba7816bf8f01cfea414140de5dae2223b00361a696177a9cf410ff61f20015ad!bin\":\"7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3-bin\"},\"id\":\"sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad!out\",\"outPath\":\"7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3\",\"signatures\":[\"hello\",\"test1234\"]}";
        let r = Realisation::from_json(s).unwrap();

        let mut deps = BTreeMap::new();
        let dp = "sha256:ba7816bf8f01cfea414140de5dae2223b00361a496177a9cf410ff61f20015ad!dev"
            .parse::<DrvOutput>()
            .unwrap();
        let sp =
            StorePath::new_from_base_name("7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3-dev")
                .unwrap();
        deps.insert(dp, sp);

        let dp = "sha256:ba7816bf8f01cfea414140de5dae2223b00361a696177a9cf410ff61f20015ad!bin"
            .parse::<DrvOutput>()
            .unwrap();
        let sp =
            StorePath::new_from_base_name("7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3-bin")
                .unwrap();
        deps.insert(dp, sp);

        let r2 = Realisation {
            id: "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad!out"
                .parse()
                .unwrap(),
            out_path: StorePath::new_from_base_name(
                "7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3",
            )
            .unwrap(),
            signatures: string_set!["test1234", "hello"],
            dependent_realisations: deps,
        };
        assert_eq!(r, r2);
        let r = s.parse::<Realisation>().unwrap();
        assert_eq!(r, r2);
        assert_eq!(s, r2.to_json_string().unwrap());
    }
}
