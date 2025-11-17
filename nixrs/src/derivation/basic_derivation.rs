use std::collections::BTreeMap;

#[cfg(feature = "nixrs-derive")]
use nixrs_derive::NixDeserialize;
#[cfg(any(test, feature = "test"))]
use proptest::prelude::{Arbitrary, BoxedStrategy};

use crate::ByteString;
use crate::store_path::{StorePath, StorePathSet};

use super::DerivationOutputs;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize))]
pub struct BasicDerivation {
    pub drv_path: StorePath,
    pub outputs: DerivationOutputs,
    pub input_srcs: StorePathSet,
    pub platform: ByteString,
    pub builder: ByteString,
    pub args: Vec<ByteString>,
    pub env: BTreeMap<ByteString, ByteString>,
}

#[cfg(any(test, feature = "test"))]
pub mod arbitrary {
    use super::*;
    use crate::{
        derivation::derivation_output::arbitrary::arb_derivation_outputs,
        test::arbitrary::arb_byte_string,
    };
    use ::proptest::prelude::*;
    use proptest::sample::SizeRange;

    impl Arbitrary for BasicDerivation {
        type Parameters = ();
        type Strategy = BoxedStrategy<BasicDerivation>;

        fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
            arb_basic_derivation().boxed()
        }
    }

    prop_compose! {
        pub fn arb_basic_derivation()
        (
            outputs in arb_derivation_outputs(1..15),
            input_srcs in any::<StorePathSet>(),
            platform in arb_byte_string(),
            builder in arb_byte_string(),
            args in proptest::collection::vec(arb_byte_string(), SizeRange::default()),
            env in proptest::collection::btree_map(arb_byte_string(), arb_byte_string(), SizeRange::default()),
            drv_path in any::<StorePath>()
        ) -> BasicDerivation
        {
            BasicDerivation {
                outputs, input_srcs, platform, builder, args, env, drv_path,
            }
        }
    }
}

#[cfg(feature = "nixrs-derive")]
mod daemon_serde {
    use crate::daemon::ser::NixSerialize;

    use super::BasicDerivation;

    impl NixSerialize for BasicDerivation {
        async fn serialize<W>(&self, mut writer: &mut W) -> Result<(), W::Error>
        where
            W: crate::daemon::ser::NixWrite,
        {
            writer.write_value(&self.drv_path).await?;
            writer.write_value(&self.outputs.len()).await?;
            for (output_name, output) in self.outputs.iter() {
                writer.write_value(output_name).await?;
                output
                    .write_output(self.drv_path.name(), output_name, &mut writer)
                    .await?;
            }
            writer.write_value(&self.input_srcs).await?;
            writer.write_value(&self.platform).await?;
            writer.write_value(&self.builder).await?;
            writer.write_value(&self.args).await?;
            writer.write_value(&self.env).await?;
            Ok(())
        }
    }

    #[cfg(test)]
    mod unittests {
        use std::io::Cursor;

        use rstest::rstest;
        use tokio::io::AsyncWriteExt;

        use crate::ByteString;
        use crate::daemon::de::{NixRead as _, NixReader};
        use crate::daemon::ser::{NixWrite as _, NixWriter};
        use crate::derivation::{BasicDerivation, DerivationOutput};
        use crate::derived_path::OutputName;
        use crate::store_path::StorePathSet;

        macro_rules! store_path_set {
            () => { StorePathSet::new() };
            ($p:expr $(, $pr:expr)*) => {{
                let mut ret = StorePathSet::new();
                let p = $p.parse::<StorePath>().unwrap();
                ret.insert(p);
                $(
                    ret.insert($pr.parse::<StorePath>().unwrap());
                )*
                ret
            }}
        }
        macro_rules! btree_map {
            () => {std::collections::BTreeMap::new()};
            ($k:expr => $v:expr
             $(, $kr:expr => $vr:expr )*$(,)?) => {{
                let mut ret = std::collections::BTreeMap::new();
                ret.insert($k, $v);
                $(
                    ret.insert($kr, $vr);
                )*
                ret
             }}
        }

        #[rstest]
        #[case::input_addressed(BasicDerivation {
            drv_path: "00000000000000000000000000000000-_.drv".parse().unwrap(),
            outputs: btree_map!(
                OutputName::default() => DerivationOutput::InputAddressed("00000000000000000000000000000000-_".parse().unwrap()),
            ),
            input_srcs: store_path_set!(),
            platform: ByteString::from_static(b"x86_64-linux"),
            builder: ByteString::from_static(b"/bin/sh"),
            args: vec![ByteString::from_static(b"-c"), ByteString::from_static(b"echo Hello")],
            env: btree_map!(),
        })]
        #[case::defered(BasicDerivation {
            drv_path: "00000000000000000000000000000000-_.drv".parse().unwrap(),
            outputs: btree_map!(
                OutputName::default() => DerivationOutput::Deferred,
            ),
            input_srcs: store_path_set!(),
            platform: ByteString::from_static(b"x86_64-linux"),
            builder: ByteString::from_static(b"/bin/sh"),
            args: vec![ByteString::from_static(b"-c"), ByteString::from_static(b"echo Hello")],
            env: btree_map!(),
        })]
        #[cfg_attr(feature = "xp-dynamic-derivations", case::ca_fixed_text(BasicDerivation {
            drv_path: "00000000000000000000000000000000-_.drv".parse().unwrap(),
            outputs: btree_map!(
                OutputName::default() => DerivationOutput::CAFixed("text:sha256:1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5s".parse().unwrap()),
            ),
            input_srcs: store_path_set!(),
            platform: ByteString::from_static(b"x86_64-linux"),
            builder: ByteString::from_static(b"/bin/sh"),
            args: vec![ByteString::from_static(b"-c"), ByteString::from_static(b"echo Hello")],
            env: btree_map!(),
        }))]
        #[case::ca_fixed_flat(BasicDerivation {
            drv_path: "00000000000000000000000000000000-_.drv".parse().unwrap(),
            outputs: btree_map!(
                OutputName::default() => DerivationOutput::CAFixed("fixed:sha256:1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5s".parse().unwrap()),
            ),
            input_srcs: store_path_set!(),
            platform: ByteString::from_static(b"x86_64-linux"),
            builder: ByteString::from_static(b"/bin/sh"),
            args: vec![ByteString::from_static(b"-c"), ByteString::from_static(b"echo Hello")],
            env: btree_map!(),
        })]
        #[case::ca_fixed_recursive(BasicDerivation {
            drv_path: "00000000000000000000000000000000-_.drv".parse().unwrap(),
            outputs: btree_map!(
                OutputName::default() => DerivationOutput::CAFixed("fixed:r:sha256:1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5s".parse().unwrap()),
            ),
            input_srcs: store_path_set!(),
            platform: ByteString::from_static(b"x86_64-linux"),
            builder: ByteString::from_static(b"/bin/sh"),
            args: vec![ByteString::from_static(b"-c"), ByteString::from_static(b"echo Hello")],
            env: btree_map!(),
        })]
        #[cfg_attr(feature = "xp-ca-derivations", case::ca_floating(BasicDerivation {
            drv_path: "00000000000000000000000000000000-_.drv".parse().unwrap(),
            outputs: btree_map!(
                OutputName::default() => DerivationOutput::CAFloating("text:sha256".parse().unwrap()),
            ),
            input_srcs: store_path_set!(),
            platform: ByteString::from_static(b"x86_64-linux"),
            builder: ByteString::from_static(b"/bin/sh"),
            args: vec![ByteString::from_static(b"-c"), ByteString::from_static(b"echo Hello")],
            env: btree_map!(),
        }))]
        #[cfg_attr(feature = "xp-impure-derivations", case::impure(BasicDerivation {
            drv_path: "00000000000000000000000000000000-_.drv".parse().unwrap(),
            outputs: btree_map!(
                OutputName::default() => DerivationOutput::Impure("text:sha256".parse().unwrap()),
            ),
            input_srcs: store_path_set!(),
            platform: ByteString::from_static(b"x86_64-linux"),
            builder: ByteString::from_static(b"/bin/sh"),
            args: vec![ByteString::from_static(b"-c"), ByteString::from_static(b"echo Hello")],
            env: btree_map!(),
        }))]
        #[tokio::test]
        async fn serde(#[case] drv: BasicDerivation) {
            let mut buf = Vec::new();
            let mut writer = NixWriter::new(&mut buf);
            writer.write_value(&drv).await.unwrap();
            writer.shutdown().await.unwrap();
            let mut reader = NixReader::new(Cursor::new(&buf));
            let actual = reader.read_value::<BasicDerivation>().await.unwrap();
            pretty_assertions::assert_eq!(drv, actual);
        }
    }
}
