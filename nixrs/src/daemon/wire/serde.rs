mod int {
    use nixrs_derive::{nix_deserialize_remote, nix_serde_remote};

    nix_deserialize_remote!(
        #[nix(try_from = "u64")]
        u8
    );
    nix_serde_remote!(
        #[nix(try_from = "u64", into = "u64")]
        u16
    );
    nix_serde_remote!(
        #[nix(try_from = "u64", into = "u64")]
        u32
    );
}

mod derivation {
    use std::collections::BTreeMap;

    use nixrs_derive::{
        NixDeserialize, NixSerialize, nix_deserialize_remote, nix_deserialize_remote_derive,
    };
    use thiserror::Error;

    use crate::ByteString;
    use crate::daemon::ser::{Error, NixSerialize, NixWrite};
    use crate::derivation::{
        BasicDerivation, DerivationOutput, DerivationOutputs, output_path_name,
    };
    use crate::derived_path::OutputName;
    use crate::hash;
    use crate::hash::fmt::ParseHashError;
    use crate::store_path::{
        ContentAddress, ContentAddressMethod, ContentAddressMethodAlgorithm, StorePath,
        StorePathName, StorePathSet,
    };

    nix_deserialize_remote_derive! {
        pub struct BasicDerivation {
            pub drv_path: StorePath,
            pub outputs: DerivationOutputs,
            pub input_srcs: StorePathSet,
            pub platform: ByteString,
            pub builder: ByteString,
            pub args: Vec<ByteString>,
            pub env: BTreeMap<ByteString, ByteString>,
        }
    }

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

    nix_deserialize_remote!(
        #[nix(try_from = "DerivationOutputData")]
        DerivationOutput
    );

    #[derive(Error, Debug, PartialEq, Clone)]
    pub enum ParseDerivationOutput {
        #[error("{0}")]
        Hash(
            #[from]
            #[source]
            hash::fmt::ParseHashError,
        ),
        #[error("{0}")]
        InvalidData(String),
        #[error("Missing experimental feature {0}")]
        MissingExperimentalFeature(String),
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
                if let Some(hash_s) = value.hash {
                    if hash_s == "impure" {
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
                            hash::fmt::NonSRI::<hash::Hash>::parse(hash_algo.algorithm(), &hash_s)?;
                        let hash = ContentAddress::from_hash(hash_algo.method(), hash).map_err(
                            |kind| ParseDerivationOutput::Hash(ParseHashError::new(hash_s, kind)),
                        )?;
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
            drv_name: &StorePathName,
            output_name: &OutputName,
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
                    let name = output_path_name(drv_name, output_name)
                        .to_string()
                        .parse()
                        .map_err(Error::unsupported_data)?;
                    let path = writer.store_dir().make_store_path_from_ca(name, *ca);
                    writer.write_value(&path).await?;
                    writer.write_value(&ca.method_algorithm()).await?;
                    writer.write_display(ca.hash().base32().bare()).await?;
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

mod derived_path {
    use nixrs_derive::nix_serde_remote;

    use crate::daemon::de::NixDeserialize;
    use crate::daemon::ser::NixSerialize;
    use crate::derived_path::{DerivedPath, LegacyDerivedPath, OutputName};

    nix_serde_remote!(
        #[nix(from_str, display)]
        OutputName
    );

    impl NixSerialize for DerivedPath {
        async fn serialize<W>(&self, writer: &mut W) -> Result<(), W::Error>
        where
            W: crate::daemon::ser::NixWrite,
        {
            let store_dir = writer.store_dir().clone();
            writer
                .write_display(store_dir.display(&self.to_legacy_format()))
                .await
        }
    }

    impl NixDeserialize for DerivedPath {
        async fn try_deserialize<R>(reader: &mut R) -> Result<Option<Self>, R::Error>
        where
            R: ?Sized + crate::daemon::de::NixRead + Send,
        {
            use crate::daemon::de::Error;
            if let Some(s) = reader.try_read_value::<String>().await? {
                let legacy = reader
                    .store_dir()
                    .parse::<LegacyDerivedPath>(&s)
                    .map_err(R::Error::invalid_data)?;
                Ok(Some(legacy.0))
            } else {
                Ok(None)
            }
        }
    }
}

mod hash {
    use nixrs_derive::{nix_deserialize_remote, nix_serde_remote};
    use std::fmt as sfmt;

    use crate::hash::fmt::{Any, Bare, Base16, Base32, CommonHash, Format};
    use crate::hash::{Algorithm, Hash, NarHash};

    nix_serde_remote!(#[nix(from_str, display, bound = "H: CommonHash + Sync + 'static")] Base32<H>);
    nix_deserialize_remote!(#[nix(from_str, bound = "H: CommonHash + Sync + 'static")] Any<H>);
    nix_serde_remote!(#[nix(
        from_str,
        display,
        bound(
            deserialize = "F: Format + Sync + 'static, <F as Format>::Hash: CommonHash",
            serialize = "F: sfmt::Display + Sync"
        )
    )] Bare<F>);

    nix_serde_remote!(
        #[nix(from_str, display)]
        Algorithm
    );
    nix_serde_remote!(
        #[nix(from = "Any<Hash>", into = "Base32<Hash>")]
        Hash
    );
    nix_serde_remote!(
        #[nix(from = "Bare<Any<NarHash>>", into = "Bare<Base16<NarHash>>")]
        NarHash
    );
}

mod log {
    use nixrs_derive::{NixDeserialize, NixSerialize, nix_serde_remote, nix_serde_remote_derive};
    use num_enum::{IntoPrimitive, TryFromPrimitive};

    use crate::ByteString;
    use crate::daemon::ser::{NixSerialize, NixWrite};
    use crate::daemon::wire::logger::RawLogMessageType;
    use crate::log::{
        Activity, ActivityResult, ActivityType, Field, LogMessage, Message, ResultType,
        StopActivity, Verbosity,
    };

    nix_serde_remote!(
        #[nix(from = "u16", into = "u16")]
        Verbosity
    );
    nix_serde_remote!(
        #[nix(try_from = "u16", into = "u16")]
        ActivityType
    );
    nix_serde_remote!(
        #[nix(try_from = "u16", into = "u16")]
        ResultType
    );

    impl NixSerialize for LogMessage {
        async fn serialize<W>(&self, writer: &mut W) -> Result<(), W::Error>
        where
            W: NixWrite,
        {
            match self {
                LogMessage::Message(msg) => {
                    writer.write_value(&RawLogMessageType::Next).await?;
                    writer.write_value(&msg.text).await?;
                }
                LogMessage::StartActivity(act) => {
                    if writer.version().minor() >= 20 {
                        writer
                            .write_value(&RawLogMessageType::StartActivity)
                            .await?;
                        writer.write_value(act).await?;
                    } else {
                        writer.write_value(&RawLogMessageType::Next).await?;
                        writer.write_value(&act.text).await?;
                    }
                }
                LogMessage::StopActivity(act) => {
                    if writer.version().minor() >= 20 {
                        writer.write_value(&RawLogMessageType::StopActivity).await?;
                        writer.write_value(&act.id).await?;
                    }
                }
                LogMessage::Result(result) => {
                    if writer.version().minor() >= 20 {
                        writer.write_value(&RawLogMessageType::Result).await?;
                        writer.write_value(result).await?;
                    }
                }
            }
            Ok(())
        }
    }

    nix_serde_remote_derive! {
        pub struct Message {
            #[nix(skip)]
            pub level: Verbosity,
            pub text: ByteString,
        }
    }

    nix_serde_remote_derive! {
        struct Activity {
            pub id: u64,
            pub level: Verbosity,
            pub activity_type: ActivityType,
            pub text: ByteString,
            pub fields: Vec<Field>,
            pub parent: u64,
        }
    }

    nix_serde_remote_derive! {
        pub struct StopActivity {
            pub id: u64,
        }
    }

    nix_serde_remote_derive! {
        pub struct ActivityResult {
            pub id: u64,
            pub result_type: ResultType,
            pub fields: Vec<Field>,
        }
    }

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
        NixDeserialize,
        NixSerialize,
    )]
    #[nix(try_from = "u16", into = "u16")]
    #[repr(u16)]
    pub enum FieldType {
        Int = 0,
        String = 1,
    }

    nix_serde_remote_derive! {
        #[nix(tag = "FieldType")]
        pub enum Field {
            Int(u64),
            String(ByteString),
        }
    }
}

mod realisation {
    use nixrs_derive::nix_serde_remote;

    use crate::daemon::de::NixDeserialize;
    use crate::daemon::ser::NixSerialize;
    use crate::realisation::{DrvOutput, Realisation};

    nix_serde_remote!(
        #[nix(from_str, display)]
        DrvOutput
    );

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
}

mod signature {
    use nixrs_derive::nix_serde_remote;

    use crate::signature::Signature;

    nix_serde_remote!(
        #[nix(from_str, display)]
        Signature
    );
}

mod store_path {
    use nixrs_derive::nix_serde_remote;

    use crate::store_path::{
        ContentAddress, ContentAddressMethodAlgorithm, StorePath, StorePathHash,
    };

    nix_serde_remote!(
        #[nix(from_str, display)]
        ContentAddressMethodAlgorithm
    );
    nix_serde_remote!(
        #[nix(from_str, display)]
        ContentAddress
    );
    nix_serde_remote!(
        #[nix(from_store_dir_str, store_dir_display)]
        StorePath
    );
    nix_serde_remote!(
        #[nix(from_str, display)]
        StorePathHash
    );
}
