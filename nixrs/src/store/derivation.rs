use std::collections::BTreeMap;
use std::path::PathBuf;

use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::flag_enum;
use crate::hash;
use crate::hash::Hash;
use crate::io::{AsyncSink, AsyncSource};

use super::content_address::FileIngestionMethod;
use super::content_address::FixedOutputHash;
use super::path::StorePathSet;
use super::ParseStorePathError;
use super::ReadStorePathError;
use super::StoreDir;
use super::StorePath;

flag_enum! {
    #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
    pub enum RepairFlag {
        NoRepair = false,
        Repair = true,
    }
}

#[derive(Error, Debug, PartialEq, Clone)]
pub enum ParseDerivationError {
    #[error("bad store path in derivation: {0}")]
    BadStorePath(
        #[from]
        #[source]
        ParseStorePathError,
    ),
    #[error("bad path '{0}' in derivation")]
    BadPath(String),
    #[error("bad hash in derivation: {0}")]
    BadHash(
        #[from]
        #[source]
        hash::ParseHashError,
    ),
}

impl From<hash::UnknownAlgorithm> for ParseDerivationError {
    fn from(v: hash::UnknownAlgorithm) -> ParseDerivationError {
        ParseDerivationError::BadHash(hash::ParseHashError::Algorithm(v))
    }
}

#[derive(Error, Debug)]
pub enum ReadDerivationError {
    #[error("{0}")]
    BadDerivation(
        #[from]
        #[source]
        ParseDerivationError,
    ),
    #[error("io error reading derivation {0}")]
    IO(
        #[from]
        #[source]
        std::io::Error,
    ),
}

impl From<ReadStorePathError> for ReadDerivationError {
    fn from(v: ReadStorePathError) -> ReadDerivationError {
        use ReadStorePathError::*;
        match v {
            BadStorePath(e) => {
                ReadDerivationError::BadDerivation(ParseDerivationError::BadStorePath(e))
            }
            IO(io) => ReadDerivationError::IO(io),
        }
    }
}

#[derive(Error, Debug)]
pub enum WriteDerivationError {
    #[error("{0}")]
    BadStorePath(
        #[from]
        #[source]
        ParseStorePathError,
    ),
    #[error("io error writing derivation {0}")]
    IO(
        #[from]
        #[source]
        std::io::Error,
    ),
    #[error("Builder '{0:?}' could not be converted to string")]
    InvalidBuilder(PathBuf),
}

fn validate_path(s: &str) -> Result<(), ParseDerivationError> {
    if s.len() == 0 || !s.starts_with("/") {
        Err(ParseDerivationError::BadPath(s.into()))
    } else {
        Ok(())
    }
}

fn output_path_name(drv_name: &str, output_name: &str) -> String {
    if output_name != "out" {
        format!("{}-{}", drv_name, output_name)
    } else {
        drv_name.to_owned()
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub enum DerivationOutput {
    /// The traditional non-fixed-output derivation type.
    InputAddressed(StorePath),
    /// Fixed-output derivations, whose output paths are content addressed
    /// according to that fixed output.
    CAFixed(FixedOutputHash),
    /// Floating-output derivations, whose output paths are content addressed, but
    /// not fixed, and so are dynamically calculated from whatever the output ends
    /// up being.
    CAFloating {
        method: FileIngestionMethod,
        hash_type: hash::Algorithm,
    },
    /// Input-addressed output which depends on a (CA) derivation whose hash isn't
    /// known atm.
    Deferred,
}

impl DerivationOutput {
    pub fn parse_output(
        store_dir: &StoreDir,
        path_s: String,
        hash_algo: String,
        hash: String,
    ) -> Result<DerivationOutput, ParseDerivationError> {
        if hash_algo != "" {
            let (method, algo) = if hash_algo.starts_with("r:") {
                (FileIngestionMethod::Recursive, &hash_algo[2..])
            } else {
                (FileIngestionMethod::Flat, &hash_algo[..])
            };
            let algorithm = algo.parse::<hash::Algorithm>()?;
            if hash != "" {
                validate_path(&path_s)?;
                let hash = Hash::parse_non_sri_unprefixed(&hash, algorithm)?;
                let hash = FixedOutputHash { method, hash };
                Ok(DerivationOutput::CAFixed(hash))
            } else {
                // TODO: settings.requireExperimentalFeature("ca-derivations");
                assert_eq!(path_s, "");
                Ok(DerivationOutput::CAFloating {
                    method,
                    hash_type: algorithm,
                })
            }
        } else {
            if path_s == "" {
                Ok(DerivationOutput::Deferred)
            } else {
                validate_path(&path_s)?;
                let path = store_dir.parse_path(&path_s)?;
                Ok(DerivationOutput::InputAddressed(path))
            }
        }
    }

    pub async fn read_output<R>(
        mut source: R,
        store_dir: &StoreDir,
    ) -> Result<DerivationOutput, ReadDerivationError>
    where
        R: AsyncRead + Unpin,
    {
        let path_s = source.read_string().await?;
        let hash_algo = source.read_string().await?;
        let hash = source.read_string().await?;
        Ok(Self::parse_output(store_dir, path_s, hash_algo, hash)?)
    }

    pub async fn write_output<W>(
        &self,
        mut sink: W,
        store_dir: &StoreDir,
        drv_name: &str,
        output_name: &str,
    ) -> Result<(), WriteDerivationError>
    where
        W: AsyncWrite + Unpin,
    {
        match self {
            DerivationOutput::InputAddressed(path) => {
                sink.write_printed(store_dir, path).await?;
                sink.write_str("").await?;
                sink.write_str("").await?;
            }
            DerivationOutput::CAFixed(dof) => {
                let path = store_dir.make_fixed_output_path(
                    dof.method,
                    dof.hash,
                    &output_path_name(drv_name, output_name),
                    &StorePathSet::new(),
                    false,
                )?;
                sink.write_printed(store_dir, &path).await?;
                sink.write_str(&format!("{:#}", dof)).await?;
                sink.write_str(&format!("{:#x}", dof.hash)).await?;
            }
            DerivationOutput::CAFloating { method, hash_type } => {
                sink.write_str("").await?;
                let hash_algo = match method {
                    FileIngestionMethod::Recursive => format!("r:{}", hash_type),
                    FileIngestionMethod::Flat => hash_type.to_string(),
                };
                sink.write_str(&hash_algo).await?;
                sink.write_str("").await?;
            }
            DerivationOutput::Deferred => {
                sink.write_str("").await?;
                sink.write_str("").await?;
                sink.write_str("").await?;
            }
        }
        Ok(())
    }

    pub fn path(
        &self,
        store_dir: &StoreDir,
        drv_name: &str,
        output_name: &str,
    ) -> Result<Option<StorePath>, ParseStorePathError> {
        match self {
            DerivationOutput::InputAddressed(path) => Ok(Some(path.clone())),
            DerivationOutput::CAFixed(dof) => Ok(Some(store_dir.make_fixed_output_path(
                dof.method,
                dof.hash,
                &output_path_name(drv_name, output_name),
                &StorePathSet::new(),
                false,
            )?)),
            DerivationOutput::CAFloating { .. } => Ok(None),
            DerivationOutput::Deferred => Ok(None),
        }
    }
}

pub type DerivationOutputs = BTreeMap<String, DerivationOutput>;

/// These are analogues to the previous DerivationOutputs data type, but they
/// also contains, for each output, the (optional) store path in which it would
/// be written. To calculate values of these types, see the corresponding
/// functions in BasicDerivation
pub type DerivationOutputsAndOptPaths = BTreeMap<String, (DerivationOutput, Option<StorePath>)>;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct BasicDerivation {
    pub outputs: DerivationOutputs, /* keyed on symbolic IDs */
    pub input_srcs: StorePathSet,   /* inputs that are sources */
    pub platform: String,
    pub builder: PathBuf,
    //#[serde(rename = "args")]
    pub arguments: Vec<String>,
    pub env: Vec<(String, String)>,
    pub name: String,
}

impl BasicDerivation {
    pub fn outputs_and_opt_paths(
        &self,
        store_dir: &StoreDir,
    ) -> Result<DerivationOutputsAndOptPaths, ParseStorePathError> {
        let mut res = DerivationOutputsAndOptPaths::new();
        for (output_name, drv_output) in self.outputs.iter() {
            res.insert(
                output_name.clone(),
                (
                    drv_output.clone(),
                    drv_output.path(store_dir, &self.name, output_name)?,
                ),
            );
        }
        Ok(res)
    }

    pub async fn read_drv<R>(
        mut source: R,
        store_dir: &StoreDir,
        name: &str,
    ) -> Result<BasicDerivation, ReadDerivationError>
    where
        R: AsyncRead + Unpin,
    {
        let name = name.to_owned();
        let nr = source.read_usize().await?;
        let mut outputs = DerivationOutputs::new();
        for _n in 0..nr {
            let name = source.read_string().await?;
            let output = DerivationOutput::read_output(&mut source, store_dir).await?;
            outputs.insert(name, output);
        }
        let input_srcs = source.read_parsed_coll(store_dir).await?;
        let platform = source.read_string().await?;
        let builder_s = source.read_string().await?;
        let builder = PathBuf::from(builder_s);
        let arguments = source.read_string_coll().await?;

        let nr = source.read_usize().await?;
        let mut env = Vec::with_capacity(nr);
        for _n in 0..nr {
            let name = source.read_string().await?;
            let value = source.read_string().await?;
            env.push((name, value));
        }
        Ok(BasicDerivation {
            env,
            name,
            arguments,
            builder,
            platform,
            input_srcs,
            outputs,
        })
    }

    pub async fn write_drv<W>(
        &self,
        mut sink: W,
        store_dir: &StoreDir,
    ) -> Result<(), WriteDerivationError>
    where
        W: AsyncWrite + Unpin,
    {
        sink.write_usize(self.outputs.len()).await?;
        for (name, output) in self.outputs.iter() {
            sink.write_str(name).await?;
            output
                .write_output(&mut sink, store_dir, &self.name, name)
                .await?;
        }
        sink.write_printed_coll(store_dir, &self.input_srcs).await?;
        sink.write_str(&self.platform).await?;
        sink.write_str(
            self.builder
                .to_str()
                .ok_or_else(|| WriteDerivationError::InvalidBuilder(self.builder.clone()))?,
        )
        .await?;
        sink.write_string_coll(&self.arguments).await?;

        sink.write_usize(self.env.len()).await?;
        for (name, value) in self.env.iter() {
            sink.write_str(name).await?;
            sink.write_str(value).await?;
        }
        Ok(())
    }
}

#[cfg(any(test, feature = "test"))]
pub mod proptest {
    use super::*;
    use crate::proptest::arb_path;
    use crate::store::path::proptest::arb_output_name;
    use ::proptest::prelude::*;

    impl Arbitrary for DerivationOutput {
        type Parameters = ();
        type Strategy = BoxedStrategy<DerivationOutput>;

        fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
            arb_derivation_output().boxed()
        }
    }

    pub fn arb_derivation_output() -> impl Strategy<Value = DerivationOutput> {
        use DerivationOutput::*;
        prop_oneof![
            any::<StorePath>().prop_map(|s| InputAddressed(s)),
            any::<FixedOutputHash>().prop_map(|s| CAFixed(s)),
            (any::<FileIngestionMethod>(), any::<hash::Algorithm>())
                .prop_map(|(method, hash_type)| CAFloating { method, hash_type }),
            Just(Deferred)
        ]
    }

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
            outputs in prop::collection::btree_map(arb_output_name(), any::<DerivationOutput>(), 0..15),
            input_srcs in any::<StorePathSet>(),
            platform in any::<String>(),
            builder in arb_path(),
            arguments in any::<Vec<String>>(),
            env in any::<Vec<(String, String)>>(),
            name in any::<String>()
        ) -> BasicDerivation
        {
            BasicDerivation {
                outputs, input_srcs, platform, builder, arguments, env, name,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_derivation_output_parse_input_addressed() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let path_s = "/nix/store/7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3".to_owned();
        let path = store_dir.parse_path(&path_s).unwrap();
        let p = DerivationOutput::parse_output(&store_dir, path_s, "".into(), "".into());
        assert_eq!(p, Ok(DerivationOutput::InputAddressed(path)));
    }

    #[test]
    fn test_derivation_output_parse_ca_fixed() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let path_s = "/nix/store/7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3".to_owned();
        let hash = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad".to_owned();
        let h = Hash::parse_non_sri_unprefixed(&hash, hash::Algorithm::SHA256).unwrap();
        let p = DerivationOutput::parse_output(&store_dir, path_s, "sha256".into(), hash);
        assert_eq!(
            p,
            Ok(DerivationOutput::CAFixed(FixedOutputHash {
                method: FileIngestionMethod::Flat,
                hash: h
            }))
        );
    }

    #[test]
    fn test_derivation_output_parse_ca_fixed_recursive() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let path_s = "/nix/store/7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3".to_owned();
        let hash = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad".to_owned();
        let h = Hash::parse_non_sri_unprefixed(&hash, hash::Algorithm::SHA256).unwrap();
        let p = DerivationOutput::parse_output(&store_dir, path_s, "r:sha256".into(), hash);
        assert_eq!(
            p,
            Ok(DerivationOutput::CAFixed(FixedOutputHash {
                method: FileIngestionMethod::Recursive,
                hash: h
            }))
        );
    }

    #[test]
    fn test_derivation_output_parse_ca_floating() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let p = DerivationOutput::parse_output(&store_dir, "".into(), "sha256".into(), "".into());
        assert_eq!(
            p,
            Ok(DerivationOutput::CAFloating {
                method: FileIngestionMethod::Flat,
                hash_type: hash::Algorithm::SHA256
            })
        );
    }

    #[test]
    fn test_derivation_output_parse_ca_floating_recursive() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let p = DerivationOutput::parse_output(&store_dir, "".into(), "r:sha256".into(), "".into());
        assert_eq!(
            p,
            Ok(DerivationOutput::CAFloating {
                method: FileIngestionMethod::Recursive,
                hash_type: hash::Algorithm::SHA256
            })
        );
    }

    #[test]
    fn test_derivation_output_parse_deferred() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let p = DerivationOutput::parse_output(&store_dir, "".into(), "".into(), "".into());
        assert_eq!(p, Ok(DerivationOutput::Deferred));
    }

    #[test]
    fn test_derivation_output_path_input_address() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let path = store_dir
            .parse_path("/nix/store/ivz5kvk528akza21x33r8jn2wl8bpsw3-konsole-18.12.3")
            .unwrap();
        let drv_out = DerivationOutput::InputAddressed(path);
        assert_eq!(
            drv_out.path(&store_dir, "konsole-18.12.3", "out").unwrap(),
            Some(
                store_dir
                    .parse_path("/nix/store/ivz5kvk528akza21x33r8jn2wl8bpsw3-konsole-18.12.3")
                    .unwrap()
            )
        );
    }

    #[test]
    fn test_derivation_output_path_fixed_output() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let hash = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad".to_owned();
        let h = Hash::parse_non_sri_unprefixed(&hash, hash::Algorithm::SHA256).unwrap();
        let drv_out = DerivationOutput::CAFixed(FixedOutputHash {
            method: FileIngestionMethod::Recursive,
            hash: h,
        });
        assert_eq!(
            drv_out.path(&store_dir, "konsole-18.12.3", "out").unwrap(),
            Some(
                store_dir
                    .parse_path("/nix/store/ivz5kvk528akza21x33r8jn2wl8bpsw3-konsole-18.12.3")
                    .unwrap()
            )
        );
    }

    #[test]
    fn test_derivation_output_path_ca_floating() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let drv_out = DerivationOutput::CAFloating {
            method: FileIngestionMethod::Flat,
            hash_type: hash::Algorithm::SHA256,
        };
        assert_eq!(
            drv_out.path(&store_dir, "konsole-18.12.3", "out").unwrap(),
            None
        );
    }

    #[test]
    fn test_derivation_output_path_deferred() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let drv_out = DerivationOutput::Deferred;
        assert_eq!(
            drv_out.path(&store_dir, "konsole-18.12.3", "out").unwrap(),
            None
        );
    }
}
