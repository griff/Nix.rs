use std::collections::BTreeMap;
use std::path::PathBuf;

use crate::flag_enum;
use bstr::ByteSlice;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::hash;
use crate::io::{AsyncSink, AsyncSource};
use crate::store_path::{
    ContentAddress, ContentAddressMethod, ContentAddressWithReferences, StorePathSet,
};
use crate::store_path::{ParseStorePathError, ReadStorePathError, StoreDir, StorePath};

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
    if s.is_empty() || !s.starts_with('/') {
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
    CAFixed(ContentAddress),
    /// Floating-output derivations, whose output paths are content addressed, but
    /// not fixed, and so are dynamically calculated from whatever the output ends
    /// up being.
    CAFloating {
        method: ContentAddressMethod,
        hash_type: hash::Algorithm,
    },
    /// Input-addressed output which depends on a (CA) derivation whose hash isn't
    /// known atm.
    Deferred,

    /// Impure output which is moved to a content-addressed location (like
    /// CAFloating) but isn't registered as a realization.
    Impure {
        /// How the file system objects will be serialized for hashing
        method: ContentAddressMethod,

        /// How the serialization will be hashed
        hash_type: hash::Algorithm,
    },
}

impl DerivationOutput {
    pub fn parse_output(
        store_dir: &StoreDir,
        path_s: String,
        hash_algo: String,
        hash: String,
    ) -> Result<DerivationOutput, ParseDerivationError> {
        if !hash_algo.is_empty() {
            let (method, algo) = ContentAddressMethod::parse_prefix(&hash_algo);
            /*
                if method == ContentAddressMethod::Text {
                    // TODO: experimentalFeatureSettings.require(Xp::DynamicDerivations);
                }
            */
            let hash_type = algo.parse::<hash::Algorithm>()?;
            if hash == "impure" {
                // TODO: experimentalFeatureSettings.require(Xp::ImpureDerivations);
                assert_eq!(path_s, "");
                Ok(DerivationOutput::Impure { method, hash_type })
            } else if !hash.is_empty() {
                validate_path(&path_s)?;
                let hash = hash::Hash::parse_non_sri_unprefixed(&hash, hash_type)?;
                let hash = ContentAddress { method, hash };
                Ok(DerivationOutput::CAFixed(hash))
            } else {
                // TODO: settings.requireExperimentalFeature("ca-derivations");
                assert_eq!(path_s, "");
                Ok(DerivationOutput::CAFloating { method, hash_type })
            }
        } else if path_s.is_empty() {
            Ok(DerivationOutput::Deferred)
        } else {
            validate_path(&path_s)?;
            let path = store_dir.parse_path(&path_s)?;
            Ok(DerivationOutput::InputAddressed(path))
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
            DerivationOutput::CAFixed(ca) => {
                let path = store_dir.make_fixed_output_path_from_ca(
                    &output_path_name(drv_name, output_name),
                    &ContentAddressWithReferences::without_refs(*ca),
                )?;
                sink.write_printed(store_dir, &path).await?;
                sink.write_string(format!("{:#}", ca)).await?;
                sink.write_string(format!("{:#x}", ca.hash)).await?;
            }
            DerivationOutput::CAFloating { method, hash_type } => {
                sink.write_str("").await?;
                sink.write_string(format!("{}{}", method, hash_type))
                    .await?;
                sink.write_str("").await?;
            }
            DerivationOutput::Deferred => {
                sink.write_str("").await?;
                sink.write_str("").await?;
                sink.write_str("").await?;
            }
            DerivationOutput::Impure { method, hash_type } => {
                sink.write_str("").await?;
                sink.write_string(format!("{}{}", method, hash_type))
                    .await?;
                sink.write_str("impure").await?;
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
            DerivationOutput::CAFixed(ca) => Ok(Some(store_dir.make_fixed_output_path_from_ca(
                &output_path_name(drv_name, output_name),
                &ContentAddressWithReferences::without_refs(*ca),
            )?)),
            DerivationOutput::CAFloating { .. } => Ok(None),
            DerivationOutput::Deferred => Ok(None),
            DerivationOutput::Impure { .. } => Ok(None),
        }
    }
}

pub type DerivationOutputs = BTreeMap<String, DerivationOutput>;

/// These are analogues to the previous DerivationOutputs data type, but they
/// also contains, for each output, the (optional) store path in which it would
/// be written. To calculate values of these types, see the corresponding
/// functions in BasicDerivation
pub type DerivationOutputsAndOptPaths = BTreeMap<String, (DerivationOutput, Option<StorePath>)>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DerivationType {
    /// Input-addressed derivation types
    InputAddressed {
        /// True iff the derivation type can't be determined statically,
        /// for instance because it (transitively) depends on a content-addressed
        /// derivation.
        deferred: bool,
    },
    /// Content-addressed derivation types
    ContentAddressed {
        /// Whether the derivation should be built safely inside a sandbox.
        sandboxed: bool,

        /// Whether the derivation's outputs' content-addresses are "fixed"
        /// or "floating".
        ///
        ///  - Fixed: content-addresses are written down as part of the
        ///    derivation itself. If the outputs don't end up matching the
        ///    build fails.
        ///
        ///  - Floating: content-addresses are not written down, we do not
        ///    know them until we perform the build.
        fixed: bool,
    },
    /// Impure derivation type
    ///
    /// This is similar at buil-time to the content addressed, not standboxed, not fixed
    /// type, but has some restrictions on its usage.
    Impure,
}

impl DerivationType {
    /// Do the outputs of the derivation have paths calculated from their
    /// content, or from the derivation itself?
    pub const fn is_ca(&self) -> bool {
        /*
        Normally we do the full `std::visit` to make sure we have
        exhaustively handled all variants, but so long as there is a
        variant called `ContentAddressed`, it must be the only one for
        which `is_ca` is true for this to make sense!.
        */
        match self {
            Self::InputAddressed { .. } => false,
            Self::ContentAddressed { .. } => true,
            Self::Impure => true,
        }
    }

    /// Is the content of the outputs fixed <em>a priori</em> via a hash?
    /// Never true for non-CA derivations.
    pub const fn is_fixed(&self) -> bool {
        match self {
            Self::InputAddressed { .. } => false,
            Self::ContentAddressed { fixed, .. } => *fixed,
            Self::Impure => false,
        }
    }

    /// Whether the derivation is fully sandboxed. If false, the sandbox
    /// is opened up, e.g. the derivation has access to the network. Note
    /// that whether or not we actually sandbox the derivation is
    /// controlled separately. Always true for non-CA derivations.
    pub const fn is_sandboxed(&self) -> bool {
        match self {
            Self::InputAddressed { .. } => true,
            Self::ContentAddressed { sandboxed, .. } => *sandboxed,
            Self::Impure => false,
        }
    }

    /// Whether the derivation is expected to produce the same result
    /// every time, and therefore it only needs to be built once. This is
    /// only false for derivations that have the attribute '__impure =
    /// true'.
    pub const fn is_pure(&self) -> bool {
        match self {
            Self::InputAddressed { .. } => true,
            Self::ContentAddressed { .. } => true,
            Self::Impure => false,
        }
    }

    /// Does the derivation knows its own output paths?
    /// Only true when there's no floating-ca derivation involved in the
    /// closure, or if fixed output.
    pub const fn has_known_output_paths(&self) -> bool {
        match self {
            Self::InputAddressed { deferred } => !*deferred,
            Self::ContentAddressed { fixed, .. } => *fixed,
            Self::Impure => false,
        }
    }
}

#[derive(Debug, Error)]
pub enum DerivationOutputsError {
    #[error("only one fixed output is allowed for now")]
    OnlyOneFixedOutputAllowed,
    #[error("single fixed output must be named \"out\"")]
    InvalidFixedOutputName,
    #[error("must have at least one output")]
    MissingOutput,
    #[error("all floating outputs must use the same hash type")]
    MixedOutputHash,
    #[error("can't mix derivation output types")]
    MixedOutputTypes,
}

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
    pub fn drv_type(&self) -> Result<DerivationType, DerivationOutputsError> {
        let mut has_input_addressed_outputs = false;
        let mut has_fixed_ca_outputs = false;
        let mut has_floating_ca_outputs = false;
        let mut has_deferred_ia_outputs = false;
        let mut has_impure_outputs = false;
        let mut floating_hash_type = None;
        for (name, output) in self.outputs.iter() {
            match output {
                DerivationOutput::InputAddressed(_) => {
                    has_input_addressed_outputs = true;
                }
                DerivationOutput::CAFixed(_) => {
                    if has_fixed_ca_outputs {
                        // FIXME: Experimental feature?
                        return Err(DerivationOutputsError::OnlyOneFixedOutputAllowed);
                    }
                    if name != "out" {
                        return Err(DerivationOutputsError::InvalidFixedOutputName);
                    }
                    has_fixed_ca_outputs = true;
                }
                DerivationOutput::CAFloating { hash_type, .. } => {
                    has_floating_ca_outputs = true;
                    if let Some(float) = floating_hash_type {
                        if float != hash_type {
                            return Err(DerivationOutputsError::MixedOutputHash);
                        }
                    } else {
                        floating_hash_type = Some(hash_type);
                    }
                }
                DerivationOutput::Deferred => {
                    has_deferred_ia_outputs = true;
                }
                DerivationOutput::Impure { .. } => {
                    has_impure_outputs = true;
                }
            }
        }
        if !has_input_addressed_outputs
            && !has_fixed_ca_outputs
            && !has_floating_ca_outputs
            && !has_deferred_ia_outputs
            && !has_impure_outputs
        {
            return Err(DerivationOutputsError::MissingOutput);
        }

        if has_input_addressed_outputs
            && !has_fixed_ca_outputs
            && !has_floating_ca_outputs
            && !has_deferred_ia_outputs
            && !has_impure_outputs
        {
            return Ok(DerivationType::InputAddressed { deferred: false });
        }

        if !has_input_addressed_outputs
            && has_fixed_ca_outputs
            && !has_floating_ca_outputs
            && !has_deferred_ia_outputs
            && !has_impure_outputs
        {
            return Ok(DerivationType::ContentAddressed {
                sandboxed: false,
                fixed: true,
            });
        }
        if !has_input_addressed_outputs
            && !has_fixed_ca_outputs
            && has_floating_ca_outputs
            && !has_deferred_ia_outputs
            && !has_impure_outputs
        {
            return Ok(DerivationType::ContentAddressed {
                sandboxed: true,
                fixed: false,
            });
        }

        if !has_input_addressed_outputs
            && !has_fixed_ca_outputs
            && !has_floating_ca_outputs
            && has_deferred_ia_outputs
            && !has_impure_outputs
        {
            return Ok(DerivationType::InputAddressed { deferred: true });
        }

        if !has_input_addressed_outputs
            && !has_fixed_ca_outputs
            && !has_floating_ca_outputs
            && !has_deferred_ia_outputs
            && has_impure_outputs
        {
            return Ok(DerivationType::Impure);
        }

        Err(DerivationOutputsError::MixedOutputTypes)
    }

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
        let bstr = <[u8]>::from_path(&self.builder)
            .ok_or_else(|| WriteDerivationError::InvalidBuilder(self.builder.clone()))?;
        sink.write_buf(bstr).await?;
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
    use crate::store_path::proptest::arb_output_name;
    use ::proptest::prelude::*;
    use ::proptest::sample::SizeRange;

    impl Arbitrary for DerivationType {
        type Parameters = ();
        type Strategy = BoxedStrategy<DerivationType>;

        fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
            arb_derivation_type().boxed()
        }
    }

    pub fn arb_derivation_type() -> impl Strategy<Value = DerivationType> {
        use DerivationType::*;
        prop_oneof![
            ::proptest::bool::ANY.prop_map(|deferred| InputAddressed { deferred }),
            (::proptest::bool::ANY, ::proptest::bool::ANY)
                .prop_map(|(sandboxed, fixed)| ContentAddressed { sandboxed, fixed }),
            Just(Impure)
        ]
    }

    pub fn arb_derivation_outputs(
        size: impl Into<SizeRange>,
    ) -> impl Strategy<Value = DerivationOutputs> {
        use DerivationOutput::*;
        let size = size.into();
        let size2 = size.clone();
        prop_oneof![
            //InputAddressed
            prop::collection::btree_map(
                arb_output_name(),
                arb_derivation_output_input_addressed(),
                size.clone()
            ),
            // CAFixed
            arb_derivation_output_fixed().prop_map(|ca| {
                let mut ret = BTreeMap::new();
                ret.insert("out".to_string(), ca);
                ret
            }),
            // CAFloating
            any::<hash::Algorithm>().prop_flat_map(move |hash_type| {
                prop::collection::btree_map(
                    arb_output_name(),
                    arb_derivation_output_floating(Just(hash_type)),
                    size2.clone(),
                )
            }),
            /*
            prop::collection::btree_map(
                arb_output_name(),
                arb_derivation_output_floating(),
                size.clone()).prop_map(|mut map| {
                    let mut first_hash = None;
                    for (_, value) in map.iter_mut() {
                        if let DerivationOutput::CAFloating { ref mut hash_type, .. } = value {
                            if first_hash.is_none() {
                                first_hash = Some(*hash_type);
                            } else {
                                *hash_type = first_hash.unwrap();
                            }
                        }
                    }
                    map
                }),
             */
            // Deferred
            prop::collection::btree_map(arb_output_name(), Just(Deferred), size.clone()),
            // Impure
            prop::collection::btree_map(
                arb_output_name(),
                arb_derivation_output_impure(),
                size.clone()
            ),
        ]
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

    pub fn arb_derivation_output_fixed() -> impl Strategy<Value = DerivationOutput> {
        any::<ContentAddress>().prop_map(DerivationOutput::CAFixed)
    }

    pub fn arb_derivation_output_impure() -> impl Strategy<Value = DerivationOutput> {
        (any::<ContentAddressMethod>(), any::<hash::Algorithm>())
            .prop_map(|(method, hash_type)| DerivationOutput::Impure { method, hash_type })
    }

    pub fn arb_derivation_output_floating<H>(
        hash_type: H,
    ) -> impl Strategy<Value = DerivationOutput>
    where
        H: Strategy<Value = hash::Algorithm>,
    {
        (any::<ContentAddressMethod>(), hash_type)
            .prop_map(|(method, hash_type)| DerivationOutput::CAFloating { method, hash_type })
    }

    pub fn arb_derivation_output() -> impl Strategy<Value = DerivationOutput> {
        use DerivationOutput::*;
        prop_oneof![
            arb_derivation_output_input_addressed(),
            arb_derivation_output_fixed(),
            arb_derivation_output_floating(any::<hash::Algorithm>()),
            Just(Deferred),
            arb_derivation_output_impure(),
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
            outputs in arb_derivation_outputs(1..15),
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
    use crate::store_path::FileIngestionMethod;

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
        let h = hash::Hash::parse_non_sri_unprefixed(&hash, hash::Algorithm::SHA256).unwrap();
        let p = DerivationOutput::parse_output(&store_dir, path_s, "sha256".into(), hash);
        assert_eq!(
            p,
            Ok(DerivationOutput::CAFixed(ContentAddress::fixed(
                FileIngestionMethod::Flat,
                h
            )))
        );
    }

    #[test]
    fn test_derivation_output_parse_ca_fixed_recursive() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let path_s = "/nix/store/7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3".to_owned();
        let hash = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad".to_owned();
        let h = hash::Hash::parse_non_sri_unprefixed(&hash, hash::Algorithm::SHA256).unwrap();
        let p = DerivationOutput::parse_output(&store_dir, path_s, "r:sha256".into(), hash);
        assert_eq!(
            p,
            Ok(DerivationOutput::CAFixed(ContentAddress::fixed(
                FileIngestionMethod::Recursive,
                h
            )))
        );
    }

    #[test]
    fn test_derivation_output_parse_ca_fixed_text() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let path_s = "/nix/store/7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3".to_owned();
        let hash = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad".to_owned();
        let h = hash::Hash::parse_non_sri_unprefixed(&hash, hash::Algorithm::SHA256).unwrap();
        let p = DerivationOutput::parse_output(&store_dir, path_s, "text:sha256".into(), hash);
        assert_eq!(p, Ok(DerivationOutput::CAFixed(ContentAddress::text(h))));
    }

    #[test]
    fn test_derivation_output_parse_ca_floating() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let p = DerivationOutput::parse_output(&store_dir, "".into(), "sha256".into(), "".into());
        assert_eq!(
            p,
            Ok(DerivationOutput::CAFloating {
                method: ContentAddressMethod::Fixed(FileIngestionMethod::Flat),
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
                method: ContentAddressMethod::Fixed(FileIngestionMethod::Recursive),
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
        let h = hash::Hash::parse_non_sri_unprefixed(&hash, hash::Algorithm::SHA256).unwrap();
        let drv_out =
            DerivationOutput::CAFixed(ContentAddress::fixed(FileIngestionMethod::Recursive, h));
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
            method: ContentAddressMethod::Fixed(FileIngestionMethod::Flat),
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
