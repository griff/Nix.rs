use std::{collections::BTreeMap, fmt};

use crate::derived_path::OutputName;
#[cfg(any(feature = "xp-ca-derivations", feature = "xp-impure-derivations"))]
use crate::store_path::ContentAddressMethodAlgorithm;
use crate::store_path::{ContentAddress, StoreDir, StorePath, StorePathName, StorePathNameError};

struct OutputPathName<'b> {
    drv_name: &'b StorePathName,
    output_name: &'b OutputName,
}
impl fmt::Display for OutputPathName<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.output_name.is_default() {
            write!(f, "{}", self.drv_name)
        } else {
            write!(f, "{}-{}", self.drv_name, self.output_name)
        }
    }
}
pub(crate) fn output_path_name<'s>(
    drv_name: &'s StorePathName,
    output_name: &'s OutputName,
) -> impl fmt::Display + 's {
    OutputPathName {
        drv_name,
        output_name,
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
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
        drv_name: &StorePathName,
        output_name: &OutputName,
    ) -> Result<Option<StorePath>, StorePathNameError> {
        match self {
            DerivationOutput::InputAddressed(store_path) => Ok(Some(store_path.clone())),
            DerivationOutput::CAFixed(ca) => {
                let name = output_path_name(drv_name, output_name)
                    .to_string()
                    .parse()?;
                Ok(Some(store_dir.make_store_path_from_ca(name, *ca)))
            }
            _ => Ok(None),
        }
    }
}

pub type DerivationOutputs = BTreeMap<OutputName, DerivationOutput>;

#[cfg(test)]
mod unittests {
    use rstest::rstest;

    use super::DerivationOutput;
    use crate::derived_path::OutputName;
    use crate::store_path::{StorePath, StorePathName, StorePathNameError};

    #[rstest]
    #[case::deffered(DerivationOutput::Deferred, "a", "a", Ok(None))]
    #[case::input(DerivationOutput::InputAddressed("00000000000000000000000000000000-_".parse().unwrap()), "a", "a", Ok(Some("00000000000000000000000000000000-_".parse().unwrap())))]
    #[case::fixed_flat(DerivationOutput::CAFixed("fixed:sha256:248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1".parse().unwrap()), "konsole-18.12.3", "out", Ok(Some("g9ngnw4w5vr9y3xkb7k2awl3mp95abrb-konsole-18.12.3".parse().unwrap())))]
    #[case::fixed_sha1(DerivationOutput::CAFixed("fixed:r:sha1:84983e441c3bd26ebaae4aa1f95129e5e54670f1".parse().unwrap()), "konsole-18.12.3", "out", Ok(Some("ag0y7g6rci9zsdz9nxcq5l1qllx3r99x-konsole-18.12.3".parse().unwrap())))]
    #[case::fixed_source(DerivationOutput::CAFixed("fixed:r:sha256:248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1".parse().unwrap()), "konsole-18.12.3", "out", Ok(Some("1w01xxn8f7s9s4n65ry6rwd7x9awf04s-konsole-18.12.3".parse().unwrap())))]
    fn test_path(
        #[case] output: DerivationOutput,
        #[case] drv_name: StorePathName,
        #[case] output_name: OutputName,
        #[case] path: Result<Option<StorePath>, StorePathNameError>,
    ) {
        let store_dir = Default::default();
        assert_eq!(path, output.path(&store_dir, &drv_name, &output_name))
    }
}
