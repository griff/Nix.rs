use std::str::FromStr;

use crate::store_path::{StorePathName, StorePathNameError, StorePathNameRef};

/// A derivation output name.
///
/// This is a derivation output name, so the 'out' or 'bin' bit that has
/// been verified to not contain invalid characters.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, derive_more::Display)]
pub struct OutputName(pub(crate) String);

impl OutputName {
    /// Returns `true` if this output name is the default of `out`.
    pub fn is_default(&self) -> bool {
        self.0 == "out"
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for OutputName {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Default for OutputName {
    fn default() -> Self {
        OutputName("out".into())
    }
}

impl FromStr for OutputName {
    type Err = StorePathNameError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let name = StorePathNameRef::from_str(s)?.to_string();
        Ok(OutputName(name))
    }
}

pub trait StorePathNameOutput {
    fn output_path_name(
        self,
        output_name: &OutputName,
    ) -> Result<StorePathName, StorePathNameError>;
}

impl StorePathNameOutput for &'_ StorePathNameRef {
    fn output_path_name(
        self,
        output_name: &OutputName,
    ) -> Result<StorePathName, StorePathNameError> {
        if output_name.is_default() {
            Ok(self.to_owned())
        } else {
            StorePathName::from_string(format!("{}-{}", self, output_name))
        }
    }
}
