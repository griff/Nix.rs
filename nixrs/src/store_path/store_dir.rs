use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use thiserror::Error;

use crate::hash;

use super::create::Fingerprint;
use super::{ContentAddress, StorePath, StorePathNameError};

#[derive(Debug, Error, PartialEq, Eq, Hash)]
#[error("path '{}' is not a store dir", .path.display())]
pub struct StoreDirError {
    path: PathBuf,
}

/// Store directory.
/// Since the [`StorePath`] abstraction is only a hash and a name we need this
/// to convert the path to a full store path string.
///
/// ```
/// # use nixrs::store_path::{StoreDir, StorePath};
/// let store = StoreDir::default();
/// let path : StorePath = store.parse("/nix/store/55xkmqns51sw7nrgykp5vnz36w4fr3cw-nix-2.1.3").unwrap();
/// assert_eq!("55xkmqns51sw7nrgykp5vnz36w4fr3cw-nix-2.1.3", path.to_string());
/// ```
///
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StoreDir(Arc<PathBuf>, Arc<String>);

impl StoreDir {
    /// Create a new StoreDir from given path.
    /// This can fail if the path contains non-UTF-8 characters and therefore can't be
    /// converted to a [`String`].
    pub fn new<P: Into<PathBuf>>(path: P) -> Result<StoreDir, StoreDirError> {
        let path = path.into();
        let path_s = path
            .to_str()
            .ok_or_else(|| StoreDirError { path: path.clone() })?
            .to_string();
        Ok(StoreDir(Arc::new(path), Arc::new(path_s)))
    }

    /// Get [`str`] representation of this StoreDir.
    ///
    /// ```
    /// # use nixrs::store_path::StoreDir;
    /// let store = StoreDir::new("/nix/store").unwrap();
    /// assert_eq!("/nix/store", store.to_str());
    /// ```
    pub fn to_str(&self) -> &str {
        self.1.as_ref()
    }

    /// Get [`path`] representation of this StoreDir.
    ///
    /// ```
    /// # use std::path::Path;
    /// # use nixrs::store_path::StoreDir;
    /// let store = StoreDir::new("/nix/store").unwrap();
    /// assert_eq!(Path::new("/nix/store"), store.to_path());
    /// ```
    pub fn to_path(&self) -> &Path {
        self.0.as_ref()
    }

    pub fn parse<F>(&self, s: &str) -> Result<F, F::Error>
    where
        F: FromStoreDirStr,
    {
        F::from_store_dir_str(self, s)
    }

    pub fn display<'v, V>(&'v self, value: &'v V) -> StoreDirDisplayImpl<'v, V>
    where
        V: StoreDirDisplay,
    {
        StoreDirDisplayImpl {
            store_dir: self,
            value,
        }
    }

    pub fn make_store_path_from_ca(
        &self,
        name: &str,
        ca: ContentAddress,
    ) -> Result<StorePath, StorePathNameError> {
        let path_type = ca.into();
        let fingerprint = Fingerprint { name, path_type };
        let finger_print_s = self.display(&fingerprint).to_string();
        StorePath::from_hash(&hash::Sha256::digest(finger_print_s), name)
    }
}

impl AsRef<Path> for StoreDir {
    fn as_ref(&self) -> &Path {
        self.to_path()
    }
}

impl AsRef<str> for StoreDir {
    fn as_ref(&self) -> &str {
        self.to_str()
    }
}

impl Default for StoreDir {
    fn default() -> Self {
        StoreDir::new("/nix/store").unwrap()
    }
}

impl fmt::Display for StoreDir {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_str())
    }
}

pub trait FromStoreDirStr: Sized {
    type Error: std::error::Error;
    fn from_store_dir_str(store_dir: &StoreDir, s: &str) -> Result<Self, Self::Error>;
}

pub trait StoreDirDisplay {
    fn fmt(&self, store_dir: &StoreDir, f: &mut fmt::Formatter<'_>) -> fmt::Result;
}

pub struct StoreDirDisplayImpl<'v, V: StoreDirDisplay> {
    store_dir: &'v StoreDir,
    value: &'v V,
}

impl<V> fmt::Display for StoreDirDisplayImpl<'_, V>
where
    V: StoreDirDisplay,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        StoreDirDisplay::fmt(self.value, self.store_dir, f)
    }
}

#[cfg(any(test, feature = "test"))]
pub mod proptest {
    use std::path::{PathBuf, MAIN_SEPARATOR_STR};

    use proptest::prelude::{any, Arbitrary, BoxedStrategy, Strategy};

    use super::StoreDir;

    pub fn arb_store_dir() -> impl Strategy<Value = StoreDir> {
        (any::<PathBuf>()).prop_map(|mut path| {
            if !path.is_absolute() {
                let mut out = PathBuf::new();
                out.push(MAIN_SEPARATOR_STR);
                out.push(path);
                path = out;
            }
            StoreDir::new(path).unwrap()
        })
    }

    impl Arbitrary for StoreDir {
        type Parameters = ();
        type Strategy = BoxedStrategy<StoreDir>;
        fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
            arb_store_dir().boxed()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::StoreDir;
    use pretty_assertions::assert_eq;
    use std::path::Path;

    #[test]
    fn test_store_dir_display() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        assert_eq!(format!("{}", store_dir), "/nix/store");
        assert_eq!(store_dir.to_str(), "/nix/store");
        let s: &str = store_dir.as_ref();
        assert_eq!(s, "/nix/store");
    }

    #[test]
    fn test_store_dir_as_ref() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let s: &Path = store_dir.as_ref();
        assert_eq!(s, Path::new("/nix/store"));
    }
}
