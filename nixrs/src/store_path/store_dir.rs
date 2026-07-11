use std::fmt;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use smol_str::SmolStr;
use thiserror::Error;

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
pub struct StoreDir(SmolStr);

const DEFAULT_DIR: &StoreDir = &StoreDir::from_static("/nix/store");

impl StoreDir {
    pub const fn nix_store() -> &'static StoreDir {
        DEFAULT_DIR
    }

    pub const fn from_static(dir: &'static str) -> Self {
        assert!(*(dir.as_bytes().first().expect("non-empty store dir")) == b'/');
        Self(SmolStr::new_static(dir))
    }

    /// Create a new StoreDir from given path.
    /// This can fail if the path contains non-UTF-8 characters and therefore can't be
    /// converted to a [`String`].
    pub fn new<P: AsRef<Path>>(path: P) -> Result<StoreDir, StoreDirError> {
        let path = path.as_ref();
        let dir = path.to_str().ok_or_else(|| StoreDirError {
            path: path.to_path_buf(),
        })?;
        Self::from_str(dir)
    }

    pub fn is_default(&self) -> bool {
        self == DEFAULT_DIR
    }

    /// Return length of StoreDir.
    ///
    /// ```
    /// # use nixrs::store_path::StoreDir;
    /// let store = StoreDir::new("/nix/store").unwrap();
    /// assert_eq!(10, store.len());
    /// ```
    #[expect(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.0.as_str().len()
    }

    /// Get [`str`] representation of this StoreDir.
    ///
    /// ```
    /// # use nixrs::store_path::StoreDir;
    /// let store = StoreDir::new("/nix/store").unwrap();
    /// assert_eq!("/nix/store", store.to_str());
    /// ```
    pub fn to_str(&self) -> &str {
        &self.0
    }

    /// Get [`Path`] representation of this StoreDir.
    ///
    /// ```
    /// # use std::path::Path;
    /// # use nixrs::store_path::StoreDir;
    /// let store = StoreDir::new("/nix/store").unwrap();
    /// assert_eq!(Path::new("/nix/store"), store.to_path());
    /// ```
    pub fn to_path(&self) -> &Path {
        Path::new(&self.0)
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
}

impl FromStr for StoreDir {
    type Err = StoreDirError;

    fn from_str(dir: &str) -> Result<Self, Self::Err> {
        if dir.starts_with("/") {
            Ok(Self(SmolStr::new(dir)))
        } else {
            Err(StoreDirError {
                path: Path::new(dir).to_path_buf(),
            })
        }
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
        DEFAULT_DIR.clone()
    }
}

impl fmt::Display for StoreDir {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.to_str())
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

pub struct StoreDirDisplayFromFn<F>(F);
impl<F> StoreDirDisplay for StoreDirDisplayFromFn<F>
where
    F: Fn(&StoreDir, &mut fmt::Formatter<'_>) -> fmt::Result,
{
    fn fmt(&self, store_dir: &StoreDir, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        (self.0)(store_dir, f)
    }
}

pub const fn display_from_fn<F>(f: F) -> StoreDirDisplayFromFn<F>
where
    F: Fn(&StoreDir, &mut fmt::Formatter<'_>) -> fmt::Result,
{
    StoreDirDisplayFromFn(f)
}

pub trait HasStoreDir {
    fn store_dir(&self) -> &StoreDir;
}

impl<T: ?Sized + HasStoreDir> HasStoreDir for &mut T {
    fn store_dir(&self) -> &StoreDir {
        (**self).store_dir()
    }
}

#[cfg(test)]
mod unittests {
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
