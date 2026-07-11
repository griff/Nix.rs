use std::{fmt, ops::Deref, path::Path, str::FromStr};

use serde_with::{DeserializeFromStr, SerializeDisplay};

use crate::store_path::{HasStoreDir, ParseStorePathError, StorePathError};

use super::{StoreDir, StorePath};

#[derive(
    Debug, Clone, SerializeDisplay, DeserializeFromStr, PartialEq, Eq, PartialOrd, Ord, Hash,
)]
pub struct FullStorePath {
    pub(crate) store_dir: StoreDir,
    pub(crate) path: StorePath,
}

impl HasStoreDir for FullStorePath {
    fn store_dir(&self) -> &StoreDir {
        &self.store_dir
    }
}

impl FullStorePath {
    pub fn path(&self) -> &StorePath {
        &self.path
    }

    pub fn into_inner(self) -> (StoreDir, StorePath) {
        (self.store_dir, self.path)
    }
}

impl Deref for FullStorePath {
    type Target = StorePath;

    fn deref(&self) -> &Self::Target {
        self.path()
    }
}

impl AsRef<StoreDir> for FullStorePath {
    fn as_ref(&self) -> &StoreDir {
        self.store_dir()
    }
}

impl AsRef<StorePath> for FullStorePath {
    fn as_ref(&self) -> &StorePath {
        self.path()
    }
}

impl From<FullStorePath> for StorePath {
    fn from(value: FullStorePath) -> Self {
        value.path
    }
}

impl From<FullStorePath> for StoreDir {
    fn from(value: FullStorePath) -> Self {
        value.store_dir
    }
}

impl fmt::Display for FullStorePath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.store_dir.display(&self.path))
    }
}

impl FromStr for FullStorePath {
    type Err = ParseStorePathError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let path = Path::new(s);

        if let Some(dir) = path.parent() {
            let store_dir = StoreDir::new(dir).map_err(|_| ParseStorePathError {
                path: s.to_string(),
                error: StorePathError::NotInStore,
            })?;
            let path = store_dir.parse(s)?;
            Ok(FullStorePath { store_dir, path })
        } else {
            Err(ParseStorePathError {
                path: s.to_string(),
                error: StorePathError::NonAbsolute,
            })
        }
    }
}
