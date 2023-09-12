use std::collections::BTreeSet;
use std::convert::TryFrom;
use std::fmt;
use std::ops::Deref;
use std::path::Path;

use nixrs_util::path::clean_path;
use nixrs_util::{base32, hash};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::StoreDir;

pub type StorePathSet = BTreeSet<StorePath>;

#[derive(Error, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub enum ParseStorePathError {
    #[error("path '{}' is not a store path", .0.display())]
    BadStorePath(std::path::PathBuf),
    #[error("path '{}' is not in the Nix store", .0.display())]
    NotInStore(std::path::PathBuf),
    #[error("invalid base32 '{1}' in store path")]
    BadBase32(nixrs_util::base32::BadBase32, String),
    #[error("store path name is empty")]
    StorePathNameEmpty,
    #[error("store path name is longer than 211 characters")]
    StorePathNameTooLong,
    #[error("store path name '{0}' contains forbidden character")]
    BadStorePathName(String),
}

#[derive(Error, Debug)]
pub enum ReadStorePathError {
    #[error("{0}")]
    BadStorePath(#[from] ParseStorePathError),
    #[error("io error reading store path {0}")]
    IO(#[from] std::io::Error),
}

/// Extension of derivations in the Nix store.
pub const DRV_EXTENSION: &str = ".drv";

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Hash, Deserialize, Serialize)]
#[serde(try_from = "String", into = "String")]
pub struct StorePath {
    pub hash: StorePathHash,
    pub name: StorePathName,
}

pub const STORE_PATH_HASH_BYTES: usize = 20;
pub const STORE_PATH_HASH_CHARS: usize = 32;

impl StorePath {
    pub fn new(path: &Path, store_dir: &StoreDir) -> Result<Self, ParseStorePathError> {
        if !path.is_absolute() {
            return Err(ParseStorePathError::BadStorePath(path.to_owned()));
        }
        let clean = clean_path(path);
        let path = clean.as_ref();
        if path.parent() != Some(store_dir.as_ref()) {
            return Err(ParseStorePathError::NotInStore(path.into()));
        }
        Self::new_from_base_name(
            path.file_name()
                .ok_or_else(|| ParseStorePathError::BadStorePath(path.into()))?
                .to_str()
                .ok_or_else(|| ParseStorePathError::BadStorePath(path.into()))?,
        )
    }

    pub fn from_parts(
        hash: [u8; STORE_PATH_HASH_BYTES],
        name: &str,
    ) -> Result<Self, ParseStorePathError> {
        Ok(StorePath {
            hash: StorePathHash(hash),
            name: StorePathName::new(name)?,
        })
    }

    pub fn from_hash(hash: &hash::Hash, name: &str) -> Result<Self, ParseStorePathError> {
        Ok(StorePath {
            hash: StorePathHash::new_from_hash(hash),
            name: StorePathName::new(name)?,
        })
    }

    pub fn new_from_base_name(base_name: &str) -> Result<Self, ParseStorePathError> {
        if base_name.len() < STORE_PATH_HASH_CHARS + 1
            || base_name.as_bytes()[STORE_PATH_HASH_CHARS] != '-' as u8
        {
            return Err(ParseStorePathError::BadStorePath(base_name.into()));
        }

        Ok(StorePath {
            hash: StorePathHash::new(&base_name[0..STORE_PATH_HASH_CHARS])?,
            name: StorePathName::new(&base_name[STORE_PATH_HASH_CHARS + 1..])?,
        })
    }

    pub fn print(&self, store_dir: &StoreDir) -> String {
        store_dir.display_path(self).to_string()
    }

    pub fn is_derivation(&self) -> bool {
        self.name.ends_with(DRV_EXTENSION)
    }

    pub fn name_from_drv(&self) -> String {
        let name_with_suffix = self.name.name();
        assert!(name_with_suffix.ends_with(DRV_EXTENSION));

        name_with_suffix[..(name_with_suffix.len() - DRV_EXTENSION.len())].to_owned()
    }
}

impl fmt::Display for StorePath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.hash, self.name)
    }
}

impl TryFrom<String> for StorePath {
    type Error = ParseStorePathError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        StorePath::new_from_base_name(&value)
    }
}

impl From<StorePath> for String {
    fn from(path: StorePath) -> Self {
        path.to_string()
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct StorePathHash([u8; STORE_PATH_HASH_BYTES]);

impl StorePathHash {
    pub fn new(s: &str) -> Result<Self, ParseStorePathError> {
        assert_eq!(s.len(), STORE_PATH_HASH_CHARS);
        let v = base32::decode(s).map_err(|e| ParseStorePathError::BadBase32(e, s.into()))?;
        assert_eq!(v.len(), STORE_PATH_HASH_BYTES);
        let mut bytes = [0u8; STORE_PATH_HASH_BYTES];
        bytes.copy_from_slice(&v[0..STORE_PATH_HASH_BYTES]);
        Ok(Self(bytes))
    }

    pub fn new_from_hash(hash: &hash::Hash) -> Self {
        let mut bytes = [0u8; STORE_PATH_HASH_BYTES];
        let buf = hash.as_ref();
        for i in 0..hash.len() {
            let idx = i % STORE_PATH_HASH_BYTES;
            bytes[idx] ^= buf[i];
        }
        StorePathHash(bytes)
    }

    pub fn hash<'a>(&'a self) -> &'a [u8; STORE_PATH_HASH_BYTES] {
        &self.0
    }
}

impl AsRef<[u8]> for StorePathHash {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl Deref for StorePathHash {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl fmt::Display for StorePathHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut buf = vec![0; STORE_PATH_HASH_CHARS];
        base32::encode_into(&self.0, &mut buf);
        f.write_str(std::str::from_utf8(&buf).unwrap())
    }
}
impl fmt::Debug for StorePathHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl Ord for StorePathHash {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Historically we've sorted store paths by their base32
        // serialization, but our base32 encodes bytes in reverse
        // order. So compare them in reverse order as well.
        self.0.iter().rev().cmp(other.0.iter().rev())
    }
}

impl PartialOrd for StorePathHash {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Hash)]
pub struct StorePathName(String);

impl StorePathName {
    pub fn new(s: &str) -> Result<Self, ParseStorePathError> {
        if s.len() == 0 {
            return Err(ParseStorePathError::StorePathNameEmpty);
        }

        if s.len() > 211 {
            return Err(ParseStorePathError::StorePathNameTooLong);
        }

        if s.starts_with('.')
            || !s.chars().all(|c| {
                c.is_ascii_alphabetic()
                    || c.is_ascii_digit()
                    || c == '+'
                    || c == '-'
                    || c == '.'
                    || c == '_'
                    || c == '?'
                    || c == '='
            })
        {
            return Err(ParseStorePathError::BadStorePathName(s.to_string()));
        }

        Ok(Self(s.to_string()))
    }

    pub fn name<'a>(&'a self) -> &'a str {
        &self.0
    }
}

impl fmt::Display for StorePathName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for StorePathName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Deref for StorePathName {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(any(test, feature = "test"))]
pub mod proptest {
    use super::*;
    use ::proptest::{arbitrary::Arbitrary, prelude::*};

    pub fn arb_output_name() -> impl Strategy<Value = String> {
        "[a-zA-Z0-9+\\-_?=][a-zA-Z0-9+\\-_?=.]{0,13}"
    }

    impl Arbitrary for StorePathHash {
        type Parameters = ();
        type Strategy = BoxedStrategy<StorePathHash>;

        fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
            any::<[u8; STORE_PATH_HASH_BYTES]>()
                .prop_map(StorePathHash)
                .boxed()
        }
    }

    pub fn arb_store_path_name(
        max: u8,
        extension: Option<String>,
    ) -> impl Strategy<Value = StorePathName> {
        "[a-zA-Z0-9+\\-_?=][a-zA-Z0-9+\\-_?=.]{0,210}".prop_map(move |mut s| {
            let mut max = max;
            let len = extension.as_ref().map(|e| e.len() + 1).unwrap_or(0) as u8;
            if max > 211 - len {
                max = 211 - len;
            }
            max = max - 1;
            if s.len() > max as usize {
                s.truncate(max as usize);
            }
            if let Some(ext) = extension.as_ref() {
                s.push('.');
                s.push_str(&ext);
            }
            StorePathName::new(&s).unwrap()
        })
    }

    impl Arbitrary for StorePathName {
        type Parameters = Option<String>;
        type Strategy = BoxedStrategy<StorePathName>;

        fn arbitrary_with(args: Self::Parameters) -> Self::Strategy {
            arb_store_path_name(211, args).boxed()
        }
    }

    pub fn arb_store_path(max: u8, extension: Option<String>) -> impl Strategy<Value = StorePath> {
        (any::<StorePathHash>(), arb_store_path_name(max, extension))
            .prop_map(|(hash, name)| StorePath { hash, name })
    }
    pub fn arb_drv_store_path() -> impl Strategy<Value = StorePath> {
        arb_store_path(211 - 4 - 15, Some("drv".into()))
    }

    impl Arbitrary for StorePath {
        type Parameters = Option<String>;
        type Strategy = BoxedStrategy<StorePath>;
        fn arbitrary_with(args: Self::Parameters) -> Self::Strategy {
            arb_store_path(211, args).boxed()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::proptest::arbitrary::any;
    use ::proptest::prop_assert_eq;
    use ::proptest::proptest;
    use assert_matches::assert_matches;
    use nixrs_util::base32::BadBase32;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_parse() {
        let s = "7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3";
        let p = StorePath::new_from_base_name(&s).unwrap();
        assert_eq!(p.name.0, "konsole-18.12.3");
        assert_eq!(p.name.name(), "konsole-18.12.3");
        assert_eq!(p.name.as_ref(), "konsole-18.12.3");
        assert_eq!(&*p.name, "konsole-18.12.3");
        let value = [
            0x9f, 0x76, 0x49, 0x20, 0xf6, 0x5d, 0xe9, 0x71, 0xc4, 0xca, 0x46, 0x21, 0xab, 0xff,
            0x9b, 0x44, 0xef, 0x87, 0x0f, 0x3c,
        ];
        assert_eq!(p.hash.0, value);
        assert_eq!(p.hash.as_ref(), &value);
        assert_eq!(&*p.hash, &value);
        assert_eq!(p.hash.hash(), &value);
        assert_eq!(
            format!("{}", p),
            "7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3"
        );
        assert_eq!(p.is_derivation(), false);
        let p2 = StorePath::from_parts(value, "konsole-18.12.3").unwrap();
        assert_eq!(p, p2);
    }

    #[test]
    fn test_parse2() {
        let s = "7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3.drv".to_owned();
        let p = StorePath::try_from(s).unwrap();
        assert_eq!(p.name.0, "konsole-18.12.3.drv");
        assert_eq!(
            format!("{}", p),
            "7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3.drv"
        );
        let s2: String = p.clone().into();
        assert_eq!(s2, "7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3.drv");
        let s3 = "7h7qgvs4kgzsn8a6rb274saxyqh4jxlz-konsole-18.12.3.drv";
        let p2 = StorePath::new_from_base_name(s3).unwrap();
        assert!(p2 > p);
        assert_eq!(p.is_derivation(), true);
    }

    #[test]
    fn test_from_parts() {
        let hash = hash::Hash::parse_any_prefixed(
            "sha256:1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5s",
        )
        .unwrap();
        let p = StorePath::from_hash(&hash, "konsole-18.12.3").unwrap();
        assert_eq!(
            format!("{}", p),
            "ldhh7c134ap5swsm86rqnc0i7cinqvrc-konsole-18.12.3"
        );
    }

    #[test]
    fn test_no_name() {
        let s = "7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-";
        assert_matches!(
            StorePath::new_from_base_name(&s),
            Err(ParseStorePathError::StorePathNameEmpty)
        );
    }

    #[test]
    fn test_no_dash() {
        let s = "7h7qgvs4kgzsn8a6rb273saxyqh4jxlz";
        assert_matches!(
            StorePath::new_from_base_name(&s),
            Err(ParseStorePathError::BadStorePath(_))
        );
    }

    #[test]
    fn test_short_hash() {
        let s = "7h7qgvs4kgzsn8a6rb273saxyqh4jxl-konsole-18.12.3";
        assert_matches!(
            StorePath::new_from_base_name(&s),
            Err(ParseStorePathError::BadStorePath(_))
        );
    }

    #[test]
    fn test_invalid_hash() {
        let s = "7h7qgvs4kgzsn8e6rb273saxyqh4jxlz-konsole-18.12.3";
        assert_matches!(
            StorePath::new_from_base_name(&s),
            Err(ParseStorePathError::BadBase32(BadBase32, _))
        );
    }

    #[test]
    fn test_long_name() {
        let s = "7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
        assert_matches!(StorePath::new_from_base_name(&s), Ok(_));
    }

    #[test]
    fn test_too_long_name() {
        let s = "7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
        assert_matches!(
            StorePath::new_from_base_name(&s),
            Err(ParseStorePathError::StorePathNameTooLong)
        );
    }

    #[test]
    fn test_bad_name() {
        let s = "7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-foo bar";
        assert_matches!(
            StorePath::new_from_base_name(&s),
            Err(ParseStorePathError::BadStorePathName(_))
        );

        let s = "7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-kónsole";
        assert_matches!(
            StorePath::new_from_base_name(&s),
            Err(ParseStorePathError::BadStorePathName(_))
        );
    }

    #[test]
    fn test_roundtrip() {
        let s = "7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3";
        assert_eq!(StorePath::new_from_base_name(&s).unwrap().to_string(), s);
    }

    #[test]
    fn test_is_drv() {
        let s = "7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3";
        let p = StorePath::new_from_base_name(&s).unwrap();
        assert!(!p.is_derivation());
    }

    #[test]
    fn test_is_drv2() {
        let s = "7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3.drv";
        let p = StorePath::new_from_base_name(&s).unwrap();
        assert!(p.is_derivation());
    }

    #[test]
    fn test_name_from_drv() {
        let s = "7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3.drv";
        let p = StorePath::new_from_base_name(&s).unwrap();
        assert_eq!(p.name_from_drv(), "konsole-18.12.3");
    }

    proptest! {
        #[test]
        fn test_string_parse(path in any::<StorePath>()) {
            let s = path.to_string();
            let parsed = StorePath::new_from_base_name(&s).unwrap();
            prop_assert_eq!(path, parsed);
        }
    }
}
