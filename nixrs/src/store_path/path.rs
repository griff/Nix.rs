use std::fmt;
use std::path::Path;
use std::str::FromStr;

use serde_with::{DeserializeFromStr, SerializeDisplay};
use thiserror::Error;

use crate::hash;
use crate::store_path::{StorePathHashError, StorePathName, StorePathNameError, StorePathNameRef};

use super::{FromStoreDirStr, StoreDir, StoreDirDisplay, StorePathHash};

#[derive(Debug, Error, PartialEq, Eq, Clone)]
#[error("could not parse '{path}', {error}")]
pub struct ParseStorePathError {
    pub path: String,
    pub error: StorePathError,
}

impl ParseStorePathError {
    pub fn new<E>(path: &str, error: E) -> ParseStorePathError
    where
        E: Into<StorePathError>,
    {
        ParseStorePathError {
            path: path.to_owned(),
            error: error.into(),
        }
    }
}

#[derive(Debug, Error, PartialEq, Eq, Clone)]
pub enum StorePathError {
    #[error("non-absolute store path")]
    NonAbsolute,
    #[error("path is not in store")]
    NotInStore,
    #[error("invalid store path hash length")]
    HashLength,
    #[error("invalid store path name length")]
    NameLength,
    #[error("invalid store path symbol {ch} at position {position}", ch = super::display_symbol(*symbol))]
    Symbol { position: usize, symbol: u8 },
}

impl StorePathError {
    pub fn from_adjust_index(prefix: usize, other: StorePathNameError) -> StorePathError {
        other.adjust_index(prefix).into()
    }

    pub fn adjust_index(self, prefix: usize) -> Self {
        match self {
            StorePathError::Symbol { position, symbol } => StorePathError::Symbol {
                position: position + prefix,
                symbol,
            },
            a => a,
        }
    }
}

impl From<StorePathHashError> for StorePathError {
    fn from(value: StorePathHashError) -> Self {
        match value {
            StorePathHashError::HashLength => StorePathError::HashLength,
            StorePathHashError::Symbol { position, symbol } => {
                StorePathError::Symbol { position, symbol }
            }
        }
    }
}

impl From<StorePathNameError> for StorePathError {
    fn from(value: StorePathNameError) -> Self {
        match value {
            StorePathNameError::NameLength => StorePathError::NameLength,
            StorePathNameError::Symbol { position, symbol } => {
                StorePathError::Symbol { position, symbol }
            }
        }
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, SerializeDisplay, DeserializeFromStr)]
pub struct StorePath {
    hash: StorePathHash,
    name: StorePathName,
}

fn strip_store<'s>(s: &'s str, store_dir: &StoreDir) -> Result<&'s str, StorePathError> {
    let path = Path::new(s);
    if !path.is_absolute() {
        return Err(StorePathError::NonAbsolute);
    }
    let name = s
        .strip_prefix(store_dir.to_str())
        .ok_or(StorePathError::NotInStore)?;
    if name.as_bytes()[0] != b'/' {
        return Err(StorePathError::NotInStore);
    }

    Ok(&name[1..])
}

impl StorePath {
    fn new(s: &str, store_dir: &StoreDir) -> Result<Self, ParseStorePathError> {
        let file_name = strip_store(s, store_dir).map_err(|error| ParseStorePathError {
            path: s.to_owned(),
            error,
        })?;
        file_name
            .parse::<Self>()
            .map_err(|error| ParseStorePathError {
                path: s.to_owned(),
                error: error.error.adjust_index(store_dir.len() + 1),
            })
    }

    fn from_bytes(buf: &[u8]) -> Result<Self, StorePathError> {
        if buf.len() < StorePathHash::encoded_len() + 1 {
            return Err(StorePathError::HashLength);
        }
        if buf[StorePathHash::encoded_len()] != b'-' {
            return Err(StorePathError::Symbol {
                position: StorePathHash::encoded_len(),
                symbol: buf[StorePathHash::encoded_len()],
            });
        }
        let hash = StorePathHash::decode_digest(&buf[..StorePathHash::encoded_len()])
            .map_err(StorePathError::from)?;
        let name = StorePathName::from_slice(&buf[(StorePathHash::encoded_len() + 1)..]).map_err(
            |err| StorePathError::from(err.adjust_index(StorePathHash::encoded_len() + 1)),
        )?;
        Ok(StorePath { hash, name })
    }

    pub fn from_hash(hash: &hash::Sha256, name: StorePathName) -> Self {
        StorePath {
            hash: StorePathHash::from_hash(hash),
            name,
        }
    }

    pub fn name(&self) -> &StorePathNameRef {
        &self.name
    }

    pub fn hash(&self) -> &StorePathHash {
        &self.hash
    }
}

impl fmt::Debug for StorePath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("StorePath")
            .field(&format_args!("{self}"))
            .finish()
    }
}

impl fmt::Display for StorePath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.hash, self.name)
    }
}

impl From<(StorePathHash, StorePathName)> for StorePath {
    fn from((hash, name): (StorePathHash, StorePathName)) -> Self {
        StorePath { hash, name }
    }
}

impl TryFrom<&[u8]> for StorePath {
    type Error = StorePathError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        StorePath::from_bytes(value)
    }
}

impl FromStr for StorePath {
    type Err = ParseStorePathError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        StorePath::from_bytes(s.as_bytes()).map_err(|error| ParseStorePathError::new(s, error))
    }
}

impl AsRef<StorePathName> for StorePath {
    fn as_ref(&self) -> &StorePathName {
        &self.name
    }
}

impl AsRef<StorePathNameRef> for StorePath {
    fn as_ref(&self) -> &StorePathNameRef {
        self.name()
    }
}

impl AsRef<StorePathHash> for StorePath {
    fn as_ref(&self) -> &StorePathHash {
        self.hash()
    }
}

impl FromStoreDirStr for StorePath {
    type Error = ParseStorePathError;

    fn from_store_dir_str(store_dir: &super::StoreDir, s: &str) -> Result<Self, Self::Error> {
        StorePath::new(s, store_dir)
    }
}

impl StoreDirDisplay for StorePath {
    fn fmt(&self, store_dir: &StoreDir, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{store_dir}/{self}")
    }
}

#[cfg(test)]
mod unittests {
    use std::cmp::Ordering;

    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case("perl5.38.0-libnet-3.12")]
    #[case::all(".-_?+=abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSSTUVWXYZ")]
    #[case::dot(".")]
    #[case::dotdot("..")]
    #[case::dotdash(".-")]
    #[case::dotdotdash("..-")]
    #[case::longest(
        "test-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    )]
    fn store_path_ok(#[case] case_name: StorePathName) {
        let case_hash =
            StorePathHash::from_str("00ljmhbmf3d12aq4l5l7yr7bxn03yqvf").expect("parses hash");
        let path_name = format!("{case_hash}-{case_name}");
        let path = path_name.parse::<StorePath>().expect("parses path");
        assert_eq!(path_name, path.to_string());
        let path2 = path_name.as_bytes().try_into().expect("parses path bytes");
        assert_eq!(path, path2);

        assert_eq!(case_name, path.name());
        assert_eq!(case_name, AsRef::<StorePathName>::as_ref(&path));

        assert_eq!(case_hash, path.hash());
        assert_eq!(case_hash, AsRef::<StorePathHash>::as_ref(&path));
    }

    #[rstest]
    #[should_panic(expected = "could not parse '', invalid store path hash length")]
    #[case::empty("")]
    #[should_panic(
        expected = "could not parse '00ljmhbmf3d12aq4l5l7yr7bxn03yqv-', invalid store path hash length"
    )]
    #[case::too_short_hash("00ljmhbmf3d12aq4l5l7yr7bxn03yqv-")]
    #[should_panic(
        expected = "could not parse '00ljmhbmf3=12aq4l5l7yr7bxn03yqvv-test', invalid store path symbol '=' at position 10"
    )]
    #[case::invalid_hash_symbol("00ljmhbmf3=12aq4l5l7yr7bxn03yqvv-test")]
    #[should_panic(
        expected = "could not parse '00ljmhbmf3=12aq4l5l7yr7bxn03yqvv.test', invalid store path symbol '.' at position 32"
    )]
    #[case::wrong_dash("00ljmhbmf3=12aq4l5l7yr7bxn03yqvv.test")]
    #[should_panic(
        expected = "could not parse '00ljmhbmf3d12aq4l5l7yr7bxn03yqvv-', invalid store path name length"
    )]
    #[case::missing_name("00ljmhbmf3d12aq4l5l7yr7bxn03yqvv-")]
    #[should_panic(
        expected = "could not parse '00ljmhbmf3d12aq4l5l7yr7bxn03yqvv-test-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', invalid store path name length"
    )]
    #[case::name_too_long(
        "00ljmhbmf3d12aq4l5l7yr7bxn03yqvv-test-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    )]
    #[should_panic(
        expected = "could not parse '00ljmhbmf3d12aq4l5l7yr7bxn03yqvv-test|more', invalid store path symbol '|' at position 37"
    )]
    #[case::name_with_invalid_char("00ljmhbmf3d12aq4l5l7yr7bxn03yqvv-test|more")]
    fn store_path_error(#[case] path: &str) {
        let err = path.parse::<StorePath>().expect_err("parse succeeded");
        panic!("{err}");
    }

    #[test]
    fn store_path_order() {
        let list = [
            "3431a7m1xm7k8ggibfqjciji1h4hcpdg-polly-12.0.1.src.tar.xz.drv",
            "3n3vph932sfznfvp472jsr02wypg00c1-apple-framework-OpenGL.drv",
            "3rf1grj8n7akzy98rm4xlw0k0bsrhhb7-apple-framework-CoreFoundation.drv",
            "3sld67h643yp9l2496k567gn8zs63xmd-string_argv___string_argv_0.0.2.tgz.drv",
            "4ql4g3ss782y7c7a5i8bdj2v7b5izs4d-codemap-diagnostic-0.1.2.drv",
            "5qgdakl33rc83dcdln68ca62llp5zy9q-guava-parent-26.0-android.pom",
            "63k48d02cs6fqc5qb4m4qij2lp21rd74-cargo-build-hook.sh.drv",
            "6ky7iz3c7bbv35d7nkb69kjgl0mpkn6b-6531da946949a94643e6d8424236174ae64fe0ca.patch.drv",
            "6vb5s19cbmsfybizb67xv1y6pgricmk9-pytest-7.1.3.tar.gz.drv",
            "84ilav9kiyhfzw7a5lppngdhw48bbihs-hatchling-1.24.2.tar.gz.drv",
            "8hlynkqwgg3dkgyx6x6m549yqrx28py7-tools.logging-1.2.4.jar.drv",
            "9klylswa11kr8sqq65x1pfcl5y2lghls-tr46___tr46_0.0.3.tgz",
            "djg6gy8iymm9arxnmg0yyz3pms831wr0-libcxx-headers-src-16.0.6.drv",
            "inhj4681cf02mqvhkw3xrbbwmy6xbn9x-perl5.38.0-gettext-1.07.drv",
            "jdsl20r8mqjnr8vasqwsjvf2yg7ykdzw-libwebp-1.4.0.drv",
            "jnklxpwl49zdvqv04g2jfiq3719ic12z-xz-5.4.7-bin",
            "kkszlid4fss1s74bchh7hbpbndxzqq6i-https___registry.npmjs.org_assert___assert_1.5.0.tgz",
            "m2iin4fliaplacyrwq7l7bxyvk7cd9y0-https___registry.npmjs.org_is_svg___is_svg_3.0.0.tgz",
            "p0fgxad36glz02fpmfkbd4v19b58ja52-https___registry.npmjs.org_string_width___string_width_1.0.2.tgz.drv",
            "pfhr3caay320aklm05bf0z39aajk4sjx-transit-js-0.8.874.jar.drv",
            "q8abgca8z91caq4jkwh6sh3qyprrqmwl-rouge-4.1.3.gem.drv",
            "q8lq5j433pf27m3j6l7ki217dy9dpdgs-jackson-coreutils-1.8.pom.drv",
            "yishjp1jmaq1gw1n84v0k8hmj73d60p9-bash52-017.drv",
            "yq0lz1byj4v2rym2ng23a3nj4n6pvqdj-pgrp-pipe-5.patch",
            "ywrs8hr8fa4244bpdxi88bd87qxqgmy0-app-home",
        ];
        let parsed_list = list.map(|i| i.parse::<StorePath>().unwrap());
        for window in parsed_list.windows(2) {
            assert_eq!(Ordering::Less, window[0].cmp(&window[1]));
        }
    }

    #[rstest]
    #[case(
        "/nix/store/ywrs8hr8fa4244bpdxi88bd87qxqgmy0-app-home",
        "ywrs8hr8fa4244bpdxi88bd87qxqgmy0-app-home"
    )]
    #[case(
        "/nix/store/ywrs8hr8fa4244bpdxi88bd87qxqgmy0-.-_?+=abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSSTUVWXYZ",
        "ywrs8hr8fa4244bpdxi88bd87qxqgmy0-.-_?+=abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSSTUVWXYZ"
    )]
    #[test]
    fn from_store_dir_str(#[case] store_path: &str, #[case] base_path: StorePath) {
        let store = StoreDir::default();
        let path: StorePath = store.parse(store_path).expect("Can parse store path");
        assert_eq!(path, base_path);
    }

    #[rstest]
    #[should_panic(expected = "could not parse '', non-absolute store path")]
    #[case::empty("")]
    #[should_panic(expected = "could not parse '/nix/store/', invalid store path hash length")]
    #[case::mising_file_name("/nix/store/")]
    #[should_panic(
        expected = "could not parse '/outsise/ywrs8hr8fa4244bpdxi88bd87qxqgmy0-app-home', path is not in store"
    )]
    #[case::not_in_store("/outsise/ywrs8hr8fa4244bpdxi88bd87qxqgmy0-app-home")]
    #[should_panic(
        expected = "could not parse '/nix/storeywrs8hr8fa4244bpdxi88bd87qxqgmy0-app-home', path is not in store"
    )]
    #[case::missing_slash("/nix/storeywrs8hr8fa4244bpdxi88bd87qxqgmy0-app-home")]
    #[should_panic(
        expected = "could not parse '/nix/store/zzcfcjwxkn4cf1nh8dh521vffyq2417', invalid store path hash length"
    )]
    #[case::too_short("/nix/store/zzcfcjwxkn4cf1nh8dh521vffyq2417")]
    #[should_panic(
        expected = "could not parse '/nix/store/zzcfcjwxkn4cf1nh8dh521vffyq24179a-app', invalid store path symbol 'a' at position 43"
    )]
    #[case::hash_too_long("/nix/store/zzcfcjwxkn4cf1nh8dh521vffyq24179a-app")]
    #[should_panic(
        expected = "could not parse '/nix/store/zzcfcjwxkn4cf1nh8dh521vffyq24179-', invalid store path name length"
    )]
    #[case::missing_name("/nix/store/zzcfcjwxkn4cf1nh8dh521vffyq24179-")]
    #[should_panic(
        expected = "could not parse '/nix/store/zzcfcjwxkn4cf1nh8dh521vffyq24179-å', invalid store path symbol \\xC3 at position 44"
    )]
    #[case::bad_name("/nix/store/zzcfcjwxkn4cf1nh8dh521vffyq24179-å")]
    #[should_panic(
        expected = "could not parse '/nix/store/zzcfcjwxkn4|f1nh8dh521vffyq24179-app', invalid store path symbol '|' at position 22"
    )]
    #[case::invalid_symbol("/nix/store/zzcfcjwxkn4|f1nh8dh521vffyq24179-app")]
    #[test]
    fn from_store_dir_str_error(#[case] store_path: &str) {
        let store = StoreDir::default();
        let err = store
            .parse::<StorePath>(store_path)
            .expect_err("parse failure");
        panic!("{err}");
    }

    #[rstest]
    #[case(
        "/nix/store/ywrs8hr8fa4244bpdxi88bd87qxqgmy0-app-home",
        "ywrs8hr8fa4244bpdxi88bd87qxqgmy0-app-home"
    )]
    #[case(
        "/nix/store/ywrs8hr8fa4244bpdxi88bd87qxqgmy0-.-_?+=abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSSTUVWXYZ",
        "ywrs8hr8fa4244bpdxi88bd87qxqgmy0-.-_?+=abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSSTUVWXYZ"
    )]
    #[test]
    fn store_dir_display(#[case] store_path: &str, #[case] base_path: StorePath) {
        let store = StoreDir::default();
        let s = store.display(&base_path).to_string();
        assert_eq!(store_path, s);
    }
}

#[cfg(test)]
mod proptests {
    use proptest::prelude::*;

    use crate::store_path::{StoreDir, StorePath};

    proptest! {
        #[test]
        fn proptest_store_path_parse_display(path in any::<StorePath>()) {
            let s = path.to_string();
            let parsed = s.parse::<StorePath>().expect("Parsing display");
            prop_assert_eq!(path, parsed);
        }
    }

    proptest! {
        #[test]
        fn proptest_store_dir_display_parse(store_dir in any::<StoreDir>(), path in any::<StorePath>()) {
            let s = store_dir.display(&path).to_string();
            let parsed = store_dir.parse::<StorePath>(&s).expect("Parsing display");
            prop_assert_eq!(path, parsed);
        }
    }
}
