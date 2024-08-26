use std::borrow::Cow;
use std::fmt;
use std::hash as std_hash;
use std::ops::Deref;
use std::str::FromStr;

use thiserror::Error;
#[cfg(feature = "nixrs-derive")]
use nixrs_derive::NixDeserialize;

use crate::base32;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize))]
#[cfg_attr(feature = "nixrs-derive", nix(from_str))]
pub struct StorePath {
    hash: StorePathHash,
    name: StorePathName,
}

impl StorePath {
    fn from_bytes(buf: &[u8]) -> Result<Self, StorePathError> {
        if buf.len() < STORE_PATH_HASH_ENCODED_SIZE + 1 {
            return Err(StorePathError::HashLength);
        }
        if buf[STORE_PATH_HASH_ENCODED_SIZE] != b'-' {
            return Err(StorePathError::Symbol(
                STORE_PATH_HASH_ENCODED_SIZE as u8,
            ));
        }
        let hash = StorePathHash::decode_digest(&buf[..STORE_PATH_HASH_ENCODED_SIZE])?;
        let name = buf[(STORE_PATH_HASH_ENCODED_SIZE + 1)..]
            .try_into()
            .map_err(|err| {
                StorePathError::adjust_index(STORE_PATH_HASH_ENCODED_SIZE as u8 + 1, err)
            })?;
        Ok(StorePath { hash, name })
    }

    pub fn name(&self) -> &StorePathName {
        &self.name
    }

    pub fn hash(&self) -> &StorePathHash {
        &self.hash
    }
}

impl fmt::Display for StorePath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.hash, self.name)
    }
}

impl TryFrom<&[u8]> for StorePath {
    type Error = StorePathError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        StorePath::from_bytes(value)
    }
}

impl FromStr for StorePath {
    type Err = StorePathError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        StorePath::from_bytes(s.as_bytes())
    }
}

impl AsRef<StorePathName> for StorePath {
    fn as_ref(&self) -> &StorePathName {
        &self.name
    }
}

impl AsRef<StorePathHash> for StorePath {
    fn as_ref(&self) -> &StorePathHash {
        &self.hash
    }
}

const STORE_PATH_HASH_SIZE: usize = 20;
const STORE_PATH_HASH_ENCODED_SIZE: usize = base32::encode_len(STORE_PATH_HASH_SIZE);

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct StorePathHash([u8; STORE_PATH_HASH_SIZE]);

impl StorePathHash {
    pub fn new(value: [u8; STORE_PATH_HASH_SIZE]) -> StorePathHash {
        StorePathHash(value)
    }

    pub fn copy_from_slice(data: &[u8]) -> StorePathHash {
        let mut digest = [0u8; STORE_PATH_HASH_SIZE];
        digest.copy_from_slice(data);
        StorePathHash::new(digest)
    }

    pub fn decode_digest(data: &[u8]) -> Result<StorePathHash, StorePathError> {
        if data.len() != base32::encode_len(STORE_PATH_HASH_SIZE) {
            return Err(StorePathError::HashLength);
        }
        let mut hash_output = [0u8; STORE_PATH_HASH_SIZE];
        base32::decode_mut(data, &mut hash_output)
            .map_err(|err| StorePathError::Symbol(err.error.position as u8))?;
        Ok(StorePathHash::new(hash_output))
    }
}

impl fmt::Debug for StorePathHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StorePathHash({})", self)
    }
}

impl fmt::Display for StorePathHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut output = [0u8; base32::encode_len(STORE_PATH_HASH_SIZE)];
        base32::encode_mut(&self.0, &mut output);

        // SAFETY: Nix Base32 is a subset of ASCII, which guarantees valid UTF-8.
        let s = unsafe { std::str::from_utf8_unchecked(&output) };
        f.write_str(s)
    }
}

impl FromStr for StorePathHash {
    type Err = StorePathError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        StorePathHash::decode_digest(s.as_bytes())
    }
}

impl std_hash::Hash for StorePathHash {
    fn hash<H: std_hash::Hasher>(&self, state: &mut H) {
        for c in self.0.iter().rev() {
            c.hash(state);
        }
    }
}

impl Ord for StorePathHash {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.iter().rev().cmp(other.0.iter().rev())
    }
}

impl PartialOrd for StorePathHash {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.0.cmp(&other.0))
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

macro_rules! partial_eq_self {
    ($own:ty) => {
        impl PartialEq<&$own> for $own {
            fn eq(&self, other: &&$own) -> bool {
                self == **other
            }
        }
        impl PartialEq<$own> for &$own {
            fn eq(&self, other: &$own) -> bool {
                *self == other
            }
        }
    };
}
macro_rules! partial_eq {
    ($own:ty, $ty:ty) => {
        impl PartialEq<$ty> for $own {
            fn eq(&self, other: &$ty) -> bool {
                self.0 == *other
            }
        }
        impl PartialEq<$own> for $ty {
            fn eq(&self, other: &$own) -> bool {
                *self == other.0
            }
        }
    };
}
partial_eq_self!(StorePathHash);
partial_eq!(StorePathHash, &'_ [u8]);
partial_eq!(StorePathHash, [u8; STORE_PATH_HASH_SIZE]);

const NAME_LOOKUP: [bool; 256] = {
    let mut ret = [false; 256];
    let mut idx = 0usize;
    while idx < u8::MAX as usize {
        let ch = idx as u8;
        ret[idx] = matches!(ch, b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'+' | b'-' | b'_' | b'?' | b'=' | b'.');
        idx += 1;
    }
    ret
};

pub fn into_name<V: AsRef<[u8]>>(s: &V) -> Result<&str, StorePathError> {
    let s = s.as_ref();
    if s.is_empty() || s.len() > 211 {
        return Err(StorePathError::NameLength);
    }

    for (idx, ch) in s.iter().enumerate() {
        if !NAME_LOOKUP[*ch as usize] {
            return Err(StorePathError::Symbol(idx as u8));
        }
    }

    // SAFETY: We checked above that it is a subset of ASCII, which guarantees valid UTF-8.
    let ret = unsafe { std::str::from_utf8_unchecked(s) };
    Ok(ret)
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StorePathName(String);

impl fmt::Display for StorePathName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<&[u8]> for StorePathName {
    type Error = StorePathError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let name = into_name(&value)?;
        Ok(StorePathName(name.into()))
    }
}

impl FromStr for StorePathName {
    type Err = StorePathError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.as_bytes().try_into()
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

partial_eq_self!(StorePathName);
partial_eq!(StorePathName, &'_ str);
partial_eq!(StorePathName, String);
partial_eq!(StorePathName, Cow<'_, str>);

#[derive(Debug, Error, PartialEq, Eq)]
pub enum StorePathError {
    #[error("invalid store path hash length")]
    HashLength,
    #[error("invalid store path name length")]
    NameLength,
    #[error("invalid store path symbol at {0}")]
    Symbol(u8),
}

impl StorePathError {
    fn adjust_index(prefix: u8, other: StorePathError) -> StorePathError {
        match other {
            StorePathError::Symbol(old) => StorePathError::Symbol(prefix + old),
            c => c,
        }
    }
}

#[cfg(test)]
mod test {
    use std::cmp::Ordering;

    use hex_literal::hex;
    use rstest::rstest;

    use super::*;

    // zzcfcjwxkn4cf1nh8dh521vffyq24179-perl5.38.0-libnet-3.12
    #[test]
    fn parse_hash() {
        let hash = "zzcfcjwxkn4cf1nh8dh521vffyq24179"
            .parse::<StorePathHash>()
            .unwrap();
        let expected = hex!("E904 22B0 776E 0751 6043 D006 C788 9D9D 4BE6 D8FF");
        assert_eq!(hash, expected);
        assert_eq!(*hash, expected);
        assert_eq!(hash.as_ref(), expected);
    }

    #[rstest]
    #[case::empty("", StorePathError::HashLength)]
    #[case::too_short("zzcfcjwxkn4cf1nh8dh521vffyq2417", StorePathError::HashLength)]
    #[case::too_long("zzcfcjwxkn4cf1nh8dh521vffyq24179a", StorePathError::HashLength)]
    #[case::invalid_symbol("zzcfcjwxkn4|f1nh8dh521vffyq24179", StorePathError::Symbol(11))]
    #[test]
    fn parse_hash_error(#[case] hash: &str, #[case] expected: StorePathError) {
        let err = hash.parse::<StorePathHash>().expect_err("parse failure");
        assert_eq!(err, expected);
    }

    #[test]
    fn hash_order() {
        let list = [
            "00ljmhbmf3d12aq4l5l7yr7bxn03yqvf",
            "0sbwqgpi6jbqr710w5vn0b4s5w6z8n8n",
            "1hghwlv8pxghnkk1q0jvhlh2pzc1sc2f",
            "22dnr9nysk3gpy0jzw44fbi6gr5czzi3",
            "24sgyxikjg8i2sifywnczf6q697yds3z",
            "2nkhabskrzm94xr1fjdag3xbxy6qx75a",
            "2v8lw59nsmgqidcpw5szkxzd890ffr49",
            "2vr9ihd95b9xjvzzpxay1a1vzi3gx0xw",
            "2vr9ihd95b9xjvzzpxay1a1vzi3gx0xx",
            "3qxrdnqxbahxqsxb2rlifnmil7j1vxjh",
            "545hv9qa2jmkpd752nalbq4v1j1vm216",
            "5xfvjkml0qv8r5lq60s9br21wkhw2dmr",
            "7h623qgw0j4vmhx7cbis2dz6pps3j1bm",
            "868l02pyyr76vzcx6s3yfh9r69axarpn",
            "9hmpxy56lak38d06hwdsihnq2cxdcjk0",
            "a4z7pxg4xh6mm66s77d72ks1myzlk777",
            "agkqfd119da4f08d0s7l26ldj8nnxhv5",
            "b3pw0k3ww2avacsm89ik46bvcc511mxv",
            "hk60ghp7kcc1a0s2zmglizyhj6hmrbad",
            "lzdk0y2liz1jh9s34dcp7fijp96sxa7d",
            "mgn0lmx7jxqs64ixm3aamppmj23lfmpj",
            "nbyybld7l13gawd2rp2b6s7wwrpwlgck",
            "v8i77z0qbdm0k8hwhagrj7wjkjd3yiw9",
            "xn1jv0bmrybmmvlvcmjix77pkqq646ci",
            "ysmbabd63la2fydhv53qky5q8k1m7kp8",
            "zs498qq1arym4p4z6bkpid3xgrbl29rj",
        ];
        let parsed_list = list.map(|i| i.parse::<StorePathHash>().unwrap());
        for window in parsed_list.windows(2) {
            assert_eq!(Ordering::Less, window[0].cmp(&window[1]));
        }
    }

    #[rstest]
    #[case("perl5.38.0-libnet-3.12")]
    #[case::all(".-_?+=abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSSTUVWXYZ")]
    #[case::dot(".")]
    #[case::dotdot("..")]
    #[case::dotdash(".-")]
    #[case::dotdotdash("..-")]
    #[case::longest("test-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")]
    fn name_ok(#[case] case: &str) {
        let name = case.parse::<StorePathName>().expect("parses");
        assert_eq!(case, name.to_string());
        assert_eq!(case, name);
        assert_eq!(case.to_string(), name);
        assert_eq!(Cow::Borrowed(case), name);
        assert_eq!(case, name.as_ref());
        assert_eq!(case.as_bytes(), name.as_bytes());
        let name2: StorePathName = case.as_bytes().try_into().expect("parses bytes");
        assert_eq!(name, name2);
    }

    #[rstest]
    #[case::empty("", StorePathError::NameLength)]
    #[case::too_long("test-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", StorePathError::NameLength)]
    #[case::invalid_char("test|more", StorePathError::Symbol(4))]
    fn name_errors(#[case] name: &str, #[case] expected: StorePathError) {
        assert_eq!(
            name.parse::<StorePathName>().expect_err("parse succeeded"),
            expected
        );
    }

    #[rstest]
    #[case("perl5.38.0-libnet-3.12")]
    #[case::all(".-_?+=abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSSTUVWXYZ")]
    #[case::dot(".")]
    #[case::dotdot("..")]
    #[case::dotdash(".-")]
    #[case::dotdotdash("..-")]
    #[case::longest("test-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")]
    fn store_path_ok(#[case] case_name: &str) {
        let case_hash = "00ljmhbmf3d12aq4l5l7yr7bxn03yqvf";
        let path_name = format!("{}-{}", case_hash, case_name);
        let path = path_name.parse::<StorePath>().expect("parses path");
        assert_eq!(path_name, path.to_string());
        let path2 = path_name.as_bytes().try_into().expect("parses path bytes");
        assert_eq!(path, path2);

        let name = case_name.parse::<StorePathName>().expect("parses name");
        assert_eq!(name, path.name());
        assert_eq!(name, AsRef::<StorePathName>::as_ref(&path));

        let hash = case_hash.parse::<StorePathHash>().expect("parses hash");
        assert_eq!(hash, path.hash());
        assert_eq!(hash, AsRef::<StorePathHash>::as_ref(&path));
    }

    #[rstest]
    #[case::empty("", StorePathError::HashLength)]
    #[case::too_short_hash("00ljmhbmf3d12aq4l5l7yr7bxn03yqv-", StorePathError::HashLength)]
    #[case::invalid_hash_symbol(
        "00ljmhbmf3=12aq4l5l7yr7bxn03yqvv-test",
        StorePathError::Symbol(10)
    )]
    #[case::wrong_dash(
        "00ljmhbmf3=12aq4l5l7yr7bxn03yqvv.test",
        StorePathError::Symbol(32)
    )]
    #[case::missing_name("00ljmhbmf3d12aq4l5l7yr7bxn03yqvv-", StorePathError::NameLength)]
    #[case::name_too_long("00ljmhbmf3d12aq4l5l7yr7bxn03yqvv-test-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", StorePathError::NameLength)]
    #[case::name_with_invalid_char(
        "00ljmhbmf3d12aq4l5l7yr7bxn03yqvv-test|more",
        StorePathError::Symbol(37)
    )]
    fn store_path_error(#[case] path: &str, #[case] expected: StorePathError) {
        assert_eq!(
            path.parse::<StorePath>().expect_err("parse succeeded"),
            expected
        );
    }

    // TODO: StorePath order
    // TODO: StorePathName proptest
    // TODO: StorePath proptest
}
