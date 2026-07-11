use std::fmt;
use std::hash as std_hash;
use std::ops::Deref;
use std::str::FromStr;

use crate::store_path::macros::partial_eq;
use crate::store_path::macros::partial_eq_self;
use crate::{base32, hash};

const STORE_PATH_HASH_SIZE: usize = 20;
const STORE_PATH_HASH_ENCODED_SIZE: usize = base32::encode_len(STORE_PATH_HASH_SIZE);

#[derive(Debug, PartialEq, Eq, Clone, thiserror::Error)]
pub enum StorePathHashError {
    #[error("invalid store path hash length")]
    HashLength,
    #[error("invalid store path hash symbol {ch} at position {position}", ch = super::display_symbol(*symbol))]
    Symbol { position: usize, symbol: u8 },
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct StorePathHash([u8; StorePathHash::len()]);

impl StorePathHash {
    pub const fn len() -> usize {
        STORE_PATH_HASH_SIZE
    }

    pub const fn encoded_len() -> usize {
        STORE_PATH_HASH_ENCODED_SIZE
    }

    pub const fn from_array(value: [u8; STORE_PATH_HASH_SIZE]) -> StorePathHash {
        StorePathHash(value)
    }

    pub fn from_hash(hash: &hash::Sha256) -> Self {
        let mut digest = [0u8; STORE_PATH_HASH_SIZE];
        for (i, item) in hash.as_ref().iter().enumerate() {
            let idx = i % STORE_PATH_HASH_SIZE;
            digest[idx] ^= item;
        }
        StorePathHash(digest)
    }

    pub const fn copy_from_slice(data: &[u8]) -> StorePathHash {
        let mut digest = [0u8; STORE_PATH_HASH_SIZE];
        digest.copy_from_slice(data);
        StorePathHash::from_array(digest)
    }

    pub const fn from_slice(data: &[u8]) -> Result<Self, StorePathHashError> {
        if data.len() != Self::len() {
            return Err(StorePathHashError::HashLength);
        }
        Ok(Self::copy_from_slice(data))
    }

    pub fn decode_digest(data: &[u8]) -> Result<StorePathHash, StorePathHashError> {
        if data.len() != base32::encode_len(STORE_PATH_HASH_SIZE) {
            return Err(StorePathHashError::HashLength);
        }
        let mut hash_output = [0u8; STORE_PATH_HASH_SIZE];
        base32::decode_mut(data, &mut hash_output).map_err(|err| StorePathHashError::Symbol {
            position: err.error.position,
            symbol: data[err.error.position],
        })?;
        Ok(StorePathHash::from_array(hash_output))
    }
}

impl fmt::Debug for StorePathHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StorePathHash({self})")
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

impl TryFrom<&[u8]> for StorePathHash {
    type Error = StorePathHashError;

    fn try_from(data: &[u8]) -> Result<Self, Self::Error> {
        StorePathHash::from_slice(data)
    }
}

impl FromStr for StorePathHash {
    type Err = StorePathHashError;

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
        Some(self.cmp(other))
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

partial_eq_self!(StorePathHash);
partial_eq!(StorePathHash, &'_ [u8]);
partial_eq!(StorePathHash, [u8; STORE_PATH_HASH_SIZE]);

#[cfg(test)]
mod unittests {
    use std::cmp::Ordering;
    use std::collections::BTreeSet;

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
    #[should_panic(expected = "invalid store path hash length")]
    #[case::empty("")]
    #[should_panic(expected = "invalid store path hash length")]
    #[case::too_short("zzcfcjwxkn4cf1nh8dh521vffyq2417")]
    #[should_panic(expected = "invalid store path hash length")]
    #[case::too_long("zzcfcjwxkn4cf1nh8dh521vffyq24179a")]
    #[should_panic(expected = "invalid store path hash symbol '|' at position 11")]
    #[case::invalid_symbol("zzcfcjwxkn4|f1nh8dh521vffyq24179")]
    #[test]
    fn parse_hash_error(#[case] hash: &str) {
        let err = hash.parse::<StorePathHash>().expect_err("parse failure");
        panic!("{err}");
    }

    #[test]
    fn set_order() {
        let e = [
            "2q000000000000000000000000000000",
            "000h0000000000000000000000000000",
        ];
        let o = [
            "000h0000000000000000000000000000",
            "2q000000000000000000000000000000",
        ];
        let e_list: Vec<_> = e
            .into_iter()
            .map(|e| e.parse::<StorePathHash>().unwrap())
            .collect();
        let o_list: Vec<_> = o
            .into_iter()
            .map(|e| e.parse::<StorePathHash>().unwrap())
            .collect();
        let e_list1: BTreeSet<_> = e_list.iter().cloned().collect();
        let o_list1: BTreeSet<_> = o_list.iter().cloned().collect();

        let mut e_list2 = BTreeSet::new();
        for item in e_list.iter() {
            e_list2.insert(*item);
        }

        let mut o_list2 = BTreeSet::new();
        for item in o_list.iter() {
            o_list2.insert(*item);
        }
        assert_eq!(e_list1, e_list2);
        assert_eq!(o_list1, o_list2);
        assert_eq!(e_list1, o_list1);
        assert_eq!(e_list2, o_list2);
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
            assert_eq!(Some(Ordering::Less), window[0].partial_cmp(&window[1]));
        }
    }
}

#[cfg(test)]
mod proptests {
    use proptest::prelude::*;

    use super::*;

    proptest! {
        #[test]
        fn store_hash_parse_display(expected in any::<StorePathHash>()) {
            let s = expected.to_string();
            let actual = s.parse::<StorePathHash>().expect("Parsing display");
            prop_assert_eq!(expected, actual);
        }
    }
}
