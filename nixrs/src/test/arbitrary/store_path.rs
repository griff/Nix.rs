use std::path::{MAIN_SEPARATOR_STR, PathBuf};

use proptest::prelude::*;

use crate::hash::{Algorithm, Hash, Sha256};
use crate::store_path::{
    ContentAddress, ContentAddressMethod, ContentAddressMethodAlgorithm, MAX_NAME_LEN, StoreDir,
    StorePath, StorePathHash, StorePathName,
};

impl Arbitrary for ContentAddressMethod {
    type Parameters = ();
    type Strategy = BoxedStrategy<ContentAddressMethod>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        prop_oneof![
            Just(ContentAddressMethod::Text),
            Just(ContentAddressMethod::Flat),
            Just(ContentAddressMethod::Recursive),
        ]
        .boxed()
    }
}

impl Arbitrary for ContentAddressMethodAlgorithm {
    type Parameters = ();
    type Strategy = BoxedStrategy<ContentAddressMethodAlgorithm>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        prop_oneof![
            Just(ContentAddressMethodAlgorithm::Text),
            any::<Algorithm>().prop_map(ContentAddressMethodAlgorithm::Flat),
            any::<Algorithm>().prop_map(ContentAddressMethodAlgorithm::Recursive),
        ]
        .boxed()
    }
}

impl Arbitrary for ContentAddress {
    type Parameters = ();
    type Strategy = BoxedStrategy<ContentAddress>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        prop_oneof![
            any::<Sha256>().prop_map(ContentAddress::Text),
            any::<Hash>().prop_map(ContentAddress::Flat),
            any::<Hash>().prop_map(ContentAddress::Recursive),
        ]
        .boxed()
    }
}

pub fn arb_output_name() -> impl Strategy<Value = String> {
    "[a-zA-Z0-9+\\-_?=][a-zA-Z0-9+\\-_?=.]{0,13}"
}

impl Arbitrary for StorePathHash {
    type Parameters = ();
    type Strategy = BoxedStrategy<StorePathHash>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        any::<[u8; StorePathHash::len()]>()
            .prop_map(StorePathHash::new)
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
        if max > MAX_NAME_LEN as u8 - len {
            max = MAX_NAME_LEN as u8 - len;
        }
        max -= 1;
        if s.len() > max as usize {
            s.truncate(max as usize);
        }
        if let Some(ext) = extension.as_ref() {
            s.push('.');
            s.push_str(ext);
        }
        s.parse().unwrap()
    })
}

impl Arbitrary for StorePathName {
    type Parameters = Option<String>;
    type Strategy = BoxedStrategy<StorePathName>;

    fn arbitrary_with(args: Self::Parameters) -> Self::Strategy {
        arb_store_path_name(MAX_NAME_LEN as u8, args).boxed()
    }
}

pub fn arb_store_path(max: u8, extension: Option<String>) -> impl Strategy<Value = StorePath> {
    (any::<StorePathHash>(), arb_store_path_name(max, extension)).prop_map(StorePath::from)
}

pub fn arb_drv_store_path() -> impl Strategy<Value = StorePath> {
    arb_store_path(MAX_NAME_LEN as u8 - 4 - 15, Some("drv".into()))
}

impl Arbitrary for StorePath {
    type Parameters = Option<String>;
    type Strategy = BoxedStrategy<StorePath>;
    fn arbitrary_with(args: Self::Parameters) -> Self::Strategy {
        arb_store_path(MAX_NAME_LEN as u8, args).boxed()
    }
}

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
