use proptest::prelude::*;

use crate::hash::{Algorithm, Hash, Sha256};
use crate::store_path::{
    ContentAddress, ContentAddressMethod, ContentAddressMethodAlgorithm, FixedOutput,
    FixedOutputMethod, FixedOutputMethodAlgorithm, FullStorePath, StoreDir, StorePath,
    StorePathHash, StorePathName,
};

impl Arbitrary for FixedOutputMethod {
    type Parameters = ();
    type Strategy = BoxedStrategy<FixedOutputMethod>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        prop_oneof![
            Just(FixedOutputMethod::Flat),
            Just(FixedOutputMethod::Recursive),
        ]
        .boxed()
    }
}

impl Arbitrary for ContentAddressMethod {
    type Parameters = ();
    type Strategy = BoxedStrategy<ContentAddressMethod>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        prop_oneof![
            Just(ContentAddressMethod::Text),
            any::<FixedOutputMethod>().prop_map(ContentAddressMethod::Fixed),
        ]
        .boxed()
    }
}

fn limit_algorithm(algo: Option<Algorithm>) -> impl Strategy<Value = Algorithm> {
    match algo {
        Some(value) => Just(value).boxed(),
        None => any::<Algorithm>().boxed(),
    }
}

impl Arbitrary for FixedOutputMethodAlgorithm {
    type Parameters = Option<Algorithm>;
    type Strategy = BoxedStrategy<FixedOutputMethodAlgorithm>;

    fn arbitrary_with(args: Self::Parameters) -> Self::Strategy {
        (any::<FixedOutputMethod>(), limit_algorithm(args))
            .prop_map(|(method, algorithm)| -> _ {
                FixedOutputMethodAlgorithm { method, algorithm }
            })
            .boxed()
    }
}

impl Arbitrary for ContentAddressMethodAlgorithm {
    type Parameters = Option<Algorithm>;
    type Strategy = BoxedStrategy<ContentAddressMethodAlgorithm>;

    fn arbitrary_with(args: Self::Parameters) -> Self::Strategy {
        prop_oneof![
            Just(ContentAddressMethodAlgorithm::Text),
            any_with::<FixedOutputMethodAlgorithm>(args)
                .prop_map(ContentAddressMethodAlgorithm::Fixed),
        ]
        .boxed()
    }
}

impl Arbitrary for FixedOutput {
    type Parameters = ();
    type Strategy = BoxedStrategy<FixedOutput>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        (any::<FixedOutputMethod>(), any::<Hash>())
            .prop_map(|(method, hash)| FixedOutput { method, hash })
            .boxed()
    }
}

impl Arbitrary for ContentAddress {
    type Parameters = ();
    type Strategy = BoxedStrategy<ContentAddress>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        prop_oneof![
            any::<Sha256>().prop_map(ContentAddress::Text),
            any::<FixedOutput>().prop_map(ContentAddress::Fixed),
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
            .prop_map(StorePathHash::from_array)
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
        if max > StorePathName::max_len() as u8 - len {
            max = StorePathName::max_len() as u8 - len;
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
        arb_store_path_name(StorePathName::max_len() as u8, args).boxed()
    }
}

pub fn arb_full_store_path(
    max: u8,
    extension: Option<String>,
) -> impl Strategy<Value = FullStorePath> {
    (arb_store_dir(), arb_store_path(max, extension))
        .prop_map(|(store_dir, path)| FullStorePath { store_dir, path })
}

pub fn arb_store_path(max: u8, extension: Option<String>) -> impl Strategy<Value = StorePath> {
    (any::<StorePathHash>(), arb_store_path_name(max, extension)).prop_map(StorePath::from)
}

pub fn arb_drv_store_path() -> impl Strategy<Value = StorePath> {
    arb_store_path(StorePathName::max_len() as u8 - 4 - 15, Some("drv".into()))
}

pub fn arb_full_drv_store_path() -> impl Strategy<Value = FullStorePath> {
    (arb_store_dir(), arb_drv_store_path())
        .prop_map(|(store_dir, path)| FullStorePath { store_dir, path })
}

impl Arbitrary for StorePath {
    type Parameters = Option<String>;
    type Strategy = BoxedStrategy<StorePath>;
    fn arbitrary_with(args: Self::Parameters) -> Self::Strategy {
        arb_store_path(StorePathName::max_len() as u8, args).boxed()
    }
}

impl Arbitrary for FullStorePath {
    type Parameters = Option<String>;
    type Strategy = BoxedStrategy<FullStorePath>;
    fn arbitrary_with(args: Self::Parameters) -> Self::Strategy {
        arb_full_store_path(StorePathName::max_len() as u8, args).boxed()
    }
}

// FUTUREWORK: Generate something sensible
pub fn arb_store_dir() -> impl Strategy<Value = StoreDir> {
    Just(StoreDir::default())
}

impl Arbitrary for StoreDir {
    type Parameters = ();
    type Strategy = BoxedStrategy<StoreDir>;
    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        arb_store_dir().boxed()
    }
}
