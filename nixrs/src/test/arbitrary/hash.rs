use proptest::prelude::*;

use crate::hash::{Algorithm, Hash, NarHash, Sha256};

impl Arbitrary for NarHash {
    type Parameters = ();
    type Strategy = BoxedStrategy<NarHash>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        any::<Sha256>().prop_map(NarHash::from).boxed()
    }
}

impl Arbitrary for Sha256 {
    type Parameters = ();
    type Strategy = BoxedStrategy<Sha256>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        any::<[u8; Algorithm::SHA256.size()]>()
            .prop_map(Sha256::from)
            .boxed()
    }
}

impl Arbitrary for Algorithm {
    type Parameters = ();
    type Strategy = BoxedStrategy<Algorithm>;
    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        prop_oneof![
            1 => Just(Algorithm::MD5),
            2 => Just(Algorithm::SHA1),
            5 => Just(Algorithm::SHA256),
            2 => Just(Algorithm::SHA512)
        ]
        .boxed()
    }
}

impl Arbitrary for Hash {
    type Parameters = Algorithm;
    type Strategy = BoxedStrategy<Hash>;

    fn arbitrary_with(algorithm: Self::Parameters) -> Self::Strategy {
        any_hash(algorithm).boxed()
    }
}

prop_compose! {
    fn any_hash(algorithm: Algorithm)
               (data in any::<Vec<u8>>()) -> Hash
    {
        algorithm.digest(data)
    }
}
