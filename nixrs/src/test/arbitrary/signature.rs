use proptest::{arbitrary::Arbitrary, prelude::*};

use crate::signature::{SIGNATURE_BYTES, Signature, SignatureSet};

pub fn arb_key_name(max: u8) -> impl Strategy<Value = String> {
    "[a-zA-Z0-9+\\-_?=][a-zA-Z0-9+\\-_?=.]{0,210}".prop_map(move |mut s| {
        if s.len() > max as usize {
            s.truncate(max as usize);
        }
        s
    })
}

pub fn arb_signature(max: u8) -> impl Strategy<Value = Signature> {
    (arb_key_name(max), any::<[u8; SIGNATURE_BYTES]>())
        .prop_map(|(name, signature)| Signature::from_parts(&name, &signature).unwrap())
}

pub fn arb_signatures() -> impl Strategy<Value = SignatureSet> {
    prop::collection::btree_set(any::<Signature>(), 0..5)
}

impl Arbitrary for Signature {
    type Parameters = ();
    type Strategy = BoxedStrategy<Signature>;
    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        arb_signature(211).boxed()
    }
}
