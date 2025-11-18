use std::collections::BTreeMap;

use proptest::prelude::*;
use proptest::sample::SizeRange;

use crate::derivation::{BasicDerivation, DerivationOutput, DerivationOutputs};
use crate::derived_path::OutputName;
use crate::hash;
use crate::store_path::{StorePath, StorePathSet};
use crate::test::arbitrary::arb_byte_string;
use crate::test::arbitrary::helpers::Union;

impl Arbitrary for BasicDerivation {
    type Parameters = ();
    type Strategy = BoxedStrategy<BasicDerivation>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        arb_basic_derivation().boxed()
    }
}

prop_compose! {
    pub fn arb_basic_derivation()
    (
        outputs in arb_derivation_outputs(1..15),
        input_srcs in any::<StorePathSet>(),
        platform in arb_byte_string(),
        builder in arb_byte_string(),
        args in proptest::collection::vec(arb_byte_string(), SizeRange::default()),
        env in proptest::collection::btree_map(arb_byte_string(), arb_byte_string(), SizeRange::default()),
        drv_path in any::<StorePath>()
    ) -> BasicDerivation
    {
        BasicDerivation {
            outputs, input_srcs, platform, builder, args, env, drv_path,
        }
    }
}

pub fn arb_derivation_outputs(
    size: impl Into<SizeRange>,
) -> impl Strategy<Value = DerivationOutputs> {
    use DerivationOutput::*;
    let size = size.into();
    #[cfg(feature = "xp-ca-derivations")]
    let size2 = size.clone();
    //InputAddressed
    let input = prop::collection::btree_map(
        any::<OutputName>(),
        arb_derivation_output_input_addressed(),
        size.clone(),
    )
    .boxed();
    // CAFixed
    let fixed = arb_derivation_output_fixed()
        .prop_map(|ca| {
            let mut ret = BTreeMap::new();
            let name = OutputName::default();
            ret.insert(name, ca);
            ret
        })
        .boxed();
    // Deferred
    let deferred =
        prop::collection::btree_map(any::<OutputName>(), Just(Deferred), size.clone()).boxed();

    #[cfg_attr(
        not(any(feature = "xp-ca-derivations", feature = "xp-impure-derivations")),
        allow(unused_mut)
    )]
    let mut ret = Union::new([input, fixed, deferred]);
    #[cfg(feature = "xp-ca-derivations")]
    {
        // CAFloating
        ret = ret.or(any::<hash::Algorithm>()
            .prop_flat_map(move |hash_type| {
                prop::collection::btree_map(
                    any::<OutputName>(),
                    arb_derivation_output_floating(Just(hash_type)),
                    size2.clone(),
                )
            })
            .boxed());
    }
    #[cfg(feature = "xp-impure-derivations")]
    {
        // Impure
        ret = ret.or(prop::collection::btree_map(
            any::<OutputName>(),
            arb_derivation_output_impure(),
            size.clone(),
        ));
    }
    ret
}

impl Arbitrary for DerivationOutput {
    type Parameters = ();
    type Strategy = BoxedStrategy<DerivationOutput>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        arb_derivation_output().boxed()
    }
}

pub fn arb_derivation_output_input_addressed() -> impl Strategy<Value = DerivationOutput> {
    any::<StorePath>().prop_map(DerivationOutput::InputAddressed)
}

#[cfg(feature = "xp-dynamic-derivations")]
pub fn arb_derivation_output_fixed() -> impl Strategy<Value = DerivationOutput> {
    any::<ContentAddress>().prop_map(DerivationOutput::CAFixed)
}

#[cfg(not(feature = "xp-dynamic-derivations"))]
pub fn arb_derivation_output_fixed() -> impl Strategy<Value = DerivationOutput> {
    use crate::store_path::ContentAddress;

    prop_oneof![
        any::<hash::Hash>().prop_map(|h| DerivationOutput::CAFixed(ContentAddress::Flat(h))),
        any::<hash::Hash>().prop_map(|h| DerivationOutput::CAFixed(ContentAddress::Recursive(h)))
    ]
}

#[cfg(feature = "xp-impure-derivations")]
pub fn arb_derivation_output_impure() -> impl Strategy<Value = DerivationOutput> {
    any::<ContentAddressMethodAlgorithm>(any::<hash::Algorithm>())
        .prop_map(|ca| DerivationOutput::Impure(ca))
}

#[cfg(feature = "xp-ca-derivations")]
pub fn arb_derivation_output_floating<H>(hash_type: H) -> impl Strategy<Value = DerivationOutput>
where
    H: Strategy<Value = hash::Algorithm>,
{
    any::<ContentAddressMethodAlgorithm>(hash_type).prop_map(|ca| DerivationOutput::CAFloating(ca))
}

pub fn arb_derivation_output() -> impl Strategy<Value = DerivationOutput> {
    use DerivationOutput::*;
    #[cfg(all(feature = "xp-ca-derivations", feature = "xp-impure-derivations"))]
    {
        prop_oneof![
            arb_derivation_output_input_addressed(),
            arb_derivation_output_fixed(),
            arb_derivation_output_floating(any::<hash::Algorithm>()),
            Just(Deferred),
            arb_derivation_output_impure(),
        ]
    }
    #[cfg(all(not(feature = "xp-ca-derivations"), feature = "xp-impure-derivations"))]
    {
        prop_oneof![
            arb_derivation_output_input_addressed(),
            arb_derivation_output_fixed(),
            Just(Deferred),
            arb_derivation_output_impure(),
        ]
    }
    #[cfg(all(feature = "xp-ca-derivations", not(feature = "xp-impure-derivations")))]
    {
        prop_oneof![
            arb_derivation_output_input_addressed(),
            arb_derivation_output_fixed(),
            arb_derivation_output_floating(any::<hash::Algorithm>()),
            Just(Deferred),
        ]
    }
    #[cfg(not(any(feature = "xp-ca-derivations", feature = "xp-impure-derivations")))]
    {
        prop_oneof![
            arb_derivation_output_input_addressed(),
            arb_derivation_output_fixed(),
            Just(Deferred),
        ]
    }
}
