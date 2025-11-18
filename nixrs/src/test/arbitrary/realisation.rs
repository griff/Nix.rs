use std::collections::BTreeMap;

use proptest::prelude::*;
use proptest::sample::SizeRange;

use crate::derived_path::OutputName;
use crate::hash;
use crate::realisation::{DrvOutput, DrvOutputs, Realisation};
use crate::store_path::StorePath;
use crate::test::arbitrary::signature::arb_signatures;

impl Arbitrary for DrvOutput {
    type Parameters = ();
    type Strategy = BoxedStrategy<DrvOutput>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        arb_drv_output().boxed()
    }
}

prop_compose! {
    pub fn arb_drv_output()
    (
        drv_hash in any::<hash::Hash>(),
        output_name in any::<OutputName>(),
    ) -> DrvOutput
    {
        DrvOutput { drv_hash, output_name }
    }
}

pub fn arb_drv_outputs(size: impl Into<SizeRange>) -> impl Strategy<Value = DrvOutputs> {
    let size = size.into();
    let min_size = size.start();
    prop::collection::vec(arb_realisation(), size)
        .prop_map(|r| {
            let mut ret = BTreeMap::new();
            for value in r {
                ret.insert(value.id.clone(), value);
            }
            ret
        })
        .prop_filter("BTreeMap minimum size", move |m| m.len() >= min_size)
}

impl Arbitrary for Realisation {
    type Parameters = ();
    type Strategy = BoxedStrategy<Realisation>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        arb_realisation().boxed()
    }
}

prop_compose! {
    pub fn arb_realisation()
    (
        id in any::<DrvOutput>(),
        out_path in any::<StorePath>(),
        signatures in arb_signatures(),
        dependent_realisations in  prop::collection::btree_map(
            arb_drv_output(),
            any::<StorePath>(),
            0..50),
    ) -> Realisation
    {
        Realisation {
            id, out_path, signatures, dependent_realisations,
        }
    }
}
