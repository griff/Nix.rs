use proptest::{collection::btree_set, prelude::*};

use crate::{
    derived_path::{DerivedPath, OutputName, OutputSpec, SingleDerivedPath},
    store_path::StorePath,
};

impl Arbitrary for OutputName {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        use crate::test::arbitrary::store_path::arb_output_name;
        arb_output_name().prop_map(OutputName).boxed()
    }
}

impl Arbitrary for OutputSpec {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        prop_oneof![
            Just(OutputSpec::All),
            btree_set(any::<OutputName>(), 1..10).prop_map(OutputSpec::Named),
        ]
        .boxed()
    }
}

impl Arbitrary for SingleDerivedPath {
    type Parameters = ();
    type Strategy = BoxedStrategy<SingleDerivedPath>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        use proptest::prelude::*;
        let opaque = any::<StorePath>().prop_map(SingleDerivedPath::Opaque);
        let leaf = prop_oneof![
            4 => opaque.clone(),
            1 => opaque.prop_recursive(6, 1, 1, |inner| {
                (any::<OutputName>(), inner).prop_map(|(output, drv_path)| {
                    SingleDerivedPath::Built {
                        drv_path: Box::new(drv_path),
                        output,
                    }
                })
            })
        ];
        leaf.boxed()
    }
}

impl Arbitrary for DerivedPath {
    type Parameters = ();
    type Strategy = BoxedStrategy<DerivedPath>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        prop_oneof![
            any::<StorePath>().prop_map(DerivedPath::Opaque),
            (any::<OutputSpec>(), any::<SingleDerivedPath>())
                .prop_map(|(outputs, drv_path)| { DerivedPath::Built { drv_path, outputs } })
        ]
        .boxed()
    }
}
