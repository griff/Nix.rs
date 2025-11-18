use std::io;

use proptest::{
    prelude::{Arbitrary, BoxedStrategy, Just, Strategy, any},
    prop_oneof,
};

use crate::test::daemon::ser::{Error, Operation, OperationType};

pub fn arb_write_number_operation() -> impl Strategy<Value = Operation> {
    (
        any::<u64>(),
        prop_oneof![
            Just(Ok(())),
            any::<u64>().prop_map(|v| Err(Error::UnexpectedNumber(v))),
            Just(Err(Error::WrongWrite(
                OperationType::WriteSlice,
                OperationType::WriteNumber
            ))),
            Just(Err(Error::WrongWrite(
                OperationType::WriteDisplay,
                OperationType::WriteNumber
            ))),
            any::<String>().prop_map(|s| Err(Error::Custom(s))),
            (any::<io::ErrorKind>(), any::<String>())
                .prop_map(|(kind, msg)| Err(Error::IO(kind, msg))),
        ],
    )
        .prop_filter("same number", |(v, res)| match res {
            Err(Error::UnexpectedNumber(exp_v)) => v != exp_v,
            _ => true,
        })
        .prop_map(|(v, res)| Operation::Number(v, res))
}

pub fn arb_write_slice_operation() -> impl Strategy<Value = Operation> {
    (
        any::<Vec<u8>>(),
        prop_oneof![
            Just(Ok(())),
            any::<Vec<u8>>().prop_map(|v| Err(Error::UnexpectedSlice(v))),
            Just(Err(Error::WrongWrite(
                OperationType::WriteNumber,
                OperationType::WriteSlice
            ))),
            Just(Err(Error::WrongWrite(
                OperationType::WriteDisplay,
                OperationType::WriteSlice
            ))),
            any::<String>().prop_map(|s| Err(Error::Custom(s))),
            (any::<io::ErrorKind>(), any::<String>())
                .prop_map(|(kind, msg)| Err(Error::IO(kind, msg))),
        ],
    )
        .prop_filter("same slice", |(v, res)| match res {
            Err(Error::UnexpectedSlice(exp_v)) => v != exp_v,
            _ => true,
        })
        .prop_map(|(v, res)| Operation::Slice(v, res))
}

#[allow(dead_code)]
pub fn arb_extra_write() -> impl Strategy<Value = Operation> {
    prop_oneof![
        any::<u64>().prop_map(|msg| {
            Operation::Number(msg, Err(Error::ExtraWrite(OperationType::WriteNumber)))
        }),
        any::<Vec<u8>>().prop_map(|msg| {
            Operation::Slice(msg, Err(Error::ExtraWrite(OperationType::WriteSlice)))
        }),
        any::<String>().prop_map(|msg| {
            Operation::Display(msg, Err(Error::ExtraWrite(OperationType::WriteDisplay)))
        }),
    ]
}

pub fn arb_write_display_operation() -> impl Strategy<Value = Operation> {
    (
        any::<String>(),
        prop_oneof![
            Just(Ok(())),
            any::<String>().prop_map(|v| Err(Error::UnexpectedDisplay(v))),
            Just(Err(Error::WrongWrite(
                OperationType::WriteNumber,
                OperationType::WriteDisplay
            ))),
            Just(Err(Error::WrongWrite(
                OperationType::WriteSlice,
                OperationType::WriteDisplay
            ))),
            any::<String>().prop_map(|s| Err(Error::Custom(s))),
            (any::<io::ErrorKind>(), any::<String>())
                .prop_map(|(kind, msg)| Err(Error::IO(kind, msg))),
        ],
    )
        .prop_filter("same string", |(v, res)| match res {
            Err(Error::UnexpectedDisplay(exp_v)) => v != exp_v,
            _ => true,
        })
        .prop_map(|(v, res)| Operation::Display(v, res))
}

pub fn arb_operation() -> impl Strategy<Value = Operation> {
    prop_oneof![
        arb_write_number_operation(),
        arb_write_slice_operation(),
        arb_write_display_operation(),
    ]
}

impl Arbitrary for Operation {
    type Parameters = ();
    type Strategy = BoxedStrategy<Operation>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        arb_operation().boxed()
    }
}
