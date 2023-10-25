use std::fmt;
use std::str::FromStr;

use thiserror::Error;

use crate::{store_path::is_name, StringSet};

#[derive(Error, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub enum ParseOutputSpecError {
    #[error("output name '{0}' contains forbidden character")]
    BadOutputName(String),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum OutputSpec {
    All,
    Names(StringSet),
}

impl TryFrom<StringSet> for OutputSpec {
    type Error = ParseOutputSpecError;
    fn try_from(value: StringSet) -> Result<Self, Self::Error> {
        if value.is_empty() {
            Ok(Self::All)
        } else {
            if let Some(name) = value.iter().find(|s| !is_name(&s)) {
                Err(ParseOutputSpecError::BadOutputName(name.to_string()))
            } else {
                Ok(Self::Names(value))
            }
        }
    }
}

impl FromStr for OutputSpec {
    type Err = ParseOutputSpecError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "*" {
            Ok(Self::All)
        } else {
            let mut names = StringSet::new();
            for name in s.split(",") {
                let name = name.to_string();
                if !is_name(&name) {
                    return Err(ParseOutputSpecError::BadOutputName(name));
                }
                names.insert(name);
            }
            Ok(Self::Names(names))
        }
    }
}

impl TryFrom<String> for OutputSpec {
    type Error = ParseOutputSpecError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl<'a> TryFrom<&'a String> for OutputSpec {
    type Error = ParseOutputSpecError;

    fn try_from(value: &'a String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl<'a> TryFrom<&'a str> for OutputSpec {
    type Error = ParseOutputSpecError;

    fn try_from(value: &'a str) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl fmt::Display for OutputSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::All => {
                write!(f, "*")
            }
            Self::Names(names) => {
                let mut first = true;
                for name in names.iter() {
                    if !first {
                        f.write_str(",")?;
                    }
                    f.write_str(name)?;
                    first = false;
                }
                Ok(())
            }
        }
    }
}

#[cfg(any(test, feature = "test"))]
pub mod proptest {
    use crate::store_path::proptest::arb_output_name;
    use ::proptest::{collection::btree_set, prelude::*, sample::SizeRange};

    use super::*;

    impl Arbitrary for OutputSpec {
        type Parameters = SizeRange;
        type Strategy = BoxedStrategy<OutputSpec>;

        fn arbitrary_with(args: Self::Parameters) -> Self::Strategy {
            arb_output_spec(args).boxed()
        }
    }

    pub fn arb_output_spec(size: impl Into<SizeRange>) -> impl Strategy<Value = OutputSpec> {
        prop_oneof![
            Just(OutputSpec::All),
            btree_set(arb_output_name(), size).prop_map(|outputs| OutputSpec::Names(outputs))
        ]
    }
}
