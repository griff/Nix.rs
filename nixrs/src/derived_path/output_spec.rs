use std::collections::BTreeSet;
use std::fmt;
use std::str::FromStr;

use crate::derivation::OutputName;
use crate::store_path::StorePathNameError;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum OutputSpec {
    All,
    Named(BTreeSet<OutputName>),
}

impl fmt::Display for OutputSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OutputSpec::All => f.write_str("*")?,
            OutputSpec::Named(outputs) => {
                let mut it = outputs.iter();
                if let Some(output) = it.next() {
                    write!(f, "{output}")?;
                    for output in it {
                        write!(f, ",{output}")?;
                    }
                }
            }
        }
        Ok(())
    }
}

impl FromStr for OutputSpec {
    type Err = StorePathNameError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "*" {
            Ok(OutputSpec::All)
        } else {
            let mut outputs = BTreeSet::new();
            for name in s.split(",") {
                let output = name.parse()?;
                outputs.insert(output);
            }
            Ok(OutputSpec::Named(outputs))
        }
    }
}

#[cfg(test)]
mod unittests {
    use rstest::rstest;

    use crate::{derived_path::OutputSpec, store_path::StorePathNameError};

    #[macro_export]
    macro_rules! set {
        () => { BTreeSet::new() };
        ($($x:expr),+ $(,)?) => {{
            let mut ret = std::collections::BTreeSet::new();
            $(
                ret.insert($x.parse().unwrap());
            )+
            ret
        }};
    }

    #[rstest]
    #[case("*", Ok(OutputSpec::All))]
    #[case("out", Ok(OutputSpec::Named(set!("out"))))]
    #[case("bin,dev,out", Ok(OutputSpec::Named(set!("bin", "dev", "out"))))]
    #[case("bin{n", Err(StorePathNameError::Symbol { position: 3, symbol: b'{' }))]
    #[case("out,bin{n", Err(StorePathNameError::Symbol { position: 3, symbol: b'{' }))]
    #[case(" bin{n", Err(StorePathNameError::Symbol { position: 0, symbol: b' ' }))]
    #[case("out,", Err(StorePathNameError::NameLength))]
    #[case("", Err(StorePathNameError::NameLength))]
    #[case(",out", Err(StorePathNameError::NameLength))]
    #[case::too_long(
        "test-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        Err(StorePathNameError::NameLength)
    )]
    fn parse(#[case] value: &str, #[case] expected: Result<OutputSpec, StorePathNameError>) {
        let actual: Result<OutputSpec, _> = value.parse();
        assert_eq!(actual, expected);
    }

    #[rstest]
    #[case(OutputSpec::All, "*")]
    #[case(OutputSpec::Named(set!("out")), "out")]
    #[case(OutputSpec::Named(set!("bin", "dev", "out")), "bin,dev,out")]
    fn display(#[case] value: OutputSpec, #[case] expected: &str) {
        assert_eq!(value.to_string(), expected);
    }
}
