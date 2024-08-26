use std::fmt;

use thiserror::Error;

use crate::io::{StateParse, StatePrint};
use crate::store_path::{ParseStorePathError, StoreDir, StorePath};

use super::{OutputSpec, ParseOutputSpecError};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SingleDerivedPath {
    Opaque(StorePath),
    Built {
        drv_path: Box<SingleDerivedPath>,
        output: String,
    },
}

impl SingleDerivedPath {
    pub fn parse(store_dir: &StoreDir, s: &str) -> Result<Self, ParseDerivedPathError> {
        if let Some(pos) = s.rfind('!') {
            let drv_path = SingleDerivedPath::parse(store_dir, &s[..pos])?;
            let output = s[(pos + 1)..].to_string();
            Ok(SingleDerivedPath::Built {
                drv_path: Box::new(drv_path),
                output,
            })
        } else {
            let path = store_dir.parse_path(s)?;
            Ok(SingleDerivedPath::Opaque(path))
        }
    }
    pub fn legacy_display<'a>(&'a self, store_dir: &'a StoreDir) -> impl fmt::Display + 'a {
        SingleDerivedPathDisplay {
            store_dir,
            seperator: "!",
            path: self,
        }
    }

    pub fn display<'a>(&'a self, store_dir: &'a StoreDir) -> impl fmt::Display + 'a {
        SingleDerivedPathDisplay {
            store_dir,
            seperator: "^",
            path: self,
        }
    }
}

impl From<StorePath> for SingleDerivedPath {
    fn from(value: StorePath) -> Self {
        SingleDerivedPath::Opaque(value)
    }
}

struct SingleDerivedPathDisplay<'a> {
    store_dir: &'a StoreDir,
    seperator: &'a str,
    path: &'a SingleDerivedPath,
}

impl<'a> fmt::Display for SingleDerivedPathDisplay<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.path {
            SingleDerivedPath::Opaque(drv_path) => {
                write!(f, "{}", self.store_dir.print_path(drv_path))
            }
            SingleDerivedPath::Built { drv_path, output } => {
                write!(
                    f,
                    "{}{}{}",
                    drv_path.legacy_display(self.store_dir),
                    self.seperator,
                    output
                )
            }
        }
    }
}

pub struct DerivedPathLegacyFormat(DerivedPath);

impl DerivedPathLegacyFormat {
    pub fn from_derived_path(path: DerivedPath) -> Self {
        Self(path)
    }
}

impl From<DerivedPath> for DerivedPathLegacyFormat {
    fn from(value: DerivedPath) -> Self {
        Self::from_derived_path(value)
    }
}

impl From<DerivedPathLegacyFormat> for DerivedPath {
    fn from(value: DerivedPathLegacyFormat) -> Self {
        value.0
    }
}

impl StatePrint<DerivedPathLegacyFormat> for StoreDir {
    fn print(&self, item: &DerivedPathLegacyFormat) -> String {
        match &item.0 {
            DerivedPath::Opaque(path) => self.print_path(path),
            DerivedPath::Built { drv_path, outputs } => {
                format!("{}!{}", drv_path.legacy_display(self), outputs)
            }
        }
    }
}

#[derive(Error, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub enum ParseDerivedPathError {
    #[error("{0}")]
    ParseStorePath(
        #[source]
        #[from]
        ParseStorePathError,
    ),
    #[error("{0}")]
    ParseOutputSpec(
        #[source]
        #[from]
        ParseOutputSpecError,
    ),
}

#[derive(Error, Debug)]
pub enum ReadDerivedPathError {
    #[error("{0}")]
    BadDerivecPath(#[from] ParseDerivedPathError),
    #[error("io error reading store path {0}")]
    IO(#[from] std::io::Error),
}

/// A "derived path" is a very simple sort of expression that evaluates
/// to (concrete) store path. It is either:
///
/// - opaque, in which case it is just a concrete store path with
///   possibly no known derivation
///
/// - built, in which case it is a pair of a derivation path and an
///   output name.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DerivedPath {
    Opaque(StorePath),
    Built {
        drv_path: SingleDerivedPath,
        outputs: OutputSpec,
    },
}

impl DerivedPath {
    pub fn print(&self, store_dir: &StoreDir) -> String {
        match self {
            Self::Opaque(path) => store_dir.print_path(path),
            Self::Built { drv_path, outputs } => match drv_path {
                SingleDerivedPath::Opaque(drv_path) => {
                    format!("{}!{}", store_dir.display_path(drv_path), outputs)
                }
                SingleDerivedPath::Built { drv_path, output } => {
                    format!(
                        "{}!{}!{}",
                        drv_path.legacy_display(store_dir),
                        output,
                        outputs
                    )
                }
            },
        }
    }

    pub fn parse(store_dir: &StoreDir, s: &str) -> Result<Self, ParseDerivedPathError> {
        if let Some(pos) = s.rfind('!') {
            let drv_path = SingleDerivedPath::parse(store_dir, &s[..pos])?;
            let o = &s[(pos + 1)..];
            let outputs = o.parse()?;
            Ok(DerivedPath::Built { drv_path, outputs })
        } else {
            let path = store_dir.parse_path(s)?;
            Ok(DerivedPath::Opaque(path))
        }
    }
}

impl StateParse<DerivedPath> for StoreDir {
    type Err = ReadDerivedPathError;

    fn parse(&self, s: &str) -> Result<DerivedPath, Self::Err> {
        Ok(DerivedPath::parse(self, s)?)
    }
}

impl StatePrint<DerivedPath> for StoreDir {
    fn print(&self, item: &DerivedPath) -> String {
        item.print(self)
    }
}

#[cfg(any(test, feature = "test"))]
pub mod proptest {
    use crate::{
        store::output_spec::proptest::arb_output_spec,
        store_path::proptest::{arb_drv_store_path, arb_output_name},
    };
    use proptest::prelude::*;

    use super::*;

    impl Arbitrary for DerivedPath {
        type Parameters = ();
        type Strategy = BoxedStrategy<DerivedPath>;

        fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
            arb_derived_path().boxed()
        }
    }

    fn arb_single_derived_path_box() -> impl Strategy<Value = Box<SingleDerivedPath>> {
        let leaf = arb_drv_store_path().prop_map(|p| Box::new(SingleDerivedPath::Opaque(p)));
        leaf.prop_recursive(
            8, // 8 levels deep
            5, // Shoot for maximum size of 256 nodes
            1, // We put up to 10 items per collection
            |inner| {
                (inner, arb_output_name()).prop_map(|(drv_path, output)| {
                    Box::new(SingleDerivedPath::Built { drv_path, output })
                })
            },
        )
    }

    pub fn arb_single_derived_path() -> impl Strategy<Value = SingleDerivedPath> {
        prop_oneof![
            arb_drv_store_path().prop_map(SingleDerivedPath::Opaque),
            (arb_single_derived_path_box(), arb_output_name())
                .prop_map(|(drv_path, output)| { SingleDerivedPath::Built { drv_path, output } })
        ]
    }

    pub fn arb_derived_path() -> impl Strategy<Value = DerivedPath> {
        prop_oneof![
            any::<StorePath>().prop_map(DerivedPath::Opaque),
            (arb_single_derived_path(), arb_output_spec(1..5))
                .prop_map(|(drv_path, outputs)| DerivedPath::Built { drv_path, outputs })
        ]
    }
}

#[cfg(test)]
mod tests {
    use crate::string_set;

    use super::*;
    use pretty_assertions::assert_eq;
    use proptest::prelude::*;
    use proptest::proptest;

    #[test]
    fn test_derived_path_parse() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let drv_path = store_dir
            .parse_path("/nix/store/7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3.drv")
            .unwrap();
        let p = DerivedPath::parse(
            &store_dir,
            "/nix/store/7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3.drv!*",
        )
        .unwrap();
        assert_eq!(
            p,
            DerivedPath::Built {
                drv_path: SingleDerivedPath::Opaque(drv_path),
                outputs: OutputSpec::All
            }
        );
    }

    #[test]
    fn test_derived_path_parse1() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let drv_path = store_dir
            .parse_path("/nix/store/7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3.drv")
            .unwrap();
        let p = DerivedPath::parse(
            &store_dir,
            "/nix/store/7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3.drv!out",
        )
        .unwrap();
        assert_eq!(
            p,
            DerivedPath::Built {
                drv_path: SingleDerivedPath::Opaque(drv_path),
                outputs: string_set!["out"].try_into().unwrap()
            }
        );
    }

    #[test]
    fn test_derived_path_parse2() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let drv_path = store_dir
            .parse_path("/nix/store/7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3.drv")
            .unwrap();
        let p = DerivedPath::parse(
            &store_dir,
            "/nix/store/7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3.drv!dev,out",
        )
        .unwrap();
        assert_eq!(
            p,
            DerivedPath::Built {
                drv_path: SingleDerivedPath::Opaque(drv_path),
                outputs: string_set!["out", "dev"].try_into().unwrap()
            }
        );
    }

    #[test]
    fn test_derived_path_parse_opaque() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let path = store_dir
            .parse_path("/nix/store/7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3")
            .unwrap();
        let p = DerivedPath::parse(
            &store_dir,
            "/nix/store/7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3",
        )
        .unwrap();
        assert_eq!(p, DerivedPath::Opaque(path));
    }

    #[test]
    fn test_derived_path_print() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let drv_path = store_dir
            .parse_path("/nix/store/7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3.drv")
            .unwrap();
        let dp = DerivedPath::Built {
            drv_path: SingleDerivedPath::Opaque(drv_path),
            outputs: OutputSpec::All,
        };
        assert_eq!(
            dp.print(&store_dir),
            "/nix/store/7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3.drv!*"
        );
    }

    #[test]
    fn test_derived_path_print1() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let drv_path = store_dir
            .parse_path("/nix/store/7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3.drv")
            .unwrap();
        let dp = DerivedPath::Built {
            drv_path: SingleDerivedPath::Opaque(drv_path),
            outputs: string_set!["out"].try_into().unwrap(),
        };
        assert_eq!(
            dp.print(&store_dir),
            "/nix/store/7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3.drv!out"
        );
    }

    #[test]
    fn test_derived_path_print2() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let drv_path = store_dir
            .parse_path("/nix/store/7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3.drv")
            .unwrap();
        let dp = DerivedPath::Built {
            drv_path: SingleDerivedPath::Opaque(drv_path),
            outputs: string_set!["out", "dev"].try_into().unwrap(),
        };
        assert_eq!(
            dp.print(&store_dir),
            "/nix/store/7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3.drv!dev,out"
        );
    }

    #[test]
    fn test_derived_path_print_opaque() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let path = store_dir
            .parse_path("/nix/store/7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3")
            .unwrap();
        let dp = DerivedPath::Opaque(path);
        assert_eq!(
            dp.print(&store_dir),
            "/nix/store/7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3"
        );
    }

    #[test]
    fn test_derived_path_parse_printed_built() {
        let store_dir = StoreDir::default();
        let path = DerivedPath::Built {
            drv_path: SingleDerivedPath::Built {
                drv_path: Box::new(SingleDerivedPath::Built {
                    drv_path: Box::new(SingleDerivedPath::Built {
                        drv_path: Box::new(SingleDerivedPath::Opaque(
                            StorePath::new_from_base_name("00000000000000000000000000000000-a.drv")
                                .unwrap(),
                        )),
                        output: "_CC7++3".into(),
                    }),
                    output: "+c=_?s".into(),
                }),
                output: "l".into(),
            },
            outputs: OutputSpec::Names(string_set!["=4+-_+.?8W"]),
        };
        let s = path.print(&store_dir);
        let path2 = DerivedPath::parse(&store_dir, &s).unwrap();
        assert_eq!(path, path2);
    }

    proptest! {
        #[test]
        fn proptest_derived_path_print_parsing(
            drv_path in any::<DerivedPath>(),
        )
        {
            let store_dir = StoreDir::default();
            let s = drv_path.print(&store_dir);
            let drv_path2 = DerivedPath::parse(&store_dir, &s).unwrap();
            assert_eq!(drv_path, drv_path2);
        }
    }
}
