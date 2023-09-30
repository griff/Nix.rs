use super::{ParseStorePathError, StoreDir, StorePath};
use crate::io::{StateParse, StatePrint};
use crate::StringSet;

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
        drv_path: StorePath,
        outputs: StringSet,
    },
}

impl DerivedPath {
    pub fn print(&self, store_dir: &StoreDir) -> String {
        match self {
            Self::Opaque(path) => store_dir.print_path(path),
            Self::Built { drv_path, outputs } => {
                let mut ret = store_dir.print_path(drv_path);
                ret.push('!');
                if outputs.is_empty() {
                    ret.push('*');
                } else {
                    let mut first = true;
                    for output in outputs.iter() {
                        if !first {
                            ret.push(',');
                        }
                        ret.push_str(output);
                        first = false;
                    }
                }
                ret
            }
        }
    }

    pub fn parse(store_dir: &StoreDir, s: &str) -> Result<Self, ParseStorePathError> {
        if let Some(pos) = s.find("!") {
            let drv_path = store_dir.parse_path(&s[..pos])?;
            let o = &s[(pos + 1)..];
            if o == "*" {
                Ok(DerivedPath::Built {
                    drv_path,
                    outputs: StringSet::new(),
                })
            } else {
                let mut outputs = StringSet::new();
                for output in o.split(",") {
                    outputs.insert(output.into());
                }
                Ok(DerivedPath::Built { drv_path, outputs })
            }
        } else {
            let path = store_dir.parse_path(s)?;
            Ok(DerivedPath::Opaque(path))
        }
    }
}

impl StateParse<DerivedPath> for StoreDir {
    type Err = ParseStorePathError;

    fn parse(&self, s: &str) -> Result<DerivedPath, Self::Err> {
        DerivedPath::parse(self, s)
    }
}

impl StatePrint<DerivedPath> for StoreDir {
    fn print(&self, item: &DerivedPath) -> String {
        item.print(self)
    }
}

#[cfg(any(test, feature = "test"))]
pub mod proptest {
    use crate::store::path::proptest::{arb_drv_store_path, arb_output_name};
    use ::proptest::{collection::btree_set, prelude::*};

    use super::*;

    impl Arbitrary for DerivedPath {
        type Parameters = ();
        type Strategy = BoxedStrategy<DerivedPath>;

        fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
            arb_derived_path().boxed()
        }
    }

    pub fn arb_derived_path() -> impl Strategy<Value = DerivedPath> {
        prop_oneof![
            any::<StorePath>().prop_map(|p| DerivedPath::Opaque(p)),
            (arb_drv_store_path(), btree_set(arb_output_name(), 0..5))
                .prop_map(|(drv_path, outputs)| DerivedPath::Built { drv_path, outputs })
        ]
    }
}

#[cfg(test)]
mod tests {
    use crate::string_set;

    use super::*;
    use pretty_assertions::assert_eq;

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
                drv_path,
                outputs: StringSet::new()
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
                drv_path,
                outputs: string_set!["out"]
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
                drv_path,
                outputs: string_set!["out", "dev"]
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
            drv_path,
            outputs: StringSet::new(),
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
            drv_path,
            outputs: string_set!["out"],
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
            drv_path,
            outputs: string_set!["out", "dev"],
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
}
