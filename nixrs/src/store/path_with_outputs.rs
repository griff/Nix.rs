use std::convert::{TryFrom, TryInto};

use super::{DerivedPath, ParseStorePathError, ReadStorePathError, StoreDir, StorePath};
use crate::io::{StateParse, StatePrint};
use crate::StringSet;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct StorePathWithOutputs {
    path: StorePath,
    outputs: StringSet,
}

impl StorePathWithOutputs {
    pub fn parse(store_dir: &StoreDir, s: &str) -> Result<Self, ParseStorePathError> {
        if let Some(pos) = s.find("!") {
            let path = store_dir.parse_path(&s[..pos])?;
            let mut outputs = StringSet::new();
            for output in s[(pos + 1)..].split(",") {
                outputs.insert(output.to_owned());
            }
            Ok(StorePathWithOutputs { path, outputs })
        } else {
            let path = store_dir.parse_path(s)?;
            Ok(StorePathWithOutputs {
                path,
                outputs: StringSet::new(),
            })
        }
    }

    pub fn print(&self, store_dir: &StoreDir) -> String {
        let mut ret = store_dir.print_path(&self.path);
        if self.outputs.is_empty() {
            ret
        } else {
            ret.push('!');
            let mut first = true;
            for output in self.outputs.iter() {
                if !first {
                    ret.push(',');
                }
                ret.push_str(output);
                first = false;
            }
            ret
        }
    }
}

impl TryFrom<DerivedPath> for StorePathWithOutputs {
    type Error = StorePath;

    fn try_from(value: DerivedPath) -> Result<Self, Self::Error> {
        match value {
            DerivedPath::Built {
                drv_path: path,
                outputs,
            } => Ok(StorePathWithOutputs { path, outputs }),
            DerivedPath::Opaque(path) => {
                if path.is_derivation() {
                    Err(path)
                } else {
                    Ok(StorePathWithOutputs {
                        outputs: StringSet::new(),
                        path,
                    })
                }
            }
        }
    }
}
impl<'a> TryFrom<&'a DerivedPath> for StorePathWithOutputs {
    type Error = StorePath;

    fn try_from(value: &'a DerivedPath) -> Result<Self, Self::Error> {
        value.clone().try_into()
    }
}

impl StateParse<StorePathWithOutputs> for StoreDir {
    type Err = ReadStorePathError;

    fn parse(&self, s: &str) -> Result<StorePathWithOutputs, Self::Err> {
        Ok(StorePathWithOutputs::parse(self, s)?)
    }
}

impl StatePrint<StorePathWithOutputs> for StoreDir {
    fn print(&self, item: &StorePathWithOutputs) -> String {
        item.print(&self)
    }
}

impl From<StorePathWithOutputs> for DerivedPath {
    fn from(path: StorePathWithOutputs) -> DerivedPath {
        if path.path.is_derivation() {
            DerivedPath::Built {
                drv_path: path.path,
                outputs: path.outputs,
            }
        } else {
            DerivedPath::Opaque(path.path)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::string_set;

    #[test]
    fn test_parse() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let s = "/nix/store/7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3";
        let path = StorePathWithOutputs::parse(&store_dir, s).unwrap();
        assert_eq!(
            path.path.to_string(),
            "7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3"
        );
        assert_eq!(path.outputs, StringSet::new());
        let s_path = store_dir.parse_path(s).unwrap();
        assert_eq!(
            path,
            StorePathWithOutputs {
                path: s_path.clone(),
                outputs: StringSet::new()
            }
        );

        let path: StorePathWithOutputs = store_dir.parse(s).unwrap();
        assert_eq!(
            path,
            StorePathWithOutputs {
                path: s_path,
                outputs: StringSet::new()
            }
        );
    }

    #[test]
    fn test_parse_1() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let s = "/nix/store/7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3!out";
        let path = StorePathWithOutputs::parse(&store_dir, s).unwrap();
        assert_eq!(
            path.path.to_string(),
            "7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3"
        );
        assert_eq!(path.outputs, string_set!["out"]);
        let s_path = store_dir
            .parse_path("/nix/store/7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3")
            .unwrap();
        assert_eq!(
            path,
            StorePathWithOutputs {
                path: s_path.clone(),
                outputs: string_set!["out"]
            }
        );

        let path: StorePathWithOutputs = store_dir.parse(s).unwrap();
        assert_eq!(
            path,
            StorePathWithOutputs {
                path: s_path.clone(),
                outputs: string_set!["out"]
            }
        );
    }

    #[test]
    fn test_parse_3() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let s = "/nix/store/7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3!out,dev,bin";
        let path = StorePathWithOutputs::parse(&store_dir, s).expect("stuff");
        assert_eq!(
            path.path.to_string(),
            "7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3"
        );
        assert_eq!(path.outputs, string_set!["out", "dev", "bin"]);
        let s_path = store_dir
            .parse_path("/nix/store/7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3")
            .unwrap();
        assert_eq!(
            path,
            StorePathWithOutputs {
                path: s_path.clone(),
                outputs: string_set!["out", "dev", "bin"]
            }
        );

        let path: StorePathWithOutputs = store_dir.parse(s).unwrap();
        assert_eq!(
            path,
            StorePathWithOutputs {
                path: s_path.clone(),
                outputs: string_set!["out", "dev", "bin"]
            }
        );
    }

    #[test]
    fn test_print() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let s = "/nix/store/7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3";
        let path = store_dir.parse_path(s).unwrap();
        let sp = StorePathWithOutputs {
            path,
            outputs: StringSet::new(),
        };

        assert_eq!(sp.print(&store_dir), s);
        assert_eq!(store_dir.print(&sp), s);
    }

    #[test]
    fn test_print_1() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let path = store_dir
            .parse_path("/nix/store/7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3")
            .unwrap();
        let sp = StorePathWithOutputs {
            path,
            outputs: string_set!["out"],
        };
        let s = "/nix/store/7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3!out";
        assert_eq!(sp.print(&store_dir), s);
        assert_eq!(store_dir.print(&sp), s);
    }

    #[test]
    fn test_print_3() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let path = store_dir
            .parse_path("/nix/store/7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3")
            .unwrap();
        let sp = StorePathWithOutputs {
            path,
            outputs: string_set!["out", "dev", "bin"],
        };
        let s = "/nix/store/7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3!bin,dev,out";
        assert_eq!(sp.print(&store_dir), s);
        assert_eq!(store_dir.print(&sp), s);
    }

    #[test]
    fn test_from_derived_path() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let path = store_dir
            .parse_path("/nix/store/7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3.drv")
            .unwrap();
        let sp2 = StorePathWithOutputs {
            path: path.clone(),
            outputs: StringSet::new(),
        };
        let dp = DerivedPath::Built {
            drv_path: path.clone(),
            outputs: StringSet::new(),
        };
        let sp: StorePathWithOutputs = StorePathWithOutputs::try_from(&dp).unwrap();
        assert_eq!(sp, sp2);
        let sp: StorePathWithOutputs = dp.try_into().unwrap();
        assert_eq!(sp, sp2);
    }

    #[test]
    fn test_from_derived_path_3() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let path = store_dir
            .parse_path("/nix/store/7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3.drv")
            .unwrap();
        let sp2 = StorePathWithOutputs {
            path: path.clone(),
            outputs: string_set!["out", "dev", "bin"],
        };
        let dp = DerivedPath::Built {
            drv_path: path.clone(),
            outputs: string_set!["out", "dev", "bin"],
        };
        let sp: StorePathWithOutputs = StorePathWithOutputs::try_from(&dp).unwrap();
        assert_eq!(sp, sp2);
        let sp: StorePathWithOutputs = dp.try_into().unwrap();
        assert_eq!(sp, sp2);
    }

    #[test]
    fn test_from_derived_path_opaque() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let path = store_dir
            .parse_path("/nix/store/7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3")
            .unwrap();
        let sp2 = StorePathWithOutputs {
            path: path.clone(),
            outputs: StringSet::new(),
        };
        let dp = DerivedPath::Opaque(path.clone());
        let sp: StorePathWithOutputs = StorePathWithOutputs::try_from(&dp).unwrap();
        assert_eq!(sp, sp2);
        let sp: StorePathWithOutputs = dp.try_into().unwrap();
        assert_eq!(sp, sp2);
    }

    #[test]
    fn test_from_derived_path_opaque_drv() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let path = store_dir
            .parse_path("/nix/store/7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3.drv")
            .unwrap();
        let dp = DerivedPath::Opaque(path.clone());
        let sp: Result<StorePathWithOutputs, StorePath> = dp.try_into();
        assert_eq!(sp, Err(path));
    }

    #[test]
    fn test_to_derived_path() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let path = store_dir
            .parse_path("/nix/store/7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3")
            .unwrap();
        let dp = DerivedPath::Opaque(path.clone());
        let sp = StorePathWithOutputs {
            path,
            outputs: string_set!["bin", "dev", "out"],
        };
        let dp2: DerivedPath = sp.into();
        assert_eq!(dp, dp2);
    }

    #[test]
    fn test_to_derived_path_built() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let path = store_dir
            .parse_path("/nix/store/7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3.drv")
            .unwrap();
        let dp = DerivedPath::Built {
            drv_path: path.clone(),
            outputs: StringSet::new(),
        };
        let sp = StorePathWithOutputs {
            path,
            outputs: StringSet::new(),
        };
        let dp2: DerivedPath = sp.into();
        assert_eq!(dp, dp2);
    }

    #[test]
    fn test_to_derived_path_built_1() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let path = store_dir
            .parse_path("/nix/store/7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3.drv")
            .unwrap();
        let dp = DerivedPath::Built {
            drv_path: path.clone(),
            outputs: string_set!["out"],
        };
        let sp = StorePathWithOutputs {
            path,
            outputs: string_set!["out"],
        };
        let dp2: DerivedPath = sp.into();
        assert_eq!(dp, dp2);
    }

    #[test]
    fn test_to_derived_path_built_3() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let path = store_dir
            .parse_path("/nix/store/7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3.drv")
            .unwrap();
        let dp = DerivedPath::Built {
            drv_path: path.clone(),
            outputs: string_set!["bin", "dev", "out"],
        };
        let sp = StorePathWithOutputs {
            path,
            outputs: string_set!["bin", "dev", "out"],
        };
        let dp2: DerivedPath = sp.into();
        assert_eq!(dp, dp2);
    }
}
