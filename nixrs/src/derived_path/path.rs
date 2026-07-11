use std::ops::Deref as _;

use crate::derivation::OutputName;
use crate::derived_path::OutputSpec;
use crate::store_path::{FromStoreDirStr, ParseStorePathError, StoreDirDisplay, StorePath};

trait StoreDirDisplaySep {
    fn fmt(
        &self,
        store_dir: &crate::store_path::StoreDir,
        sep: char,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result;
}

impl<D> StoreDirDisplaySep for &D
where
    D: StoreDirDisplaySep,
{
    fn fmt(
        &self,
        store_dir: &crate::store_path::StoreDir,
        sep: char,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        (**self).fmt(store_dir, sep, f)
    }
}

pub trait FromStoreDirStrSep: Sized {
    type Error: std::error::Error;

    fn from_store_dir_str_sep(
        store_dir: &crate::store_path::StoreDir,
        sep: char,
        s: &str,
    ) -> Result<Self, Self::Error>;
}

struct DisplayPath<'d, D>(char, &'d D);
impl<D> StoreDirDisplay for DisplayPath<'_, D>
where
    D: StoreDirDisplaySep,
{
    fn fmt(
        &self,
        store_dir: &crate::store_path::StoreDir,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        self.1.fmt(store_dir, self.0, f)
    }
}
pub struct ParsePath<const C: char, D>(pub D);
impl<const C: char, D> FromStoreDirStr for ParsePath<C, D>
where
    D: FromStoreDirStrSep,
    <D as FromStoreDirStrSep>::Error: std::error::Error,
{
    type Error = D::Error;

    fn from_store_dir_str(
        store_dir: &crate::store_path::StoreDir,
        s: &str,
    ) -> Result<Self, Self::Error> {
        Ok(ParsePath(
            <D as FromStoreDirStrSep>::from_store_dir_str_sep(store_dir, C, s)?,
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SingleDerivedPath {
    Opaque(StorePath),
    Built {
        drv_path: Box<SingleDerivedPath>,
        output: OutputName,
    },
}

impl SingleDerivedPath {
    pub fn to_legacy_format(&self) -> impl StoreDirDisplay + '_ {
        DisplayPath('!', self)
    }
}

impl StoreDirDisplaySep for SingleDerivedPath {
    fn fmt(
        &self,
        store_dir: &crate::store_path::StoreDir,
        sep: char,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        match self {
            SingleDerivedPath::Opaque(store_path) => write!(f, "{}", store_dir.display(store_path)),
            SingleDerivedPath::Built { drv_path, output } => {
                let path = DisplayPath(sep, drv_path.deref());
                write!(f, "{}{}{}", store_dir.display(&path), sep, output)
            }
        }
    }
}

impl StoreDirDisplay for SingleDerivedPath {
    fn fmt(
        &self,
        store_dir: &crate::store_path::StoreDir,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        let path = DisplayPath('^', self);
        write!(f, "{}", store_dir.display(&path))
    }
}

impl FromStoreDirStrSep for SingleDerivedPath {
    type Error = ParseStorePathError;

    fn from_store_dir_str_sep(
        store_dir: &crate::store_path::StoreDir,
        sep: char,
        s: &str,
    ) -> Result<Self, Self::Error> {
        if let Some((prefix, output_s)) = s.rsplit_once(sep) {
            let drv_path = SingleDerivedPath::from_store_dir_str_sep(store_dir, sep, prefix)?;
            let output = output_s.parse::<OutputName>().map_err(|error| {
                ParseStorePathError::new(s, error.adjust_index(prefix.len() + sep.len_utf8()))
            })?;
            Ok(SingleDerivedPath::Built {
                drv_path: Box::new(drv_path),
                output,
            })
        } else {
            Ok(SingleDerivedPath::Opaque(store_dir.parse(s)?))
        }
    }
}

impl FromStoreDirStr for SingleDerivedPath {
    type Error = ParseStorePathError;

    fn from_store_dir_str(
        store_dir: &crate::store_path::StoreDir,
        s: &str,
    ) -> Result<Self, Self::Error> {
        SingleDerivedPath::from_store_dir_str_sep(store_dir, '^', s)
    }
}

/// A deriving path.
///
/// Deriving paths are a way to refer to store objects that may or may not yet
/// be realised. There are two forms:
///     - opaque: just a store path.
///     - built: a pair of a store path to a store derivation and an output name.
///
/// See: <https://nix.dev/manual/nix/latest/store/derivation/#deriving-path>
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DerivedPath {
    Opaque(StorePath),
    Built {
        drv_path: SingleDerivedPath,
        outputs: OutputSpec,
    },
}

impl DerivedPath {
    pub fn to_legacy_format(&self) -> impl StoreDirDisplay + '_ {
        DisplayPath('!', self)
    }
}

impl StoreDirDisplaySep for DerivedPath {
    fn fmt(
        &self,
        store_dir: &crate::store_path::StoreDir,
        sep: char,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        match self {
            DerivedPath::Opaque(store_path) => write!(f, "{}", store_dir.display(store_path)),
            DerivedPath::Built { drv_path, outputs } => {
                let path = DisplayPath(sep, drv_path);
                write!(f, "{}{}{}", store_dir.display(&path), sep, outputs)
            }
        }
    }
}

impl StoreDirDisplay for DerivedPath {
    fn fmt(
        &self,
        store_dir: &crate::store_path::StoreDir,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        let path = DisplayPath('^', self);
        write!(f, "{}", store_dir.display(&path))
    }
}

impl FromStoreDirStrSep for DerivedPath {
    type Error = ParseStorePathError;

    fn from_store_dir_str_sep(
        store_dir: &crate::store_path::StoreDir,
        sep: char,
        s: &str,
    ) -> Result<Self, Self::Error> {
        if let Some((prefix, outputs_s)) = s.rsplit_once(sep) {
            let drv_path = SingleDerivedPath::from_store_dir_str_sep(store_dir, sep, prefix)?;
            let outputs = outputs_s.parse::<OutputSpec>().map_err(|error| {
                ParseStorePathError::new(s, error.adjust_index(prefix.len() + sep.len_utf8()))
            })?;
            Ok(DerivedPath::Built { drv_path, outputs })
        } else {
            Ok(DerivedPath::Opaque(store_dir.parse(s)?))
        }
    }
}

impl FromStoreDirStr for DerivedPath {
    type Error = ParseStorePathError;

    fn from_store_dir_str(
        store_dir: &crate::store_path::StoreDir,
        s: &str,
    ) -> Result<Self, Self::Error> {
        DerivedPath::from_store_dir_str_sep(store_dir, '^', s)
    }
}

/// Format a [`DerivedPath`] in the "legacy" format.
///
/// Normally a [`DerivedPath::Built`] it formatted like
/// `/nix/store/00000000000000000000000000000000-test.drv^out`. But in some
/// places (most notably in the [Nix daemon protocol]) a format like
/// `/nix/store/00000000000000000000000000000000-test.drv!out` is used.
///
/// This formatter implements [`FromStr`] and [`fmt::Display`] that use this format.
///
/// [Nix daemon protocol]: http://snix.dev/docs/reference/nix-daemon-protocol/intro/
pub type LegacyDerivedPath = ParsePath<'!', DerivedPath>;

#[cfg(test)]
mod unittests {
    use rstest::rstest;

    use super::*;
    use crate::store_path::{StoreDir, StorePathError};

    #[rstest]
    #[case("/nix/store/00000000000000000000000000000000-test.drv", Ok(DerivedPath::Opaque("00000000000000000000000000000000-test.drv".parse().unwrap())))]
    #[case("/nix/store/00000000000000000000000000000000-test.drv^out", Ok(DerivedPath::Built {
        drv_path: SingleDerivedPath::Opaque("00000000000000000000000000000000-test.drv".parse().unwrap()),
        outputs: "out".parse().unwrap(),
    }))]
    #[case("/nix/store/00000000000000000000000000000000-test.drv^*", Ok(DerivedPath::Built {
        drv_path: SingleDerivedPath::Opaque("00000000000000000000000000000000-test.drv".parse().unwrap()),
        outputs: "*".parse().unwrap(),
    }))]
    #[case("/nix/store/00000000000000000000000000000000-test.drv^bin,lib", Ok(DerivedPath::Built {
        drv_path: SingleDerivedPath::Opaque("00000000000000000000000000000000-test.drv".parse().unwrap()),
        outputs: "bin,lib".parse().unwrap(),
    }))]
    #[case("/nix/store/00000000000000000000000000000000-test.drv^out^bin,lib", Ok(DerivedPath::Built {
        drv_path: SingleDerivedPath::Built {
            drv_path: Box::new(SingleDerivedPath::Opaque("00000000000000000000000000000000-test.drv".parse().unwrap())),
            output: "out".parse().unwrap(),
        },
        outputs: "bin,lib".parse().unwrap(),
    }))]
    #[case("/nix/store/00000000000000000000000000000000-test.drv^out^bin^lib", Ok(DerivedPath::Built {
        drv_path: SingleDerivedPath::Built {
            drv_path: Box::new(SingleDerivedPath::Built {
                drv_path: Box::new(SingleDerivedPath::Opaque("00000000000000000000000000000000-test.drv".parse().unwrap())),
                output: "out".parse().unwrap(),
            }),
            output: "bin".parse().unwrap(),
        },
        outputs: "lib".parse().unwrap(),
    }))]
    #[case("/nix/store/00000000000000000000000000000000-test.drv!out", Err(ParseStorePathError {
        path: "/nix/store/00000000000000000000000000000000-test.drv!out".into(),
        error: StorePathError::Symbol { position: 52, symbol: b'!' },
    }))]
    #[case("/nix/store/00000000000000000000000000000000-test.drv!out^bin", Err(ParseStorePathError {
        path: "/nix/store/00000000000000000000000000000000-test.drv!out".into(),
        error: StorePathError::Symbol { position: 52, symbol: b'!' },
    }))]
    #[case("/nix/store/00000000000000000000000000000000-test.drv^out^bin!out^lib", Err(ParseStorePathError {
        path: "/nix/store/00000000000000000000000000000000-test.drv^out^bin!out".into(),
        error: StorePathError::Symbol { position: 60, symbol: b'!' },
    }))]
    fn parse_path(#[case] input: &str, #[case] expected: Result<DerivedPath, ParseStorePathError>) {
        let store_dir = StoreDir::default();
        let actual: Result<DerivedPath, _> = store_dir.parse(input);
        assert_eq!(actual, expected);
    }

    #[rstest]
    #[should_panic = "could not parse '/nix/store/00000000000000000000000000000000-test.drv!out', invalid store path symbol '!' at position 52"]
    #[case("/nix/store/00000000000000000000000000000000-test.drv!out")]
    #[should_panic = "could not parse '/nix/store/00000000000000000000000000000000-test.drv!out', invalid store path symbol '!' at position 52"]
    #[case("/nix/store/00000000000000000000000000000000-test.drv!out^bin")]
    #[should_panic = "could not parse '/nix/store/00000000000000000000000000000000-test.drv^out^bin!out', invalid store path symbol '!' at position 60"]
    #[case("/nix/store/00000000000000000000000000000000-test.drv^out^bin!out^lib")]
    fn parse_path_errors(#[case] input: &str) {
        let store_dir = StoreDir::default();
        let actual = store_dir.parse::<DerivedPath>(input).unwrap_err();
        panic!("{actual}");
    }

    #[rstest]
    #[case("/nix/store/00000000000000000000000000000000-test.drv", Ok(DerivedPath::Opaque("00000000000000000000000000000000-test.drv".parse().unwrap())))]
    #[case("/nix/store/00000000000000000000000000000000-test.drv!out", Ok(DerivedPath::Built {
        drv_path: SingleDerivedPath::Opaque("00000000000000000000000000000000-test.drv".parse().unwrap()),
        outputs: "out".parse().unwrap(),
    }))]
    #[case("/nix/store/00000000000000000000000000000000-test.drv!*", Ok(DerivedPath::Built {
        drv_path: SingleDerivedPath::Opaque("00000000000000000000000000000000-test.drv".parse().unwrap()),
        outputs: "*".parse().unwrap(),
    }))]
    #[case("/nix/store/00000000000000000000000000000000-test.drv!bin,lib", Ok(DerivedPath::Built {
        drv_path: SingleDerivedPath::Opaque("00000000000000000000000000000000-test.drv".parse().unwrap()),
        outputs: "bin,lib".parse().unwrap(),
    }))]
    #[case("/nix/store/00000000000000000000000000000000-test.drv!out!bin,lib", Ok(DerivedPath::Built {
        drv_path: SingleDerivedPath::Built {
            drv_path: Box::new(SingleDerivedPath::Opaque("00000000000000000000000000000000-test.drv".parse().unwrap())),
            output: "out".parse().unwrap(),
        },
        outputs: "bin,lib".parse().unwrap(),
    }))]
    #[case("/nix/store/00000000000000000000000000000000-test.drv!out!bin!lib", Ok(DerivedPath::Built {
        drv_path: SingleDerivedPath::Built {
            drv_path: Box::new(SingleDerivedPath::Built {
                drv_path: Box::new(SingleDerivedPath::Opaque("00000000000000000000000000000000-test.drv".parse().unwrap())),
                output: "out".parse().unwrap(),
            }),
            output: "bin".parse().unwrap(),
        },
        outputs: "lib".parse().unwrap(),
    }))]
    #[case("/nix/store/00000000000000000000000000000000-test.drv^out", Err(ParseStorePathError {
        path: "/nix/store/00000000000000000000000000000000-test.drv^out".into(),
        error: StorePathError::Symbol { position: 52, symbol: b'^' },
    }))]
    #[case("/nix/store/00000000000000000000000000000000-test.drv^out!bin", Err(ParseStorePathError {
        path: "/nix/store/00000000000000000000000000000000-test.drv^out".into(),
        error: StorePathError::Symbol { position: 52, symbol: b'^' },
    }))]
    #[case("/nix/store/00000000000000000000000000000000-test.drv!out!bin^out!lib", Err(ParseStorePathError {
        path: "/nix/store/00000000000000000000000000000000-test.drv!out!bin^out".into(),
        error: StorePathError::Symbol { position: 60, symbol: b'^' },
    }))]
    fn parse_legacy_path(
        #[case] input: &str,
        #[case] expected: Result<DerivedPath, ParseStorePathError>,
    ) {
        let store_dir = StoreDir::default();
        let actual: Result<LegacyDerivedPath, _> = store_dir.parse(input);
        assert_eq!(actual.map(|p| p.0), expected);
    }

    #[rstest]
    #[case("/nix/store/00000000000000000000000000000000-test.drv", Ok(SingleDerivedPath::Opaque("00000000000000000000000000000000-test.drv".parse().unwrap())))]
    #[case("/nix/store/00000000000000000000000000000000-test.drv^bin", Ok(SingleDerivedPath::Built {
        drv_path: Box::new(SingleDerivedPath::Opaque("00000000000000000000000000000000-test.drv".parse().unwrap())),
        output: "bin".parse().unwrap(),
    }))]
    #[case("/nix/store/00000000000000000000000000000000-test.drv^out^bin", Ok(SingleDerivedPath::Built {
        drv_path: Box::new(SingleDerivedPath::Built {
            drv_path: Box::new(SingleDerivedPath::Opaque("00000000000000000000000000000000-test.drv".parse().unwrap())),
            output: "out".parse().unwrap(),
        }),
        output: "bin".parse().unwrap(),
    }))]
    #[case("/nix/store/00000000000000000000000000000000-test.drv^out^bin^lib", Ok(SingleDerivedPath::Built {
        drv_path: Box::new(SingleDerivedPath::Built {
            drv_path: Box::new(SingleDerivedPath::Built {
                drv_path: Box::new(SingleDerivedPath::Opaque("00000000000000000000000000000000-test.drv".parse().unwrap())),
                output: "out".parse().unwrap(),
            }),
            output: "bin".parse().unwrap(),
        }),
        output: "lib".parse().unwrap(),
    }))]
    #[case("/nix/store/00000000000000000000000000000000-test.drv!out", Err(ParseStorePathError {
        path: "/nix/store/00000000000000000000000000000000-test.drv!out".into(),
        error: StorePathError::Symbol { position: 52, symbol: b'!' },
    }))]
    #[case("/nix/store/00000000000000000000000000000000-test.drv!out^bin", Err(ParseStorePathError {
        path: "/nix/store/00000000000000000000000000000000-test.drv!out".into(),
        error: StorePathError::Symbol { position: 52, symbol: b'!' },
    }))]
    #[case("/nix/store/00000000000000000000000000000000-test.drv^out^bin!out^lib", Err(ParseStorePathError {
        path: "/nix/store/00000000000000000000000000000000-test.drv^out^bin!out".into(),
        error: StorePathError::Symbol { position: 60, symbol: b'!' },
    }))]
    fn parse_single_path(
        #[case] input: &str,
        #[case] expected: Result<SingleDerivedPath, ParseStorePathError>,
    ) {
        let store_dir = StoreDir::default();
        let actual: Result<SingleDerivedPath, _> = store_dir.parse(input);
        assert_eq!(actual, expected);
    }

    #[rstest]
    #[case(DerivedPath::Opaque("00000000000000000000000000000000-test.drv".parse().unwrap()), "/nix/store/00000000000000000000000000000000-test.drv")]
    #[case(DerivedPath::Built {
        drv_path: SingleDerivedPath::Opaque("00000000000000000000000000000000-test.drv".parse().unwrap()),
        outputs: "out".parse().unwrap(),
    }, "/nix/store/00000000000000000000000000000000-test.drv^out")]
    #[case(DerivedPath::Built {
        drv_path: SingleDerivedPath::Opaque("00000000000000000000000000000000-test.drv".parse().unwrap()),
        outputs: "*".parse().unwrap(),
    }, "/nix/store/00000000000000000000000000000000-test.drv^*")]
    #[case(DerivedPath::Built {
        drv_path: SingleDerivedPath::Opaque("00000000000000000000000000000000-test.drv".parse().unwrap()),
        outputs: "bin,lib".parse().unwrap(),
    }, "/nix/store/00000000000000000000000000000000-test.drv^bin,lib")]
    #[case(DerivedPath::Built {
        drv_path: SingleDerivedPath::Built {
            drv_path: Box::new(SingleDerivedPath::Opaque("00000000000000000000000000000000-test.drv".parse().unwrap())),
            output: "out".parse().unwrap(),
        },
        outputs: "bin,lib".parse().unwrap(),
    }, "/nix/store/00000000000000000000000000000000-test.drv^out^bin,lib")]
    #[case(DerivedPath::Built {
        drv_path: SingleDerivedPath::Built {
            drv_path: Box::new(SingleDerivedPath::Built {
                drv_path: Box::new(SingleDerivedPath::Opaque("00000000000000000000000000000000-test.drv".parse().unwrap())),
                output: "out".parse().unwrap(),
            }),
            output: "bin".parse().unwrap(),
        },
        outputs: "lib".parse().unwrap(),
    }, "/nix/store/00000000000000000000000000000000-test.drv^out^bin^lib")]
    fn display_path(#[case] value: DerivedPath, #[case] expected: &str) {
        let store_dir = StoreDir::default();
        assert_eq!(store_dir.display(&value).to_string(), expected);
    }

    #[rstest]
    #[case(DerivedPath::Opaque("00000000000000000000000000000000-test.drv".parse().unwrap()), "/nix/store/00000000000000000000000000000000-test.drv")]
    #[case(DerivedPath::Built {
        drv_path: SingleDerivedPath::Opaque("00000000000000000000000000000000-test.drv".parse().unwrap()),
        outputs: "out".parse().unwrap(),
    }, "/nix/store/00000000000000000000000000000000-test.drv!out")]
    #[case(DerivedPath::Built {
        drv_path: SingleDerivedPath::Opaque("00000000000000000000000000000000-test.drv".parse().unwrap()),
        outputs: "*".parse().unwrap(),
    }, "/nix/store/00000000000000000000000000000000-test.drv!*")]
    #[case(DerivedPath::Built {
        drv_path: SingleDerivedPath::Opaque("00000000000000000000000000000000-test.drv".parse().unwrap()),
        outputs: "bin,lib".parse().unwrap(),
    }, "/nix/store/00000000000000000000000000000000-test.drv!bin,lib")]
    #[case(DerivedPath::Built {
        drv_path: SingleDerivedPath::Built {
            drv_path: Box::new(SingleDerivedPath::Opaque("00000000000000000000000000000000-test.drv".parse().unwrap())),
            output: "out".parse().unwrap(),
        },
        outputs: "bin,lib".parse().unwrap(),
    }, "/nix/store/00000000000000000000000000000000-test.drv!out!bin,lib")]
    #[case(DerivedPath::Built {
        drv_path: SingleDerivedPath::Built {
            drv_path: Box::new(SingleDerivedPath::Built {
                drv_path: Box::new(SingleDerivedPath::Opaque("00000000000000000000000000000000-test.drv".parse().unwrap())),
                output: "out".parse().unwrap(),
            }),
            output: "bin".parse().unwrap(),
        },
        outputs: "lib".parse().unwrap(),
    }, "/nix/store/00000000000000000000000000000000-test.drv!out!bin!lib")]
    fn display_legacy_path(#[case] value: DerivedPath, #[case] expected: &str) {
        let store_dir = StoreDir::default();
        assert_eq!(
            store_dir.display(&value.to_legacy_format()).to_string(),
            expected
        );
    }

    #[rstest]
    #[case(SingleDerivedPath::Opaque("00000000000000000000000000000000-test.drv".parse().unwrap()), "/nix/store/00000000000000000000000000000000-test.drv")]
    #[case(SingleDerivedPath::Built {
        drv_path: Box::new(SingleDerivedPath::Opaque("00000000000000000000000000000000-test.drv".parse().unwrap())),
        output: "bin".parse().unwrap(),
    }, "/nix/store/00000000000000000000000000000000-test.drv^bin")]
    #[case(SingleDerivedPath::Built {
        drv_path: Box::new(SingleDerivedPath::Built {
            drv_path: Box::new(SingleDerivedPath::Opaque("00000000000000000000000000000000-test.drv".parse().unwrap())),
            output: "out".parse().unwrap(),
        }),
        output: "bin".parse().unwrap(),
    }, "/nix/store/00000000000000000000000000000000-test.drv^out^bin")]
    #[case(SingleDerivedPath::Built {
        drv_path: Box::new(SingleDerivedPath::Built {
            drv_path: Box::new(SingleDerivedPath::Built {
                drv_path: Box::new(SingleDerivedPath::Opaque("00000000000000000000000000000000-test.drv".parse().unwrap())),
                output: "out".parse().unwrap(),
            }),
            output: "bin".parse().unwrap(),
        }),
        output: "lib".parse().unwrap(),
    }, "/nix/store/00000000000000000000000000000000-test.drv^out^bin^lib")]
    fn display_single_path(#[case] value: SingleDerivedPath, #[case] expected: &str) {
        let store_dir = StoreDir::default();
        assert_eq!(store_dir.display(&value).to_string(), expected);
    }

    #[rstest]
    #[case(SingleDerivedPath::Opaque("00000000000000000000000000000000-test.drv".parse().unwrap()), "/nix/store/00000000000000000000000000000000-test.drv")]
    #[case(SingleDerivedPath::Built {
        drv_path: Box::new(SingleDerivedPath::Opaque("00000000000000000000000000000000-test.drv".parse().unwrap())),
        output: "bin".parse().unwrap(),
    }, "/nix/store/00000000000000000000000000000000-test.drv!bin")]
    #[case(SingleDerivedPath::Built {
        drv_path: Box::new(SingleDerivedPath::Built {
            drv_path: Box::new(SingleDerivedPath::Opaque("00000000000000000000000000000000-test.drv".parse().unwrap())),
            output: "out".parse().unwrap(),
        }),
        output: "bin".parse().unwrap(),
    }, "/nix/store/00000000000000000000000000000000-test.drv!out!bin")]
    #[case(SingleDerivedPath::Built {
        drv_path: Box::new(SingleDerivedPath::Built {
            drv_path: Box::new(SingleDerivedPath::Built {
                drv_path: Box::new(SingleDerivedPath::Opaque("00000000000000000000000000000000-test.drv".parse().unwrap())),
                output: "out".parse().unwrap(),
            }),
            output: "bin".parse().unwrap(),
        }),
        output: "lib".parse().unwrap(),
    }, "/nix/store/00000000000000000000000000000000-test.drv!out!bin!lib")]
    fn display_single_legacy_path(#[case] value: SingleDerivedPath, #[case] expected: &str) {
        let store_dir = StoreDir::default();
        assert_eq!(
            store_dir.display(&value.to_legacy_format()).to_string(),
            expected
        );
    }
}
