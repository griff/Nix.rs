use std::fmt;
use std::str::FromStr;

use thiserror::Error;

use nixrs_util::hash::{self, Algorithm, Hash, ParseHashError};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub enum FileIngestionMethod {
    Flat,
    Recursive,
}

impl fmt::Display for FileIngestionMethod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use FileIngestionMethod::*;
        if f.alternate() {
            if let Recursive = self {
                write!(f, "r:")?;
            }
        } else {
            match self {
                Recursive => write!(f, "recursive")?,
                Flat => write!(f, "flat")?,
            }
        }
        Ok(())
    }
}

/// Pair of a hash, and how the file system was ingested
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct FixedOutputHash {
    pub(crate) method: FileIngestionMethod,
    pub(crate) hash: Hash,
}

impl fmt::Display for FixedOutputHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if f.alternate() {
            write!(f, "{:#}{}", self.method, self.hash.algorithm())
        } else {
            write!(f, "{:#}{:x}", self.method, self.hash)
        }
    }
}


#[derive(Error, Debug, PartialEq, Clone)]
pub enum ParseContentAddressError {
    #[error("content address hash was invalid {0}")]
    InvalidHash(#[from] #[source] ParseHashError),
    #[error("content address prefix '{0}' is unrecognized. Recogonized prefixes are 'text' or 'fixed'")]
    UnknownPrefix(String),
    #[error("not a content address because it is not in the form '<prefix>:<rest>': {0}")]
    InvalidForm(String),
    #[error("text content address hash should use {expected}, but instead uses {actual}")]
    InvalidTextHash {
        expected: Algorithm,
        actual: Algorithm
    },
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub enum ContentAddress {
    TextHash(Hash),
    FixedOutputHash(FixedOutputHash),
}

impl ContentAddress {
    pub fn parse(s: &str) -> Result<ContentAddress, ParseContentAddressError> {
        if s.starts_with("text:") {
            let hash = Hash::parse_non_sri_prefixed(&s[5..])?;
            if hash.algorithm() != Algorithm::SHA256 {
                return Err(ParseContentAddressError::InvalidTextHash {
                    expected: Algorithm::SHA256,
                    actual: hash.algorithm(),
                });
            }
            Ok(ContentAddress::TextHash(hash))
        } else if s.starts_with("fixed:") {
            let mut rest = &s[6..];
            let method = if rest.starts_with("r:") {
                rest = &rest[2..];
                FileIngestionMethod::Recursive
            } else {
                FileIngestionMethod::Flat
            };
            let hash = Hash::parse_non_sri_prefixed(rest)?;
            Ok(ContentAddress::FixedOutputHash(FixedOutputHash {
                hash, method
            }))
        } else {
            if let Some((prefix, _rest)) = hash::split_prefix(s, ":") {
                Err(ParseContentAddressError::UnknownPrefix(prefix.to_string()))
            } else {
                Err(ParseContentAddressError::InvalidForm(s.to_string()))
            }
        }
    }
}

impl fmt::Display for ContentAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ContentAddress::TextHash(hash) => {
                write!(f, "text:{}", hash.to_base32())
            },
            &ContentAddress::FixedOutputHash(foh) => {
                write!(f, "fixed:{:#}{}", foh.method, foh.hash.to_base32())
            }
        }
    }
}

impl FromStr for ContentAddress {
    type Err = ParseContentAddressError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
    
}

#[cfg(any(test, feature="test"))]
pub mod proptest {
    use ::proptest::prelude::*;
    use super::*;

    impl Arbitrary for FileIngestionMethod {
        type Parameters = ();
        type Strategy = BoxedStrategy<FileIngestionMethod>;

        fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
            prop_oneof![
                Just(FileIngestionMethod::Flat),
                Just(FileIngestionMethod::Recursive)
            ].boxed()
        }
    }

    impl Arbitrary for FixedOutputHash {
        type Parameters = ();
        type Strategy = BoxedStrategy<FixedOutputHash>;

        fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
            (
                any::<FileIngestionMethod>(),
                any::<hash::Hash>()
            ).prop_map(|(method, hash)| {
                FixedOutputHash { method, hash }
            }).boxed()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nixrs_util::hash;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_file_ingestion_method() {
        assert_eq!("recursive", FileIngestionMethod::Recursive.to_string());
        assert_eq!("flat", FileIngestionMethod::Flat.to_string());
        assert_eq!("r:", format!("{:#}", FileIngestionMethod::Recursive));
        assert_eq!("", format!("{:#}", FileIngestionMethod::Flat));
    }

    #[test]
    fn test_fixed_output_hash() {
        let hash_s = "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
        let hash = hash_s.parse::<Hash>().unwrap();

        assert_eq!(format!("r:{}", hash_s), FixedOutputHash {
            method: FileIngestionMethod::Recursive,
            hash: hash.clone(),
        }.to_string());
        assert_eq!(format!("r:{}", hash_s), format!("{}", FixedOutputHash {
            method: FileIngestionMethod::Recursive,
            hash: hash.clone(),
        }));
        assert_eq!("r:sha256", format!("{:#}", FixedOutputHash {
            method: FileIngestionMethod::Recursive,
            hash: hash.clone(),
        }));

        assert_eq!(hash_s, FixedOutputHash {
            method: FileIngestionMethod::Flat,
            hash: hash.clone(),
        }.to_string());
        assert_eq!(hash_s, format!("{}", FixedOutputHash {
            method: FileIngestionMethod::Flat,
            hash: hash.clone(),
        }));
        assert_eq!("sha256", format!("{:#}", FixedOutputHash {
            method: FileIngestionMethod::Flat,
            hash: hash.clone(),
        }));
    }

    #[test]
    fn test_text_content_address() {
        let s1 = "abc";
        let hash = hash::digest(Algorithm::SHA256, s1);
        let content_address = ContentAddress::TextHash(hash);

        let v = "text:sha256:1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5s";
        assert_eq!(content_address.to_string(), v);
        assert_eq!(content_address, v.parse().unwrap());
    }

    #[test]
    fn test_fixed_content_address_1() {
        let s1 = "abc";
        let hash = hash::digest(Algorithm::SHA256, s1);
        let content_address = ContentAddress::FixedOutputHash(FixedOutputHash {
            hash, method: FileIngestionMethod::Flat
        });

        let v = "fixed:sha256:1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5s";
        assert_eq!(content_address.to_string(), v);
        assert_eq!(content_address, v.parse().unwrap());
    }

    #[test]
    fn test_fixed_content_address_2() {
        let s1 = "abc";
        let hash = hash::digest(Algorithm::SHA256, s1);
        let content_address = ContentAddress::FixedOutputHash(FixedOutputHash {
            hash, method: FileIngestionMethod::Recursive
        });

        let v = "fixed:r:sha256:1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5s";
        assert_eq!(content_address.to_string(), v);
        assert_eq!(content_address, v.parse().unwrap());
    }

    #[test]
    fn test_content_address_error() {
        assert_eq!(Err(ParseContentAddressError::InvalidHash(ParseHashError::WrongHashLength(Algorithm::SHA256, "1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5".into()))),
            "text:sha256:1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5".parse::<ContentAddress>());
        assert_eq!(Err(ParseContentAddressError::InvalidHash(ParseHashError::WrongHashLength(Algorithm::SHA256, "1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5".into()))),
            "fixed:sha256:1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5".parse::<ContentAddress>());
        assert_eq!(Err(ParseContentAddressError::UnknownPrefix("test".into())),
            "test:sha256:1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5s".parse::<ContentAddress>());
        assert_eq!(Err(ParseContentAddressError::InvalidForm("test-12345".into())),
            "test-12345".parse::<ContentAddress>());
        assert_eq!(Err(ParseContentAddressError::InvalidTextHash { expected: Algorithm::SHA256, actual: Algorithm::SHA1 }),
            "text:sha1:kpcd173cq987hw957sx6m0868wv3x6d9".parse::<ContentAddress>());
    }
}