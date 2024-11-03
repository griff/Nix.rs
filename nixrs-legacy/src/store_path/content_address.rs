use std::fmt;
use std::str::FromStr;

use thiserror::Error;

use crate::hash::{self, Algorithm, Hash, ParseHashError};

use super::StorePathSet;

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

/// An enumeration of all the ways we can serialize file system objects.
///
/// Just the type of a content address. Combine with the hash itself, and
/// we have a `ContentAddress` as defined below. Combine that, in turn,
/// with info on references, and we have `ContentAddressWithReferences`,
/// as defined further below.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub enum ContentAddressMethod {
    Text,
    Fixed(FileIngestionMethod),
}

impl ContentAddressMethod {
    pub fn parse_prefix(m: &str) -> (ContentAddressMethod, &str) {
        if let Some(ret) = m.strip_prefix("r:") {
            (
                ContentAddressMethod::Fixed(FileIngestionMethod::Recursive),
                ret,
            )
        } else if let Some(ret) = m.strip_prefix("text:") {
            (ContentAddressMethod::Text, ret)
        } else {
            (ContentAddressMethod::Fixed(FileIngestionMethod::Flat), m)
        }
    }
}

impl fmt::Display for ContentAddressMethod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ContentAddressMethod::Text => write!(f, "text:"),
            ContentAddressMethod::Fixed(FileIngestionMethod::Recursive) => write!(f, "r:"),
            ContentAddressMethod::Fixed(FileIngestionMethod::Flat) => Ok(()),
        }
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
    InvalidHash(
        #[from]
        #[source]
        ParseHashError,
    ),
    #[error(
        "content address prefix '{0}' is unrecognized. Recogonized prefixes are 'text' or 'fixed'"
    )]
    UnknownPrefix(String),
    #[error("not a content address because it is not in the form '<prefix>:<rest>': {0}")]
    InvalidForm(String),
    #[error("text content address hash should use {expected}, but instead uses {actual}")]
    InvalidTextHash {
        expected: Algorithm,
        actual: Algorithm,
    },
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct ContentAddress {
    pub method: ContentAddressMethod,
    pub hash: Hash,
}

impl ContentAddress {
    pub fn text(hash: Hash) -> ContentAddress {
        ContentAddress {
            method: ContentAddressMethod::Text,
            hash,
        }
    }
    pub fn fixed(fim: FileIngestionMethod, hash: Hash) -> ContentAddress {
        ContentAddress {
            method: ContentAddressMethod::Fixed(fim),
            hash,
        }
    }
    pub fn parse(s: &str) -> Result<ContentAddress, ParseContentAddressError> {
        if let Some(rest) = s.strip_prefix("text:") {
            let hash = Hash::parse_non_sri_prefixed(rest)?;
            if hash.algorithm() != Algorithm::SHA256 {
                return Err(ParseContentAddressError::InvalidTextHash {
                    expected: Algorithm::SHA256,
                    actual: hash.algorithm(),
                });
            }
            Ok(ContentAddress {
                method: ContentAddressMethod::Text,
                hash,
            })
        } else if let Some(mut rest) = s.strip_prefix("fixed:") {
            let method = if let Some(other) = rest.strip_prefix("r:") {
                rest = other;
                FileIngestionMethod::Recursive
            } else {
                FileIngestionMethod::Flat
            };
            let hash = Hash::parse_non_sri_prefixed(rest)?;
            Ok(ContentAddress {
                method: ContentAddressMethod::Fixed(method),
                hash,
            })
        } else if let Some((prefix, _rest)) = hash::split_prefix(s, ':') {
            Err(ParseContentAddressError::UnknownPrefix(prefix.to_string()))
        } else {
            Err(ParseContentAddressError::InvalidForm(s.to_string()))
        }
    }
}

impl fmt::Display for ContentAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.method {
            ContentAddressMethod::Text => {
                if f.alternate() {
                    write!(f, "text:{}", self.hash.algorithm())
                } else {
                    write!(f, "text:{}", self.hash.to_base32())
                }
            }
            ContentAddressMethod::Fixed(fim) => {
                if f.alternate() {
                    write!(f, "{:#}{}", fim, self.hash.algorithm())
                } else {
                    write!(f, "fixed:{:#}{}", fim, self.hash.to_base32())
                }
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

/// A set of references to other store objects.
///
/// References to other store objects are tracked with store paths, self
/// references however are tracked with a boolean.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreReferences {
    /// References to other store objects
    pub others: StorePathSet,
    /// Reference to this store object
    pub self_ref: bool,
}

impl StoreReferences {
    pub fn new() -> StoreReferences {
        StoreReferences {
            others: StorePathSet::new(),
            self_ref: false,
        }
    }
    /// true iff no references, i.e. others is empty and self_ref is false.
    pub fn is_empty(&self) -> bool {
        !self.self_ref && self.others.is_empty()
    }

    /// Returns the numbers of references, i.e. the len of others + 1
    /// if self_ref is true.
    pub fn len(&self) -> usize {
        if self.self_ref {
            self.others.len() + 1
        } else {
            self.others.len()
        }
    }
}

impl Default for StoreReferences {
    fn default() -> Self {
        Self::new()
    }
}

/// This matches the additional info that we need for makeTextPath
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextInfo {
    /// Hash of the contents of the text/file.
    pub hash: Hash,
    /// References to other store objects only; self references disallowed
    pub references: StorePathSet,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FixedOutputInfo {
    /// How the file system objects are serialized
    pub method: FileIngestionMethod,
    /// Hash of that serialization
    pub hash: Hash,
    /// References to other store objects or this one.
    pub references: StoreReferences,
}

/// Ways of content addressing but not a complete ContentAddress.
///
/// A ContentAddress without a Hash.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContentAddressWithReferences {
    /// This matches the additional info that we need for makeTextPath
    Text(TextInfo),
    Fixed(FixedOutputInfo),
}

impl ContentAddressWithReferences {
    /// Create a `ContentAddressWithReferences` from a mere
    /// `ContentAddress`, by claiming no references.
    pub fn without_refs(ca: ContentAddress) -> ContentAddressWithReferences {
        use ContentAddressMethod::*;
        match ca.method {
            Text => Self::Text(TextInfo {
                hash: ca.hash,
                references: StorePathSet::new(),
            }),
            Fixed(method) => Self::Fixed(FixedOutputInfo {
                method,
                hash: ca.hash,
                references: StoreReferences::new(),
            }),
        }
    }

    /// Create a `ContentAddressWithReferences` from 3 parts:
    ///
    /// Do note that not all combinations are supported; `None` is
    /// returns for invalid combinations.
    pub fn from_parts_opt(
        method: ContentAddressMethod,
        hash: Hash,
        references: StoreReferences,
    ) -> Option<ContentAddressWithReferences> {
        use ContentAddressMethod::*;
        match method {
            Text => {
                if references.self_ref {
                    return None;
                }
                Some(Self::Text(TextInfo {
                    hash,
                    references: references.others,
                }))
            }
            Fixed(method) => Some(Self::Fixed(FixedOutputInfo {
                method,
                hash,
                references,
            })),
        }
    }

    pub fn method(&self) -> ContentAddressMethod {
        use ContentAddressWithReferences::*;
        match self {
            Text(_) => ContentAddressMethod::Text,
            Fixed(foi) => ContentAddressMethod::Fixed(foi.method),
        }
    }
}

#[cfg(any(test, feature = "test"))]
pub mod proptest {
    use super::*;
    use ::proptest::prelude::*;

    impl Arbitrary for FileIngestionMethod {
        type Parameters = ();
        type Strategy = BoxedStrategy<FileIngestionMethod>;

        fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
            prop_oneof![
                Just(FileIngestionMethod::Flat),
                Just(FileIngestionMethod::Recursive)
            ]
            .boxed()
        }
    }

    impl Arbitrary for ContentAddressMethod {
        type Parameters = ();
        type Strategy = BoxedStrategy<ContentAddressMethod>;

        fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
            prop_oneof![
                Just(ContentAddressMethod::Text),
                any::<FileIngestionMethod>().prop_map(ContentAddressMethod::Fixed)
            ]
            .boxed()
        }
    }

    impl Arbitrary for ContentAddress {
        type Parameters = ();
        type Strategy = BoxedStrategy<ContentAddress>;

        fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
            (any::<ContentAddressMethod>(), any::<hash::Hash>())
                .prop_map(|(method, hash)| ContentAddress { method, hash })
                .boxed()
        }
    }

    impl Arbitrary for FixedOutputHash {
        type Parameters = ();
        type Strategy = BoxedStrategy<FixedOutputHash>;

        fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
            (any::<FileIngestionMethod>(), any::<hash::Hash>())
                .prop_map(|(method, hash)| FixedOutputHash { method, hash })
                .boxed()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash;
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

        assert_eq!(
            format!("r:{}", hash_s),
            FixedOutputHash {
                method: FileIngestionMethod::Recursive,
                hash,
            }
            .to_string()
        );
        assert_eq!(
            format!("r:{}", hash_s),
            format!(
                "{}",
                FixedOutputHash {
                    method: FileIngestionMethod::Recursive,
                    hash,
                }
            )
        );
        assert_eq!(
            "r:sha256",
            format!(
                "{:#}",
                FixedOutputHash {
                    method: FileIngestionMethod::Recursive,
                    hash,
                }
            )
        );

        assert_eq!(
            hash_s,
            FixedOutputHash {
                method: FileIngestionMethod::Flat,
                hash,
            }
            .to_string()
        );
        assert_eq!(
            hash_s,
            format!(
                "{}",
                FixedOutputHash {
                    method: FileIngestionMethod::Flat,
                    hash,
                }
            )
        );
        assert_eq!(
            "sha256",
            format!(
                "{:#}",
                FixedOutputHash {
                    method: FileIngestionMethod::Flat,
                    hash,
                }
            )
        );
    }

    #[test]
    fn test_text_content_address() {
        let s1 = "abc";
        let hash = hash::digest(Algorithm::SHA256, s1);
        let content_address = ContentAddress::text(hash);

        let v = "text:sha256:1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5s";
        assert_eq!(content_address.to_string(), v);
        assert_eq!(format!("{:#}", content_address), "text:sha256");
        assert_eq!(content_address, v.parse().unwrap());
    }

    #[test]
    fn test_fixed_content_address_1() {
        let s1 = "abc";
        let hash = hash::digest(Algorithm::SHA256, s1);
        let content_address = ContentAddress::fixed(FileIngestionMethod::Flat, hash);

        let v = "fixed:sha256:1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5s";
        assert_eq!(content_address.to_string(), v);
        assert_eq!(format!("{:#}", content_address), "sha256");
        assert_eq!(content_address, v.parse().unwrap());
    }

    #[test]
    fn test_fixed_content_address_2() {
        let s1 = "abc";
        let hash = hash::digest(Algorithm::SHA256, s1);
        let content_address = ContentAddress::fixed(FileIngestionMethod::Recursive, hash);

        let v = "fixed:r:sha256:1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5s";
        assert_eq!(content_address.to_string(), v);
        assert_eq!(format!("{:#}", content_address), "r:sha256");
        assert_eq!(content_address, v.parse().unwrap());
    }

    #[test]
    fn test_content_address_error() {
        assert_eq!(
            Err(ParseContentAddressError::InvalidHash(
                ParseHashError::WrongHashLength(
                    Algorithm::SHA256,
                    "1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5".into()
                )
            )),
            "text:sha256:1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5"
                .parse::<ContentAddress>()
        );
        assert_eq!(
            Err(ParseContentAddressError::InvalidHash(
                ParseHashError::WrongHashLength(
                    Algorithm::SHA256,
                    "1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5".into()
                )
            )),
            "fixed:sha256:1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5"
                .parse::<ContentAddress>()
        );
        assert_eq!(
            Err(ParseContentAddressError::UnknownPrefix("test".into())),
            "test:sha256:1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5s"
                .parse::<ContentAddress>()
        );
        assert_eq!(
            Err(ParseContentAddressError::InvalidForm("test-12345".into())),
            "test-12345".parse::<ContentAddress>()
        );
        assert_eq!(
            Err(ParseContentAddressError::InvalidTextHash {
                expected: Algorithm::SHA256,
                actual: Algorithm::SHA1
            }),
            "text:sha1:kpcd173cq987hw957sx6m0868wv3x6d9".parse::<ContentAddress>()
        );
    }
}
