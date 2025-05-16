use std::str::FromStr;

use derive_more::Display;
#[cfg(feature = "nixrs-derive")]
use nixrs_derive::{NixDeserialize, NixSerialize};
#[cfg(any(test, feature = "test"))]
use proptest_derive::Arbitrary;
use thiserror::Error;

use crate::hash::{Algorithm, Hash, ParseHashError, Sha256, UnknownAlgorithm};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(any(test, feature = "test"), derive(Arbitrary))]
pub enum ContentAddressMethod {
    Text,
    Flat,
    Recursive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display)]
#[cfg_attr(any(test, feature = "test"), derive(Arbitrary))]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
#[cfg_attr(feature = "nixrs-derive", nix(from_str, display))]
pub enum ContentAddressMethodAlgorithm {
    #[display(fmt = "text:sha256")]
    Text,
    #[display(fmt = "{}", _0)]
    Flat(Algorithm),
    #[display(fmt = "r:{}", _0)]
    Recursive(Algorithm),
}

impl ContentAddressMethodAlgorithm {
    pub fn algorithm(&self) -> Algorithm {
        match self {
            ContentAddressMethodAlgorithm::Text => Algorithm::SHA256,
            ContentAddressMethodAlgorithm::Flat(algorithm) => *algorithm,
            ContentAddressMethodAlgorithm::Recursive(algorithm) => *algorithm,
        }
    }

    pub fn method(&self) -> ContentAddressMethod {
        match self {
            ContentAddressMethodAlgorithm::Text => ContentAddressMethod::Text,
            ContentAddressMethodAlgorithm::Flat(_) => ContentAddressMethod::Flat,
            ContentAddressMethodAlgorithm::Recursive(_) => ContentAddressMethod::Recursive,
        }
    }
}

impl FromStr for ContentAddressMethodAlgorithm {
    type Err = ParseContentAddressError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "text:sha256" {
            Ok(Self::Text)
        } else if let Some(algo) = s.strip_prefix("r:") {
            Ok(Self::Recursive(algo.parse()?))
        } else {
            Ok(Self::Flat(s.parse()?))
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display)]
#[cfg_attr(any(test, feature = "test"), derive(Arbitrary))]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
#[cfg_attr(feature = "nixrs-derive", nix(from_str, display))]
pub enum ContentAddress {
    #[display(fmt = "text:sha256:{}", _0)]
    Text(Sha256),
    #[display(fmt = "fixed:{}", _0)]
    Flat(Hash),
    #[display(fmt = "fixed:r:{}", _0)]
    Recursive(Hash),
}

impl ContentAddress {
    pub fn from_hash(
        method: ContentAddressMethod,
        hash: Hash,
    ) -> Result<ContentAddress, UnknownAlgorithm> {
        Ok(match method {
            ContentAddressMethod::Text => ContentAddress::Text(hash.try_into()?),
            ContentAddressMethod::Flat => ContentAddress::Flat(hash),
            ContentAddressMethod::Recursive => ContentAddress::Recursive(hash),
        })
    }
    pub fn algorithm(&self) -> Algorithm {
        self.method_algorithm().algorithm()
    }
    pub fn method(&self) -> ContentAddressMethod {
        match self {
            ContentAddress::Text(_) => ContentAddressMethod::Text,
            ContentAddress::Flat(_) => ContentAddressMethod::Flat,
            ContentAddress::Recursive(_) => ContentAddressMethod::Recursive,
        }
    }

    pub fn method_algorithm(&self) -> ContentAddressMethodAlgorithm {
        match self {
            ContentAddress::Text(_) => ContentAddressMethodAlgorithm::Text,
            ContentAddress::Flat(hash) => ContentAddressMethodAlgorithm::Flat(hash.algorithm()),
            ContentAddress::Recursive(hash) => {
                ContentAddressMethodAlgorithm::Recursive(hash.algorithm())
            }
        }
    }

    pub fn hash(&self) -> Hash {
        match *self {
            ContentAddress::Text(sha256) => sha256.into(),
            ContentAddress::Flat(hash) => hash,
            ContentAddress::Recursive(hash) => hash,
        }
    }
}

impl FromStr for ContentAddress {
    type Err = ParseContentAddressError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(hash) = s.strip_prefix("text:sha256:") {
            Ok(Self::Text(hash.parse()?))
        } else if let Some(hash) = s.strip_prefix("fixed:r:") {
            Ok(Self::Recursive(Hash::parse_non_sri_prefixed(hash)?))
        } else if let Some(hash) = s.strip_prefix("fixed:") {
            Ok(Self::Flat(Hash::parse_non_sri_prefixed(hash)?))
        } else {
            Err(ParseContentAddressError::InvalidForm(s.into()))
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
    #[error("{0} for content address")]
    UnknownAlgorithm(
        #[from]
        #[source]
        UnknownAlgorithm,
    ),
    #[error(
        "content address method '{0}' is unrecognized. Recogonized methods are 'text', 'fixed' or 'fixed:r'"
    )]
    InvalidMethod(String),
    #[error("not a content address because it is not in the form '<prefix>:<rest>': {0}")]
    InvalidForm(String),
}

#[cfg(test)]
mod unittests {
    use rstest::rstest;

    use super::*;
    use crate::hash::{Algorithm, ParseHashError};

    #[rstest]
    #[case::text(
        "text:sha256:1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5s",
        ContentAddressMethod::Text,
        Algorithm::SHA256
    )]
    #[case::flat(
        "fixed:sha256:1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5s",
        ContentAddressMethod::Flat,
        Algorithm::SHA256
    )]
    #[case::recursive(
        "fixed:r:sha256:1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5s",
        ContentAddressMethod::Recursive,
        Algorithm::SHA256
    )]
    fn content_address_parse(
        #[case] v: &str,
        #[case] method: ContentAddressMethod,
        #[case] algo: Algorithm,
    ) {
        let s1 = "abc";
        let hash = algo.digest(s1);
        let content_address = ContentAddress::from_hash(method, hash).unwrap();

        assert_eq!(content_address.to_string(), v);
        assert_eq!(content_address, v.parse().unwrap());
    }

    #[rstest]
    #[case(
        "text:sha256:1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5",
        ParseContentAddressError::InvalidHash(
            ParseHashError::WrongHashLength(
                Algorithm::SHA256,
                "1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5".into()
            )
        )
    )]
    #[case(
        "fixed:sha256:1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5",
        ParseContentAddressError::InvalidHash(
            ParseHashError::WrongHashLength(
                Algorithm::SHA256,
                "1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5".into()
            )
        )
    )]
    #[case(
        "test:sha256:1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5s",
        ParseContentAddressError::InvalidForm("test:sha256:1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5s".into())
    )]
    #[case(
        "test-12345",
        ParseContentAddressError::InvalidForm("test-12345".into())
    )]
    #[case(
        "text:sha1:kpcd173cq987hw957sx6m0868wv3x6d9",
        ParseContentAddressError::InvalidForm("text:sha1:kpcd173cq987hw957sx6m0868wv3x6d9".into())
    )]
    fn test_content_address_error(#[case] value: &str, #[case] error: ParseContentAddressError) {
        assert_eq!(Err(error), value.parse::<ContentAddress>());
    }

    /*
    #[rstest]
    #[case(ContentAddressMethod::Text, "text:")]
    #[case(ContentAddressMethod::Flat, "")]
    #[case(ContentAddressMethod::Recursive, "r:")]
    fn content_address_method_parse(#[case] method: ContentAddressMethod, #[case] value: &str) {
        assert_eq!(method.to_string(), value);
        let actual = value.parse::<ContentAddressMethod>().unwrap();
        assert_eq!(actual, method);
    }
    */

    #[rstest]
    #[case(ContentAddressMethodAlgorithm::Text, "text:sha256")]
    #[case(ContentAddressMethodAlgorithm::Flat(Algorithm::MD5), "md5")]
    #[case(ContentAddressMethodAlgorithm::Flat(Algorithm::SHA1), "sha1")]
    #[case(ContentAddressMethodAlgorithm::Flat(Algorithm::SHA256), "sha256")]
    #[case(ContentAddressMethodAlgorithm::Flat(Algorithm::SHA512), "sha512")]
    #[case(ContentAddressMethodAlgorithm::Recursive(Algorithm::MD5), "r:md5")]
    #[case(ContentAddressMethodAlgorithm::Recursive(Algorithm::SHA1), "r:sha1")]
    #[case(
        ContentAddressMethodAlgorithm::Recursive(Algorithm::SHA256),
        "r:sha256"
    )]
    #[case(
        ContentAddressMethodAlgorithm::Recursive(Algorithm::SHA512),
        "r:sha512"
    )]
    fn content_address_method_algo_parse(
        #[case] method: ContentAddressMethodAlgorithm,
        #[case] value: &str,
    ) {
        assert_eq!(method.to_string(), value);
        let actual = value.parse::<ContentAddressMethodAlgorithm>().unwrap();
        assert_eq!(actual, method);
    }
}
