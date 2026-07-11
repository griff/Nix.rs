use std::str::FromStr;

use crate::hash::fmt::{NonSRI, ParseHashError, ParseHashErrorKind};
use crate::hash::{Algorithm, Hash, Sha256, UnknownAlgorithm};
use crate::store_path::{FixedOutput, FixedOutputMethod, FixedOutputMethodAlgorithm};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, derive_more::Display)]
pub enum ContentAddressMethod {
    #[display("text")]
    Text,
    #[display("{_0}")]
    Fixed(FixedOutputMethod),
}

impl ContentAddressMethod {
    pub const fn fixed_flat() -> Self {
        Self::Fixed(FixedOutputMethod::Flat)
    }

    pub const fn fixed_recursive() -> Self {
        Self::Fixed(FixedOutputMethod::Recursive)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, derive_more::Display)]
pub enum ContentAddressMethodAlgorithm {
    #[display("text:sha256")]
    Text,
    #[display("{_0}")]
    Fixed(FixedOutputMethodAlgorithm),
}

impl ContentAddressMethodAlgorithm {
    pub const fn fixed_flat(algorithm: Algorithm) -> Self {
        Self::Fixed(FixedOutputMethodAlgorithm::flat(algorithm))
    }

    pub const fn fixed_recursive(algorithm: Algorithm) -> Self {
        Self::Fixed(FixedOutputMethodAlgorithm::recursive(algorithm))
    }

    pub const fn algorithm(&self) -> Algorithm {
        match self {
            ContentAddressMethodAlgorithm::Text => Algorithm::SHA256,
            ContentAddressMethodAlgorithm::Fixed(fod) => fod.algorithm,
        }
    }

    pub fn method(&self) -> ContentAddressMethod {
        match self {
            ContentAddressMethodAlgorithm::Text => ContentAddressMethod::Text,
            ContentAddressMethodAlgorithm::Fixed(fod) => fod.method.into(),
        }
    }
}

impl FromStr for ContentAddressMethodAlgorithm {
    type Err = ParseContentAddressError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "text:sha256" {
            Ok(Self::Text)
        } else {
            Ok(Self::Fixed(s.parse()?))
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, derive_more::Display)]
pub enum ContentAddress {
    #[display("text:{}", _0.as_base32())]
    Text(Sha256),
    #[display("{_0}")]
    Fixed(FixedOutput),
}

impl ContentAddress {
    pub const fn fixed_flat(hash: Hash) -> Self {
        Self::Fixed(FixedOutput::flat(hash))
    }

    pub const fn fixed_recursive(hash: Hash) -> Self {
        Self::Fixed(FixedOutput::recursive(hash))
    }

    pub fn from_hash(
        method: ContentAddressMethod,
        hash: Hash,
    ) -> Result<ContentAddress, ParseHashErrorKind> {
        Ok(match method {
            ContentAddressMethod::Text => ContentAddress::Text(hash.try_into()?),
            ContentAddressMethod::Fixed(method) => FixedOutput::from_hash(method, hash).into(),
        })
    }
    pub fn algorithm(&self) -> Algorithm {
        self.method_algorithm().algorithm()
    }
    pub fn method(&self) -> ContentAddressMethod {
        match self {
            ContentAddress::Text(_) => ContentAddressMethod::Text,
            ContentAddress::Fixed(fo) => fo.method.into(),
        }
    }

    pub fn method_algorithm(&self) -> ContentAddressMethodAlgorithm {
        match self {
            ContentAddress::Text(_) => ContentAddressMethodAlgorithm::Text,
            ContentAddress::Fixed(fo) => fo.method_algorithm().into(),
        }
    }

    pub fn hash(&self) -> Hash {
        match *self {
            ContentAddress::Text(sha256) => sha256.into(),
            ContentAddress::Fixed(fo) => fo.hash,
        }
    }

    pub fn is_source(&self) -> bool {
        matches!(self, ContentAddress::Fixed(fo) if fo.is_source())
    }
}

impl FromStr for ContentAddress {
    type Err = ParseContentAddressError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(hash_s) = s.strip_prefix("text:") {
            let sha256 = hash_s
                .parse::<NonSRI<Sha256>>()
                .map_err(|err| {
                    ParseContentAddressError::InvalidHash(ContentAddressMethod::Text, err)
                })?
                .into_hash();
            Ok(Self::Text(sha256))
        } else {
            Ok(s.parse::<FixedOutput>()?.into())
        }
    }
}

#[derive(Debug, PartialEq, Clone, thiserror::Error)]
pub enum ParseContentAddressError {
    #[error("content address {0} {1}")]
    InvalidHash(ContentAddressMethod, #[source] ParseHashError),
    #[error("{0} for content address")]
    UnknownAlgorithm(
        #[from]
        #[source]
        UnknownAlgorithm,
    ),
    #[error("'{0}' is not a content address because it is not in the form '<fixed | text>:<rest>'")]
    InvalidForm(String),
}

#[cfg(test)]
mod unittests {
    use rstest::rstest;

    use super::*;
    use crate::hash::Algorithm;

    #[rstest]
    #[case::text(
        "text:sha256:1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5s",
        ContentAddressMethod::Text,
        Algorithm::SHA256
    )]
    #[case::flat(
        "fixed:sha256:1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5s",
        ContentAddressMethod::fixed_flat(),
        Algorithm::SHA256
    )]
    #[case::recursive(
        "fixed:r:sha256:1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5s",
        ContentAddressMethod::fixed_recursive(),
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
    #[should_panic = "content address text hash 'sha256:1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5' has wrong length for hash type 'sha256'"]
    #[case("text:sha256:1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5")]
    #[should_panic = "content address fixed hash 'sha256:1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5' has wrong length for hash type 'sha256'"]
    #[case("fixed:sha256:1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5")]
    #[should_panic = "content address fixed:r hash 'sha256:1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5' has wrong length for hash type 'sha256'"]
    #[case("fixed:r:sha256:1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5")]
    #[should_panic = "'test:sha256:1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5' is not a content address because it is not in the form '<fixed | text>:<rest>'"]
    #[case("test:sha256:1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5")]
    #[should_panic = "'test-12345' is not a content address because it is not in the form '<fixed | text>:<rest>'"]
    #[case("test-12345")]
    #[should_panic = "content address text hash 'sha1:kpcd173cq987hw957sx6m0868wv3x6d9' should have type 'sha256' but got 'sha1'"]
    #[case("text:sha1:kpcd173cq987hw957sx6m0868wv3x6d9")]
    fn test_content_address_error(#[case] value: &str) {
        let actual = value.parse::<ContentAddress>().unwrap_err();
        panic!("{actual}");
    }

    #[rstest]
    #[case(ContentAddressMethodAlgorithm::Text, "text:sha256")]
    #[case(ContentAddressMethodAlgorithm::fixed_flat(Algorithm::MD5), "md5")]
    #[case(ContentAddressMethodAlgorithm::fixed_flat(Algorithm::SHA1), "sha1")]
    #[case(ContentAddressMethodAlgorithm::fixed_flat(Algorithm::SHA256), "sha256")]
    #[case(ContentAddressMethodAlgorithm::fixed_flat(Algorithm::SHA512), "sha512")]
    #[case(
        ContentAddressMethodAlgorithm::fixed_recursive(Algorithm::MD5),
        "r:md5"
    )]
    #[case(
        ContentAddressMethodAlgorithm::fixed_recursive(Algorithm::SHA1),
        "r:sha1"
    )]
    #[case(
        ContentAddressMethodAlgorithm::fixed_recursive(Algorithm::SHA256),
        "r:sha256"
    )]
    #[case(
        ContentAddressMethodAlgorithm::fixed_recursive(Algorithm::SHA512),
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
