use std::fmt;
use std::str::FromStr;

use crate::hash::fmt::NonSRI;
use crate::hash::{Algorithm, Hash, Sha256};
use crate::store_path::{
    ContentAddress, ContentAddressMethod, ContentAddressMethodAlgorithm, ParseContentAddressError,
    StoreDirDisplay, StorePath, display_from_fn,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, derive_more::Display)]
pub enum FixedOutputMethod {
    #[display("fixed")]
    Flat,
    #[display("fixed:r")]
    Recursive,
}

impl FixedOutputMethod {
    pub const fn prefix(&self) -> &'static str {
        match self {
            FixedOutputMethod::Flat => "",
            FixedOutputMethod::Recursive => "r:",
        }
    }
}

impl From<FixedOutputMethod> for ContentAddressMethod {
    fn from(value: FixedOutputMethod) -> Self {
        ContentAddressMethod::Fixed(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, derive_more::Display)]
#[display("{}{algorithm}", method.prefix())]
pub struct FixedOutputMethodAlgorithm {
    pub method: FixedOutputMethod,
    pub algorithm: Algorithm,
}

impl FixedOutputMethodAlgorithm {
    pub const fn flat(algorithm: Algorithm) -> Self {
        Self {
            method: FixedOutputMethod::Flat,
            algorithm,
        }
    }

    pub const fn recursive(algorithm: Algorithm) -> Self {
        Self {
            method: FixedOutputMethod::Recursive,
            algorithm,
        }
    }
}

impl FromStr for FixedOutputMethodAlgorithm {
    type Err = ParseContentAddressError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(algo) = s.strip_prefix("r:") {
            Ok(Self {
                method: FixedOutputMethod::Recursive,
                algorithm: algo.parse()?,
            })
        } else {
            Ok(Self {
                method: FixedOutputMethod::Flat,
                algorithm: s.parse()?,
            })
        }
    }
}

impl From<FixedOutputMethodAlgorithm> for ContentAddressMethodAlgorithm {
    fn from(value: FixedOutputMethodAlgorithm) -> Self {
        ContentAddressMethodAlgorithm::Fixed(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, derive_more::Display)]
#[display("fixed:{}{}", method.prefix(), hash.as_base32())]
pub struct FixedOutput {
    pub method: FixedOutputMethod,
    pub hash: Hash,
}

impl FixedOutput {
    pub const fn flat(hash: Hash) -> Self {
        Self {
            method: FixedOutputMethod::Flat,
            hash,
        }
    }

    pub const fn recursive(hash: Hash) -> Self {
        Self {
            method: FixedOutputMethod::Recursive,
            hash,
        }
    }

    pub const fn from_hash(method: FixedOutputMethod, hash: Hash) -> Self {
        Self { method, hash }
    }

    pub const fn algorithm(&self) -> Algorithm {
        self.hash.algorithm()
    }

    pub const fn method_algorithm(&self) -> FixedOutputMethodAlgorithm {
        FixedOutputMethodAlgorithm {
            method: self.method,
            algorithm: self.hash.algorithm(),
        }
    }

    pub fn is_source(&self) -> bool {
        self.method == FixedOutputMethod::Recursive && self.hash.algorithm() == Algorithm::SHA256
    }

    pub fn fod_display(&self) -> impl fmt::Display {
        fmt::from_fn(|f| write!(f, "fixed:out:{}{:x}:", self.method.prefix(), self.hash))
    }

    pub fn fod_digest(&self) -> Sha256 {
        Sha256::digest_display(self.fod_display())
    }

    pub fn fod_output_display(&self, output_path: &StorePath) -> impl StoreDirDisplay {
        display_from_fn(|store_dir, f| {
            write!(
                f,
                "fixed:out:{}{:x}:{}",
                self.method.prefix(),
                self.hash,
                store_dir.display(output_path)
            )
        })
    }
}

impl FromStr for FixedOutput {
    type Err = ParseContentAddressError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (method, hash_s) = if let Some(hash_s) = s.strip_prefix("fixed:r:") {
            (FixedOutputMethod::Recursive, hash_s)
        } else if let Some(hash_s) = s.strip_prefix("fixed:") {
            (FixedOutputMethod::Flat, hash_s)
        } else {
            return Err(ParseContentAddressError::InvalidForm(s.into()));
        };
        let hash = hash_s
            .parse::<NonSRI<Hash>>()
            .map_err(|err| ParseContentAddressError::InvalidHash(method.into(), err))?
            .into_hash();
        Ok(Self { method, hash })
    }
}

impl From<FixedOutput> for ContentAddress {
    fn from(value: FixedOutput) -> Self {
        ContentAddress::Fixed(value)
    }
}
