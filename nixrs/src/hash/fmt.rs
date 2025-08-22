use std::{fmt as sfmt, str::FromStr};

use data_encoding::{BASE64, DecodeError, DecodeKind, HEXLOWER_PERMISSIVE};
#[cfg(feature = "nixrs-derive")]
use nixrs_derive::{NixDeserialize, NixSerialize};

use crate::base32;
use crate::hash;

mod private {
    pub trait Sealed {}
}

pub trait CommonHash: private::Sealed + Sized {
    fn from_slice(algorithm: hash::Algorithm, hash: &[u8]) -> Result<Self, hash::ParseHashError>;
    fn implied_algorithm() -> Option<hash::Algorithm>;
    fn algorithm(&self) -> hash::Algorithm;
    fn digest_bytes(&self) -> &[u8];

    fn as_base16(&self) -> &Base16<Self>;
    fn as_base32(&self) -> &Base32<Self>;
    fn as_base64(&self) -> &Base64<Self>;
    fn as_sri(&self) -> &SRI<Self>;

    #[inline]
    fn base16(self) -> Base16<Self> {
        Base16(self)
    }

    #[inline]
    fn base32(self) -> Base32<Self> {
        Base32(self)
    }

    #[inline]
    fn base64(self) -> Base64<Self> {
        Base64(self)
    }

    #[inline]
    fn sri(self) -> SRI<Self> {
        SRI(self)
    }
}

impl hash::Hash {
    #[inline]
    pub fn as_base16(&self) -> &Base16<Self> {
        // SAFETY: `Hash` and `Base16<hash::Hash>` have the same ABI
        unsafe { &*(self as *const Self as *const Base16<Self>) }
    }

    #[inline]
    pub const fn base16(self) -> Base16<Self> {
        Base16(self)
    }

    #[inline]
    pub fn as_base32(&self) -> &Base32<Self> {
        // SAFETY: `Hash` and `Base32<hash::Hash>` have the same ABI
        unsafe { &*(self as *const Self as *const Base32<Self>) }
    }

    #[inline]
    pub const fn base32(self) -> Base32<Self> {
        Base32(self)
    }

    #[inline]
    pub fn as_base64(&self) -> &Base64<Self> {
        // SAFETY: `Hash` and `Base64` have the same ABI
        unsafe { &*(self as *const Self as *const Base64<Self>) }
    }

    #[inline]
    pub const fn base64(self) -> Base64<Self> {
        Base64(self)
    }

    #[inline]
    pub fn as_sri(&self) -> &SRI<Self> {
        // SAFETY: `Hash` and `SRIHash` have the same ABI
        unsafe { &*(self as *const hash::Hash as *const SRI<Self>) }
    }

    #[inline]
    pub const fn sri(self) -> SRI<Self> {
        SRI(self)
    }
}

impl private::Sealed for hash::Hash {}
impl CommonHash for hash::Hash {
    #[inline]
    fn from_slice(algorithm: hash::Algorithm, hash: &[u8]) -> Result<Self, hash::ParseHashError> {
        hash::Hash::from_slice(algorithm, hash)
    }

    #[inline]
    fn implied_algorithm() -> Option<hash::Algorithm> {
        None
    }

    #[inline]
    fn algorithm(&self) -> hash::Algorithm {
        self.algorithm
    }

    #[inline]
    fn digest_bytes(&self) -> &[u8] {
        self.as_ref()
    }

    #[inline]
    fn as_base16(&self) -> &Base16<Self> {
        // SAFETY: `Hash` and `Base16<Hash>` have the same ABI
        unsafe { &*(self as *const Self as *const Base16<Self>) }
    }

    #[inline]
    fn as_base32(&self) -> &Base32<Self> {
        // SAFETY: `Hash` and `Base32<Hash>` have the same ABI
        unsafe { &*(self as *const Self as *const Base32<Self>) }
    }

    #[inline]
    fn as_base64(&self) -> &Base64<Self> {
        // SAFETY: `Hash` and `Base64<Hash>` have the same ABI
        unsafe { &*(self as *const Self as *const Base64<Self>) }
    }

    #[inline]
    fn as_sri(&self) -> &SRI<Self> {
        // SAFETY: `Hash` and `SRI<Hash>` have the same ABI
        unsafe { &*(self as *const Self as *const SRI<Self>) }
    }
}

/*
impl FromStr for hash::Hash {
    type Err = hash::ParseHashError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<Any<Self>>().map(From::from)
    }
}

impl sfmt::Display for hash::Hash {
    fn fmt(&self, f: &mut sfmt::Formatter<'_>) -> sfmt::Result {
        self.as_base32().fmt(f)
    }
}
*/

impl sfmt::Debug for hash::Hash {
    fn fmt(&self, f: &mut sfmt::Formatter<'_>) -> sfmt::Result {
        f.debug_struct("Hash")
            .field("algorithm", &self.algorithm)
            .field("data", &format_args!("{}", self.as_base32()))
            .finish()
    }
}

impl sfmt::LowerHex for hash::Hash {
    fn fmt(&self, f: &mut sfmt::Formatter<'_>) -> sfmt::Result {
        if !f.alternate() {
            write!(f, "{}:", self.algorithm())?;
        }
        for val in self.digest_bytes() {
            write!(f, "{val:02x}")?;
        }
        Ok(())
    }
}

impl sfmt::UpperHex for hash::Hash {
    fn fmt(&self, f: &mut sfmt::Formatter<'_>) -> sfmt::Result {
        if !f.alternate() {
            write!(f, "{}:", self.algorithm())?;
        }
        for val in self.digest_bytes() {
            write!(f, "{val:02X}")?;
        }
        Ok(())
    }
}

impl hash::Sha256 {
    #[inline]
    pub fn as_base16(&self) -> &Base16<Self> {
        // SAFETY: `Sha256` and `Base16<Sha256>` have the same ABI
        unsafe { &*(self as *const Self as *const Base16<Self>) }
    }

    #[inline]
    pub const fn base16(self) -> Base16<Self> {
        Base16(self)
    }

    #[inline]
    pub fn as_base32(&self) -> &Base32<Self> {
        // SAFETY: `Sha256` and `Base32<Sha256>` have the same ABI
        unsafe { &*(self as *const Self as *const Base32<Self>) }
    }

    #[inline]
    pub const fn base32(self) -> Base32<Self> {
        Base32(self)
    }

    #[inline]
    pub fn as_base64(&self) -> &Base64<Self> {
        // SAFETY: `Sha256` and `Base64<Sha256>` have the same ABI
        unsafe { &*(self as *const Self as *const Base64<Self>) }
    }

    #[inline]
    pub const fn base64(self) -> Base64<Self> {
        Base64(self)
    }
}

impl private::Sealed for hash::Sha256 {}
impl CommonHash for hash::Sha256 {
    #[inline]
    fn from_slice(algorithm: hash::Algorithm, hash: &[u8]) -> Result<Self, hash::ParseHashError> {
        if algorithm != hash::Algorithm::SHA256 {
            return Err(hash::ParseHashError::TypeMismatch {
                expected: hash::Algorithm::SHA256,
                actual: algorithm,
                hash: base32::encode_string(hash),
            });
        }
        hash::Sha256::from_slice(hash)
    }

    #[inline]
    fn implied_algorithm() -> Option<hash::Algorithm> {
        Some(hash::Algorithm::SHA256)
    }

    #[inline]
    fn algorithm(&self) -> hash::Algorithm {
        hash::Algorithm::SHA256
    }

    #[inline]
    fn digest_bytes(&self) -> &[u8] {
        self.as_ref()
    }

    #[inline]
    fn as_base16(&self) -> &Base16<Self> {
        // SAFETY: `Sha256` and `Base16<Sha256>` have the same ABI
        unsafe { &*(self as *const Self as *const Base16<Self>) }
    }

    #[inline]
    fn as_base32(&self) -> &Base32<Self> {
        // SAFETY: `Sha256` and `Base32<Sha256>` have the same ABI
        unsafe { &*(self as *const Self as *const Base32<Self>) }
    }

    #[inline]
    fn as_base64(&self) -> &Base64<Self> {
        // SAFETY: `Sha256` and `Base64<Sha256>` have the same ABI
        unsafe { &*(self as *const Self as *const Base64<Self>) }
    }

    #[inline]
    fn as_sri(&self) -> &SRI<Self> {
        // SAFETY: `Sha256` and `SRI<Sha256>` have the same ABI
        unsafe { &*(self as *const Self as *const SRI<Self>) }
    }
}

/*
impl FromStr for hash::Sha256 {
    type Err = hash::ParseHashError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<Bare<NonSRI<Self>>>().map(From::from)
    }
}

impl sfmt::Display for hash::Sha256 {
    fn fmt(&self, f: &mut sfmt::Formatter<'_>) -> sfmt::Result {
        self.as_base32().as_bare().fmt(f)
    }
}
 */

impl sfmt::Debug for hash::Sha256 {
    fn fmt(&self, f: &mut sfmt::Formatter<'_>) -> sfmt::Result {
        f.debug_tuple("Sha256")
            .field(&format_args!("{}", self.as_base32().as_bare()))
            .finish()
    }
}

impl sfmt::LowerHex for hash::Sha256 {
    fn fmt(&self, f: &mut sfmt::Formatter<'_>) -> sfmt::Result {
        for val in self.digest_bytes() {
            write!(f, "{val:02x}")?;
        }
        Ok(())
    }
}

impl sfmt::UpperHex for hash::Sha256 {
    fn fmt(&self, f: &mut sfmt::Formatter<'_>) -> sfmt::Result {
        for val in self.digest_bytes() {
            write!(f, "{val:02X}")?;
        }
        Ok(())
    }
}

impl hash::NarHash {
    #[inline]
    pub fn as_base16(&self) -> &Base16<Self> {
        // SAFETY: `NarHash` and `Base16<NarHash>` have the same ABI
        unsafe { &*(self as *const Self as *const Base16<Self>) }
    }

    #[inline]
    pub const fn base16(self) -> Base16<Self> {
        Base16(self)
    }

    #[inline]
    pub fn as_base32(&self) -> &Base32<Self> {
        // SAFETY: `NarHash` and `Base32<NarHash>` have the same ABI
        unsafe { &*(self as *const Self as *const Base32<Self>) }
    }

    #[inline]
    pub const fn base32(self) -> Base32<Self> {
        Base32(self)
    }

    #[inline]
    pub fn as_base64(&self) -> &Base64<Self> {
        // SAFETY: `NarHash` and `Base64<NarHash>` have the same ABI
        unsafe { &*(self as *const Self as *const Base64<Self>) }
    }

    #[inline]
    pub const fn base64(self) -> Base64<Self> {
        Base64(self)
    }
}

impl private::Sealed for hash::NarHash {}
impl CommonHash for hash::NarHash {
    #[inline]
    fn from_slice(algorithm: hash::Algorithm, hash: &[u8]) -> Result<Self, hash::ParseHashError> {
        if algorithm != hash::Algorithm::SHA256 {
            return Err(hash::ParseHashError::TypeMismatch {
                expected: hash::Algorithm::SHA256,
                actual: algorithm,
                hash: base32::encode_string(hash),
            });
        }
        hash::NarHash::from_slice(hash)
    }

    #[inline]
    fn implied_algorithm() -> Option<hash::Algorithm> {
        Some(hash::Algorithm::SHA256)
    }

    #[inline]
    fn algorithm(&self) -> hash::Algorithm {
        hash::Algorithm::SHA256
    }

    #[inline]
    fn digest_bytes(&self) -> &[u8] {
        self.0.digest_bytes()
    }

    #[inline]
    fn as_base16(&self) -> &Base16<Self> {
        // SAFETY: `NarHash` and `Base16<NarHash>` have the same ABI
        unsafe { &*(self as *const Self as *const Base16<Self>) }
    }

    #[inline]
    fn as_base32(&self) -> &Base32<Self> {
        // SAFETY: `NarHash` and `Base32<NarHash>` have the same ABI
        unsafe { &*(self as *const Self as *const Base32<Self>) }
    }

    #[inline]
    fn as_base64(&self) -> &Base64<Self> {
        // SAFETY: `NarHash` and `Base64<NarHash>` have the same ABI
        unsafe { &*(self as *const Self as *const Base64<Self>) }
    }

    #[inline]
    fn as_sri(&self) -> &SRI<Self> {
        // SAFETY: `NarHash` and `SRI<NarHash>` have the same ABI
        unsafe { &*(self as *const Self as *const SRI<Self>) }
    }
}

/*
impl FromStr for hash::NarHash {
    type Err = hash::ParseHashError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<Bare<Base16<Self>>>().map(From::from)
    }
}

impl sfmt::Display for hash::NarHash {
    fn fmt(&self, f: &mut sfmt::Formatter<'_>) -> sfmt::Result {
        self.as_base16().as_bare().fmt(f)
    }
}
*/

impl sfmt::Debug for hash::NarHash {
    fn fmt(&self, f: &mut sfmt::Formatter<'_>) -> sfmt::Result {
        f.debug_tuple("NarHash")
            .field(&format_args!("{}", self.as_base16().as_bare()))
            .finish()
    }
}

impl sfmt::LowerHex for hash::NarHash {
    fn fmt(&self, f: &mut sfmt::Formatter<'_>) -> sfmt::Result {
        for val in self.0.as_ref() {
            write!(f, "{val:02x}")?;
        }
        Ok(())
    }
}

impl sfmt::UpperHex for hash::NarHash {
    fn fmt(&self, f: &mut sfmt::Formatter<'_>) -> sfmt::Result {
        for val in self.0.as_ref() {
            write!(f, "{val:02X}")?;
        }
        Ok(())
    }
}

pub trait Format: private::Sealed {
    type Hash;
    fn parse(algorithm: hash::Algorithm, s: &str) -> Result<Self::Hash, hash::ParseHashError>;
    fn into_inner(self) -> Self::Hash;
    fn from_inner(inner: Self::Hash) -> Self;
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct Base16<H>(H);
impl<H: CommonHash + Sized> Base16<H> {
    pub const fn from_hash(hash: H) -> Self {
        Self(hash)
    }

    pub const fn as_hash(&self) -> &H {
        &self.0
    }

    /// Consumes the [`Base16`], returning the underlying [`Hash`](hash::Hash).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use nixrs::hash;
    ///
    /// let hex = hash::Algorithm::SHA256.digest(b"Hello World!").base16();
    /// assert_eq!(hex.into_hash(), hash::Algorithm::SHA256.digest(b"Hello World!"));
    /// ```
    #[inline]
    pub fn into_hash(self) -> H {
        self.0
    }

    #[inline]
    pub fn as_bare(&self) -> &Bare<Self> {
        // SAFETY: `Base16<H>` and `Bare<Base16<H>>` have the same ABI
        unsafe { &*(self as *const Self as *const Bare<Self>) }
    }

    #[inline]
    pub fn bare(self) -> Bare<Self> {
        Bare(self)
    }
}
impl<H: CommonHash> Base16<H> {
    pub fn parse(algorithm: hash::Algorithm, s: &str) -> Result<H, hash::ParseHashError> {
        <Self as Format>::parse(algorithm, s)
    }
}
impl<H> private::Sealed for Base16<H> {}
impl<H: CommonHash> Format for Base16<H> {
    type Hash = H;

    fn parse(algorithm: hash::Algorithm, s: &str) -> Result<Self::Hash, hash::ParseHashError> {
        if s.len() != algorithm.base16_len() {
            return Err(hash::ParseHashError::WrongHashLength(
                algorithm,
                s.to_string(),
            ));
        }
        let mut hash = [0u8; hash::LARGEST_ALGORITHM.size()];
        HEXLOWER_PERMISSIVE
            .decode_mut(s.as_bytes(), &mut hash[..algorithm.size()])
            .map_err(|err| hash::ParseHashError::BadEncoding(s.into(), "hex".into(), err.error))?;
        H::from_slice(algorithm, &hash[..algorithm.size()])
    }

    fn into_inner(self) -> Self::Hash {
        self.0
    }

    fn from_inner(inner: Self::Hash) -> Self {
        Self(inner)
    }
}

impl<H: CommonHash> sfmt::Display for Base16<H> {
    fn fmt(&self, f: &mut sfmt::Formatter<'_>) -> sfmt::Result {
        if !f.alternate() {
            write!(f, "{}:", self.0.algorithm())?;
        }
        for val in self.0.digest_bytes() {
            write!(f, "{val:02x}")?;
        }
        Ok(())
    }
}

/// Parse base16 (hexidecimal) prefixed hash
///
/// These have the format `<type>:<base16>`,
impl<H: CommonHash> FromStr for Base16<H> {
    type Err = hash::ParseHashError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some((prefix, rest)) = s.split_once(':') {
            let algorithm: hash::Algorithm = prefix.parse()?;
            Self::parse(algorithm, rest).map(Self)
        } else {
            Err(hash::ParseHashError::MissingType(s.to_string()))
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd, Ord)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
#[cfg_attr(
    feature = "nixrs-derive",
    nix(from_str, display, bound = "H: CommonHash + Sync + 'static")
)]
#[repr(transparent)]
pub struct Base32<H>(H);
impl<H: CommonHash + Sized> Base32<H> {
    pub const fn from_hash(hash: H) -> Self {
        Self(hash)
    }

    pub const fn as_hash(&self) -> &H {
        &self.0
    }

    /// Consumes the [`Base32`], returning the underlying [`Hash`](hash::Hash).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use nixrs::hash;
    ///
    /// let base32 = hash::Algorithm::SHA256.digest(b"Hello World!").base32();
    /// assert_eq!(base32.into_hash(), hash::Algorithm::SHA256.digest(b"Hello World!"));
    /// ```
    #[inline]
    pub fn into_hash(self) -> H {
        self.0
    }

    #[inline]
    pub fn as_bare(&self) -> &Bare<Self> {
        // SAFETY: `Base32<H>` and `Bare<Base32<H>>` have the same ABI
        unsafe { &*(self as *const Self as *const Bare<Self>) }
    }

    #[inline]
    pub fn bare(self) -> Bare<Self> {
        Bare(self)
    }
}
impl<H: CommonHash> Base32<H> {
    pub fn parse(algorithm: hash::Algorithm, s: &str) -> Result<H, hash::ParseHashError> {
        <Self as Format>::parse(algorithm, s)
    }
}
impl<H> private::Sealed for Base32<H> {}
impl<H: CommonHash> Format for Base32<H> {
    type Hash = H;

    fn parse(algorithm: hash::Algorithm, s: &str) -> Result<Self::Hash, hash::ParseHashError> {
        if s.len() != algorithm.base32_len() {
            return Err(hash::ParseHashError::WrongHashLength(
                algorithm,
                s.to_string(),
            ));
        }
        let mut hash = [0u8; hash::LARGEST_ALGORITHM.size()];
        base32::decode_mut(s.as_bytes(), &mut hash[..algorithm.size()]).map_err(|err| {
            hash::ParseHashError::BadEncoding(s.into(), "nixbase32".into(), err.error)
        })?;
        H::from_slice(algorithm, &hash[..algorithm.size()])
    }

    fn into_inner(self) -> Self::Hash {
        self.0
    }

    fn from_inner(inner: Self::Hash) -> Self {
        Self(inner)
    }
}

impl<H: CommonHash> sfmt::Display for Base32<H> {
    fn fmt(&self, f: &mut sfmt::Formatter<'_>) -> sfmt::Result {
        let mut buf = [0u8; hash::LARGEST_ALGORITHM.base32_len()];
        let output = &mut buf[..self.0.algorithm().base32_len()];
        base32::encode_mut(self.0.digest_bytes(), output);

        // SAFETY: Nix Base32 is a subset of ASCII, which guarantees valid UTF-8.
        let s = unsafe { std::str::from_utf8_unchecked(output) };
        if f.alternate() {
            f.write_str(s)
        } else {
            write!(f, "{}:{}", self.0.algorithm(), s)
        }
    }
}

/// Parse nixbase32 prefixed hash
///
/// These have the format `<type>:<base32>`,
impl<H: CommonHash> FromStr for Base32<H> {
    type Err = hash::ParseHashError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some((prefix, rest)) = s.split_once(':') {
            let algorithm: hash::Algorithm = prefix.parse()?;
            Self::parse(algorithm, rest).map(Self)
        } else {
            Err(hash::ParseHashError::MissingType(s.to_string()))
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct Base64<H>(H);
impl<H: CommonHash> Base64<H> {
    pub const fn from_hash(hash: H) -> Self {
        Self(hash)
    }

    pub const fn as_hash(&self) -> &H {
        &self.0
    }

    /// Consumes the [`Base64`], returning the underlying [`Hash`](hash::Hash).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use nixrs::hash;
    ///
    /// let base64 = hash::Algorithm::SHA256.digest(b"Hello World!").base64();
    /// assert_eq!(base64.into_hash(), hash::Algorithm::SHA256.digest(b"Hello World!"));
    /// ```
    pub fn into_hash(self) -> H {
        self.0
    }

    #[inline]
    pub fn as_bare(&self) -> &Bare<Self> {
        // SAFETY: `Base64<H>` and `Bare<Base64<H>>` have the same ABI
        unsafe { &*(self as *const Self as *const Bare<Self>) }
    }

    #[inline]
    pub fn bare(self) -> Bare<Self> {
        Bare(self)
    }
}
impl<H: CommonHash> Base64<H> {
    pub fn parse(algorithm: hash::Algorithm, s: &str) -> Result<H, hash::ParseHashError> {
        <Self as Format>::parse(algorithm, s)
    }
}
impl<H: CommonHash> private::Sealed for Base64<H> {}
impl<H: CommonHash> Format for Base64<H> {
    type Hash = H;

    fn parse(algorithm: hash::Algorithm, s: &str) -> Result<Self::Hash, hash::ParseHashError> {
        if s.len() != algorithm.base64_len() {
            return Err(hash::ParseHashError::WrongHashLength(
                algorithm,
                s.to_string(),
            ));
        }
        let mut hash = [0u8; hash::LARGEST_ALGORITHM.base64_decoded()];
        let len = BASE64
            .decode_mut(s.as_bytes(), &mut hash[..algorithm.base64_decoded()])
            .map_err(|err| {
                hash::ParseHashError::BadEncoding(s.into(), "base64".into(), err.error)
            })?;
        if len != algorithm.size() {
            Err(hash::ParseHashError::BadEncoding(
                s.to_string(),
                "base64".into(),
                DecodeError {
                    position: 0,
                    kind: DecodeKind::Length,
                },
            ))
        } else {
            H::from_slice(algorithm, &hash[..algorithm.size()])
        }
    }

    fn into_inner(self) -> Self::Hash {
        self.0
    }

    fn from_inner(inner: Self::Hash) -> Self {
        Self(inner)
    }
}

impl<H: CommonHash> sfmt::Display for Base64<H> {
    fn fmt(&self, f: &mut sfmt::Formatter<'_>) -> sfmt::Result {
        let mut buf = [0u8; hash::LARGEST_ALGORITHM.base64_len()];
        let output = &mut buf[..self.0.algorithm().base64_len()];
        BASE64.encode_mut(self.0.digest_bytes(), output);

        // SAFETY: Bas64 is a subset of ASCII, which guarantees valid UTF-8.
        let s = unsafe { std::str::from_utf8_unchecked(output) };

        if f.alternate() {
            f.write_str(s)
        } else {
            write!(f, "{}:{s}", self.0.algorithm())
        }
    }
}

/// Parse base64 prefixed hash
///
/// These have the format `<type>:<base64>`,
impl<H: CommonHash> FromStr for Base64<H> {
    type Err = hash::ParseHashError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some((prefix, rest)) = s.split_once(':') {
            let algorithm: hash::Algorithm = prefix.parse()?;
            Self::parse(algorithm, rest).map(Self)
        } else {
            Err(hash::ParseHashError::MissingType(s.to_string()))
        }
    }
}

/// Parse the hash from a string representation.
///
/// This will parse a hash in the format
/// `[<type>:]<base16|base32|base64>` or `<type>-<base64>` (a
/// Subresource Integrity hash expression).
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd, Ord)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize))]
#[cfg_attr(
    feature = "nixrs-derive",
    nix(from_str, bound = "H: CommonHash + Sync + 'static")
)]
pub struct Any<H>(H);
impl<H> Any<H> {
    pub const fn as_hash(&self) -> &H {
        &self.0
    }

    pub fn into_hash(self) -> H {
        self.0
    }
}
impl<H: CommonHash> Any<H> {
    pub fn parse(algorithm: hash::Algorithm, s: &str) -> Result<H, hash::ParseHashError> {
        <Self as Format>::parse(algorithm, s)
    }
}
impl<H> private::Sealed for Any<H> {}
impl<H: CommonHash> Format for Any<H> {
    type Hash = H;

    fn parse(algorithm: hash::Algorithm, s: &str) -> Result<Self::Hash, hash::ParseHashError> {
        if s.len() == algorithm.base16_len() {
            Ok(Base16::parse(algorithm, s)?)
        } else if s.len() == algorithm.base32_len() {
            Ok(Base32::parse(algorithm, s)?)
        } else if s.len() == algorithm.base64_len() {
            Ok(Base64::parse(algorithm, s)?)
        } else {
            Err(hash::ParseHashError::WrongHashLength(
                algorithm,
                s.to_string(),
            ))
        }
    }

    fn into_inner(self) -> Self::Hash {
        self.0
    }

    fn from_inner(inner: Self::Hash) -> Self {
        Any(inner)
    }
}

impl<H: CommonHash> FromStr for Any<H> {
    type Err = hash::ParseHashError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some((prefix, rest)) = s.split_once(':') {
            let algorithm: hash::Algorithm = prefix.parse()?;
            Ok(Self(Self::parse(algorithm, rest)?))
        } else if let Some((prefix, rest)) = s.split_once('-') {
            let algorithm: hash::Algorithm = prefix.parse()?;
            if rest.len() == algorithm.base64_len() {
                Ok(Self(Base64::parse(algorithm, rest)?))
            } else {
                Err(hash::ParseHashError::WrongHashLength(
                    algorithm,
                    rest.to_string(),
                ))
            }
        } else if let Some(algorithm) = H::implied_algorithm() {
            Ok(Self(Self::parse(algorithm, s)?))
        } else {
            Err(Self::Err::MissingType(s.to_string()))
        }
    }
}

/// Subresource Integrity hash expression.
///
/// These have the format `<type>-<base64>`,
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct SRI<H>(H);
impl<H: CommonHash + Sized> SRI<H> {
    pub const fn from_hash(hash: H) -> Self {
        Self(hash)
    }

    pub const fn as_hash(&self) -> &H {
        &self.0
    }

    /// Consumes the [`SRI`], returning the underlying [`Hash`](hash::Hash).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use nixrs::hash;
    ///
    /// let sri = hash::Algorithm::SHA256.digest(b"Hello World!").sri();
    /// assert_eq!(sri.into_hash(), hash::Algorithm::SHA256.digest(b"Hello World!"));
    /// ```
    pub fn into_hash(self) -> H {
        self.0
    }
}

impl<H: CommonHash> sfmt::Display for SRI<H> {
    fn fmt(&self, f: &mut sfmt::Formatter<'_>) -> sfmt::Result {
        write!(f, "{}-{}", self.0.algorithm(), self.0.as_base64().as_bare())
    }
}

/// Parse Subresource Integrity hash expression.
///
/// These have the format `<type>-<base64>`,
impl<H: CommonHash> FromStr for SRI<H> {
    type Err = hash::ParseHashError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some((prefix, rest)) = s.split_once('-') {
            let algorithm: hash::Algorithm = prefix.parse()?;
            Base64::parse(algorithm, rest).map(Self)
        } else {
            Err(hash::ParseHashError::NotSRI(s.to_owned()))
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub struct NonSRI<H>(H);
impl<H> NonSRI<H> {
    pub const fn as_hash(&self) -> &H {
        &self.0
    }

    pub fn into_hash(self) -> H {
        self.0
    }
}
impl<H: CommonHash> NonSRI<H> {
    pub fn parse(algorithm: hash::Algorithm, s: &str) -> Result<H, hash::ParseHashError> {
        <Self as Format>::parse(algorithm, s)
    }
}
impl<H> private::Sealed for NonSRI<H> {}
impl<H: CommonHash> Format for NonSRI<H> {
    type Hash = H;

    fn parse(algorithm: hash::Algorithm, s: &str) -> Result<Self::Hash, hash::ParseHashError> {
        if s.len() == algorithm.base16_len() {
            Ok(Base16::parse(algorithm, s)?)
        } else if s.len() == algorithm.base32_len() {
            Ok(Base32::parse(algorithm, s)?)
        } else if s.len() == algorithm.base64_len() {
            Ok(Base64::parse(algorithm, s)?)
        } else {
            Err(hash::ParseHashError::WrongHashLength(
                algorithm,
                s.to_string(),
            ))
        }
    }

    fn into_inner(self) -> Self::Hash {
        self.0
    }

    fn from_inner(inner: Self::Hash) -> Self {
        Self(inner)
    }
}

impl<H: CommonHash> FromStr for NonSRI<H> {
    type Err = hash::ParseHashError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some((prefix, rest)) = s.split_once(':') {
            let algorithm: hash::Algorithm = prefix.parse()?;
            Ok(Self(Self::parse(algorithm, rest)?))
        } else if let Some(algorithm) = H::implied_algorithm() {
            Ok(Self(Self::parse(algorithm, s)?))
        } else {
            Err(Self::Err::MissingType(s.to_string()))
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
#[cfg_attr(
    feature = "nixrs-derive",
    nix(
        from_str,
        display,
        bound(
            deserialize = "F: Format + Sync + 'static, <F as Format>::Hash: CommonHash",
            serialize = "F: sfmt::Display + Sync"
        )
    )
)]
#[repr(transparent)]
pub struct Bare<F>(F);
impl<F> sfmt::Display for Bare<F>
where
    F: sfmt::Display,
{
    fn fmt(&self, f: &mut sfmt::Formatter<'_>) -> sfmt::Result {
        write!(f, "{:#}", self.0)
    }
}

impl<F> FromStr for Bare<F>
where
    F: Format,
    <F as Format>::Hash: CommonHash,
{
    type Err = hash::ParseHashError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(algorithm) = <<F as Format>::Hash as CommonHash>::implied_algorithm() {
            Ok(Bare(F::from_inner(F::parse(algorithm, s)?)))
        } else {
            Err(Self::Err::MissingType(s.to_string()))
        }
    }
}

macro_rules! impl_fmt_from {
    ($T:ident<>) => {
        impl_fmt_from!($T, hash::Hash);
    };
    ($T:ident<$IT:path>) => {
        impl_fmt_from!($T<$IT>, $IT);
    };
    ($T:path, $FT:path) => {
        impl From<$FT> for $T {
            #[inline]
            fn from(value: $FT) -> Self {
                $T(value)
            }
        }

        impl From<$T> for $FT {
            #[inline]
            fn from(value: $T) -> Self {
                value.0
            }
        }

        impl AsRef<$FT> for $T {
            #[inline]
            fn as_ref(&self) -> &$FT {
                &self.0
            }
        }

        impl std::borrow::Borrow<$FT> for $T {
            #[inline]
            fn borrow(&self) -> &$FT {
                &self.0
            }
        }

        impl AsRef<[u8]> for $T {
            #[inline]
            fn as_ref(&self) -> &[u8] {
                self.0.digest_bytes()
            }
        }
    };
}

impl_fmt_from!(hash::NarHash, hash::Sha256);
impl_fmt_from!(SRI<hash::Hash>);
impl_fmt_from!(SRI<hash::NarHash>);
impl_fmt_from!(SRI<hash::Sha256>);

macro_rules! impl_format_from {
    ($T:ident<$IT:path>) => {
        impl_format_from!($T<$IT>, $IT);
    };
    ($T:ident<$($IT:path)?>, $FT:path) => {
        impl_fmt_from!($T<$($IT)?>, $FT);

        impl From<$FT> for Bare<$T<$($IT)?>> {
            #[inline]
            fn from(value: $FT) -> Self {
                Bare($T(value))
            }
        }

        impl From<Bare<$T<$($IT)?>>> for $FT {
            #[inline]
            fn from(value: Bare<$T<$($IT)?>>) -> Self {
                value.0.0
            }
        }

        impl AsRef<$FT> for Bare<$T<$($IT)?>> {
            #[inline]
            fn as_ref(&self) -> &$FT {
                &self.0.0
            }
        }

        impl std::borrow::Borrow<$FT> for Bare<$T<$($IT)?>> {
            #[inline]
            fn borrow(&self) -> &$FT {
                &self.0.0
            }
        }

        impl AsRef<[u8]> for Bare<$T<$($IT)?>> {
            #[inline]
            fn as_ref(&self) -> &[u8] {
                self.0.as_ref()
            }
        }
    };
}

impl_format_from!(Base64<hash::Hash>);
impl_format_from!(Base64<hash::NarHash>);
impl_format_from!(Base64<hash::Sha256>);
impl_format_from!(Base32<hash::Hash>);
impl_format_from!(Base32<hash::NarHash>);
impl_format_from!(Base32<hash::Sha256>);
impl_format_from!(Base16<hash::Hash>);
impl_format_from!(Base16<hash::NarHash>);
impl_format_from!(Base16<hash::Sha256>);
impl_format_from!(Any<hash::Hash>);
impl_format_from!(Any<hash::NarHash>);
impl_format_from!(Any<hash::Sha256>);
impl_format_from!(NonSRI<hash::Hash>);
impl_format_from!(NonSRI<hash::NarHash>);
impl_format_from!(NonSRI<hash::Sha256>);

#[cfg(test)]
mod unittests {
    use hex_literal::hex;

    use super::*;
    use crate::hash::{Algorithm, Hash};

    struct HashFormats {
        hash: Hash,
        algorithm: &'static str,
        base16: &'static str,
        base32: &'static str,
        base64: &'static str,
    }

    /// value taken from: https://tools.ietf.org/html/rfc1321
    const MD5_EMPTY: HashFormats = HashFormats {
        hash: Hash::new(Algorithm::MD5, &hex!("d41d8cd98f00b204e9800998ecf8427e")),
        algorithm: "md5",
        base16: "d41d8cd98f00b204e9800998ecf8427e",
        base32: "3y8bwfr609h3lh9ch0izcqq7fl",
        base64: "1B2M2Y8AsgTpgAmY7PhCfg==",
    };

    /// value taken from: https://tools.ietf.org/html/rfc1321
    const MD5_ABC: HashFormats = HashFormats {
        hash: Hash::new(Algorithm::MD5, &hex!("900150983cd24fb0d6963f7d28e17f72")),
        algorithm: "md5",
        base16: "900150983cd24fb0d6963f7d28e17f72",
        base32: "3jgzhjhz9zjvbb0kyj7jc500ch",
        base64: "kAFQmDzST7DWlj99KOF/cg==",
    };

    /// value taken from: https://tools.ietf.org/html/rfc3174
    const SHA1_ABC: HashFormats = HashFormats {
        hash: Hash::new(
            Algorithm::SHA1,
            &hex!("a9993e364706816aba3e25717850c26c9cd0d89d"),
        ),
        algorithm: "sha1",
        base16: "a9993e364706816aba3e25717850c26c9cd0d89d",
        base32: "kpcd173cq987hw957sx6m0868wv3x6d9",
        base64: "qZk+NkcGgWq6PiVxeFDCbJzQ2J0=",
    };

    /// value taken from: https://tools.ietf.org/html/rfc3174
    const SHA1_LONG: HashFormats = HashFormats {
        hash: Hash::new(
            Algorithm::SHA1,
            &hex!("84983e441c3bd26ebaae4aa1f95129e5e54670f1"),
        ),
        algorithm: "sha1",
        base16: "84983e441c3bd26ebaae4aa1f95129e5e54670f1",
        base32: "y5q4drg5558zk8aamsx6xliv3i23x644",
        base64: "hJg+RBw70m66rkqh+VEp5eVGcPE=",
    };

    /// value taken from: https://tools.ietf.org/html/rfc4634
    const SHA256_ABC: HashFormats = HashFormats {
        hash: Hash::new(
            Algorithm::SHA256,
            &hex!("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"),
        ),
        algorithm: "sha256",
        base16: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        base32: "1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5s",
        base64: "ungWv48Bz+pBQUDeXa4iI7ADYaOWF3qctBD/YfIAFa0=",
    };

    /// value taken from: https://tools.ietf.org/html/rfc4634
    const SHA256_LONG: HashFormats = HashFormats {
        hash: Hash::new(
            Algorithm::SHA256,
            &hex!("248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"),
        ),
        algorithm: "sha256",
        base16: "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1",
        base32: "1h86vccx9vgcyrkj3zv4b7j3r8rrc0z0r4r6q3jvhf06s9hnm394",
        base64: "JI1qYdIGOLjlwCaTDD5gOaM85Flk/yFn9uzt1BnbBsE=",
    };

    /// value taken from: https://tools.ietf.org/html/rfc4634
    const SHA512_ABC: HashFormats = HashFormats {
        hash: Hash::new(
            Algorithm::SHA512,
            &hex!(
                "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f"
            ),
        ),
        algorithm: "sha512",
        base16: "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f",
        base32: "2gs8k559z4rlahfx0y688s49m2vvszylcikrfinm30ly9rak69236nkam5ydvly1ai7xac99vxfc4ii84hawjbk876blyk1jfhkbbyx",
        base64: "3a81oZNherrMQXNJriBBMRLm+k6JqX6iCp7u5ktV05ohkpkqJ0/BqDa6PCOj/uu9RU1EI2Q86A4qmslPpUyknw==",
    };

    /// value taken from: https://tools.ietf.org/html/rfc4634
    const SHA512_LONG: HashFormats = HashFormats {
        hash: Hash::new(
            Algorithm::SHA512,
            &hex!(
                "8e959b75dae313da8cf4f72814fc143f8f7779c6eb9f7fa17299aeadb6889018501d289e4900f7e4331b99dec4b5433ac7d329eeb6dd26545e96e55b874be909"
            ),
        ),
        algorithm: "sha512",
        base16: "8e959b75dae313da8cf4f72814fc143f8f7779c6eb9f7fa17299aeadb6889018501d289e4900f7e4331b99dec4b5433ac7d329eeb6dd26545e96e55b874be909",
        base32: "04yjjw7bgjrcpjl4vfvdvi9sg3klhxmqkg9j6rkwkvh0jcy50fm064hi2vavblrfahpz7zbqrwpg3rz2ky18a7pyj6dl4z3v9srp5cf",
        base64: "jpWbddrjE9qM9PcoFPwUP493ecbrn3+hcpmurbaIkBhQHSieSQD35DMbmd7EtUM6x9Mp7rbdJlReluVbh0vpCQ==",
    };

    #[rstest_reuse::template]
    #[rstest::rstest]
    #[case::md5_empty(MD5_EMPTY)]
    #[case::md5_abc(MD5_ABC)]
    #[case::sha1_abc(SHA1_ABC)]
    #[case::sha1_long(SHA1_LONG)]
    #[case::sha256_abc(SHA256_ABC)]
    #[case::sha256_long(SHA256_LONG)]
    #[case::sha512_abc(SHA512_ABC)]
    #[case::sha512_long(SHA512_LONG)]
    fn hash_formats(#[case] hash: HashFormats) {}

    #[rstest_reuse::apply(hash_formats)]
    fn lower_hex(#[case] hash: HashFormats) {
        assert_eq!(
            format!("{}:{}", hash.algorithm, hash.base16),
            format!("{:x}", hash.hash)
        );
    }

    #[rstest_reuse::apply(hash_formats)]
    fn lower_hex_alt(#[case] hash: HashFormats) {
        assert_eq!(hash.base16, format!("{:#x}", hash.hash));
    }

    #[rstest_reuse::apply(hash_formats)]
    fn upper_hex(#[case] hash: HashFormats) {
        assert_eq!(
            format!("{}:{}", hash.algorithm, hash.base16.to_uppercase()),
            format!("{:X}", hash.hash)
        );
    }

    #[rstest_reuse::apply(hash_formats)]
    fn upper_hex_alt(#[case] hash: HashFormats) {
        assert_eq!(hash.base16.to_uppercase(), format!("{:#X}", hash.hash));
    }

    mod base16 {
        use super::*;

        #[rstest_reuse::apply(hash_formats)]
        fn hash_display(#[case] hash: HashFormats) {
            assert_eq!(
                format!("{}:{}", hash.algorithm, hash.base16),
                hash.hash.base16().to_string()
            );
        }

        #[rstest_reuse::apply(hash_formats)]
        fn hash_display_alt(#[case] hash: HashFormats) {
            assert_eq!(hash.base16, format!("{:#}", hash.hash.base16()));
        }

        #[rstest_reuse::apply(hash_formats)]
        fn hash_display_bare(#[case] hash: HashFormats) {
            assert_eq!(hash.base16, hash.hash.base16().bare().to_string());
        }

        #[rstest_reuse::apply(hash_formats)]
        fn hash_display_bare_alt(#[case] hash: HashFormats) {
            assert_eq!(hash.base16, format!("{:#}", hash.hash.base16().bare()));
        }

        #[rstest_reuse::apply(hash_formats)]
        fn hash_from_str(#[case] hash: HashFormats) {
            let s = format!("{}:{}", hash.algorithm, hash.base16);
            assert_eq!(*hash.hash.as_base16(), s.parse::<Base16<Hash>>().unwrap());
        }
    }

    mod base32 {
        use super::*;

        #[rstest_reuse::apply(hash_formats)]
        fn hash_display(#[case] hash: HashFormats) {
            assert_eq!(
                format!("{}:{}", hash.algorithm, hash.base32),
                hash.hash.base32().to_string()
            );
        }

        #[rstest_reuse::apply(hash_formats)]
        fn hash_display_alt(#[case] hash: HashFormats) {
            assert_eq!(hash.base32, format!("{:#}", hash.hash.base32()));
        }

        #[rstest_reuse::apply(hash_formats)]
        fn hash_display_bare(#[case] hash: HashFormats) {
            assert_eq!(hash.base32, hash.hash.base32().bare().to_string());
        }

        #[rstest_reuse::apply(hash_formats)]
        fn hash_display_bare_alt(#[case] hash: HashFormats) {
            assert_eq!(hash.base32, format!("{:#}", hash.hash.base32().bare()));
        }

        #[rstest_reuse::apply(hash_formats)]
        fn hash_from_str(#[case] hash: HashFormats) {
            let s = format!("{}:{}", hash.algorithm, hash.base32);
            assert_eq!(*hash.hash.as_base32(), s.parse::<Base32<Hash>>().unwrap());
        }
    }

    mod base64 {
        use super::*;

        #[rstest_reuse::apply(hash_formats)]
        fn hash_display(#[case] hash: HashFormats) {
            assert_eq!(
                format!("{}:{}", hash.algorithm, hash.base64),
                hash.hash.base64().to_string()
            );
        }

        #[rstest_reuse::apply(hash_formats)]
        fn hash_display_alt(#[case] hash: HashFormats) {
            assert_eq!(hash.base64, format!("{:#}", hash.hash.base64()));
        }

        #[rstest_reuse::apply(hash_formats)]
        fn hash_display_bare(#[case] hash: HashFormats) {
            assert_eq!(hash.base64, hash.hash.base64().bare().to_string());
        }

        #[rstest_reuse::apply(hash_formats)]
        fn hash_display_bare_alt(#[case] hash: HashFormats) {
            assert_eq!(hash.base64, format!("{:#}", hash.hash.base64().bare()));
        }

        #[rstest_reuse::apply(hash_formats)]
        fn hash_from_str(#[case] hash: HashFormats) {
            let s = format!("{}:{}", hash.algorithm, hash.base64);
            assert_eq!(*hash.hash.as_base64(), s.parse::<Base64<Hash>>().unwrap());
        }
    }

    mod sri {
        use super::*;

        #[rstest_reuse::apply(hash_formats)]
        fn hash_display(#[case] hash: HashFormats) {
            let s = format!("{}-{}", hash.algorithm, hash.base64);
            assert_eq!(s, format!("{}", hash.hash.sri()));
        }

        #[rstest_reuse::apply(hash_formats)]
        fn hash_display_alt(#[case] hash: HashFormats) {
            let s = format!("{}-{}", hash.algorithm, hash.base64);
            assert_eq!(s, format!("{:#}", hash.hash.sri()));
        }

        #[rstest_reuse::apply(hash_formats)]
        fn hash_from_str(#[case] hash: HashFormats) {
            let s = format!("{}-{}", hash.algorithm, hash.base64);
            assert_eq!(*hash.hash.as_sri(), s.parse::<SRI<Hash>>().unwrap());
        }
    }

    mod any {
        use super::*;

        #[rstest_reuse::apply(hash_formats)]
        fn hash_from_str_base16(#[case] hash: HashFormats) {
            let s = format!("{}:{}", hash.algorithm, hash.base16);
            assert_eq!(hash.hash, s.parse::<Any<hash::Hash>>().unwrap().into_hash());
        }

        #[rstest_reuse::apply(hash_formats)]
        fn hash_from_str_base32(#[case] hash: HashFormats) {
            let s = format!("{}:{}", hash.algorithm, hash.base32);
            assert_eq!(hash.hash, s.parse::<Any<hash::Hash>>().unwrap().into_hash());
        }

        #[rstest_reuse::apply(hash_formats)]
        fn hash_from_str_base64(#[case] hash: HashFormats) {
            let s = format!("{}:{}", hash.algorithm, hash.base64);
            assert_eq!(hash.hash, s.parse::<Any<hash::Hash>>().unwrap().into_hash());
        }

        #[rstest_reuse::apply(hash_formats)]
        fn hash_from_str_sri(#[case] hash: HashFormats) {
            let s = format!("{}-{}", hash.algorithm, hash.base64);
            assert_eq!(hash.hash, s.parse::<Any<hash::Hash>>().unwrap().into_hash());
        }
    }

    mod non_sri {
        use super::*;

        #[rstest_reuse::apply(hash_formats)]
        fn hash_from_str_base16(#[case] hash: HashFormats) {
            let s = format!("{}:{}", hash.algorithm, hash.base16);
            assert_eq!(
                hash.hash,
                s.parse::<NonSRI<hash::Hash>>().unwrap().into_hash()
            );
        }

        #[rstest_reuse::apply(hash_formats)]
        fn hash_from_str_base32(#[case] hash: HashFormats) {
            let s = format!("{}:{}", hash.algorithm, hash.base32);
            assert_eq!(
                hash.hash,
                s.parse::<NonSRI<hash::Hash>>().unwrap().into_hash()
            );
        }

        #[rstest_reuse::apply(hash_formats)]
        fn hash_from_str_base64(#[case] hash: HashFormats) {
            let s = format!("{}:{}", hash.algorithm, hash.base64);
            assert_eq!(
                hash.hash,
                s.parse::<NonSRI<hash::Hash>>().unwrap().into_hash()
            );
        }
    }

    /*
    mod hash_from_str {
        use super::*;

        #[rstest_reuse::apply(hash_formats)]
        fn base16(#[case] hash: HashFormats) {
            let s = format!("{}:{}", hash.algorithm, hash.base16);
            assert_eq!(hash.hash, s.parse().unwrap());
        }

        #[rstest_reuse::apply(hash_formats)]
        fn base32(#[case] hash: HashFormats) {
            let s = format!("{}:{}", hash.algorithm, hash.base32);
            assert_eq!(hash.hash, s.parse().unwrap());
        }

        #[rstest_reuse::apply(hash_formats)]
        fn base64(#[case] hash: HashFormats) {
            let s = format!("{}:{}", hash.algorithm, hash.base64);
            assert_eq!(hash.hash, s.parse().unwrap());
        }

        #[rstest_reuse::apply(hash_formats)]
        fn sri(#[case] hash: HashFormats) {
            let s = format!("{}-{}", hash.algorithm, hash.base64);
            assert_eq!(hash.hash, s.parse().unwrap());
        }
    }
     */
}
