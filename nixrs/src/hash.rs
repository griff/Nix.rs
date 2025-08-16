use std::convert::TryFrom;
use std::convert::TryInto;
use std::fmt;
use std::fmt::LowerHex;
use std::str::FromStr;

use data_encoding::BASE64;
use data_encoding::DecodeError;
use data_encoding::DecodeKind;
use data_encoding::HEXLOWER_PERMISSIVE;
use derive_more::Display;
#[cfg(feature = "nixrs-derive")]
use nixrs_derive::{NixDeserialize, NixSerialize};
#[cfg(any(test, feature = "test"))]
use proptest_derive::Arbitrary;
use ring::digest;
use thiserror::Error;

use crate::wire::base64_len;

use super::base32;

const MD5_SIZE: usize = 128 / 8;
const SHA1_SIZE: usize = 160 / 8;
const SHA256_SIZE: usize = 256 / 8;
const SHA512_SIZE: usize = 512 / 8;
const LARGEST_ALGORITHM: Algorithm = Algorithm::SHA512;
const MAX_SIZE: usize = LARGEST_ALGORITHM.size();

/// A digest algorithm.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Hash, Display, Default)]
pub enum Algorithm {
    #[display(fmt = "md5")]
    MD5,
    #[display(fmt = "sha1")]
    SHA1,
    #[default]
    #[display(fmt = "sha256")]
    SHA256,
    #[display(fmt = "sha512")]
    SHA512,
}

impl Algorithm {
    /// Returns the size in bytes of this hash.
    #[inline]
    pub const fn size(&self) -> usize {
        match &self {
            Algorithm::MD5 => MD5_SIZE,
            Algorithm::SHA1 => SHA1_SIZE,
            Algorithm::SHA256 => SHA256_SIZE,
            Algorithm::SHA512 => SHA512_SIZE,
        }
    }

    /// Returns the length of a base-16 representation of this hash.
    #[inline]
    pub const fn base16_len(&self) -> usize {
        self.size() * 2
    }

    /// Returns the length of a base-32 representation of this hash.
    #[inline]
    pub const fn base32_len(&self) -> usize {
        base32::encode_len(self.size())
    }

    /// Returns the length of a base-64 representation of this hash.
    #[inline]
    pub const fn base64_len(&self) -> usize {
        base64_len(self.size())
    }

    #[inline]
    const fn base64_decoded(&self) -> usize {
        self.base64_len() / 4 * 3
    }

    #[inline]
    fn digest_algorithm(&self) -> &'static digest::Algorithm {
        match self {
            Algorithm::SHA1 => &digest::SHA1_FOR_LEGACY_USE_ONLY,
            Algorithm::SHA256 => &digest::SHA256,
            Algorithm::SHA512 => &digest::SHA512,
            _ => panic!("Unsupported digest algorithm {self:?}"),
        }
    }

    /// Returns the digest of `data` using the given digest algorithm.
    ///
    /// ```
    /// # use nixrs::hash::Algorithm;
    /// let hash = Algorithm::SHA256.digest("abc");
    ///
    /// assert_eq!("sha256:1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5s", hash.to_string());
    /// ```
    pub fn digest<B: AsRef<[u8]>>(&self, data: B) -> Hash {
        match *self {
            #[cfg(feature = "md5")]
            Algorithm::MD5 => Hash::new(Algorithm::MD5, md5::compute(data).as_ref()),
            _ => digest::digest(self.digest_algorithm(), data.as_ref())
                .try_into()
                .unwrap(),
        }
    }
}

#[derive(Error, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
#[error("Unsupported digest algorithm {0}")]
pub struct UnknownAlgorithm(String);

impl<'a> TryFrom<&'a digest::Algorithm> for Algorithm {
    type Error = UnknownAlgorithm;
    fn try_from(value: &'a digest::Algorithm) -> Result<Self, Self::Error> {
        if *value == digest::SHA1_FOR_LEGACY_USE_ONLY {
            Ok(Algorithm::SHA1)
        } else if *value == digest::SHA256 {
            Ok(Algorithm::SHA256)
        } else if *value == digest::SHA512 {
            Ok(Algorithm::SHA512)
        } else {
            Err(UnknownAlgorithm(format!("{value:?}")))
        }
    }
}

impl FromStr for Algorithm {
    type Err = UnknownAlgorithm;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.eq_ignore_ascii_case("sha256") {
            Ok(Algorithm::SHA256)
        } else if s.eq_ignore_ascii_case("sha512") {
            Ok(Algorithm::SHA512)
        } else if s.eq_ignore_ascii_case("sha1") {
            Ok(Algorithm::SHA1)
        } else if s.eq_ignore_ascii_case("md5") {
            Ok(Algorithm::MD5)
        } else {
            Err(UnknownAlgorithm(s.to_owned()))
        }
    }
}

#[derive(Error, Debug, PartialEq, Clone)]
pub enum ParseHashError {
    #[error("{0}")]
    Algorithm(
        #[from]
        #[source]
        UnknownAlgorithm,
    ),
    #[error("Hash '{0}' is not SRI")]
    NotSRI(String),
    #[error("Hash '{0}' does not include a type")]
    MissingTypePrefix(String),
    #[error("Hash '{hash}' should have type '{expected}'")]
    TypeMismatch {
        expected: Algorithm,
        actual: Algorithm,
        hash: String,
    },
    #[error("Hash '{0}' does not include a type, nor is the type otherwise known from context")]
    MissingType(String),
    #[error("invalid {1} encoding '{0}'")]
    BadEncoding(String, String, #[source] data_encoding::DecodeError),
    /*
    #[error("invalid base-16 hash '{0}'")]
    BadBase16Hash(String, #[source] FromHexError),
    #[error("invalid base-32 hash '{0}'")]
    BadBase32Hash(String, #[source] base32::BadBase32),
    #[error("invalid base-64 hash '{0}'")]
    BadBase64Hash(String, #[source] base64::DecodeError),
    */
    #[error("invalid SRI hash '{0}'")]
    BadSRIHash(String),
    #[error("hash '{1}' has wrong length for hash type '{0}'")]
    WrongHashLength(Algorithm, String),
    #[error("hash has wrong length {1} for hash type '{0}'")]
    WrongHashLength2(Algorithm, usize),
}

pub(crate) fn parse_prefix(s: &str) -> Result<Option<(Algorithm, bool, &str)>, UnknownAlgorithm> {
    if let Some((prefix, rest)) = s.split_once(':') {
        let a: Algorithm = prefix.parse()?;
        Ok(Some((a, false, rest)))
    } else if let Some((prefix, rest)) = s.split_once('-') {
        let a: Algorithm = prefix.parse()?;
        Ok(Some((a, true, rest)))
    } else {
        Ok(None)
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Hash)]
pub struct Hash {
    algorithm: Algorithm,
    data: [u8; MAX_SIZE],
}

impl Hash {
    pub fn new(algorithm: Algorithm, hash: &[u8]) -> Hash {
        let mut data = [0u8; MAX_SIZE];
        data[0..algorithm.size()].copy_from_slice(hash);
        Hash { algorithm, data }
    }

    pub fn from_slice(algorithm: Algorithm, hash: &[u8]) -> Result<Hash, ParseHashError> {
        if hash.len() != algorithm.size() {
            return Err(ParseHashError::WrongHashLength2(algorithm, hash.len()));
        }
        Ok(Hash::new(algorithm, hash))
    }

    fn from_str(rest: &str, a: Algorithm, is_sri: bool) -> Result<Hash, ParseHashError> {
        let mut data = [0u8; MAX_SIZE];
        if !is_sri && rest.len() == a.base16_len() {
            HEXLOWER_PERMISSIVE
                .decode_mut(rest.as_bytes(), &mut data[..a.size()])
                .map_err(|err| ParseHashError::BadEncoding(rest.into(), "hex".into(), err.error))?;
            Ok(Hash { algorithm: a, data })
        } else if !is_sri && rest.len() == a.base32_len() {
            base32::decode_mut(rest.as_bytes(), &mut data[..a.size()]).map_err(|err| {
                ParseHashError::BadEncoding(rest.into(), "nixbase32".into(), err.error)
            })?;

            Ok(Hash { algorithm: a, data })
        } else if is_sri || rest.len() == a.base64_len() {
            let len = if a.base64_decoded() < MAX_SIZE {
                BASE64
                    .decode_mut(rest.as_bytes(), &mut data[..a.base64_decoded()])
                    .map_err(|err| {
                        ParseHashError::BadEncoding(rest.into(), "base64".into(), err.error)
                    })?
            } else {
                let mut buf = [0u8; LARGEST_ALGORITHM.base64_decoded()];
                let ret = BASE64
                    .decode_mut(rest.as_bytes(), &mut buf[0..a.base64_decoded()])
                    .map_err(|err| {
                        ParseHashError::BadEncoding(rest.into(), "base64".into(), err.error)
                    })?;
                data[..ret].copy_from_slice(&buf[0..ret]);
                ret
            };
            if len != a.size() {
                if is_sri {
                    Err(ParseHashError::BadSRIHash(rest.to_string()))
                } else {
                    Err(ParseHashError::BadEncoding(
                        rest.to_string(),
                        "base64".into(),
                        DecodeError {
                            position: 0,
                            kind: DecodeKind::Length,
                        },
                    ))
                }
            } else {
                Ok(Hash { algorithm: a, data })
            }
        } else {
            Err(ParseHashError::WrongHashLength(a, rest.to_string()))
        }
    }

    /// Parse Subresource Integrity hash expression.
    ///
    /// These have the format `<type>-<base64>`,
    pub fn parse_sri(s: &str) -> Result<Hash, ParseHashError> {
        if let Some((prefix, rest)) = s.split_once('-') {
            let a: Algorithm = prefix.parse()?;
            Hash::from_str(rest, a, true)
        } else {
            Err(ParseHashError::NotSRI(s.to_owned()))
        }
    }

    /// Parse the hash from a string representation.
    ///
    /// This will parse a hash in the format
    /// `[<type>:]<base16|base32|base64>` or `<type>-<base64>` (a
    /// Subresource Integrity hash expression). If the 'type' argument
    /// is not present, then the hash type must be specified in the
    /// in the `algorithm` argument.
    pub fn parse_any(s: &str, algorithm: Option<Algorithm>) -> Result<Hash, ParseHashError> {
        if let Some((a, is_sri, rest)) = parse_prefix(s)? {
            if let Some(expected) = algorithm {
                if expected != a {
                    return Err(ParseHashError::TypeMismatch {
                        expected,
                        actual: a,
                        hash: s.to_string(),
                    });
                }
            }
            Hash::from_str(rest, a, is_sri)
        } else if let Some(a) = algorithm {
            Hash::from_str(s, a, false)
        } else {
            Err(ParseHashError::MissingType(s.to_string()))
        }
    }

    /// Parse a hash from a string representation like the above, except the
    /// type prefix is mandatory is there is no separate arguement.
    pub fn parse_any_prefixed(s: &str) -> Result<Hash, ParseHashError> {
        if let Some((a, is_sri, rest)) = parse_prefix(s)? {
            Hash::from_str(rest, a, is_sri)
        } else {
            Err(ParseHashError::MissingTypePrefix(s.to_string()))
        }
    }

    pub fn parse_non_sri_prefixed(s: &str) -> Result<Hash, ParseHashError> {
        if let Some((prefix, rest)) = s.split_once(':') {
            let a = prefix.parse()?;
            Hash::from_str(rest, a, false)
        } else {
            Err(ParseHashError::MissingTypePrefix(s.to_string()))
        }
    }

    /// Parse a plain hash that must not have any prefix indicating the type.
    /// The type is passed in to disambiguate.
    pub fn parse_non_sri_unprefixed(s: &str, algorithm: Algorithm) -> Result<Hash, ParseHashError> {
        Hash::from_str(s, algorithm, false)
    }

    pub fn algorithm(&self) -> Algorithm {
        self.algorithm
    }

    pub fn data(&self) -> &[u8] {
        &self.data[0..(self.algorithm.size())]
    }

    pub fn encode_base16(&self) -> String {
        format!("{self:#x}")
    }

    pub fn encode_base32(&self) -> String {
        format!("{:#}", self.to_base32())
    }

    pub fn encode_base64(&self) -> String {
        format!("{:#}", self.to_base64())
    }

    pub fn bare(&self) -> impl fmt::Display + '_ {
        Bare(self.to_base32())
    }

    pub fn to_base16(&self) -> &Base16Hash<Hash> {
        // SAFETY: `Hash` and `Base16Hash` have the same ABI
        unsafe { &*(self as *const Hash as *const Base16Hash<Hash>) }
    }

    pub fn to_base32(&self) -> impl fmt::Display + '_ {
        Base32Hash(self)
    }

    pub fn to_base64(&self) -> impl fmt::Display + '_ {
        Base64Hash(self)
    }

    pub fn to_sri(&self) -> impl fmt::Display + '_ {
        SRIHash(self)
    }
}

impl std::ops::Deref for Hash {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        self.data()
    }
}

impl AsRef<[u8]> for Hash {
    fn as_ref(&self) -> &[u8] {
        self.data()
    }
}

impl fmt::LowerHex for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !f.alternate() {
            write!(f, "{}:", self.algorithm())?;
        }
        for val in self.as_ref() {
            write!(f, "{val:02x}")?;
        }
        Ok(())
    }
}

impl fmt::UpperHex for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !f.alternate() {
            write!(f, "{}:", self.algorithm())?;
        }
        for val in self.as_ref() {
            write!(f, "{val:02X}")?;
        }
        Ok(())
    }
}

impl TryFrom<digest::Digest> for Hash {
    type Error = UnknownAlgorithm;
    fn try_from(digest: digest::Digest) -> Result<Self, Self::Error> {
        Ok(Hash::new(digest.algorithm().try_into()?, digest.as_ref()))
    }
}

impl FromStr for Hash {
    type Err = ParseHashError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Hash::parse_any_prefixed(s)
    }
}

impl fmt::Debug for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Hash")
            .field("algorithm", &self.algorithm)
            .field("data", &format_args!("{}", self.to_base32()))
            .finish()
    }
}

impl fmt::Display for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.to_base32().fmt(f)
    }
}

struct Bare<M>(M);
impl<M> fmt::Display for Bare<M>
where
    M: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:#}", self.0)
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Hash)]
#[repr(transparent)]
pub struct Base16Hash<H>(H);
impl<H: LowerHex> fmt::Display for Base16Hash<H> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if f.alternate() {
            write!(f, "{:#x}", self.0)
        } else {
            write!(f, "{:x}", self.0)
        }
    }
}

struct Base32Hash<'a>(&'a Hash);
impl fmt::Display for Base32Hash<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut buf = [0u8; LARGEST_ALGORITHM.base32_len()];
        let output = &mut buf[..self.0.algorithm.base32_len()];
        base32::encode_mut(self.0.as_ref(), output);

        // SAFETY: Nix Base32 is a subset of ASCII, which guarantees valid UTF-8.
        let s = unsafe { std::str::from_utf8_unchecked(output) };
        if f.alternate() {
            f.write_str(s)
        } else {
            write!(f, "{}:{}", self.0.algorithm(), s)
        }
    }
}

struct Base64Hash<'a>(&'a Hash);
impl fmt::Display for Base64Hash<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut buf = [0u8; LARGEST_ALGORITHM.base64_len()];
        let output = &mut buf[..self.0.algorithm.base64_len()];
        BASE64.encode_mut(self.0.as_ref(), output);

        // SAFETY: Nix Base32 is a subset of ASCII, which guarantees valid UTF-8.
        let s = unsafe { std::str::from_utf8_unchecked(output) };

        if f.alternate() {
            f.write_str(s)
        } else {
            write!(f, "{}:{s}", self.0.algorithm())
        }
    }
}

struct SRIHash<'a>(&'a Hash);
impl fmt::Display for SRIHash<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{:#}", self.0.algorithm(), self.0.to_base64())
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Hash)]
#[cfg_attr(any(test, feature = "test"), derive(Arbitrary))]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
#[cfg_attr(feature = "nixrs-derive", nix(from_str, display))]
pub struct NarHash([u8; Algorithm::SHA256.size()]);

impl NarHash {
    pub fn new(digest: &[u8]) -> NarHash {
        let mut data = [0u8; Algorithm::SHA256.size()];
        data.copy_from_slice(digest);
        NarHash(data)
    }

    pub fn from_slice(digest: &[u8]) -> Result<NarHash, ParseHashError> {
        if digest.len() != Algorithm::SHA256.size() {
            return Err(ParseHashError::WrongHashLength2(
                Algorithm::SHA256,
                digest.len(),
            ));
        }
        Ok(NarHash::new(digest))
    }

    pub fn digest<D: AsRef<[u8]>>(data: D) -> Self {
        Self::new(&Algorithm::SHA256.digest(data))
    }
}

impl AsRef<[u8]> for NarHash {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl FromStr for NarHash {
    type Err = ParseHashError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() != Algorithm::SHA256.base16_len() {
            return Err(ParseHashError::WrongHashLength(
                Algorithm::SHA256,
                s.to_string(),
            ));
        }
        let mut data = [8u8; Algorithm::SHA256.size()];
        HEXLOWER_PERMISSIVE
            .decode_mut(s.as_bytes(), &mut data)
            .map_err(|err| ParseHashError::BadEncoding(s.into(), "hex".into(), err.error))?;
        Ok(NarHash(data))
    }
}

impl fmt::Debug for NarHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("NarHash")
            .field(&format_args!("{self}"))
            .finish()
    }
}

impl fmt::Display for NarHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:x}")
    }
}

impl fmt::LowerHex for NarHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for val in self.as_ref() {
            write!(f, "{val:02x}")?;
        }
        Ok(())
    }
}

impl fmt::UpperHex for NarHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for val in self.as_ref() {
            write!(f, "{val:02X}")?;
        }
        Ok(())
    }
}

impl From<NarHash> for Hash {
    fn from(value: NarHash) -> Self {
        Hash::new(Algorithm::SHA256, value.as_ref())
    }
}

impl TryFrom<Hash> for NarHash {
    type Error = UnknownAlgorithm;

    fn try_from(value: Hash) -> Result<Self, Self::Error> {
        if value.algorithm() != Algorithm::SHA256 {
            return Err(UnknownAlgorithm(value.algorithm().to_string()));
        }
        Ok(NarHash::new(value.as_ref()))
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Hash)]
#[cfg_attr(any(test, feature = "test"), derive(Arbitrary))]
pub struct Sha256([u8; Algorithm::SHA256.size()]);
impl Sha256 {
    pub fn new(digest: &[u8]) -> Self {
        let mut data = [0u8; Algorithm::SHA256.size()];
        data.copy_from_slice(digest);
        Self(data)
    }

    pub fn from_slice(digest: &[u8]) -> Result<Self, ParseHashError> {
        if digest.len() != Algorithm::SHA256.size() {
            return Err(ParseHashError::WrongHashLength2(
                Algorithm::SHA256,
                digest.len(),
            ));
        }
        Ok(Self::new(digest))
    }

    /// Returns the digest of `data` using the sha256
    ///
    /// ```
    /// # use nixrs::hash::Sha256;
    /// let hash = Sha256::digest("abc");
    ///
    /// assert_eq!("1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5s", hash.to_string());
    /// ```
    pub fn digest<B: AsRef<[u8]>>(data: B) -> Self {
        Algorithm::SHA256.digest(data).try_into().unwrap()
    }
}

impl fmt::Debug for Sha256 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Sha256")
            .field(&format_args!("{self}"))
            .finish()
    }
}

impl fmt::Display for Sha256 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut buf = [0u8; Algorithm::SHA256.base32_len()];
        base32::encode_mut(self.0.as_ref(), &mut buf);

        // SAFETY: Nix Base32 is a subset of ASCII, which guarantees valid UTF-8.
        let s = unsafe { std::str::from_utf8_unchecked(&buf) };
        f.write_str(s)
    }
}

impl fmt::LowerHex for Sha256 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for val in self.as_ref() {
            write!(f, "{val:02x}")?;
        }
        Ok(())
    }
}

impl FromStr for Sha256 {
    type Err = ParseHashError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let hash = Hash::parse_non_sri_unprefixed(s, Algorithm::SHA256)?;
        Ok(hash.try_into()?)
    }
}

impl AsRef<[u8]> for Sha256 {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<Sha256> for Hash {
    fn from(value: Sha256) -> Self {
        Hash::new(Algorithm::SHA256, value.as_ref())
    }
}

impl TryFrom<Hash> for Sha256 {
    type Error = UnknownAlgorithm;

    fn try_from(value: Hash) -> Result<Self, Self::Error> {
        if value.algorithm() != Algorithm::SHA256 {
            return Err(UnknownAlgorithm(value.algorithm().to_string()));
        }
        Ok(Self::new(value.as_ref()))
    }
}

#[derive(Clone)]
enum InnerContext {
    #[cfg(feature = "md5")]
    MD5(md5::Context),
    Ring(digest::Context),
}

/// A context for multi-step (Init-Update-Finish) digest calculation.
///
/// # Examples
///
/// ```
/// use nixrs::hash;
///
/// let one_shot = hash::Algorithm::SHA256.digest("hello, world");
///
/// let mut ctx = hash::Context::new(hash::Algorithm::SHA256);
/// ctx.update("hello");
/// ctx.update(", ");
/// ctx.update("world");
/// let multi_path = ctx.finish();
///
/// assert_eq!(one_shot, multi_path);
/// ```
#[derive(Clone)]
pub struct Context(Algorithm, InnerContext);

impl Context {
    /// Constructs a new context with `algorithm`.
    pub fn new(algorithm: Algorithm) -> Self {
        match algorithm {
            #[cfg(feature = "md5")]
            Algorithm::MD5 => Context(algorithm, InnerContext::MD5(md5::Context::new())),
            _ => Context(
                algorithm,
                InnerContext::Ring(digest::Context::new(algorithm.digest_algorithm())),
            ),
        }
    }

    /// Update the digest with all the data in `data`.
    /// `update` may be called zero or more times before `finish` is called.
    pub fn update<D: AsRef<[u8]>>(&mut self, data: D) {
        let data = data.as_ref();
        match &mut self.1 {
            #[cfg(feature = "md5")]
            InnerContext::MD5(ctx) => ctx.consume(data),
            InnerContext::Ring(ctx) => ctx.update(data),
        }
    }

    /// Finalizes the digest calculation and returns the [`Hash`] value.
    /// This consumes the context to prevent misuse.
    ///
    /// [`Hash`]: struct@Hash
    pub fn finish(self) -> Hash {
        match self.1 {
            #[cfg(feature = "md5")]
            InnerContext::MD5(ctx) => Hash::new(self.0, ctx.compute().as_ref()),
            InnerContext::Ring(ctx) => ctx.finish().try_into().unwrap(),
        }
    }

    /// The algorithm that this context is using.
    pub fn algorithm(&self) -> Algorithm {
        self.0
    }
}

impl fmt::Debug for Context {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Context").field(&self.0).finish()
    }
}

/// A hash sink that implements [`AsyncWrite`].
///
/// # Examples
///
/// ```
/// use tokio::io;
/// use nixrs::hash;
///
/// # #[tokio::main]
/// # async fn main() -> std::io::Result<()> {
/// let mut reader: &[u8] = b"hello, world";
/// let mut sink = hash::HashSink::new(hash::Algorithm::SHA256);
///
/// io::copy(&mut reader, &mut sink).await?;
/// let (size, hash) = sink.finish();
///
/// let one_shot = hash::Algorithm::SHA256.digest("hello, world");
/// assert_eq!(one_shot, hash);
/// assert_eq!(12, size);
/// # Ok(())
/// # }
/// ```
///
/// [`AsyncWrite`]: tokio::io::AsyncWrite
#[derive(Debug)]
pub struct HashSink(Option<(u64, Context)>);
impl HashSink {
    /// Constructs a new sink with `algorithm`.
    pub fn new(algorithm: Algorithm) -> HashSink {
        HashSink(Some((0, Context::new(algorithm))))
    }

    /// Finalizes this sink and returns the hash and number of bytes written to the sink.
    pub fn finish(self) -> (u64, Hash) {
        let (read, ctx) = self.0.unwrap();
        (read, ctx.finish())
    }
}

impl tokio::io::AsyncWrite for HashSink {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        match self.0.as_mut() {
            None => panic!("write after completion"),
            Some((read, ctx)) => {
                *read += buf.len() as u64;
                ctx.update(buf)
            }
        }
        std::task::Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
}

#[cfg(any(test, feature = "test"))]
pub mod proptests {
    use super::*;
    use ::proptest::prelude::*;

    impl Arbitrary for Algorithm {
        type Parameters = ();
        type Strategy = BoxedStrategy<Algorithm>;
        fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
            prop_oneof![
                1 => Just(Algorithm::MD5),
                2 => Just(Algorithm::SHA1),
                5 => Just(Algorithm::SHA256),
                2 => Just(Algorithm::SHA512)
            ]
            .boxed()
        }
    }

    impl Arbitrary for Hash {
        type Parameters = Algorithm;
        type Strategy = BoxedStrategy<Hash>;

        fn arbitrary_with(algorithm: Self::Parameters) -> Self::Strategy {
            any_hash(algorithm).boxed()
        }
    }

    prop_compose! {
        fn any_hash(algorithm: Algorithm)
                   (data in any::<Vec<u8>>()) -> Hash
        {
            algorithm.digest(data)
        }
    }
}

#[cfg(test)]
mod unittests {
    use data_encoding::DecodeError;
    use hex_literal::hex;
    use once_cell::sync::Lazy;
    use pretty_assertions::assert_eq;
    use rstest::rstest;

    use super::*;

    /// value taken from: https://tools.ietf.org/html/rfc1321
    static MD5_EMPTY: Lazy<Hash> =
        Lazy::new(|| Hash::new(Algorithm::MD5, &hex!("d41d8cd98f00b204e9800998ecf8427e")));
    /// value taken from: https://tools.ietf.org/html/rfc1321
    static MD5_ABC: Lazy<Hash> =
        Lazy::new(|| Hash::new(Algorithm::MD5, &hex!("900150983cd24fb0d6963f7d28e17f72")));

    /// value taken from: https://tools.ietf.org/html/rfc3174
    static SHA1_ABC: Lazy<Hash> = Lazy::new(|| {
        Hash::new(
            Algorithm::SHA1,
            &hex!("a9993e364706816aba3e25717850c26c9cd0d89d"),
        )
    });
    /// value taken from: https://tools.ietf.org/html/rfc3174
    static SHA1_LONG: Lazy<Hash> = Lazy::new(|| {
        Hash::new(
            Algorithm::SHA1,
            &hex!("84983e441c3bd26ebaae4aa1f95129e5e54670f1"),
        )
    });

    /// value taken from: https://tools.ietf.org/html/rfc4634
    static SHA256_ABC: Lazy<Hash> = Lazy::new(|| {
        Hash::new(
            Algorithm::SHA256,
            &hex!("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"),
        )
    });
    /// value taken from: https://tools.ietf.org/html/rfc4634
    static SHA256_LONG: Lazy<Hash> = Lazy::new(|| {
        Hash::new(
            Algorithm::SHA256,
            &hex!("248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"),
        )
    });

    /// value taken from: https://tools.ietf.org/html/rfc4634
    static SHA512_ABC: Lazy<Hash> = Lazy::new(|| {
        Hash::new(
            Algorithm::SHA512,
            &hex!(
                "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f"
            ),
        )
    });
    /// value taken from: https://tools.ietf.org/html/rfc4634
    static SHA512_LONG: Lazy<Hash> = Lazy::new(|| {
        Hash::new(
            Algorithm::SHA512,
            &hex!(
                "8e959b75dae313da8cf4f72814fc143f8f7779c6eb9f7fa17299aeadb6889018501d289e4900f7e4331b99dec4b5433ac7d329eeb6dd26545e96e55b874be909"
            ),
        )
    });

    #[rstest]
    #[case::md5(Algorithm::MD5, 16, 32, 26, 24, 18)]
    #[case::sha1(Algorithm::SHA1, 20, 40, 32, 28, 21)]
    #[case::sha256(Algorithm::SHA256, 32, 64, 52, 44, 33)]
    #[case::sha512(Algorithm::SHA512, 64, 128, 103, 88, 66)]
    fn algorithm_size(
        #[case] algorithm: Algorithm,
        #[case] size: usize,
        #[case] base16_len: usize,
        #[case] base32_len: usize,
        #[case] base64_len: usize,
        #[case] base64_decoded: usize,
    ) {
        assert_eq!(algorithm.size(), size, "mismatched size");
        assert_eq!(algorithm.base16_len(), base16_len, "mismatched base16_len");
        assert_eq!(algorithm.base32_len(), base32_len, "mismatched base32_len");
        assert_eq!(algorithm.base64_len(), base64_len, "mismatched base64_len");
        assert_eq!(
            algorithm.base64_decoded(),
            base64_decoded,
            "mismatched base64_decoded"
        );
    }

    #[rstest]
    #[cfg_attr(feature = "md5", case::md5_empty(&MD5_EMPTY, ""))]
    #[cfg_attr(feature = "md5", case::abc(&MD5_ABC, "abc"))]
    #[case::sha1_abc(&SHA1_ABC, "abc")]
    #[case::sha1_long(&SHA1_LONG, "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq")]
    #[case::sha256_abc(&SHA256_ABC, "abc")]
    #[case::sha256_long(&SHA256_LONG, "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq")]
    #[case::sha512_abc(&SHA512_ABC, "abc")]
    #[case::sha512_long(&SHA512_LONG, "abcdefghbcdefghicdefghijdefghijkefghijklfghijklmghijklmnhijklmnoijklmnopjklmnopqklmnopqrlmnopqrsmnopqrstnopqrstu")]
    fn test_digest(#[case] expected: &Hash, #[case] input: &str) {
        let actual = expected.algorithm().digest(input);
        assert_eq!(actual, *expected);
    }

    #[rstest]
    #[case::md5_empty(&MD5_EMPTY, "d41d8cd98f00b204e9800998ecf8427e")]
    #[case::md5_abc(&MD5_ABC, "900150983cd24fb0d6963f7d28e17f72")]
    #[case::sha1_abc(&SHA1_ABC, "a9993e364706816aba3e25717850c26c9cd0d89d")]
    #[case::sha1_long(&SHA1_LONG, "84983e441c3bd26ebaae4aa1f95129e5e54670f1")]
    #[case::sha256_abc(&SHA256_ABC, "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")]
    #[case::sha256_long(&SHA256_LONG, "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1")]
    #[case::sha512_abc(&SHA512_ABC, "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f")]
    #[case::sha512_long(&SHA512_LONG, "8e959b75dae313da8cf4f72814fc143f8f7779c6eb9f7fa17299aeadb6889018501d289e4900f7e4331b99dec4b5433ac7d329eeb6dd26545e96e55b874be909")]
    fn base16_encode(#[case] hash: &Hash, #[case] base16: &str) {
        let algo = hash.algorithm();
        let base16_h = base16.to_uppercase();
        let base16_p = format!("{algo}:{base16}");
        let base16_hp = format!("{algo}:{base16_h}");

        assert_eq!(format!("{:x}", hash), base16_p);
        assert_eq!(format!("{:#x}", hash), base16);
        assert_eq!(format!("{:X}", hash), base16_hp);
        assert_eq!(format!("{:#X}", hash), base16_h);

        assert_eq!(format!("{}", hash.to_base16()), base16_p);
        assert_eq!(format!("{:#}", hash.to_base16()), base16);
        assert_eq!(hash.encode_base16(), base16);
    }

    #[rstest]
    #[case::base16_md5_empty(&MD5_EMPTY, "d41d8cd98f00b204e9800998ecf8427e")]
    #[case::base16_md5_abc(&MD5_ABC, "900150983cd24fb0d6963f7d28e17f72")]
    #[case::base16_sha1_abc(&SHA1_ABC, "a9993e364706816aba3e25717850c26c9cd0d89d")]
    #[case::base16_sha1_long(&SHA1_LONG, "84983e441c3bd26ebaae4aa1f95129e5e54670f1")]
    #[case::base16_sha256_abc(&SHA256_ABC, "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")]
    #[case::base16_sha256_long(&SHA256_LONG, "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1")]
    #[case::base16_sha512_abc(&SHA512_ABC, "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f")]
    #[case::base16_sha512_long(&SHA512_LONG, "8e959b75dae313da8cf4f72814fc143f8f7779c6eb9f7fa17299aeadb6889018501d289e4900f7e4331b99dec4b5433ac7d329eeb6dd26545e96e55b874be909")]
    #[case::nixbase32_md5_empty(&MD5_EMPTY, "3y8bwfr609h3lh9ch0izcqq7fl")]
    #[case::nixbase32_md5_abc(&MD5_ABC, "3jgzhjhz9zjvbb0kyj7jc500ch")]
    #[case::nixbase32_sha1_abc(&SHA1_ABC, "kpcd173cq987hw957sx6m0868wv3x6d9")]
    #[case::nixbase32_sha1_long(&SHA1_LONG, "y5q4drg5558zk8aamsx6xliv3i23x644")]
    #[case::nixbase32_sha256_abc(&SHA256_ABC, "1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5s")]
    #[case::nixbase32_sha256_long(&SHA256_LONG, "1h86vccx9vgcyrkj3zv4b7j3r8rrc0z0r4r6q3jvhf06s9hnm394")]
    #[case::nixbase32_sha512_abc(&SHA512_ABC, "2gs8k559z4rlahfx0y688s49m2vvszylcikrfinm30ly9rak69236nkam5ydvly1ai7xac99vxfc4ii84hawjbk876blyk1jfhkbbyx")]
    #[case::nixbase32_sha512_long(&SHA512_LONG, "04yjjw7bgjrcpjl4vfvdvi9sg3klhxmqkg9j6rkwkvh0jcy50fm064hi2vavblrfahpz7zbqrwpg3rz2ky18a7pyj6dl4z3v9srp5cf")]
    fn parse(#[case] hash: &Hash, #[case] base_x: &str) {
        let algo = hash.algorithm();

        let base_xp = format!("{algo}:{base_x}");
        assert_eq!(*hash, Hash::parse_any_prefixed(&base_xp).unwrap());
        assert_eq!(*hash, base_xp.parse().unwrap());
        assert_eq!(*hash, Hash::parse_any(&base_xp, None).unwrap());
        assert_eq!(*hash, Hash::parse_any(&base_xp, Some(algo)).unwrap());
        assert_eq!(*hash, Hash::parse_any(base_x, Some(algo)).unwrap());
        assert_eq!(*hash, Hash::parse_non_sri_unprefixed(base_x, algo).unwrap());

        /*
        let base_xh = base_x.to_uppercase();
        let base_xhp = format!("{}:{}", algo, base_xh);
        assert_eq!(*hash, Hash::parse_any_prefixed(&base_xhp).unwrap());
        assert_eq!(*hash, base_xhp.parse().unwrap());
        assert_eq!(*hash, Hash::parse_any(&base_xhp, None).unwrap());
        assert_eq!(*hash, Hash::parse_any(&base_xhp, Some(algo)).unwrap());
        assert_eq!(*hash, Hash::parse_any(&base_xh, Some(algo)).unwrap());
        assert_eq!(
            *hash,
            Hash::parse_non_sri_unprefixed(&base_xh, algo).unwrap()
        );
         */
    }

    #[rstest]
    #[case::md5_empty(&MD5_EMPTY, "3y8bwfr609h3lh9ch0izcqq7fl")]
    #[case::md5_abc(&MD5_ABC, "3jgzhjhz9zjvbb0kyj7jc500ch")]
    #[case::sha1_abc(&SHA1_ABC, "kpcd173cq987hw957sx6m0868wv3x6d9")]
    #[case::sha1_long(&SHA1_LONG, "y5q4drg5558zk8aamsx6xliv3i23x644")]
    #[case::sha256_abc(&SHA256_ABC, "1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5s")]
    #[case::sha256_long(&SHA256_LONG, "1h86vccx9vgcyrkj3zv4b7j3r8rrc0z0r4r6q3jvhf06s9hnm394")]
    #[case::sha512_abc(&SHA512_ABC, "2gs8k559z4rlahfx0y688s49m2vvszylcikrfinm30ly9rak69236nkam5ydvly1ai7xac99vxfc4ii84hawjbk876blyk1jfhkbbyx")]
    #[case::sha512_long(&SHA512_LONG, "04yjjw7bgjrcpjl4vfvdvi9sg3klhxmqkg9j6rkwkvh0jcy50fm064hi2vavblrfahpz7zbqrwpg3rz2ky18a7pyj6dl4z3v9srp5cf")]
    fn nixbase32_encode(#[case] hash: &Hash, #[case] base32: &str) {
        let base32_p = format!("{}:{}", hash.algorithm(), base32);

        assert_eq!(format!("{}", hash), base32_p);
        assert_eq!(format!("{:#}", hash), base32);
        assert_eq!(format!("{}", hash.to_base32()), base32_p);
        assert_eq!(format!("{:#}", hash.to_base32()), base32);
        assert_eq!(hash.encode_base32(), base32);
    }

    #[rstest]
    #[case::md5_empty(&MD5_EMPTY, "1B2M2Y8AsgTpgAmY7PhCfg==")]
    #[case::md5_abc(&MD5_ABC, "kAFQmDzST7DWlj99KOF/cg==")]
    #[case::sha1_abc(&SHA1_ABC, "qZk+NkcGgWq6PiVxeFDCbJzQ2J0=")]
    #[case::sha1_long(&SHA1_LONG, "hJg+RBw70m66rkqh+VEp5eVGcPE=")]
    #[case::sha256_abc(&SHA256_ABC, "ungWv48Bz+pBQUDeXa4iI7ADYaOWF3qctBD/YfIAFa0=")]
    #[case::sha256_long(&SHA256_LONG, "JI1qYdIGOLjlwCaTDD5gOaM85Flk/yFn9uzt1BnbBsE=")]
    #[case::sha512_abc(&SHA512_ABC, "3a81oZNherrMQXNJriBBMRLm+k6JqX6iCp7u5ktV05ohkpkqJ0/BqDa6PCOj/uu9RU1EI2Q86A4qmslPpUyknw==")]
    #[case::sha512_long(&SHA512_LONG, "jpWbddrjE9qM9PcoFPwUP493ecbrn3+hcpmurbaIkBhQHSieSQD35DMbmd7EtUM6x9Mp7rbdJlReluVbh0vpCQ==")]
    fn base64_encode(#[case] hash: &Hash, #[case] base64: &str) {
        let algo = hash.algorithm();
        let base64_p = format!("{algo}:{base64}");

        assert_eq!(format!("{}", hash.to_base64()), base64_p);
        assert_eq!(format!("{:#}", hash.to_base64()), base64);
        assert_eq!(hash.encode_base64(), base64);

        let sri = format!("{algo}-{base64}");
        assert_eq!(format!("{}", hash.to_sri()), sri);
    }

    #[rstest]
    #[case::md5_empty(&MD5_EMPTY, "1B2M2Y8AsgTpgAmY7PhCfg==")]
    #[case::md5_abc(&MD5_ABC, "kAFQmDzST7DWlj99KOF/cg==")]
    #[case::sha1_abc(&SHA1_ABC, "qZk+NkcGgWq6PiVxeFDCbJzQ2J0=")]
    #[case::sha1_long(&SHA1_LONG, "hJg+RBw70m66rkqh+VEp5eVGcPE=")]
    #[case::sha256_abc(&SHA256_ABC, "ungWv48Bz+pBQUDeXa4iI7ADYaOWF3qctBD/YfIAFa0=")]
    #[case::sha256_long(&SHA256_LONG, "JI1qYdIGOLjlwCaTDD5gOaM85Flk/yFn9uzt1BnbBsE=")]
    #[case::sha512_abc(&SHA512_ABC, "3a81oZNherrMQXNJriBBMRLm+k6JqX6iCp7u5ktV05ohkpkqJ0/BqDa6PCOj/uu9RU1EI2Q86A4qmslPpUyknw==")]
    #[case::sha512_long(&SHA512_LONG, "jpWbddrjE9qM9PcoFPwUP493ecbrn3+hcpmurbaIkBhQHSieSQD35DMbmd7EtUM6x9Mp7rbdJlReluVbh0vpCQ==")]
    fn base64_parse(#[case] hash: &Hash, #[case] base64: &str) {
        let algo = hash.algorithm();
        let base64_p = format!("{algo}:{base64}");

        assert_eq!(*hash, base64_p.parse().unwrap());
        assert_eq!(*hash, Hash::parse_any(&base64_p, None).unwrap());
        assert_eq!(*hash, Hash::parse_any(&base64_p, Some(algo)).unwrap());
        assert_eq!(*hash, Hash::parse_any(base64, Some(algo)).unwrap());
        assert_eq!(*hash, Hash::parse_non_sri_unprefixed(base64, algo).unwrap());

        let sri = format!("{algo}-{base64}");
        assert_eq!(*hash, sri.parse().unwrap());
        assert_eq!(*hash, Hash::parse_any(&sri, None).unwrap());
        assert_eq!(*hash, Hash::parse_any(&sri, Some(algo)).unwrap());
        assert_eq!(*hash, Hash::parse_sri(&sri).unwrap());
    }

    #[test]
    fn unknown_algorithm() {
        assert_eq!(
            Err(UnknownAlgorithm("test".into())),
            "test".parse::<Algorithm>()
        );
    }

    #[test]
    fn unknown_digest() {
        assert_eq!(
            Err(UnknownAlgorithm("SHA384".into())),
            Algorithm::try_from(&digest::SHA384)
        );
    }

    #[test]
    fn hash_unknown_algorithm() {
        assert_eq!(
            Err(ParseHashError::Algorithm(UnknownAlgorithm("test".into()))),
            Hash::parse_any_prefixed("test:12345")
        );
    }

    #[test]
    fn hash_not_sri() {
        assert_eq!(
            Err(ParseHashError::NotSRI("sha256:1234".into())),
            Hash::parse_sri("sha256:1234")
        );
    }

    #[test]
    fn parse_any_prefied_missing() {
        assert_eq!(
            Err(ParseHashError::MissingTypePrefix("12345".into())),
            Hash::parse_any_prefixed("12345")
        );
    }

    #[test]
    fn parse_non_sri_prefixed_missing() {
        assert_eq!(
            Err(ParseHashError::MissingTypePrefix("12345".into())),
            Hash::parse_non_sri_prefixed("12345")
        );
    }

    #[test]
    fn parse_any_type_mismatch() {
        assert_eq!(
            Err(ParseHashError::TypeMismatch {
                expected: Algorithm::SHA256,
                actual: Algorithm::SHA1,
                hash: "sha1:12345".into(),
            }),
            Hash::parse_any("sha1:12345", Some(Algorithm::SHA256))
        );
    }

    #[test]
    fn parse_any_missing_type() {
        assert_eq!(
            Err(ParseHashError::MissingType("12345".into())),
            Hash::parse_any("12345", None)
        );
    }

    #[rstest]
    #[case::bad_hex(ParseHashError::BadEncoding(
        "k9993e364706816aba3e25717850c26c9cd0d89d".into(),
        "hex".into(),
        DecodeError { position: 0, kind: data_encoding::DecodeKind::Symbol }
    ), "sha1:k9993e364706816aba3e25717850c26c9cd0d89d")]
    #[case::bad_nixbase32(ParseHashError::BadEncoding(
        "!pcd173cq987hw957sx6m0868wv3x6d9".into(),
        "nixbase32".into(),
        DecodeError { position: 0, kind: data_encoding::DecodeKind::Symbol }
    ), "sha1:!pcd173cq987hw957sx6m0868wv3x6d9")]
    #[case::bad_base64_symbol(ParseHashError::BadEncoding(
        "!Zk+NkcGgWq6PiVxeFDCbJzQ2J0=".into(),
        "base64".into(),
        DecodeError { position: 0, kind: data_encoding::DecodeKind::Symbol }
    ), "sha1:!Zk+NkcGgWq6PiVxeFDCbJzQ2J0=")]
    #[case::bad_base64_length(ParseHashError::BadEncoding(
        "qZk+NkcGgWq6PiVxeFDCbJzQ2J0a".into(),
        "base64".into(),
        DecodeError { position: 0, kind: data_encoding::DecodeKind::Length }
    ), "sha1:qZk+NkcGgWq6PiVxeFDCbJzQ2J0a")]
    #[case::bad_sri(ParseHashError::BadSRIHash(
        "qZk+NkcGgWq6PiVxeFDCbJzQ2J0a".into()
    ), "sha1-qZk+NkcGgWq6PiVxeFDCbJzQ2J0a")]
    #[case::wrong_length(ParseHashError::WrongHashLength(
        Algorithm::SHA1,
        "12345".into()
    ), "sha1:12345")]
    #[test]
    fn parse_errors(#[case] error: ParseHashError, #[case] input: &str) {
        assert_eq!(Err(error), input.parse::<Hash>());
    }
}
