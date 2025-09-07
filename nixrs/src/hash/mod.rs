use std::convert::TryFrom;
use std::convert::TryInto;
use std::fmt as sfmt;
use std::str::FromStr;

use derive_more::Display;
#[cfg(feature = "nixrs-derive")]
use nixrs_derive::{NixDeserialize, NixSerialize};
#[cfg(any(test, feature = "test"))]
use proptest_derive::Arbitrary;
use ring::digest;
use thiserror::Error;

use crate::wire::base64_len;

use super::base32;

pub mod fmt;

const MD5_SIZE: usize = 128 / 8;
const SHA1_SIZE: usize = 160 / 8;
const SHA256_SIZE: usize = 256 / 8;
const SHA512_SIZE: usize = 512 / 8;
const LARGEST_ALGORITHM: Algorithm = Algorithm::SHA512;

/// A digest algorithm.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Hash, Display, Default)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
#[cfg_attr(feature = "nixrs-derive", nix(from_str, display))]
pub enum Algorithm {
    #[display("md5")]
    MD5,
    #[display("sha1")]
    SHA1,
    #[default]
    #[display("sha256")]
    SHA256,
    #[display("sha512")]
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
    /// assert_eq!("sha256:1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5s", hash.as_base32().to_string());
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
#[error("unsupported digest algorithm '{0}'")]
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

#[derive(Error, Debug, PartialEq, Eq, Clone, Copy)]
#[error("hash has wrong length {length} != {} for hash type '{algorithm}'", algorithm.size())]
pub struct InvalidHashError {
    algorithm: Algorithm,
    length: usize,
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Hash)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
#[cfg_attr(
    feature = "nixrs-derive",
    nix(from = "fmt::Any<Hash>", into = "fmt::Base32<Hash>")
)]
pub struct Hash {
    algorithm: Algorithm,
    data: [u8; LARGEST_ALGORITHM.size()],
}

impl Hash {
    pub const fn new(algorithm: Algorithm, hash: &[u8]) -> Hash {
        let mut data = [0u8; LARGEST_ALGORITHM.size()];
        let (hash_data, _postfix) = data.split_at_mut(algorithm.size());
        hash_data.copy_from_slice(hash);
        Hash { algorithm, data }
    }

    pub fn from_slice(algorithm: Algorithm, hash: &[u8]) -> Result<Hash, InvalidHashError> {
        if hash.len() != algorithm.size() {
            return Err(InvalidHashError {
                algorithm,
                length: hash.len(),
            });
        }
        Ok(Hash::new(algorithm, hash))
    }

    #[inline]
    pub fn algorithm(&self) -> Algorithm {
        self.algorithm
    }

    #[inline]
    pub fn digest_bytes(&self) -> &[u8] {
        &self.data[0..(self.algorithm.size())]
    }
}

impl std::ops::Deref for Hash {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        self.digest_bytes()
    }
}

impl AsRef<[u8]> for Hash {
    fn as_ref(&self) -> &[u8] {
        self.digest_bytes()
    }
}

impl TryFrom<digest::Digest> for Hash {
    type Error = UnknownAlgorithm;
    fn try_from(digest: digest::Digest) -> Result<Self, Self::Error> {
        Ok(Hash::new(digest.algorithm().try_into()?, digest.as_ref()))
    }
}

#[derive(Clone, Copy, Eq, Hash, PartialEq, PartialOrd, Ord)]
#[cfg_attr(any(test, feature = "test"), derive(Arbitrary))]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
#[cfg_attr(
    feature = "nixrs-derive",
    nix(
        from = "fmt::Bare<fmt::Any<NarHash>>",
        into = "fmt::Bare<fmt::Base16<NarHash>>"
    )
)]
#[repr(transparent)]
pub struct NarHash(Sha256);

impl NarHash {
    pub const fn new(digest: &[u8]) -> NarHash {
        NarHash(Sha256::new(digest))
    }

    pub fn from_slice(digest: &[u8]) -> Result<NarHash, InvalidHashError> {
        Sha256::from_slice(digest).map(NarHash)
    }

    pub fn digest<D: AsRef<[u8]>>(data: D) -> Self {
        Self::new(&Algorithm::SHA256.digest(data))
    }

    #[inline]
    pub fn digest_bytes(&self) -> &[u8] {
        self.0.digest_bytes()
    }
}

impl From<NarHash> for Hash {
    fn from(value: NarHash) -> Self {
        value.0.into()
    }
}

impl TryFrom<Hash> for NarHash {
    type Error = fmt::ParseHashErrorKind;

    fn try_from(value: Hash) -> Result<Self, Self::Error> {
        Ok(NarHash(value.try_into()?))
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Hash)]
#[cfg_attr(any(test, feature = "test"), derive(Arbitrary))]
pub struct Sha256([u8; Algorithm::SHA256.size()]);
impl Sha256 {
    pub const fn new(digest: &[u8]) -> Self {
        let mut data = [0u8; Algorithm::SHA256.size()];
        data.copy_from_slice(digest);
        Self(data)
    }

    pub const fn from_slice(digest: &[u8]) -> Result<Self, InvalidHashError> {
        if digest.len() != Algorithm::SHA256.size() {
            return Err(InvalidHashError {
                algorithm: Algorithm::SHA256,
                length: digest.len(),
            });
        }
        Ok(Self::new(digest))
    }

    /// Returns the digest of `data` using the sha256
    ///
    /// ```
    /// # use nixrs::hash::Sha256;
    /// let hash = Sha256::digest("abc");
    ///
    /// assert_eq!("1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5s", hash.as_base32().as_bare().to_string());
    /// ```
    pub fn digest<B: AsRef<[u8]>>(data: B) -> Self {
        Algorithm::SHA256.digest(data).try_into().unwrap()
    }

    #[inline]
    pub fn digest_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl AsRef<[u8]> for Sha256 {
    fn as_ref(&self) -> &[u8] {
        self.digest_bytes()
    }
}

impl From<Sha256> for Hash {
    fn from(value: Sha256) -> Self {
        Hash::new(Algorithm::SHA256, value.as_ref())
    }
}

impl TryFrom<Hash> for Sha256 {
    type Error = fmt::ParseHashErrorKind;

    fn try_from(value: Hash) -> Result<Self, Self::Error> {
        if value.algorithm() != Algorithm::SHA256 {
            return Err(fmt::ParseHashErrorKind::TypeMismatch {
                expected: Algorithm::SHA256,
                actual: value.algorithm(),
            });
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

impl sfmt::Debug for Context {
    fn fmt(&self, f: &mut sfmt::Formatter<'_>) -> sfmt::Result {
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
mod proptests {
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
    use hex_literal::hex;
    use pretty_assertions::assert_eq;
    use rstest::rstest;

    use super::*;

    /// value taken from: https://tools.ietf.org/html/rfc1321
    #[cfg(feature = "md5")]
    const MD5_EMPTY: Hash = Hash::new(Algorithm::MD5, &hex!("d41d8cd98f00b204e9800998ecf8427e"));
    /// value taken from: https://tools.ietf.org/html/rfc1321
    #[cfg(feature = "md5")]
    const MD5_ABC: Hash = Hash::new(Algorithm::MD5, &hex!("900150983cd24fb0d6963f7d28e17f72"));

    /// value taken from: https://tools.ietf.org/html/rfc3174
    const SHA1_ABC: Hash = Hash::new(
        Algorithm::SHA1,
        &hex!("a9993e364706816aba3e25717850c26c9cd0d89d"),
    );
    /// value taken from: https://tools.ietf.org/html/rfc3174
    const SHA1_LONG: Hash = Hash::new(
        Algorithm::SHA1,
        &hex!("84983e441c3bd26ebaae4aa1f95129e5e54670f1"),
    );

    /// value taken from: https://tools.ietf.org/html/rfc4634
    const SHA256_ABC: Hash = Hash::new(
        Algorithm::SHA256,
        &hex!("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"),
    );
    /// value taken from: https://tools.ietf.org/html/rfc4634
    const SHA256_LONG: Hash = Hash::new(
        Algorithm::SHA256,
        &hex!("248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"),
    );

    /// value taken from: https://tools.ietf.org/html/rfc4634
    const SHA512_ABC: Hash = Hash::new(
        Algorithm::SHA512,
        &hex!(
            "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f"
        ),
    );
    /// value taken from: https://tools.ietf.org/html/rfc4634
    const SHA512_LONG: Hash = Hash::new(
        Algorithm::SHA512,
        &hex!(
            "8e959b75dae313da8cf4f72814fc143f8f7779c6eb9f7fa17299aeadb6889018501d289e4900f7e4331b99dec4b5433ac7d329eeb6dd26545e96e55b874be909"
        ),
    );

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
    #[case::md5("md5", Algorithm::MD5)]
    #[case::sha1("sha1", Algorithm::SHA1)]
    #[case::sha256("sha256", Algorithm::SHA256)]
    #[case::sha512("sha512", Algorithm::SHA512)]
    #[case::md5_upper("MD5", Algorithm::MD5)]
    #[case::sha1_upper("SHA1", Algorithm::SHA1)]
    #[case::sha256_upper("SHA256", Algorithm::SHA256)]
    #[case::sha512_upper("SHA512", Algorithm::SHA512)]
    #[case::md5_mixed("mD5", Algorithm::MD5)]
    #[case::sha1_mixed("ShA1", Algorithm::SHA1)]
    #[case::sha256_mixed("ShA256", Algorithm::SHA256)]
    #[case::sha512_mixed("ShA512", Algorithm::SHA512)]
    fn algorithm_from_str(#[case] input: &str, #[case] expected: Algorithm) {
        let actual = input.parse().unwrap();
        assert_eq!(expected, actual);
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
}
