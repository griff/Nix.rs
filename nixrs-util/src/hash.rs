use std::convert::TryFrom;
use std::convert::TryInto;
use std::fmt;
use std::str::FromStr;

use derive_more::Display;
use hex::FromHexError;
use ring::digest;
use thiserror::Error;

use super::base32;

const MD5_SIZE: usize = 128 / 8;
const SHA1_SIZE: usize = 160 / 8;
const SHA256_SIZE: usize = 256 / 8;
const SHA512_SIZE: usize = 512 / 8;
const MAX_SIZE: usize = SHA512_SIZE;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Hash, Display)]
pub enum Algorithm {
    #[display(fmt = "md5")]
    MD5,
    #[display(fmt = "sha1")]
    SHA1,
    #[display(fmt = "sha256")]
    SHA256,
    #[display(fmt = "sha512")]
    SHA512,
}

/// The default algorithm is currently SHA-256
impl Default for Algorithm {
    fn default() -> Self {
        Algorithm::SHA256
    }
}

impl Algorithm {
    #[inline]
    pub fn size(&self) -> usize {
        match &self {
            Algorithm::MD5 => MD5_SIZE,
            Algorithm::SHA1 => SHA1_SIZE,
            Algorithm::SHA256 => SHA256_SIZE,
            Algorithm::SHA512 => SHA512_SIZE,
        }
    }

    /// Returns the length of a base-16 representation of this hash.
    #[inline]
    pub fn base16_len(&self) -> usize {
        return self.size() * 2;
    }

    /// Returns the length of a base-32 representation of this hash.
    #[inline]
    pub fn base32_len(&self) -> usize {
        return (self.size() * 8 - 1) / 5 + 1;
    }

    /// Returns the length of a base-64 representation of this hash.
    #[inline]
    pub fn base64_len(&self) -> usize {
        return ((4 * self.size() / 3) + 3) & !3;
    }

    fn digest_algorithm(&self) -> &'static digest::Algorithm {
        match self {
            Algorithm::SHA1 => &digest::SHA1_FOR_LEGACY_USE_ONLY,
            Algorithm::SHA256 => &digest::SHA256,
            Algorithm::SHA512 => &digest::SHA512,
            a => panic!("Unsupported digest algorithm {:?}", a),
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
            Err(UnknownAlgorithm(format!("{:?}", value)))
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
    #[error("invalid base-16 hash '{0}'")]
    BadBase16Hash(String, #[source] FromHexError),
    #[error("invalid base-32 hash '{0}'")]
    BadBase32Hash(String, #[source] base32::BadBase32),
    #[error("invalid base-64 hash '{0}'")]
    BadBase64Hash(String, #[source] base64::DecodeError),
    #[error("invalid SRI hash '{0}'")]
    BadSRIHash(String),
    #[error("hash '{1}' has wrong length for hash type '{0}'")]
    WrongHashLength(Algorithm, String),
}

pub fn split_prefix<'a>(s: &'a str, prefix: &str) -> Option<(&'a str, &'a str)> {
    let mut it = s.splitn(2, prefix);
    let prefix = it.next().unwrap();
    if let Some(rest) = it.next() {
        Some((prefix, rest))
    } else {
        None
    }
}

pub(crate) fn parse_prefix(s: &str) -> Result<Option<(Algorithm, bool, &str)>, UnknownAlgorithm> {
    if let Some((prefix, rest)) = split_prefix(s, ":") {
        let a: Algorithm = prefix.parse()?;
        Ok(Some((a, false, rest)))
    } else if let Some((prefix, rest)) = split_prefix(s, "-") {
        let a: Algorithm = prefix.parse()?;
        Ok(Some((a, true, rest)))
    } else {
        Ok(None)
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Hash)]
pub struct Hash {
    algorithm: Algorithm,
    data: [u8; MAX_SIZE],
}

impl Hash {
    pub fn new(algorithm: Algorithm, hash: &[u8]) -> Hash {
        let mut data = [0u8; MAX_SIZE];
        (&mut data[0..algorithm.size()]).copy_from_slice(&hash);
        Hash { algorithm, data }
    }

    fn from_str(rest: &str, a: Algorithm, is_sri: bool) -> Result<Hash, ParseHashError> {
        if !is_sri && rest.len() == a.base16_len() {
            let mut data = [0u8; MAX_SIZE];
            let slice = match a {
                Algorithm::MD5 => &mut data[0..MD5_SIZE],
                Algorithm::SHA1 => &mut data[0..SHA1_SIZE],
                Algorithm::SHA256 => &mut data[0..SHA256_SIZE],
                Algorithm::SHA512 => &mut data[0..SHA512_SIZE],
            };
            hex::decode_to_slice(rest, slice)
                .map_err(|err| ParseHashError::BadBase16Hash(rest.to_string(), err))?;
            Ok(Hash { algorithm: a, data })
        } else if !is_sri && rest.len() == a.base32_len() {
            let data = base32::decode(rest)
                .map_err(|err| ParseHashError::BadBase32Hash(rest.to_string(), err))?;

            Ok(Hash::new(a, &data))
        } else if is_sri || rest.len() == a.base64_len() {
            let data = base64::decode(rest)
                .map_err(|err| ParseHashError::BadBase64Hash(rest.to_string(), err))?;
            if data.len() != a.size() {
                if is_sri {
                    Err(ParseHashError::BadSRIHash(rest.to_string()))
                } else {
                    Err(ParseHashError::BadBase64Hash(
                        rest.to_string(),
                        base64::DecodeError::InvalidLength,
                    ))
                }
            } else {
                Ok(Hash::new(a, &data))
            }
        } else {
            Err(ParseHashError::WrongHashLength(a, rest.to_string()))
        }
    }

    /// Parse Subresource Integrity hash expression.
    ///
    /// These have the format "<type>-<base64>",
    pub fn parse_sri(s: &str) -> Result<Hash, ParseHashError> {
        if let Some((prefix, rest)) = split_prefix(s, "-") {
            let a: Algorithm = prefix.parse()?;
            Hash::from_str(rest, a, true)
        } else {
            Err(ParseHashError::NotSRI(s.to_owned()))
        }
    }

    /// Parse the hash from a string representation.
    ///
    //  This will parse a hash in the format
    /// "[<type>:]<base16|base32|base64>" or "<type>-<base64>" (a
    /// Subresource Integrity hash expression). If the 'type' argument
    /// is not present, then the hash type must be specified in the
    /// string.
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
        if let Some((prefix, rest)) = split_prefix(s, ":") {
            let a = prefix.parse()?;
            Hash::from_str(rest, a, false)
        } else {
            Err(ParseHashError::MissingTypePrefix(s.to_string()))
        }
    }

    /// Parse a plain hash that musst not have any prefix indicating the type.
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
        format!("{:#x}", self)
    }

    pub fn encode_base32(&self) -> String {
        base32::encode(self.as_ref())
    }

    pub fn encode_base64(&self) -> String {
        base64::encode(self.as_ref())
    }

    pub fn to_base16<'a>(&'a self) -> impl fmt::Display + 'a {
        Base16Hash(self)
    }

    pub fn to_base32<'a>(&'a self) -> impl fmt::Display + 'a {
        Base32Hash(self)
    }

    pub fn to_base64<'a>(&'a self) -> impl fmt::Display + 'a {
        Base64Hash(self)
    }

    pub fn to_sri<'a>(&'a self) -> impl fmt::Display + 'a {
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
            write!(f, "{:02x}", val)?;
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
            write!(f, "{:02X}", val)?;
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

impl fmt::Display for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = base32::encode(self.as_ref());
        if f.alternate() {
            write!(f, "{}", s)
        } else {
            write!(f, "{}:{}", self.algorithm(), s)
        }
    }
}

struct Base16Hash<'a>(&'a Hash);
impl<'a> fmt::Display for Base16Hash<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if f.alternate() {
            write!(f, "{:#x}", self.0)
        } else {
            write!(f, "{:x}", self.0)
        }
    }
}

struct Base32Hash<'a>(&'a Hash);
impl<'a> fmt::Display for Base32Hash<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = base32::encode(self.0.as_ref());
        if f.alternate() {
            write!(f, "{}", s)
        } else {
            write!(f, "{}:{}", self.0.algorithm(), s)
        }
    }
}

struct Base64Hash<'a>(&'a Hash);
impl<'a> fmt::Display for Base64Hash<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = base64::encode(self.0.as_ref());
        if f.alternate() {
            write!(f, "{}", s)
        } else {
            write!(f, "{}:{}", self.0.algorithm(), s)
        }
    }
}

struct SRIHash<'a>(&'a Hash);
impl<'a> fmt::Display for SRIHash<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = base64::encode(self.0.as_ref());
        write!(f, "{}-{}", self.0.algorithm(), s)
    }
}

pub fn digest<B: AsRef<[u8]>>(algorithm: Algorithm, data: B) -> Hash {
    match algorithm {
        #[cfg(feature = "md5")]
        Algorithm::MD5 => Hash::new(Algorithm::MD5, md5::compute(data).as_ref()),
        _ => digest::digest(algorithm.digest_algorithm(), data.as_ref())
            .try_into()
            .unwrap(),
    }
}

enum InnerContext {
    #[cfg(feature = "md5")]
    MD5(md5::Context),
    Ring(digest::Context),
}

pub struct Context(Algorithm, InnerContext);

impl Context {
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

    pub fn update<D: AsRef<[u8]>>(&mut self, data: D) {
        let data = data.as_ref();
        match &mut self.1 {
            InnerContext::MD5(ctx) => ctx.consume(data),
            InnerContext::Ring(ctx) => ctx.update(data),
        }
    }

    pub fn finish(self) -> Hash {
        match self.1 {
            InnerContext::MD5(ctx) => Hash::new(self.0, ctx.compute().as_ref()),
            InnerContext::Ring(ctx) => ctx.finish().try_into().unwrap(),
        }
    }
}

pub struct HashSink(Option<(u64, Context)>);
impl HashSink {
    pub fn new(algorithm: Algorithm) -> HashSink {
        HashSink(Some((0, Context::new(algorithm))))
    }

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
pub mod proptest {
    use super::*;
    use ::proptest::prelude::*;

    impl Arbitrary for Algorithm {
        type Parameters = ();
        type Strategy = BoxedStrategy<Algorithm>;
        fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
            prop_oneof![
                #[cfg(feature="md5")]
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
            digest(algorithm, data)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    /// digest

    fn test_hash(s1: &str, algo: Algorithm, base16: &str, base32: &str, base64: &str) {
        let hash = digest(algo, s1);
        let base16_h = base16.to_uppercase();
        let base16_p = format!("{}:{}", algo, base16);
        let base16_hp = format!("{}:{}", algo, base16_h);
        let base32_p = format!("{}:{}", algo, base32);
        let base64_p = format!("{}:{}", algo, base64);
        let sri = format!("{}-{}", algo, base64);
        assert_eq!(format!("{:x}", hash), base16_p);
        assert_eq!(format!("{:#x}", hash), base16);
        assert_eq!(format!("{:X}", hash), base16_hp);
        assert_eq!(format!("{:#X}", hash), base16_h);
        assert_eq!(format!("{}", hash), base32_p);
        assert_eq!(format!("{:#}", hash), base32);
        assert_eq!(format!("{}", hash.to_base16()), base16_p);
        assert_eq!(format!("{:#}", hash.to_base16()), base16);
        assert_eq!(hash.encode_base16(), base16);
        assert_eq!(format!("{}", hash.to_base32()), base32_p);
        assert_eq!(format!("{:#}", hash.to_base32()), base32);
        assert_eq!(hash.encode_base32(), base32);
        assert_eq!(format!("{}", hash.to_base64()), base64_p);
        assert_eq!(format!("{:#}", hash.to_base64()), base64);
        assert_eq!(hash.encode_base64(), base64);
        assert_eq!(format!("{}", hash.to_sri()), sri);
        assert_eq!(*hash, *Hash::parse_any_prefixed(&base16_p).unwrap());
        assert_eq!(hash, base16_p.parse().unwrap());
        assert_eq!(hash, base16_hp.parse().unwrap());
        assert_eq!(hash, Hash::parse_any(&base16_p, None).unwrap());
        assert_eq!(hash, Hash::parse_any(&base16_p, Some(algo)).unwrap());
        assert_eq!(hash, Hash::parse_any(base16, Some(algo)).unwrap());
        assert_eq!(hash, Hash::parse_non_sri_unprefixed(base16, algo).unwrap());
        assert_eq!(
            hash,
            Hash::parse_non_sri_unprefixed(&base16_h, algo).unwrap()
        );
        assert_eq!(hash, base32_p.parse().unwrap());
        assert_eq!(hash, Hash::parse_any(&base32_p, None).unwrap());
        assert_eq!(hash, Hash::parse_any(&base32_p, Some(algo)).unwrap());
        assert_eq!(hash, Hash::parse_any(base32, Some(algo)).unwrap());
        assert_eq!(hash, Hash::parse_non_sri_unprefixed(base32, algo).unwrap());
        assert_eq!(hash, base64_p.parse().unwrap());
        assert_eq!(hash, Hash::parse_any(&base64_p, None).unwrap());
        assert_eq!(hash, Hash::parse_any(&base64_p, Some(algo)).unwrap());
        assert_eq!(hash, Hash::parse_any(base64, Some(algo)).unwrap());
        assert_eq!(hash, Hash::parse_non_sri_unprefixed(base64, algo).unwrap());
        assert_eq!(hash, sri.parse().unwrap());
        assert_eq!(hash, Hash::parse_sri(&sri).unwrap());
    }

    #[cfg(feature = "md5")]
    #[test]
    fn test_known_md5_hashes_1() {
        // values taken from: https://tools.ietf.org/html/rfc1321
        let s1 = "";
        let algo = Algorithm::MD5;
        let base16 = "d41d8cd98f00b204e9800998ecf8427e";
        let base32 = "3y8bwfr609h3lh9ch0izcqq7fl";
        let base64 = "1B2M2Y8AsgTpgAmY7PhCfg==";
        test_hash(s1, algo, base16, base32, base64);
    }

    #[cfg(feature = "md5")]
    #[test]
    fn test_known_md5_hashes_2() {
        // values taken from: https://tools.ietf.org/html/rfc1321
        let s1 = "abc";
        let algo = Algorithm::MD5;
        let base16 = "900150983cd24fb0d6963f7d28e17f72";
        let base32 = "3jgzhjhz9zjvbb0kyj7jc500ch";
        let base64 = "kAFQmDzST7DWlj99KOF/cg==";
        test_hash(s1, algo, base16, base32, base64);
    }

    #[test]
    fn test_known_sha1_hashes_1() {
        // values taken from: https://tools.ietf.org/html/rfc3174
        let s1 = "abc";
        let algo = Algorithm::SHA1;
        let base16 = "a9993e364706816aba3e25717850c26c9cd0d89d";
        let base32 = "kpcd173cq987hw957sx6m0868wv3x6d9";
        let base64 = "qZk+NkcGgWq6PiVxeFDCbJzQ2J0=";
        test_hash(s1, algo, base16, base32, base64);
    }

    #[test]
    fn test_known_sha1_hashes_2() {
        // values taken from: https://tools.ietf.org/html/rfc3174
        let s1 = "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq";
        let algo = Algorithm::SHA1;
        let base16 = "84983e441c3bd26ebaae4aa1f95129e5e54670f1";
        let base32 = "y5q4drg5558zk8aamsx6xliv3i23x644";
        let base64 = "hJg+RBw70m66rkqh+VEp5eVGcPE=";
        test_hash(s1, algo, base16, base32, base64);
    }

    #[test]
    fn test_known_sha256_hashes_1() {
        // values taken from: https://tools.ietf.org/html/rfc4634
        let s1 = "abc";
        let algo = Algorithm::SHA256;
        let base16 = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
        let base32 = "1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5s";
        let base64 = "ungWv48Bz+pBQUDeXa4iI7ADYaOWF3qctBD/YfIAFa0=";
        test_hash(s1, algo, base16, base32, base64);
    }

    #[test]
    fn test_known_sha256_hashes_2() {
        // values taken from: https://tools.ietf.org/html/rfc4634
        let s1 = "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq";
        let algo = Algorithm::SHA256;
        let base16 = "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1";
        let base32 = "1h86vccx9vgcyrkj3zv4b7j3r8rrc0z0r4r6q3jvhf06s9hnm394";
        let base64 = "JI1qYdIGOLjlwCaTDD5gOaM85Flk/yFn9uzt1BnbBsE=";
        test_hash(s1, algo, base16, base32, base64);
    }

    #[test]
    fn test_known_sha512_hashes_1() {
        // values taken from: https://tools.ietf.org/html/rfc4634
        let s1 = "abc";
        let algo = Algorithm::SHA512;
        let base16 = "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f";
        let base32 = "2gs8k559z4rlahfx0y688s49m2vvszylcikrfinm30ly9rak69236nkam5ydvly1ai7xac99vxfc4ii84hawjbk876blyk1jfhkbbyx";
        let base64 = "3a81oZNherrMQXNJriBBMRLm+k6JqX6iCp7u5ktV05ohkpkqJ0/BqDa6PCOj/uu9RU1EI2Q86A4qmslPpUyknw==";
        test_hash(s1, algo, base16, base32, base64);
    }

    #[test]
    fn test_known_sha512_hashes_2() {
        // values taken from: https://tools.ietf.org/html/rfc4634
        let s1 = "abcdefghbcdefghicdefghijdefghijkefghijklfghijklmghijklmnhijklmnoijklmnopjklmnopqklmnopqrlmnopqrsmnopqrstnopqrstu";
        let algo = Algorithm::SHA512;
        let base16 = "8e959b75dae313da8cf4f72814fc143f8f7779c6eb9f7fa17299aeadb6889018501d289e4900f7e4331b99dec4b5433ac7d329eeb6dd26545e96e55b874be909";
        let base32 = "04yjjw7bgjrcpjl4vfvdvi9sg3klhxmqkg9j6rkwkvh0jcy50fm064hi2vavblrfahpz7zbqrwpg3rz2ky18a7pyj6dl4z3v9srp5cf";
        let base64 = "jpWbddrjE9qM9PcoFPwUP493ecbrn3+hcpmurbaIkBhQHSieSQD35DMbmd7EtUM6x9Mp7rbdJlReluVbh0vpCQ==";
        test_hash(s1, algo, base16, base32, base64);
    }

    #[test]
    fn test_errors() {
        assert_eq!(
            Err(UnknownAlgorithm("test".into())),
            "test".parse::<Algorithm>()
        );
        assert_eq!(
            Err(UnknownAlgorithm("SHA384".into())),
            Algorithm::try_from(&digest::SHA384)
        );
        assert_eq!(
            Err(ParseHashError::Algorithm(UnknownAlgorithm("test".into()))),
            Hash::parse_any_prefixed("test:12345")
        );
        assert_eq!(
            Err(ParseHashError::NotSRI("test:1234".into())),
            Hash::parse_sri("test:1234")
        );
        assert_eq!(
            Err(ParseHashError::MissingTypePrefix("12345".into())),
            Hash::parse_any_prefixed("12345")
        );
        assert_eq!(
            Err(ParseHashError::MissingTypePrefix("12345".into())),
            Hash::parse_non_sri_prefixed("12345")
        );
        assert_eq!(
            Err(ParseHashError::TypeMismatch {
                expected: Algorithm::SHA256,
                actual: Algorithm::SHA1,
                hash: "sha1:12345".into(),
            }),
            Hash::parse_any("sha1:12345", Some(Algorithm::SHA256))
        );
        assert_eq!(
            Err(ParseHashError::MissingType("12345".into())),
            Hash::parse_any("12345", None)
        );
        assert_eq!(
            Err(ParseHashError::BadBase16Hash(
                "k9993e364706816aba3e25717850c26c9cd0d89d".into(),
                FromHexError::InvalidHexCharacter { c: 'k', index: 0 }
            )),
            "sha1:k9993e364706816aba3e25717850c26c9cd0d89d".parse::<Hash>()
        );
        assert_eq!(
            Err(ParseHashError::BadBase32Hash(
                "!pcd173cq987hw957sx6m0868wv3x6d9".into(),
                base32::BadBase32
            )),
            "sha1:!pcd173cq987hw957sx6m0868wv3x6d9".parse::<Hash>()
        );
        assert_eq!(
            Err(ParseHashError::BadBase64Hash(
                "!Zk+NkcGgWq6PiVxeFDCbJzQ2J0=".into(),
                base64::DecodeError::InvalidByte(0, b'!')
            )),
            "sha1:!Zk+NkcGgWq6PiVxeFDCbJzQ2J0=".parse::<Hash>()
        );
        assert_eq!(
            Err(ParseHashError::BadBase64Hash(
                "qZk+NkcGgWq6PiVxeFDCbJzQ2J0a".into(),
                base64::DecodeError::InvalidLength
            )),
            "sha1:qZk+NkcGgWq6PiVxeFDCbJzQ2J0a".parse::<Hash>()
        );
        assert_eq!(
            Err(ParseHashError::BadSRIHash(
                "qZk+NkcGgWq6PiVxeFDCbJzQ2J0a".into()
            )),
            "sha1-qZk+NkcGgWq6PiVxeFDCbJzQ2J0a".parse::<Hash>()
        );
        assert_eq!(
            Err(ParseHashError::WrongHashLength(
                Algorithm::SHA1,
                "12345".into()
            )),
            "sha1:12345".parse::<Hash>()
        );
    }
}
