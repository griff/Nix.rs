use std::convert::Infallible;
use std::str::FromStr;
use std::{collections::BTreeMap, fmt, num::ParseIntError, time::SystemTime};

use thiserror::Error;

use super::ValidPathInfo;
use crate::hash::{Hash, ParseHashError};
use crate::io::StateParse;
use crate::signature::{ParseSignatureError, SignatureSet};
use crate::store_path::{
    ContentAddress, ParseContentAddressError, ParseStorePathError, StoreDir, StorePath,
    StorePathSet,
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Compression {
    None,
    BZip2,
    Compress,
    GRZip,
    GZip,
    LRZip,
    LZ4,
    LZip,
    LZMA,
    LZOP,
    XZ,
    ZStd,
    BR,
    Unknown(String),
}

impl Compression {
    pub fn is_none(&self) -> bool {
        matches!(self, Self::None)
    }
    pub fn as_str(&self) -> &str {
        match self {
            Self::None => "none",
            Self::BZip2 => "bzip2",
            Self::Compress => "compress",
            Self::GRZip => "grzip",
            Self::GZip => "gzip",
            Self::LRZip => "lrzip",
            Self::LZ4 => "lz4",
            Self::LZip => "lzip",
            Self::LZMA => "lzma",
            Self::LZOP => "lzop",
            Self::XZ => "xz",
            Self::ZStd => "zstd",
            Self::BR => "br",
            Self::Unknown(s) => s,
        }
    }
}

impl Default for Compression {
    fn default() -> Self {
        Self::BZip2
    }
}

impl fmt::Display for Compression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<String> for Compression {
    fn from(value: String) -> Self {
        From::from(value.as_ref())
    }
}

impl<'a> From<&'a str> for Compression {
    fn from(value: &'a str) -> Self {
        match value {
            "none" => Self::None,
            "" => Self::BZip2,
            "bzip2" => Self::BZip2,
            "compress" => Self::Compress,
            "grzip" => Self::GRZip,
            "gzip" => Self::GZip,
            "lrzip" => Self::LRZip,
            "lz4" => Self::LZ4,
            "lzip" => Self::LZip,
            "lzma" => Self::LZMA,
            "lzop" => Self::LZOP,
            "xz" => Self::XZ,
            "zstd" => Self::ZStd,
            "br" => Self::BR,
            s => Self::Unknown(s.to_string()),
        }
    }
}

impl FromStr for Compression {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(s.into())
    }
}

#[derive(Debug, Eq, PartialOrd, Ord, Clone)]
pub struct NarInfo {
    pub path_info: ValidPathInfo,
    pub url: String,
    pub compression: Compression,
    pub file_hash: Option<Hash>,
    pub file_size: u64,
    pub extra: BTreeMap<String, String>,
}

struct DisplayNarInfo<'a> {
    store_dir: &'a StoreDir,
    nar_info: &'a NarInfo,
}

impl fmt::Display for DisplayNarInfo<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "StorePath: {}",
            self.store_dir.display_path(&self.nar_info.path_info.path)
        )?;
        writeln!(f, "URL: {}", self.nar_info.url)?;
        //assert!(self.nar_info.compression != "");
        if !self.nar_info.compression.is_none() {
            writeln!(f, "Compression: {}", self.nar_info.compression)?;
        }
        //let file_hash = self.nar_info.file_hash.as_ref().unwrap();
        //assert!(file_hash.algorithm() == Algorithm::SHA256);
        if let Some(file_hash) = self.nar_info.file_hash.as_ref() {
            writeln!(f, "FileHash: {}", file_hash.to_base32())?;
        }
        writeln!(f, "FileSize: {}", self.nar_info.file_size)?;
        //assert!(self.nar_info.path_info.nar_hash.algorithm() == Algorithm::SHA256);
        writeln!(
            f,
            "NarHash: {}",
            self.nar_info.path_info.nar_hash.to_base32()
        )?;
        writeln!(f, "NarSize: {}", self.nar_info.path_info.nar_size)?;

        write!(f, "References: ")?;
        let mut first = true;
        for reference in &self.nar_info.path_info.references {
            if first {
                write!(f, "{reference}")?;
                first = false;
            } else {
                write!(f, " {reference}")?;
            }
        }
        writeln!(f)?;

        if let Some(deriver) = self.nar_info.path_info.deriver.as_ref() {
            writeln!(f, "Deriver: {deriver}")?;
        }

        for sig in &self.nar_info.path_info.sigs {
            writeln!(f, "Sig: {sig}")?;
        }

        if let Some(ca) = self.nar_info.path_info.ca.as_ref() {
            writeln!(f, "CA: {ca}")?;
        }

        for (key, value) in &self.nar_info.extra {
            writeln!(f, "{key}: {value}")?;
        }

        Ok(())
    }
}

impl NarInfo {
    pub fn new(path: StorePath, nar_hash: Hash) -> NarInfo {
        let path_info = ValidPathInfo::new(path, nar_hash);
        NarInfo {
            path_info,
            url: String::new(),
            compression: Default::default(),
            file_hash: None,
            file_size: 0,
            extra: BTreeMap::new(),
        }
    }

    pub fn parse(store_dir: &StoreDir, s: &str) -> Result<NarInfo, ParseNarInfoError> {
        let mut path = None;
        let mut url = String::new();
        let mut compression = Default::default();
        let mut file_hash = None;
        let mut file_size = 0;
        let mut nar_hash = None;
        let mut nar_size = 0;
        let mut references = StorePathSet::new();
        let mut deriver = None;
        let mut sigs = SignatureSet::new();
        let mut ca = None;
        let mut extra = BTreeMap::new();

        for line in s.split('\n') {
            let mut kv = line.splitn(2, ": ");
            let key = kv.next().unwrap();
            if let Some(value) = kv.next() {
                match key {
                    "StorePath" => {
                        path = Some(store_dir.parse_path(value)?);
                    }
                    "URL" => url = value.into(),
                    "Compression" => compression = value.into(),
                    "FileHash" => file_hash = Some(Hash::parse_any_prefixed(value)?),
                    "FileSize" => file_size = value.parse::<u64>()?,
                    "NarHash" => nar_hash = Some(Hash::parse_any_prefixed(value)?),
                    "NarSize" => nar_size = value.parse::<u64>()?,
                    "References" => {
                        if !value.trim().is_empty() {
                            for reference in value.split(' ') {
                                let ref_path = StorePath::new_from_base_name(reference)?;
                                references.insert(ref_path);
                            }
                        }
                    }
                    "Deriver" => {
                        if value != "unknown-deriver" {
                            deriver = Some(StorePath::new_from_base_name(value)?);
                        }
                    }
                    "Sig" => {
                        sigs.insert(value.parse()?);
                    }
                    "CA" => {
                        if !value.is_empty() {
                            ca = Some(ContentAddress::parse(value)?);
                        }
                    }
                    e => {
                        extra.insert(key.into(), e.into());
                    }
                }
            } else if !line.trim().is_empty() {
                return Err(ParseNarInfoError::InvalidLine(line.into()));
            }
        }
        if path.is_none() {
            return Err(ParseNarInfoError::MissingStorePath);
        }
        if nar_hash.is_none() {
            return Err(ParseNarInfoError::MissingNarHash);
        }
        if url.is_empty() {
            return Err(ParseNarInfoError::MissingURL);
        }
        if nar_size == 0 {
            return Err(ParseNarInfoError::MissingNarSize);
        }
        let path = path.unwrap();
        let nar_hash = nar_hash.unwrap();
        let path_info = ValidPathInfo {
            path,
            deriver,
            nar_size,
            nar_hash,
            references,
            sigs,
            ca,
            registration_time: SystemTime::UNIX_EPOCH,
            ultimate: false,
        };
        Ok(NarInfo {
            path_info,
            url,
            compression,
            file_hash,
            file_size,
            extra,
        })
    }

    pub fn display<'a>(&'a self, store_dir: &'a StoreDir) -> impl fmt::Display + 'a {
        DisplayNarInfo {
            store_dir,
            nar_info: self,
        }
    }

    pub fn to_string(&self, store_dir: &StoreDir) -> String {
        self.display(store_dir).to_string()
    }
}

impl PartialEq for NarInfo {
    fn eq(&self, other: &Self) -> bool {
        self.path_info == other.path_info
    }
}

impl PartialEq<ValidPathInfo> for NarInfo {
    fn eq(&self, other: &ValidPathInfo) -> bool {
        self.path_info == *other
    }
}

impl PartialEq<NarInfo> for ValidPathInfo {
    fn eq(&self, other: &NarInfo) -> bool {
        self == &other.path_info
    }
}

impl From<ValidPathInfo> for NarInfo {
    fn from(path_info: ValidPathInfo) -> Self {
        NarInfo {
            path_info,
            url: String::new(),
            compression: Default::default(),
            file_hash: None,
            file_size: 0,
            extra: BTreeMap::new(),
        }
    }
}

impl StateParse<NarInfo> for StoreDir {
    type Err = ParseNarInfoError;

    fn parse(&self, s: &str) -> Result<NarInfo, Self::Err> {
        NarInfo::parse(self, s)
    }
}

#[derive(Debug, Error)]
pub enum ParseNarInfoError {
    #[error("error parsing int {0}")]
    ParseIntError(
        #[from]
        #[source]
        ParseIntError,
    ),
    #[error("error parsing hash {0}")]
    ParseHashError(
        #[from]
        #[source]
        ParseHashError,
    ),
    #[error("error parsing signature {0}")]
    ParseSignatureError(
        #[from]
        #[source]
        ParseSignatureError,
    ),
    #[error("error parsing store path {0}")]
    ParseStorePathError(
        #[from]
        #[source]
        ParseStorePathError,
    ),
    #[error("error parsing content address {0}")]
    ParseContentAddressError(
        #[from]
        #[source]
        ParseContentAddressError,
    ),
    #[error("invalid line {0}")]
    InvalidLine(String),
    #[error("missing StorePath")]
    MissingStorePath,
    #[error("missing URL")]
    MissingURL,
    #[error("missing NarHash")]
    MissingNarHash,
    #[error("missing NarSize")]
    MissingNarSize,
    #[error("missing references")]
    MissingReferences,
}

#[cfg(test)]
mod tests {
    use crate::signature::{PublicKey, Signature};
    use crate::store_path::{StoreDir, StorePath};

    use super::*;

    #[test]
    fn test_narinfo_parse_gcc() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let data = std::fs::read("test-data/binary-cache/7rjj86a15146cq1d3qy068lml7n7ykzm.narinfo")
            .unwrap();
        let data_s = String::from_utf8(data).unwrap();
        let info = NarInfo::parse(&store_dir, &data_s).unwrap();
        assert_eq!(
            info.path_info.path,
            StorePath::new_from_base_name("7rjj86a15146cq1d3qy068lml7n7ykzm-gcc-wrapper-12.3.0")
                .unwrap()
        );
        assert_eq!(
            info.path_info.nar_hash,
            "sha256:0kq5fqd4kqf898517kzkk2c60fk6kfx6ly0inwmsk4p5xnxzxkkx"
                .parse::<Hash>()
                .unwrap()
        );
        assert_eq!(info.path_info.nar_size, 57_024);
        assert_eq!(
            info.path_info.deriver,
            Some(
                StorePath::new_from_base_name(
                    "bkvcpfrw9l7xk6kq1jdcxwkzz6vzlq4x-gcc-wrapper-12.3.0.drv"
                )
                .unwrap()
            )
        );

        let mut paths = StorePathSet::new();
        paths.insert(
            StorePath::new_from_base_name("1a6gwg8f25jii16sjsw0icb586g81d7h-coreutils-9.3")
                .unwrap(),
        );
        paths.insert(
            StorePath::new_from_base_name("7rjj86a15146cq1d3qy068lml7n7ykzm-gcc-wrapper-12.3.0")
                .unwrap(),
        );
        paths.insert(
            StorePath::new_from_base_name("avpf9xk8zh78r45v1sypnj3wa1bm1cd2-gnugrep-3.11").unwrap(),
        );
        paths.insert(
            StorePath::new_from_base_name(
                "axqkmprf67z895q5dk3gval6hc28nkxp-expand-response-params",
            )
            .unwrap(),
        );
        paths.insert(
            StorePath::new_from_base_name(
                "d2dcqvhpmi22c06xh9mbm4q9kg1vijr7-cctools-binutils-darwin-wrapper-973.0.1",
            )
            .unwrap(),
        );
        paths.insert(
            StorePath::new_from_base_name("hlxsqazc1ggvlh9cha75mn881hc0d7ai-gcc-12.3.0-lib")
                .unwrap(),
        );
        paths.insert(
            StorePath::new_from_base_name("rik0icxjshvcq6z9ccf8rlg147abisgn-gcc-12.3.0").unwrap(),
        );
        paths.insert(
            StorePath::new_from_base_name("s2ps2rq1k0k7sqw47yc7mi5311y1kqfl-bash-5.2-p15").unwrap(),
        );
        paths.insert(
            StorePath::new_from_base_name("vw0zbvb4n6c1mwfj5x4ggngqlkfgb070-Libsystem-1238.60.2")
                .unwrap(),
        );
        assert_eq!(info.path_info.references, paths);

        let mut sigs = SignatureSet::new();
        let sig = "cache.nixos.org-1:NWIUOETMPCgFRRR00C40Zxc4mzBhdP9LLSUbshFuoSsVPJhxy8LMcSVlM3Up51izrOuZPa6jtqBLkTpUG3TNDA==".parse::<Signature>().unwrap();
        sigs.insert(sig.clone());
        assert_eq!(info.path_info.sigs, sigs);

        assert_eq!(
            info.url,
            "nar/187yfzibyhdcv024a1aj6kmxdcwbrd3rqdrkdrkbw8l41wc79gpn.nar.xz"
        );
        assert_eq!(info.compression, Compression::XZ);
        assert_eq!(
            info.file_hash,
            Some(
                "sha256:187yfzibyhdcv024a1aj6kmxdcwbrd3rqdrkdrkbw8l41wc79gpn"
                    .parse::<Hash>()
                    .unwrap()
            )
        );
        assert_eq!(info.file_size, 9_360);
        assert_eq!(info.extra, BTreeMap::new());

        let key = "cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY="
            .parse::<PublicKey>()
            .unwrap();
        let fingerprint = info.path_info.fingerprint(&store_dir).unwrap().to_string();

        assert!(key.verify(fingerprint, &sig));
    }

    #[test]
    fn test_narinfo_parse_hello() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let data = std::fs::read("test-data/binary-cache/ycbqd7822qcnasaqy0mmiv2j9n9m62yl.narinfo")
            .unwrap();
        let data_s = String::from_utf8(data).unwrap();
        let info = NarInfo::parse(&store_dir, &data_s).unwrap();
        assert_eq!(
            info.path_info.path,
            StorePath::new_from_base_name("ycbqd7822qcnasaqy0mmiv2j9n9m62yl-hello-2.12.1").unwrap()
        );
        assert_eq!(
            info.path_info.nar_hash,
            "sha256:1bnz0km10yckg8808px5ifdbd7hwkl8fhi2hbvzdlnf269xmb55a"
                .parse::<Hash>()
                .unwrap()
        );
        assert_eq!(info.path_info.nar_size, 74_704);
        assert_eq!(
            info.path_info.deriver,
            Some(
                StorePath::new_from_base_name("niifikxjcqw16azkyii083q0wzbbz0gk-hello-2.12.1.drv")
                    .unwrap()
            )
        );
        assert_eq!(info.path_info.references, StorePathSet::new());

        let mut sigs = SignatureSet::new();
        let sig = "cache.nixos.org-1:eeCTVmM4dOCaEx2bJIszz+/3Vdkr/w1Xgy2hmknmifaMieBbUL5wi0TWlIkPNalGB5VRD4p9l8LCPjWKwaXPDQ==".parse::<Signature>().unwrap();
        sigs.insert(sig.clone());
        assert_eq!(info.path_info.sigs, sigs);

        assert_eq!(
            info.url,
            "nar/0vpy0ghvb98n2s928ldw855rnk2qadi4pyqmy74fvwnl2x086kyc.nar.xz"
        );
        assert_eq!(info.compression, Compression::XZ);
        assert_eq!(
            info.file_hash,
            Some(
                "sha256:0vpy0ghvb98n2s928ldw855rnk2qadi4pyqmy74fvwnl2x086kyc"
                    .parse::<Hash>()
                    .unwrap()
            )
        );
        assert_eq!(info.file_size, 25_288);
        assert_eq!(info.extra, BTreeMap::new());

        let key = "cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY="
            .parse::<PublicKey>()
            .unwrap();
        let fingerprint = info.path_info.fingerprint(&store_dir).unwrap().to_string();

        assert!(key.verify(fingerprint, &sig));
    }
}
