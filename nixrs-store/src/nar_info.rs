use std::{fmt, collections::BTreeMap, num::ParseIntError, time::SystemTime};

use nixrs_util::StringSet;
use nixrs_util::hash::{Hash, ParseHashError};
use nixrs_util::io::StateParse;
use thiserror::Error;

use crate::{ValidPathInfo, StorePath, StoreDir, ParseStorePathError, StorePathSet, content_address::{ContentAddress, ParseContentAddressError}};

#[derive(Debug, Eq, PartialOrd, Ord, Clone)]
pub struct NarInfo {
    pub path_info: ValidPathInfo,
    pub url: String,
    pub compression: String,
    pub file_hash: Option<Hash>,
    pub file_size: u64,
    pub extra: BTreeMap<String,String>,
}

struct DisplayNarInfo<'a> {
    store_dir: &'a StoreDir,
    nar_info: &'a NarInfo,
}

impl<'a> fmt::Display for DisplayNarInfo<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StorePath: {}\n", self.store_dir.display_path(&self.nar_info.path_info.path))?;
        write!(f, "URL: {}\n", self.nar_info.url)?;
        //assert!(self.nar_info.compression != "");
        if self.nar_info.compression != "" {
            write!(f, "Compression: {}\n", self.nar_info.compression)?;
        }
        //let file_hash = self.nar_info.file_hash.as_ref().unwrap();
        //assert!(file_hash.algorithm() == Algorithm::SHA256);
        if let Some(file_hash) = self.nar_info.file_hash.as_ref() {
            write!(f, "FileHash: {}\n", file_hash.to_base32())?;
        }
        write!(f, "FileSize: {}\n", self.nar_info.file_size)?;
        //assert!(self.nar_info.path_info.nar_hash.algorithm() == Algorithm::SHA256);
        write!(f, "NarHash: {}\n", self.nar_info.path_info.nar_hash.to_base32())?;
        write!(f, "NarSize: {}\n", self.nar_info.path_info.nar_size)?;

        write!(f, "References: ")?;
        let mut first = true;
        for reference in &self.nar_info.path_info.references {
            if first {
                write!(f, "{}", reference)?;
                first = false;
            } else {
                write!(f, " {}", reference)?;
            }
        }
        write!(f, "\n")?;

        if let Some(deriver) = self.nar_info.path_info.deriver.as_ref() {
            write!(f, "Deriver: {}\n", deriver)?;
        }

        for sig in &self.nar_info.path_info.sigs {
            write!(f, "Sig: {}\n", sig)?;
        }

        if let Some(ca) = self.nar_info.path_info.ca.as_ref() {
            write!(f, "CA: {}\n", ca)?;
        }

        for (key, value) in &self.nar_info.extra {
            write!(f, "{}: {}\n", key, value)?;
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
            compression: String::new(),
            file_hash: None,
            file_size: 0,
            extra: BTreeMap::new(),
        }
    }

    pub fn parse(store_dir: &StoreDir, s: &str) -> Result<NarInfo, ParseNarInfoError> {
        let mut path = None;
        let mut url = String::new();
        let mut compression = String::new();
        let mut file_hash = None;
        let mut file_size = 0;
        let mut nar_hash = None;
        let mut nar_size = 0;
        let mut references = StorePathSet::new();
        let mut deriver = None;
        let mut sigs = StringSet::new();
        let mut ca = None;
        let mut extra = BTreeMap::new();

        for line in s.split("\n") {
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
                    "NarHash" => nar_hash = Some(Hash::parse_any_prefixed(s)?),
                    "NarSize" => nar_size = value.parse::<u64>()?,
                    "References" => {
                        for reference in value.split(" ") {
                            let ref_path = store_dir.parse_path(reference)?;
                            references.insert(ref_path);
                        }
                        if references.is_empty() {
                            return Err(ParseNarInfoError::MissingReferences);
                        }
                    },
                    "Deriver" => {
                        if value != "unknown-deriver" {
                            deriver = Some(store_dir.parse_path(value)?);
                        }
                    }
                    "Sig" => {
                        sigs.insert(value.into());
                    }
                    "CA" => {
                        if value != "" {
                            ca = Some(ContentAddress::parse(value)?);
                        }
                    }
                    e => {
                        extra.insert(key.into(), e.into());
                    }
                }
            } else {
                return Err(ParseNarInfoError::InvalidLine(line.into()))
            }
        }
        if path.is_none() {
            return Err(ParseNarInfoError::MissingStorePath);
        }
        if nar_hash.is_none() {
            return Err(ParseNarInfoError::MissingNarHash);
        }
        if url == "" {
            return Err(ParseNarInfoError::MissingURL);
        }
        if nar_size == 0 {
            return Err(ParseNarInfoError::MissingNarSize);
        }
        if compression == "" {
            compression = "bzip2".into();
        }
        let path = path.unwrap();
        let nar_hash = nar_hash.unwrap();
        let path_info = ValidPathInfo {
            path, deriver, nar_size, nar_hash, references, sigs, ca,
            registration_time: SystemTime::UNIX_EPOCH,
            ultimate: false,
        };
        Ok(NarInfo {
            path_info, url, compression, file_hash, file_size, extra,
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
            compression: String::new(),
            file_hash: None,
            file_size: 0,
            extra: BTreeMap::new(),
        }
    }
}

impl StateParse<NarInfo> for StoreDir {
    type Err = ParseNarInfoError;

    fn parse(&self, s: &str) -> Result<NarInfo, Self::Err> {
        Ok(NarInfo::parse(self, s)?)
    }
}

#[derive(Debug, Error)]
pub enum ParseNarInfoError {
    #[error("error parsing int {0}")]
    ParseIntError(#[from] #[source] ParseIntError),
    #[error("error parsing hash {0}")]
    ParseHashError(#[from] #[source] ParseHashError),
    #[error("error parsing store path {0}")]
    ParseStorePathError(#[from] #[source] ParseStorePathError),
    #[error("error parsing content address {0}")]
    ParseContentAddressError(#[from] #[source] ParseContentAddressError),
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
    MissingReferences
}