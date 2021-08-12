use nixrs_util::hash;
use thiserror::Error;

use crate::content_address::ParseContentAddressError;
use crate::legacy_local_store::ServeCommand;
use crate::{
    ParseDrvOutputError, ParseStorePathError, ReadDerivationError, ReadStorePathError,
    WriteDerivationError,
};

#[derive(Debug, Error)]
pub enum Error {
    #[error("wanted to fetch '{0}' but the legacy ssh protocol doesn't support merely substituting drv files via the build paths command. It would build them instead. Try using ssh-ng://")]
    WantedFetchInLegacy(String),
    #[error("{0}")]
    StorePath(
        #[from]
        #[source]
        ReadStorePathError,
    ),
    #[error("{0}")]
    BadDerivation(
        #[from]
        #[source]
        ReadDerivationError,
    ),
    #[error("{0}")]
    DerivationWrite(
        #[from]
        #[source]
        WriteDerivationError,
    ),
    #[error("{0}")]
    BadDrvOutput(
        #[from]
        #[source]
        ParseDrvOutputError,
    ),
    #[error("path '{0}' is not a valid store path")]
    InvalidPath(String),
    #[error("path '{}' is not a store path", .0.display())]
    BadStorePath(std::path::PathBuf),
    #[error("path '{}' is not in the Nix store", .0.display())]
    NotInStore(std::path::PathBuf),
    #[error(".narinfo file is corrupt")]
    BadNarInfo,
    #[error("invalid base32 string")]
    BadBase32(
        #[from]
        #[source]
        nixrs_util::base32::BadBase32,
    ),
    #[error("store path name is empty")]
    StorePathNameEmpty,
    #[error("store path name is longer than 211 characters")]
    StorePathNameTooLong,
    #[error("store path name '{0}' contains forbidden character")]
    BadStorePathName(String),
    #[error("size field in NAR is too big")]
    NarSizeFieldTooBig,
    #[error("NAR string is not valid UTF-8")]
    BadNarString,
    #[error("NAR padding is not zero")]
    BadNarPadding,
    #[error("unsupported NAR version")]
    BadNarVersionMagic,
    #[error("NAR open tag is missing")]
    MissingNarOpenTag,
    #[error("NAR close tag is missing")]
    MissingNarCloseTag,
    #[error("expected NAR field is missing")]
    MissingNarField,
    #[error("unrecognized NAR field '{0}'")]
    BadNarField(String),
    #[error("bad 'executable' field in NAR")]
    BadExecutableField,
    #[error("I/O error: {0}")]
    IOError(
        #[from]
        #[source]
        std::io::Error,
    ),
    #[cfg(unused)]
    #[error("HTTP error: {0}")]
    HttpError(
        #[from]
        #[source]
        hyper::error::Error,
    ),
    #[error("{0}")]
    Misc(String),
    #[error("JSON error: {0}")]
    JSONError(
        #[from]
        #[source]
        serde_json::Error,
    ),
    #[error("{0} is not allowed")]
    WriteOnlyLegacyStore(ServeCommand),
    #[error("tar archive contains illegal file name '{0}'")]
    BadTarFileMemberName(String),
    #[error("protocol mismatch 0x{0:x}")]
    LegacyProtocolServeMismatch(u64),
    #[error("protocol mismatch with 'nix-store --serve' on '{0}'")]
    LegacyProtocolMismatch(String),
    #[error("unsupported 'nix-store --serve' protocol version on '{0}'")]
    UnsupportedLegacyProtocol(String),
    #[error("failed to add path '{0}' to remote host '{1}")]
    FailedToAddToStore(String, String),
    #[error("NAR hash is now mandatory")]
    MandatoryNARHash,
    #[error("{0}")]
    BadHash(
        #[from]
        #[source]
        hash::ParseHashError,
    ),
    #[error("{0}")]
    BadContentAddress(
        #[from]
        #[source]
        ParseContentAddressError,
    ),
    #[error("{1}")]
    Custom(u64, String),
}

impl Error {
    pub fn exit_code(&self) -> u64 {
        match self {
            Error::Custom(exit, _) => *exit,
            Error::LegacyProtocolServeMismatch(_) => 2,
            _ => 1,
        }
    }
}

impl From<ParseStorePathError> for Error {
    fn from(v: ParseStorePathError) -> Error {
        Error::StorePath(ReadStorePathError::BadStorePath(v))
    }
}

/*
impl From<hash::UnknownAlgorithm> for Error {
    fn from(v: hash::UnknownAlgorithm) -> Error {
        Error::BadHash(hash::ParseHashError::Algorithm(v))
    }
} */
