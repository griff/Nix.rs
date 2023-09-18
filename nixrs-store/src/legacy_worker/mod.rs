use std::fmt;

use derive_more::{LowerHex, UpperHex};
use nixrs_util::num_enum;

pub mod client;
pub mod server;
mod traits;
mod wrap;

pub use self::traits::LegacyStore;
pub use self::wrap::LegacyWrapStore;

pub const SERVE_MAGIC_1: u64 = 0x390c9deb;
pub const SERVE_MAGIC_2: u64 = 0x5452eecb;

pub const SERVE_PROTOCOL_VERSION: u64 = 2 << 8 | 6;

num_enum! {
    #[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, UpperHex, LowerHex)]
    pub enum ServeCommand {
        Unknown(u64),
        CmdQueryValidPaths = 1,
        CmdQueryPathInfos = 2,
        CmdDumpStorePath = 3,
        CmdImportPaths = 4,
        CmdExportPaths = 5,
        CmdBuildPaths = 6,
        CmdQueryClosure = 7,
        CmdBuildDerivation = 8,
        CmdAddToStoreNar = 9
    }
}

impl fmt::Display for ServeCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use ServeCommand::*;
        match self {
            Unknown(cmd) => write!(f, "unknown command {}", cmd),
            CmdQueryValidPaths => write!(f, "query valid paths"),
            CmdQueryPathInfos => write!(f, "query path infos"),
            CmdDumpStorePath => write!(f, "dump store path"),
            CmdImportPaths => write!(f, "import paths"),
            CmdExportPaths => write!(f, "exports paths"),
            CmdBuildPaths => write!(f, "build paths"),
            CmdQueryClosure => write!(f, "query closure"),
            CmdBuildDerivation => write!(f, "build derviation"),
            CmdAddToStoreNar => write!(f, "add to store"),
        }
    }
}
