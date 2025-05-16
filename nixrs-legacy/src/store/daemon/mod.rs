use std::fmt;

use derive_more::{LowerHex, UpperHex};

use crate::flag_enum;
use crate::num_enum::num_enum;

mod client;
mod server;
mod traits;
mod wrap;

pub use client::DaemonStoreClient;
pub use server::{run_server, run_server_raw};
pub use traits::{DaemonStore, QueryMissingResult};

macro_rules! get_protocol_major {
    ($x:expr) => {
        (($x) & 0xff00) >> 8
    };
}

pub(crate) use get_protocol_major;

macro_rules! get_protocol_minor {
    ($x:expr) => {
        ($x) & 0x00ff
    };
}
pub(crate) use get_protocol_minor;

const WORKER_MAGIC_1: u64 = 0x6e697863;
const WORKER_MAGIC_2: u64 = 0x6478696f;

/// | Nix version     | Protocol |
/// |-----------------|----------|
/// | 0.11            | 1.02     |
/// | 0.12            | 1.04     |
/// | 0.13            | 1.05     |
/// | 0.14            | 1.05     |
/// | 0.15            | 1.05     |
/// | 0.16            | 1.06     |
/// | 1.0             | 1.10     |
/// | 1.1             | 1.11     |
/// | 1.2             | 1.12     |
/// | 1.3 - 1.5.3     | 1.13     |
/// | 1.6 - 1.10      | 1.14     |
/// | 1.11 - 1.11.16  | 1.15     |
/// | 2.0 - 2.0.4     | 1.20     |
/// | 2.1 - 2.3.18    | 1.21     |
/// | 2.4 - 2.6.1     | 1.32     |
/// | 2.7.0           | 1.33     |
/// | 2.8.0 - 2.14.1  | 1.34     |
/// | 2.15.0 - 2.19.4 | 1.35     |
/// | 2.20.0 - 2.22.0 | 1.37     |
const PROTOCOL_VERSION: u64 = (1 << 8) | 35;

const STDERR_NEXT: u64 = 0x6f6c6d67;
const STDERR_READ: u64 = 0x64617461; // data needed from source
const STDERR_WRITE: u64 = 0x64617416; // data for sink
const STDERR_LAST: u64 = 0x616c7473;
const STDERR_ERROR: u64 = 0x63787470;
const STDERR_START_ACTIVITY: u64 = 0x53545254;
const STDERR_STOP_ACTIVITY: u64 = 0x53544f50;
const STDERR_RESULT: u64 = 0x52534c54;

num_enum! {
    #[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, UpperHex, LowerHex)]
    pub enum WorkerProtoOp {
        Unknown(u64),
        IsValidPath = 1,
        HasSubstitutes = 3,
        QueryPathHash = 4, // obsolete
        QueryReferences = 5, // obsolete
        QueryReferrers = 6,
        AddToStore = 7,
        AddTextToStore = 8, // obsolete since 1.25, Nix 3.0. Use WorkerProto::Op::AddToStore
        BuildPaths = 9,
        EnsurePath = 10,
        AddTempRoot = 11,
        AddIndirectRoot = 12,
        SyncWithGC = 13,
        FindRoots = 14,
        ExportPath = 16, // obsolete
        QueryDeriver = 18, // obsolete
        SetOptions = 19,
        CollectGarbage = 20,
        QuerySubstitutablePathInfo = 21,
        QueryDerivationOutputs = 22, // obsolete
        QueryAllValidPaths = 23,
        QueryFailedPaths = 24,
        ClearFailedPaths = 25,
        QueryPathInfo = 26,
        ImportPaths = 27, // obsolete
        QueryDerivationOutputNames = 28, // obsolete
        QueryPathFromHashPart = 29,
        QuerySubstitutablePathInfos = 30,
        QueryValidPaths = 31,
        QuerySubstitutablePaths = 32,
        QueryValidDerivers = 33,
        OptimiseStore = 34,
        VerifyStore = 35,
        BuildDerivation = 36,
        AddSignatures = 37,
        NarFromPath = 38,
        AddToStoreNar = 39,
        QueryMissing = 40,
        QueryDerivationOutputMap = 41,
        RegisterDrvOutput = 42,
        QueryRealisation = 43,
        AddMultipleToStore = 44,
        AddBuildLog = 45,
        BuildPathsWithResults = 46,
    }
}

impl fmt::Display for WorkerProtoOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use WorkerProtoOp::*;
        match self {
            Unknown(cmd) => write!(f, "unknown command {}", cmd),
            IsValidPath => write!(f, "is valid path"),
            HasSubstitutes => write!(f, "has substitutes"),
            QueryPathHash => write!(f, "query paths hash"),
            QueryReferences => write!(f, "query references"),
            QueryReferrers => write!(f, "query referrers"),
            AddToStore => write!(f, "add to store"),
            AddTextToStore => write!(f, "add text to store"),
            BuildPaths => write!(f, "build paths"),
            EnsurePath => write!(f, "ensure path"),
            AddTempRoot => write!(f, "add temp root"),
            AddIndirectRoot => write!(f, "add indirect root"),
            SyncWithGC => write!(f, "sync with GC"),
            FindRoots => write!(f, "find roots"),
            ExportPath => write!(f, "export path"),
            QueryDeriver => write!(f, "query deriver"),
            SetOptions => write!(f, "set options"),
            CollectGarbage => write!(f, "collect garbage"),
            QuerySubstitutablePathInfo => write!(f, "query substitutable path info"),
            QueryDerivationOutputs => write!(f, "query derivation outputs"),
            QueryAllValidPaths => write!(f, "query all valid paths"),
            QueryFailedPaths => write!(f, "query failed paths"),
            ClearFailedPaths => write!(f, "clear failed paths"),
            QueryPathInfo => write!(f, "query path info"),
            ImportPaths => write!(f, "import paths"),
            QueryDerivationOutputNames => write!(f, "query derivation output names"),
            QueryPathFromHashPart => write!(f, "query path from hash part"),
            QuerySubstitutablePathInfos => write!(f, "query substitutable path infos"),
            QueryValidPaths => write!(f, "query valid paths"),
            QuerySubstitutablePaths => write!(f, "query substitutable paths"),
            QueryValidDerivers => write!(f, "query valid derivers"),
            OptimiseStore => write!(f, "optimize store"),
            VerifyStore => write!(f, "verify store"),
            BuildDerivation => write!(f, "build derivation"),
            AddSignatures => write!(f, "add signature"),
            NarFromPath => write!(f, "nar from path"),
            AddToStoreNar => write!(f, "add to store nar"),
            QueryMissing => write!(f, "query missing"),
            QueryDerivationOutputMap => write!(f, "query derivation output map"),
            RegisterDrvOutput => write!(f, "register drv output"),
            QueryRealisation => write!(f, "query realisation"),
            AddMultipleToStore => write!(f, "add multiple to store"),
            AddBuildLog => write!(f, "add build log"),
            BuildPathsWithResults => write!(f, "build paths with results"),
        }
    }
}

flag_enum! {
    #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
    pub enum TrustedFlag {
        NotTrusted = false,
        Trusted = true
    }
}
