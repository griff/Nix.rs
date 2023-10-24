use std::fmt;

use derive_more::{LowerHex, UpperHex};

use crate::{num_enum::num_enum, flag_enum::flag_enum};

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

const WORKER_MAGIC_1 : u64 = 0x6e697863;
const WORKER_MAGIC_2 : u64 = 0x6478696f;

// Nix 2.0    1 << 8 | 20
// Nix 2.0.1
// Nix 2.0.2
// Nix 2.0.3
// Nix 2.0.4
// Nix 2.1    1 << 8 | 21
// Nix 2.1.1
// Nix 2.1.2
// Nix 2.1.3
// Nix 2.2    1 << 8 | 21
// Nix 2.2.1
// Nix 2.2.2
// Nix 2.3    1 << 8 | 21
// Nix 2.3.1
// Nix 2.3.2
// Nix 2.3.3
// Nix 2.3.4
// Nix 2.3.5
// Nix 2.3.6
// Nix 2.3.7
// Nix 2.3.8
// Nix 2.3.9
// Nix 2.3.10
// Nix 2.3.11
// Nix 2.3.12
// Nix 2.3.13
// Nix 2.3.14
// Nix 2.3.15
// Nix 2.3.16
// Nix 2.4    1 << 8 | 32
// Nix 2.5.0  1 << 8 | 32
// Nix 2.5.1
// Nix 2.6.0  1 << 8 | 32
// Nix 2.6.1
// Nix 2.7.0  1 << 8 | 33
// Nix 2.8.0  1 << 8 | 34
// Nix 2.8.1
// Nix 2.9.0  1 << 8 | 34
// Nix 2.9.1
// Nix 2.9.2
// Nix 2.10.0 1 << 8 | 34
// Nix 2.10.1
// Nix 2.10.2
// Nix 2.10.3
// Nix 2.11.0 1 << 8 | 34
// Nix 2.11.1
// Nix 2.12.0 1 << 8 | 34
// Nix 2.12.1
// Nix 2.13.0 1 << 8 | 34
// Nix 2.13.1
// Nix 2.13.2
// Nix 2.13.3
// Nix 2.13.4
// Nix 2.13.5
// Nix 2.14.0 1 << 8 | 34
// Nix 2.14.1
// Nix 2.15.0 1 << 8 | 35
// Nix 2.15.1
// Nix 2.15.2
// Nix 2.16.0 1 << 8 | 35
// Nix 2.16.1
// Nix 2.17.0 1 << 8 | 35
// Nix 2.17.1
// Nix 2.18.0 1 << 8 | 35
// Nix 2.18.1
const PROTOCOL_VERSION : u64 = 1 << 8 | 35;

const STDERR_NEXT : u64 =  0x6f6c6d67;
const STDERR_READ : u64 =  0x64617461; // data needed from source
const STDERR_WRITE : u64 = 0x64617416; // data for sink
const STDERR_LAST : u64 =  0x616c7473;
const STDERR_ERROR : u64 = 0x63787470;
const STDERR_START_ACTIVITY : u64 = 0x53545254;
const STDERR_STOP_ACTIVITY : u64 =  0x53544f50;
const STDERR_RESULT : u64 =         0x52534c54;


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