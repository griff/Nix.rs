@0xb83d96947a7e4ccb;

using Rust = import "rust.capnp";
$Rust.parentModule("capnp");
using ByteStream = import "byte-stream.capnp".ByteStream;

struct Map(Key, Value) {
  entries @0 :List(Entry);
  struct Entry {
    key @0 :Key;
    value @1 :Value;
  }
}

struct StorePath {
    hash @0 :Data;
    name @1 :Text;
}

using DaemonInt = UInt32;
using DaemonTime = Int64;
using DaemonString = Data;
using Microseconds = Int64;

struct ClientOptions {
    keepFailed @0 :Bool;
    keepGoing @1 :Bool;
    tryFallback @2 :Bool;
    verbosity @3 :Verbosity;
    maxBuildJobs @4 :DaemonInt;
    maxSilentTime @5 :DaemonTime;
    verboseBuild @6 :Verbosity;
    buildCores @7 :DaemonInt;
    useSubstitutes @8 :Bool;
    otherSettings @9 :Map(Text, DaemonString);
}

using NarHash = Data;
using Sha256 = Data;

struct Signature {
    key @0 :Text;
    hash @1 :Data;
}
enum HashAlgo {
    md5 @0;
    sha1 @1;
    sha256 @2;
    sha512 @3;
}
struct Hash {
    algo @0 :HashAlgo;
    digest @1 :Data;
}
struct ContentAddress {
    union {
        text @0 :Sha256;
        flat @1 :Hash;
        recursive @2 :Hash;
    }
}
struct ContentAddressMethodAlgorithm {
    union {
        text @0 :Void;
        flat @1 :HashAlgo;
        recursive @2 :HashAlgo;
    }
}
struct ValidPathInfo {
    path @0 :StorePath;
    info @1 :UnkeyedValidPathInfo;
}
struct UnkeyedValidPathInfo {
    deriver @0 :StorePath;
    narHash @1 :NarHash;
    references @2 :List(StorePath);
    registrationTime @3 :DaemonTime;
    narSize @4 :UInt64;
    ultimate @5 :Bool;
    signatures @6 :List(Signature);
    ca @7 :ContentAddress;
}
struct DrvOutput {
    drvHash @0 :Hash;
    outputName @1 :OutputName;
}
struct Realisation {
    id @0 :DrvOutput;
    outPath @1 :StorePath;
    signatures @2 :List(Signature);
    dependentRealisations @3 :Map(DrvOutput, StorePath);
}
enum BuildStatus {
    built @0;
    substituted @1;
    alreadyValid @2;
    permanentFailure @3;
    inputRejected @4;
    outputRejected @5;
    transientFailure @6;
    cachedFailure @7;
    timedOut @8;
    miscFailure @9;
    dependencyFailed @10;
    logLimitExceeded @11;
    notDeterministic @12;
    resolvesToAlreadyValid @13;
    noSubstituters @14;
}
struct BuildResult {
    status @0 :BuildStatus;
    errorMsg @1 :DaemonString;
    timesBuilt @2 :DaemonInt;
    isNonDeterministic @3 :Bool;
    startTime @4 :DaemonTime;
    stopTime @5 :DaemonTime;
    cpuUser @6 :Microseconds = -1;
    cpuSystem @7 :Microseconds = -1;
    builtOutputs @8 :Map(DrvOutput, Realisation);
}
struct KeyedBuildResult {
    path @0 :DerivedPath;
    result @1 :BuildResult;
}

using OutputName = Text;
struct SingleDerivedPath {
    union {
        opaque @0 :StorePath;
        built :group {
            drvPath @1 :SingleDerivedPath;
            output @2 :OutputName;
        }
    }
}
struct OutputSpec {
    union {
        all @0 :Void;
        named @1 :List(OutputName);
    }
}
struct DerivedPath {
    union {
        opaque @0 :StorePath;
        built :group {
            drvPath @1 :SingleDerivedPath;
            outputs @2 :OutputSpec;
        }
    }
}
struct DerivationOutput {
    union {
        inputAddressed @0 :StorePath;
        caFixed @1 :ContentAddress;
        deferred @2 :Void;
        caFloating @3 :ContentAddressMethodAlgorithm;
        impure @4 :ContentAddressMethodAlgorithm;
    }
}

struct BasicDerivation {
    drvPath @0 :StorePath;
    outputs @1 :Map(OutputName, DerivationOutput);
    inputSrcs @2 :List(StorePath);
    platform @3 :Data;
    builder @4 :Data;
    args @5 :List(Data);
    env @6 :Map(Data, Data);
}
struct QueryMissingResult {
    willBuild @0 :List(StorePath);
    willSubstitute @1 :List(StorePath);
    unknown @2 :List(StorePath);
    downloadSize @3 :UInt64;
    narSize @4 :UInt64;
}
interface AddMultipleStream {
    add @0 (info :ValidPathInfo) -> (stream :ByteStream);
}
enum BuildMode {
    normal @0;
    repair @1;
    check @2;
}

interface NixDaemon {
    end @0 ();
    setOptions @1 (options :ClientOptions);
    isValidPath @2 (path :StorePath) -> (valid :Bool);
    queryValidPaths @3 (paths :List(StorePath), substitute :Bool) -> (validSet :List(StorePath));
    queryPathInfo @4 (path :StorePath) -> (info :UnkeyedValidPathInfo);
    narFromPath @5 (path :StorePath, stream :ByteStream);
    buildPaths @6 (drvs :List(DerivedPath), mode :BuildMode);
    buildPathsWithResults @7 (drvs :List(DerivedPath), mode :BuildMode) -> (results :List(KeyedBuildResult));
    buildDerivation @8 (drv :BasicDerivation, mode :BuildMode) -> (result :BuildResult);
    queryMissing @9 (paths :List(DerivedPath)) -> (missing :QueryMissingResult);
    addToStoreNar @10 (info :ValidPathInfo, repair :Bool, dontCheckSigs :Bool) -> (stream :ByteStream);
    addMultipleToStore @11 (repair :Bool, dontCheckSigs :Bool, count :UInt16) -> (stream :AddMultipleStream);
}

enum ResultType {
    fileLinked @0;
    buildLogLine @1;
    untrustedPath @2;
    corruptedPath @3;
    setPhase @4;
    progress @5;
    setExpected @6;
    postBuildLogLine @7;
    fetchStatus @8;
}

enum Verbosity {
    error @0;
    warn @1;
    notice @2;
    info @3;
    talkative @4;
    chatty @5;
    debug @6;
    vomit @7;
}

enum ActivityType {
    unknown @0;
    copyPath @1;
    fileTransfer @2;
    realise @3;
    copyPaths @4;
    builds @5;
    build @6;
    optimiseStore @7;
    verifyPaths @8;
    substitute @9;
    queryPathInfo @10;
    postBuildHook @11;
    buildWaiting @12;
    fetchTree @13;
}

struct Field {
    union {
        int @0 :UInt64;
        string @1 :DaemonString;
    }
}

struct LogMessage {
    union {
        next @0 :DaemonString;
        startActivity :group {
            act @1 :UInt64;
            level @2 :Verbosity;
            activityType @3 :ActivityType;
            text @4 :DaemonString;
            fields @5 :List(Field);
            parent @6 :UInt64;
        }
        stopActivity :group {
            act @7 :UInt64;
        }
        result :group {
            act @8 :UInt64;
            resultType @9 :ResultType;
            fields @10 :List(Field);
        }
    }
}

interface Logger {
    write @0 (event :LogMessage) -> stream;
    end @1 ();
}

interface LoggedNixDaemon {
    captureLogs @0 (logger :Logger) -> (daemon :NixDaemon);
}

struct Matcher {
    capType @0 :UInt64;
    params @1 :AnyPointer;
}

interface NixBootstrap {
    bootstrap @0 (priority :List(Matcher)) -> (capType :UInt64, cap :Capability);
}