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
using DaemonTime = UInt64;
using Verbosity = UInt16;

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
    otherSettings @9 :Map(Text, Data);
}

using NarHash = Data;

struct Signature {
    key @0 :Text;
    hash @1 :Data;
}
using ContentAddress = Text;
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

interface NixDaemon {
    setOptions @0 (options :ClientOptions);
    isValidPath @1 (path :StorePath) -> (valid :Bool);
    queryValidPaths @2 (paths :List(StorePath), substitute :Bool) -> (validSet :List(StorePath));
    queryPathInfo @3 (path :StorePath) -> (info :UnkeyedValidPathInfo);
    narFromPath @4 (path :StorePath, stream :ByteStream);
}

struct Matcher {
    capType @0 :UInt64;
    params @1 :AnyPointer;
}

interface NixBootstrap {
    bootstrap @0 (priority :List(Matcher)) -> (capType :UInt64, cap :Capability);
}