@0xd1110b8bb62d8737;

using Rust = import "rust.capnp";
$Rust.parentModule("capnp");
using ByteStream = import "byte-stream.capnp".ByteStream;
using NixDaemon = import "nix-daemon.capnp";

struct NamedNode {
    name @0 :Data;
    node @1 :Node;
}

struct Extra {
    id @0 :UInt64;
    value @1 :AnyPointer;
}

struct Node {
    union {
        directory :group {
            size @0 :UInt64;
            extra @1 :List(Extra);
        }
        file :group {
            size @2 :UInt64;
            extra @3 :List(Extra);
            executable @4 :Bool;
        }
        symlink :group {
            target @5 :Data;
        }
    }
}

interface DirectoryAccess {
    getSize @0 () -> (size :UInt64);
    getExtra @1 (id :UInt64) -> (value :AnyPointer);
    getExtras @2 () -> (extras :List(Extra));

    list @3 () -> (list :List(NamedNode));
    lookup @4 (name :Data) -> (node :NodeAccess);
}

interface Blob {
    writeTo @0 (stream :ByteStream, startAtOffset :UInt64 = 0);
    getSlice @1 (offset :UInt64, size :UInt32) -> (data :Data);
}

interface FileAccess extends(Blob) {
    getSize @0 () -> (size :UInt64);
    getExecutable @1 () -> (flag :Bool);
    getExtra @2 (id :UInt64) -> (value :AnyPointer);
    getExtras @3 () -> (extras :List(Extra));
}

interface NodeAccess {
    node @0 () -> (node :Node);
    asDirectory @1 () -> (directory :DirectoryAccess);
    asFile @2 () -> (file :FileAccess);
}

interface Nar {
    writeTo @0 (stream :ByteStream);
    content @1 () -> (node :NodeAccess);
}

struct PathInfo {
    storePath @0 :NixDaemon.StorePath;
    deriver @1 :PathAccess;
    narHash @2 :NixDaemon.NarHash;
    references @3 :List(PathAccess);
    registrationTime @4 :NixDaemon.DaemonTime;
    narSize @5 :UInt64;
    ultimate @6 :Bool;
    signatures @7 :List(NixDaemon.Signature);
    ca @8 :NixDaemon.ContentAddress;
}

interface PathAccess {
    getStorePath @0 () -> (path :NixDaemon.StorePath);
    getDeriver @1 () -> (deriver :PathAccess);
    getNarHash @2 () -> (hash :NixDaemon.NarHash);
    getReferences @3 () -> (references :List(PathAccess));
    getRegistrationTime @4 () -> (time :NixDaemon.DaemonTime);
    getSize @5 () -> (size :UInt64);
    getNarSize @6 () -> (size :UInt64);
    isUltimate @7 () -> (trusted :Bool);
    getSignature @8 () -> (signatures :List(NixDaemon.Signature));
    info @9 () -> (info :PathInfo);
    isValid @10 () -> (valid :Bool);
    nar @11 () -> (nar :Nar);
}