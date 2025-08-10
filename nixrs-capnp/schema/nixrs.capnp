@0xd1110b8bb62d8737;

using ByteStream = import "byte-stream.capnp".ByteStream;
using Types = import "nix-types.capnp";


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

    list @3 () -> (list :List(NodeAccess));
    lookup @4 (name :Data) -> (node :NodeAccess);
}

interface Blob {
    getSize @0 () -> (size :UInt64);
    writeTo @1 (stream :ByteStream, startAtOffset :UInt64 = 0);
    getSlice @2 (offset :UInt64, size :UInt32) -> (data :Data);
}

interface FileAccess extends(Blob) {
    getExecutable @0 () -> (flag :Bool);
    getExtra @1 (id :UInt64) -> (value :AnyPointer);
    getExtras @2 () -> (extras :List(Extra));
}

interface NodeAccess {
    node @0 () -> (node :Node);
    getName @3 () -> (name :Data);
    asDirectory @1 () -> (directory :DirectoryAccess);
    asFile @2 () -> (file :FileAccess);
}

interface Nar {
    writeTo @0 (stream :ByteStream);
    content @1 () -> (node :NodeAccess);
    narHash @2 () -> (hash :Types.NarHash);
    narSize @3 () -> (size :UInt64);
}

struct PathInfo {
    storePath @0 :Types.StorePath;
    deriver @1 :PathAccess;
    narHash @2 :Types.NarHash;
    narSize @3 :UInt64;
    references @4 :List(PathAccess);
    registrationTime @5 :Types.Time;
    ultimate @6 :Bool;
    signatures @7 :List(Types.Signature);
    ca @8 :Types.ContentAddress;
    nar @9 :Nar;
}

interface PathAccess {
    getStorePath @0 () -> (path :Types.StorePath);
    getDeriver @1 () -> (deriver :PathAccess);
    getReferences @2 () -> (references :List(PathAccess));
    getRegistrationTime @3 () -> (time :Types.Time);
    getSize @4 () -> (size :UInt64);
    isUltimate @5 () -> (trusted :Bool);
    getSignatures @6 () -> (signatures :List(Types.Signature));
    info @7 () -> (info :PathInfo);
    nar @8 () -> (nar :Nar);
}

struct LookupParams {
    union {
        byStorePath @0 :Types.StorePath;
        byHash @1 :Types.StorePathHash;
    }
}

interface PathStore {
    list @0 () -> (paths :List(PathAccess));
    lookup @1 (params :LookupParams) -> (path :PathAccess);
    add @2 (path :PathAccess);
}