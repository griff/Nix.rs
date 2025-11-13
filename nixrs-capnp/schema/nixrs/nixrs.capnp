@0xd1110b8bb62d8737;

using ByteStream = import "/byte-stream.capnp".ByteStream;
using Types = import "nix-types.capnp";
using Extra = import "lookup.capnp".Extra;

struct Node {
    name @0 :Types.FileName;
    extra @1 :List(Extra);
    union {
        directory @2 :Void;
        file :group {
            size @3 :UInt64;
            executable @4 :Bool;
        }
        symlink :group {
            target @5 :Types.Path;
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
    getName @3 () -> (name :Types.FileName);
    asDirectory @1 () -> (directory :DirectoryAccess);
    asFile @2 () -> (file :FileAccess);
    stream @4 (handler :NodeHandler);
}

interface NodeHandler {
    symlink @0 (name :Types.FileName, target :Types.Path) -> stream;
    file @1 (name :Types.FileName, size :UInt64, executable :Bool) -> (writeTo :ByteStream);
    startDirectory @2 (name :Types.FileName) -> stream;
    finishDirectory @3 () -> stream;
    end @4 ();
}

interface NarCache {
    lookup @0 (nar :Nar) -> (nar :Nar);
}

interface Nar {
    writeTo @0 (stream :ByteStream);
    content @1 () -> (node :NodeAccess);
    stream @4 (handler :NodeHandler);
    narHash @2 () -> (hash :Types.NarHash);
    narSize @3 () -> (size :UInt64);
}

struct StorePathInfo {
    storePath @0 :Types.StorePath;
    deriver @1 :RemoteStorePath;
    narHash @2 :Types.NarHash;
    narSize @3 :UInt64;
    references @4 :List(RemoteStorePath);
    registrationTime @5 :Types.Time;
    ultimate @6 :Bool;
    signatures @7 :List(Types.Signature);
    ca @8 :Types.ContentAddress;
    nar @9 :Nar;
}

interface StorePathAccess {
    getStorePath @0 () -> (path :Types.StorePath);
    getDeriver @1 () -> (deriver :RemoteStorePath);
    getReferences @2 () -> (references :List(RemoteStorePath));
    getRegistrationTime @3 () -> (time :Types.Time);
    getSize @4 () -> (size :UInt64);
    isUltimate @5 () -> (trusted :Bool);
    getSignatures @6 () -> (signatures :List(Types.Signature));
    info @7 () -> (info :StorePathInfo);
    getReferrers @9 () -> (referrers :List(RemoteStorePath));
    nar @8 () -> (nar :Nar);
}

struct RemoteStorePath {
    storePath @0 :Types.StorePath;
    access @1 :StorePathAccess;
}

struct LookupParams {
    substitute @2 :Bool = false;
    union {
        byStorePath @0 :Types.StorePath;
        byHash @1 :Types.StorePathHash;
    }
}

interface StorePathStore {
    list @0 () -> (paths :List(RemoteStorePath));
    lookup @1 (params :LookupParams) -> (path :RemoteStorePath);
    add @2 (path :RemoteStorePath, repair :Bool = false, dontCheckSigs :Bool = false, substitute :Bool = false);
}

struct GenerationInfo {
    number @0 :UInt64;
    creationTime @1 :Types.Time;
    storePath @2 :RemoteStorePath;
}

interface GenerationCap {
    info @0 () -> (info :GenerationInfo);
    switch @1 ();
    delete @2 ();
}

struct Generation {
    info @0 :GenerationInfo;
    cap @1 :GenerationCap;
}

interface Profile {
    struct LookupParams {
        path @0 :Types.Path;
    }

    currentGeneration @0 () -> (generation :Generation);
    listGenerations @1 () -> (generations :List(Generation));
    createGeneration @2 (store_path :Types.StorePath) -> (generation :Generation);
}