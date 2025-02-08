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

interface PathInfo {
    getPath @0 () -> (path :NixDaemon.StorePath);
    getSize @1 () -> (size :UInt64);
    getNarSize @2 () -> (size :UInt64);
    getNarHash @3 () -> (hash :NixDaemon.NarHash);
    isValid @4 () -> (valid :Bool);
    info @5 () -> (info :NixDaemon.UnkeyedValidPathInfo);
    nar @6 () -> (nar :Nar);
}