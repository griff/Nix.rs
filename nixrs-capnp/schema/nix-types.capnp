@0xac08bad3d41cd190;

struct Map(Key, Value) {
  entries @0 :List(Entry);
  struct Entry {
    key @0 :Key;
    value @1 :Value;
  }
}

using StorePathHash = Data;
struct StorePath {
    hash @0 :StorePathHash;
    name @1 :Text;
}

using Time = Int64;
using NarHash = Data;

struct Signature {
    key @0 :Text;
    hash @1 :Data;
}

using Sha256 = Data;

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