@0x8ad55cd319f426e4;

using IpAddress = import "ip.capnp".IpAddress;

struct Extra {
    id @0 :UInt64;
    value @1 :AnyPointer;
}

struct Matcher {
    capType @0 :UInt64;
    params @1 :AnyPointer;
}
struct SelectedCap {
    capType @0 :UInt64;
    cap @1 :Capability;
}

interface CapLookup {
    lookup @0 (priority :List(Matcher)) -> (selected :SelectedCap);
}

struct Certificate {
    publicKey @1 :PublicKey;
    data :union {
        pem @0 :Text;
        der @2 :Data;
    }
}

enum KeyAlgorithm {
    ed25519 @0;
    rsa @1;
}

struct PublicKey {
    algorithm @0 :KeyAlgorithm;
    keyData @1 :Data;
}

struct ConnectAddress {
    union {
        tcp :group {
            address @0 :IpAddress;
            port @1 :UInt16;
            encryption :union {
                none @6 :Void;
                tls @7 :Void;
            }
        }
        unix @2 :Text;
        unknown @3 :Void;
        hidden @5 :Void;
        extended @4 :Extra;
    }
}

struct Principal {
    union {
        posix :group {
            uid @0 :UInt32;
            gid @1 :UInt32;
            pid @2 :UInt32;
        }
        x509Certificate @3 :Certificate;
        key @4 :PublicKey;
    }
}

struct ConnectionInfo {
    address @0 :ConnectAddress;
    principals @1 :List(Principal);
}

struct Forward {
    by @0 :ConnectionInfo;
    for @1 :ConnectionInfo;
    via @2 :Text;
    proto @3 :Text;
}

interface ProxyCapLookup {
    forward @0 (forwards :List(Forward)) -> (lookup :CapLookup);
}