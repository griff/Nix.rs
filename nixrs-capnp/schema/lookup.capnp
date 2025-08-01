@0x8ad55cd319f426e4;

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
