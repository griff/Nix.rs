pub enum RangeCompare {
    Disjoint(RangeDisjoint),
    Intersect(RangeIntersect),
}

pub enum RangeDisjoint {
    /// A = B = {}
    EmptyBoth,

    /// A = {}
    EmptyLhs,

    /// B = {}
    EmptyRhs,

    /// |-A-| |-B-|
    Less,

    /// |-A-||-B-|
    LessAdjacent,

    /// |-B-||-A-|
    GreaterAdjacent,

    /// |-B-| |-A-|
    Greater,
}

pub enum RangeIntersect {
    /// |-A-|
    ///    |-B-|
    OverlapBeginning,

    /// |-A-|
    /// |--B--|
    Prefixes,

    /// |--A--|
    /// |-B-|
    HasPrefix,

    ///  |-A-|
    /// |--B--|
    ContainedBy,

    /// |--A--|
    ///  |-B-|
    Contains,

    ///   |-A-|
    /// |--B--|
    Suffixes,

    /// |--A--|
    ///   |-B-|
    HasSuffix,

    /// |-A-|
    /// |-B-|
    Equal,

    ///    |-A-|
    /// |-B-|
    OverlapsEnding,
}

trait RangeOps {
    fn min(&self) -> Self;
    fn max(&self) -> Self;
    fn next(&self) -> Option<Self>;
    fn prev(&self) -> Optioon<Self>;
}

pub trait RangeCompare {}
