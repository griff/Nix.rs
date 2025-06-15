use std::{
    fmt,
    ops::{
        Bound, Range, RangeBounds, RangeFrom, RangeFull, RangeInclusive, RangeTo, RangeToInclusive,
    },
};

#[cfg(feature = "nixrs-derive")]
use nixrs_derive::{NixDeserialize, NixSerialize};

pub const NIX_VERSION: &str = "Nix.rs 1.0";
const PROTOCOL_VERSION_MAJOR: u8 = 1;
pub const PROTOCOL_VERSION: ProtocolVersion =
    ProtocolVersion::from_parts(PROTOCOL_VERSION_MAJOR, 37);
pub const PROTOCOL_VERSION_MIN: ProtocolVersion =
    ProtocolVersion::from_parts(PROTOCOL_VERSION_MAJOR, 21);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
#[cfg_attr(feature = "nixrs-derive", nix(from = "u16", into = "u16"))]
pub struct ProtocolVersion(u8, u8);
impl ProtocolVersion {
    pub const fn max() -> Self {
        PROTOCOL_VERSION
    }

    pub const fn min() -> Self {
        PROTOCOL_VERSION_MIN
    }

    pub const fn from_parts(major: u8, minor: u8) -> Self {
        Self(major, minor)
    }

    #[inline]
    pub const fn major(&self) -> u8 {
        self.0
    }

    #[inline]
    pub const fn minor(&self) -> u8 {
        self.1
    }

    pub const fn next(&self) -> ProtocolVersion {
        ProtocolVersion(self.0, self.1 + 1)
    }

    pub const fn previous(&self) -> ProtocolVersion {
        ProtocolVersion(self.0, self.1 - 1)
    }
}

impl Default for ProtocolVersion {
    fn default() -> Self {
        PROTOCOL_VERSION
    }
}

impl From<u16> for ProtocolVersion {
    fn from(value: u16) -> Self {
        ProtocolVersion::from_parts(((value & 0xff00) >> 8) as u8, (value & 0x00ff) as u8)
    }
}

impl From<(u8, u8)> for ProtocolVersion {
    fn from((major, minor): (u8, u8)) -> Self {
        ProtocolVersion::from_parts(major, minor)
    }
}

impl From<ProtocolVersion> for u16 {
    fn from(value: ProtocolVersion) -> Self {
        ((value.major() as u16) << 8) | (value.minor() as u16)
    }
}

impl fmt::Display for ProtocolVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.major(), self.minor())
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ProtocolRange {
    Full,
    To(ProtocolVersion),
    From(ProtocolVersion),
    Between(ProtocolVersion, ProtocolVersion),
}

impl ProtocolRange {
    pub const fn from_minor(from: u8, to_inclusive: u8) -> ProtocolRange {
        ProtocolRange::Between(
            ProtocolVersion(PROTOCOL_VERSION_MAJOR, from),
            ProtocolVersion(PROTOCOL_VERSION_MAJOR, to_inclusive).next(),
        )
    }
    pub fn intersect(&self, other: &ProtocolRange) -> Option<ProtocolRange> {
        use std::ops::Bound::*;
        let (self_start, self_end) = (self.start_bound(), self.end_bound());
        let (other_start, other_end) = (other.start_bound(), other.end_bound());

        let start = match (self_start, other_start) {
            (Included(a), Included(b)) => Included(Ord::max(a, b)),
            (Excluded(a), Excluded(b)) => Excluded(Ord::max(a, b)),
            (Unbounded, Unbounded) => Unbounded,

            (x, Unbounded) | (Unbounded, x) => x,

            (Included(i), Excluded(e)) | (Excluded(e), Included(i)) => {
                if i > e {
                    Included(i)
                } else {
                    Excluded(e)
                }
            }
        };
        let end = match (self_end, other_end) {
            (Included(a), Included(b)) => Included(Ord::min(a, b)),
            (Excluded(a), Excluded(b)) => Excluded(Ord::min(a, b)),
            (Unbounded, Unbounded) => Unbounded,

            (x, Unbounded) | (Unbounded, x) => x,

            (Included(i), Excluded(e)) | (Excluded(e), Included(i)) => {
                if i < e {
                    Included(i)
                } else {
                    Excluded(e)
                }
            }
        };

        match (start, end) {
            (Included(from), Excluded(to)) => Some(Self::Between(*from, *to)),
            (Included(from), Unbounded) => Some(Self::From(*from)),
            (Unbounded, Excluded(to)) => Some(Self::To(*to)),
            (Unbounded, Unbounded) => Some(Self::Full),
            (Included(_), Included(_)) => None,
            (Unbounded, Included(_)) => None,
            (Excluded(_), Included(_)) => None,
            (Excluded(_), Excluded(_)) => None,
            (Excluded(_), Unbounded) => None,
        }
    }

    pub fn min(&self) -> ProtocolVersion {
        match self.start_bound() {
            Bound::Included(from) => *from,
            _ => ProtocolVersion::min(),
        }
    }

    pub fn max(&self) -> ProtocolVersion {
        match self.end_bound() {
            Bound::Excluded(from) => (*from).previous(),
            _ => ProtocolVersion::max(),
        }
    }
}

impl RangeBounds<ProtocolVersion> for ProtocolRange {
    fn start_bound(&self) -> Bound<&ProtocolVersion> {
        match self {
            ProtocolRange::Full => Bound::Unbounded,
            ProtocolRange::To(_to) => Bound::Unbounded,
            ProtocolRange::From(from) => Bound::Included(from),
            ProtocolRange::Between(from, _to) => Bound::Included(from),
        }
    }

    fn end_bound(&self) -> Bound<&ProtocolVersion> {
        match self {
            ProtocolRange::Full => Bound::Unbounded,
            ProtocolRange::To(to) => Bound::Excluded(to),
            ProtocolRange::From(_from) => Bound::Unbounded,
            ProtocolRange::Between(_from, to) => Bound::Excluded(to),
        }
    }
}

impl From<RangeFull> for ProtocolRange {
    fn from(_value: RangeFull) -> Self {
        ProtocolRange::Full
    }
}

impl From<RangeTo<ProtocolVersion>> for ProtocolRange {
    fn from(value: RangeTo<ProtocolVersion>) -> Self {
        ProtocolRange::To(value.end)
    }
}

impl From<RangeTo<u8>> for ProtocolRange {
    fn from(value: RangeTo<u8>) -> Self {
        ProtocolRange::To(ProtocolVersion(PROTOCOL_VERSION_MAJOR, value.end))
    }
}

impl From<RangeFrom<ProtocolVersion>> for ProtocolRange {
    fn from(value: RangeFrom<ProtocolVersion>) -> Self {
        ProtocolRange::From(value.start)
    }
}

impl From<RangeFrom<u8>> for ProtocolRange {
    fn from(value: RangeFrom<u8>) -> Self {
        ProtocolRange::From(ProtocolVersion(PROTOCOL_VERSION_MAJOR, value.start))
    }
}

impl From<Range<ProtocolVersion>> for ProtocolRange {
    fn from(value: Range<ProtocolVersion>) -> Self {
        ProtocolRange::Between(value.start, value.end)
    }
}
impl From<Range<u8>> for ProtocolRange {
    fn from(value: Range<u8>) -> Self {
        ProtocolRange::Between(
            ProtocolVersion(PROTOCOL_VERSION_MAJOR, value.start),
            ProtocolVersion(PROTOCOL_VERSION_MAJOR, value.end),
        )
    }
}

impl From<RangeToInclusive<ProtocolVersion>> for ProtocolRange {
    fn from(value: RangeToInclusive<ProtocolVersion>) -> Self {
        ProtocolRange::To(value.end.next())
    }
}

impl From<RangeToInclusive<u8>> for ProtocolRange {
    fn from(value: RangeToInclusive<u8>) -> Self {
        ProtocolRange::To(ProtocolVersion(PROTOCOL_VERSION_MAJOR, value.end).next())
    }
}

impl From<RangeInclusive<ProtocolVersion>> for ProtocolRange {
    fn from(value: RangeInclusive<ProtocolVersion>) -> Self {
        ProtocolRange::Between(*value.start(), value.end().next())
    }
}
impl From<RangeInclusive<u8>> for ProtocolRange {
    fn from(value: RangeInclusive<u8>) -> Self {
        ProtocolRange::Between(
            ProtocolVersion(PROTOCOL_VERSION_MAJOR, *value.start()),
            ProtocolVersion(PROTOCOL_VERSION_MAJOR, *value.end()).next(),
        )
    }
}

impl Default for ProtocolRange {
    fn default() -> Self {
        ProtocolRange::Between(ProtocolVersion::min(), ProtocolVersion::max().next())
    }
}

#[cfg(any(test, feature = "test"))]
mod arbitrary {
    use proptest::prelude::*;

    use super::{ProtocolRange, ProtocolVersion};

    impl Arbitrary for ProtocolVersion {
        type Parameters = ProtocolRange;
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(args: Self::Parameters) -> Self::Strategy {
            let major = args.min().major();
            (args.min().minor()..=args.max().minor())
                .prop_map(move |minor| Self::from_parts(major, minor))
                .no_shrink()
                .boxed()
        }
    }
}
