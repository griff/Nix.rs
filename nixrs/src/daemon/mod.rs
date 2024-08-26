use std::fmt;

pub mod de;
#[cfg(feature="nixrs-derive")]
mod types;

pub trait DaemonStore {
    
}

pub const PROTOCOL_VERSION: ProtocolVersion = ProtocolVersion::from_parts(1, 35);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProtocolVersion(u8, u8);
impl ProtocolVersion {
    pub const fn from_parts(major: u8, minor: u8) -> ProtocolVersion {
        ProtocolVersion(major, minor)
    }

    #[inline]
    pub const fn major(&self) -> u8 {
        self.0
    }

    #[inline]
    pub const fn minor(&self) -> u8 {
        self.1
    }
}

impl Default for ProtocolVersion {
    fn default() -> Self {
        PROTOCOL_VERSION
    }
}

impl From<u16> for ProtocolVersion {
    fn from(value: u16) -> Self {
        ProtocolVersion::from_parts((value & 0xff00 >> 8) as u8, (value & 0x00ff) as u8)
    }
}

impl From<(u8, u8)> for ProtocolVersion {
    fn from((major, minor): (u8, u8)) -> Self {
        ProtocolVersion::from_parts(major, minor)
    }
}

impl From<ProtocolVersion> for u16 {
    fn from(value: ProtocolVersion) -> Self {
        (value.major() as u16) << 8 | (value.minor() as u16)
    }
}

impl fmt::Display for ProtocolVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.major(), self.minor())
    }
}
