use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use byteorder::{ByteOrder as _, NetworkEndian};
use capnp_convert::{BuildFrom as _, ReadFrom, ReadInto as _, SetInto};

use crate::capnp::ip_capnp::ip_address;

impl<'b> SetInto<ip_address::Builder<'b>> for IpAddr {
    fn set_into(&self, builder: &mut ip_address::Builder<'b>) -> capnp::Result<()> {
        let ipv6 = match *self {
            IpAddr::V4(ipv4_addr) => ipv4_addr.to_ipv6_mapped(),
            IpAddr::V6(ipv6_addr) => ipv6_addr,
        };
        builder.build_from(&ipv6)
    }
}

impl<'r> ReadFrom<ip_address::Reader<'r>> for IpAddr {
    fn read_from(reader: ip_address::Reader<'r>) -> Result<Self, capnp::Error> {
        let ipv6: Ipv6Addr = reader.read_into()?;
        if let Some(ipv4) = ipv6.to_ipv4_mapped() {
            Ok(IpAddr::V4(ipv4))
        } else {
            Ok(IpAddr::V6(ipv6))
        }
    }
}

impl<'b> SetInto<ip_address::Builder<'b>> for Ipv4Addr {
    fn set_into(&self, builder: &mut ip_address::Builder<'b>) -> capnp::Result<()> {
        builder.build_from(&self.to_ipv6_mapped())
    }
}

impl<'r> ReadFrom<ip_address::Reader<'r>> for Ipv4Addr {
    fn read_from(reader: ip_address::Reader<'r>) -> Result<Self, capnp::Error> {
        let ipv6: Ipv6Addr = reader.read_into()?;
        ipv6.to_ipv4_mapped()
            .ok_or_else(|| capnp::Error::failed(format!("ip {ipv6} is not an IPv4 mapped address")))
    }
}

impl<'b> SetInto<ip_address::Builder<'b>> for Ipv6Addr {
    fn set_into(&self, builder: &mut ip_address::Builder<'b>) -> capnp::Result<()> {
        let mut ul = [0u64; 2];
        NetworkEndian::read_u64_into(&self.octets(), &mut ul);
        builder.set_upper64(ul[0]);
        builder.set_lower64(ul[1]);
        Ok(())
    }
}

impl<'r> ReadFrom<ip_address::Reader<'r>> for Ipv6Addr {
    fn read_from(reader: ip_address::Reader<'r>) -> Result<Self, capnp::Error> {
        let mut ul = [0u64; 2];
        ul[0] = reader.get_upper64();
        ul[1] = reader.get_lower64();
        let mut octets = [0u8; 16];
        NetworkEndian::write_u64_into(&ul, &mut octets);
        let bits = NetworkEndian::read_u128(&octets);
        Ok(Ipv6Addr::from_bits(bits))
    }
}
