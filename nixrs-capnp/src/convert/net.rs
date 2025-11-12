use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use byteorder::{ByteOrder as _, NetworkEndian};
use capnp::traits::{FromPointerBuilder as _, SetterInput};

use crate::{
    capnp::ip_capnp::ip_address,
    convert::{BuildFrom, ReadFrom, ReadInto as _},
};

impl SetterInput<ip_address::Owned> for IpAddr {
    fn set_pointer_builder(
        builder: capnp::private::layout::PointerBuilder<'_>,
        input: Self,
        canonicalize: bool,
    ) -> capnp::Result<()> {
        let ipv6 = match input {
            IpAddr::V4(ipv4_addr) => ipv4_addr.to_ipv6_mapped(),
            IpAddr::V6(ipv6_addr) => ipv6_addr,
        };
        Ipv6Addr::set_pointer_builder(builder, ipv6, canonicalize)
    }
}

impl<'b> BuildFrom<IpAddr> for ip_address::Builder<'b> {
    fn build_from(&mut self, input: &IpAddr) -> Result<(), capnp::Error> {
        let ipv6 = match *input {
            IpAddr::V4(ipv4_addr) => ipv4_addr.to_ipv6_mapped(),
            IpAddr::V6(ipv6_addr) => ipv6_addr,
        };
        self.build_from(&ipv6)
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

impl SetterInput<ip_address::Owned> for Ipv4Addr {
    fn set_pointer_builder(
        builder: capnp::private::layout::PointerBuilder<'_>,
        input: Self,
        canonicalize: bool,
    ) -> capnp::Result<()> {
        Ipv6Addr::set_pointer_builder(builder, input.to_ipv6_mapped(), canonicalize)
    }
}

impl<'b> BuildFrom<Ipv4Addr> for ip_address::Builder<'b> {
    fn build_from(&mut self, input: &Ipv4Addr) -> Result<(), capnp::Error> {
        self.build_from(&input.to_ipv6_mapped())
    }
}

impl<'r> ReadFrom<ip_address::Reader<'r>> for Ipv4Addr {
    fn read_from(reader: ip_address::Reader<'r>) -> Result<Self, capnp::Error> {
        let ipv6: Ipv6Addr = reader.read_into()?;
        ipv6.to_ipv4_mapped()
            .ok_or_else(|| capnp::Error::failed(format!("ip {ipv6} is not an IPv4 mapped address")))
    }
}

impl SetterInput<ip_address::Owned> for Ipv6Addr {
    fn set_pointer_builder(
        builder: capnp::private::layout::PointerBuilder<'_>,
        input: Self,
        _canonicalize: bool,
    ) -> capnp::Result<()> {
        let mut ip = ip_address::Builder::init_pointer(builder, 0);
        let mut ul = [0u64; 2];
        NetworkEndian::read_u64_into(&input.octets(), &mut ul);
        ip.set_upper64(ul[0]);
        ip.set_lower64(ul[1]);
        Ok(())
    }
}

impl<'b> BuildFrom<Ipv6Addr> for ip_address::Builder<'b> {
    fn build_from(&mut self, input: &Ipv6Addr) -> Result<(), capnp::Error> {
        let mut ul = [0u64; 2];
        NetworkEndian::read_u64_into(&input.octets(), &mut ul);
        self.set_upper64(ul[0]);
        self.set_lower64(ul[1]);
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
