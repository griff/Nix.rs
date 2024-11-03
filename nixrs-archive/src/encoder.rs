use std::io;

use bytes::BufMut;
use bytes::BytesMut;
use tokio_util::codec::Encoder;
use tracing::debug;

use nixrs_io::calc_padding;

use super::NAREvent;

impl NAREvent {
    pub fn encoded_size(&self) -> usize {
        match self {
            NAREvent::Magic(magic) => {
                let bytes = magic.as_bytes();
                let padding = calc_padding(bytes.len() as u64) as usize;
                // 1 * u64 = 8 bytes
                8 + bytes.len() + padding
            }
            NAREvent::RegularNode {
                offset: _,
                executable,
                size,
            } => {
                // 5 * u64 = 40 bytes
                // 32 bytes strings
                let mut needed = 40 + 32;
                if *executable {
                    // 2 * u64 = 16 bytes
                    // 16 bytes strings
                    needed += 16 + 16;
                }
                if *size == 0 {
                    // When size is 0 no Contents events are sent so also include
                    // size of that:
                    // 1 * u64 = 8
                    // 8 byte from string
                    needed += 8 + 8;
                }
                needed
            }
            NAREvent::Contents { total, index, buf } => {
                if buf.len() as u64 + index == *total {
                    // Last contents buffer
                    let padding = calc_padding(*total) as usize;
                    // 1 * u64 = 8 bytes
                    // 8 bytes from strings
                    buf.len() + padding + 8 + 8
                } else {
                    buf.len()
                }
            }
            NAREvent::SymlinkNode { target } => {
                let padding = calc_padding(target.len() as u64) as usize;
                // 6 * u64 = 48 bytes
                // 40 bytes from strings
                48 + 40 + target.len() + padding
            }
            NAREvent::Directory => {
                // 3 * u64 = 24 bytes
                // 32 bytes from strings
                24 + 32
            }
            NAREvent::DirectoryEntry { name } => {
                let padding = calc_padding(name.len() as u64) as usize;
                // 5 * u64 = 40 bytes
                // 32 bytes from strings
                40 + 32 + name.len() + padding
            }
            NAREvent::EndDirectory | NAREvent::EndDirectoryEntry => {
                // 1 * u64 = 8 bytes
                // 8 byte from string
                8 + 8
            }
        }
    }

    pub fn encode_into(&self, dst: &mut BytesMut) {
        dst.reserve(self.encoded_size());
        match self {
            NAREvent::Magic(magic) => {
                let bytes = magic.as_bytes();
                let padding = calc_padding(bytes.len() as u64) as usize;
                dst.put_u64_le(bytes.len() as u64);
                dst.put_slice(bytes);
                if padding > 0 {
                    let zero = [0u8; 8];
                    dst.put_slice(&zero[..padding]);
                }
            }
            NAREvent::RegularNode {
                offset: _,
                executable,
                size,
            } => {
                dst.put_u64_le(1);
                dst.put_slice(b"(\0\0\0\0\0\0\0");
                dst.put_u64_le(4);
                dst.put_slice(b"type\0\0\0\0");
                dst.put_u64_le(7);
                dst.put_slice(b"regular\0");
                if *executable {
                    dst.put_u64_le(10);
                    dst.put_slice(b"executable\0\0\0\0\0\0");
                    dst.put_u64_le(0);
                }
                dst.put_u64_le(8);
                dst.put_slice(b"contents");
                dst.put_u64_le(*size);
                if *size == 0 {
                    // When size is 0 no Contents events are sent so close the node.
                    dst.put_u64_le(1);
                    dst.put_slice(b")\0\0\0\0\0\0\0");
                }
            }
            NAREvent::Contents { total, index, buf } => {
                if buf.len() as u64 + index == *total {
                    // Last contents buffer
                    let padding = calc_padding(*total) as usize;
                    dst.put_slice(buf);
                    if padding > 0 {
                        let zero = [0u8; 8];
                        dst.put_slice(&zero[0..padding]);
                    }
                    dst.put_u64_le(1);
                    dst.put_slice(b")\0\0\0\0\0\0\0");
                } else {
                    dst.extend_from_slice(buf);
                }
            }
            NAREvent::SymlinkNode { target } => {
                let padding = calc_padding(target.len() as u64) as usize;
                dst.put_u64_le(1);
                dst.put_slice(b"(\0\0\0\0\0\0\0");
                dst.put_u64_le(4);
                dst.put_slice(b"type\0\0\0\0");
                dst.put_u64_le(7);
                dst.put_slice(b"symlink\0");
                dst.put_u64_le(6);
                dst.put_slice(b"target\0\0");
                dst.put_u64_le(target.len() as u64);
                dst.put_slice(target);
                if padding > 0 {
                    let zero = [0u8; 8];
                    dst.put_slice(&zero[0..padding]);
                }
                dst.put_u64_le(1);
                dst.put_slice(b")\0\0\0\0\0\0\0");
            }
            NAREvent::Directory => {
                dst.put_u64_le(1);
                dst.put_slice(b"(\0\0\0\0\0\0\0");
                dst.put_u64_le(4);
                dst.put_slice(b"type\0\0\0\0");
                dst.put_u64_le(9);
                dst.put_slice(b"directory\0\0\0\0\0\0\0");
            }
            NAREvent::DirectoryEntry { name } => {
                let padding = calc_padding(name.len() as u64) as usize;
                dst.put_u64_le(5);
                dst.put_slice(b"entry\0\0\0");
                dst.put_u64_le(1);
                dst.put_slice(b"(\0\0\0\0\0\0\0");
                dst.put_u64_le(4);
                dst.put_slice(b"name\0\0\0\0");
                dst.put_u64_le(name.len() as u64);
                dst.put_slice(name);
                if padding > 0 {
                    let zero = [0u8; 8];
                    dst.put_slice(&zero[..padding]);
                }
                dst.put_u64_le(4);
                dst.put_slice(b"node\0\0\0\0");
            }
            NAREvent::EndDirectory | NAREvent::EndDirectoryEntry => {
                dst.put_u64_le(1);
                dst.put_slice(b")\0\0\0\0\0\0\0");
            }
        }
    }
}

pub struct NAREncoder;

impl Encoder<NAREvent> for NAREncoder {
    type Error = io::Error;

    fn encode(&mut self, item: NAREvent, dst: &mut BytesMut) -> Result<(), Self::Error> {
        debug!("Encode {} {:?}", item.encoded_size(), item);
        item.encode_into(dst);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use futures::{stream::iter, StreamExt, TryStreamExt};
    use pretty_assertions::assert_eq;
    use std::io;
    use tempfile::tempdir;
    use tokio::fs::{self, File};
    use tokio_util::{codec::FramedWrite, io::InspectWriter};

    use crate::{parse_nar, proptest::arb_nar_events, test_data};
    use crate::pretty_prop_assert_eq;
    use proptest::proptest;

    use super::*;

    #[tokio::test]
    async fn test_encode_nar_dir() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test-dir.nar");

        let io = File::create(&path).await.unwrap();
        let encoder = FramedWrite::new(io, NAREncoder);
        let stream = iter(test_data::dir_example()).map(|e| Ok(e) as io::Result<NAREvent>);
        stream.forward(encoder).await.unwrap();

        let io = File::open(path).await.unwrap();
        let s = parse_nar(io).try_collect::<Vec<NAREvent>>().await.unwrap();
        assert_eq!(s, test_data::dir_example());
    }

    #[tokio::test]
    async fn test_encode_nar_text() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test-text.nar");

        let io = File::create(&path).await.unwrap();
        let encoder = FramedWrite::new(io, NAREncoder);
        let stream = iter(test_data::text_file()).map(|e| Ok(e) as io::Result<NAREvent>);
        stream.forward(encoder).await.unwrap();

        let io = File::open(path).await.unwrap();
        let s = parse_nar(io).try_collect::<Vec<NAREvent>>().await.unwrap();
        assert_eq!(s, test_data::text_file());
    }

    #[tokio::test]
    async fn test_encode_nar_exec() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test-exec.nar");

        let io = File::create(&path).await.unwrap();
        let encoder = FramedWrite::new(io, NAREncoder);
        let stream = iter(test_data::exec_file()).map(|e| Ok(e) as io::Result<NAREvent>);
        stream.forward(encoder).await.unwrap();

        let io = File::open(path).await.unwrap();
        let s = parse_nar(io).try_collect::<Vec<NAREvent>>().await.unwrap();
        assert_eq!(s, test_data::exec_file());
    }

    #[tokio::test]
    async fn test_encode_nar_empty() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test-empty.nar");

        let io = File::create(&path).await.unwrap();
        let encoder = FramedWrite::new(io, NAREncoder);
        let stream = iter(test_data::empty_file()).map(|e| Ok(e) as io::Result<NAREvent>);
        stream.forward(encoder).await.unwrap();

        let io = File::open(path).await.unwrap();
        let s = parse_nar(io).try_collect::<Vec<NAREvent>>().await.unwrap();
        assert_eq!(s, test_data::empty_file());
    }

    #[tokio::test]
    async fn test_encode_nar_empty_file_in_dir() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test-empty.nar");

        let io = File::create(&path).await.unwrap();
        let encoder = FramedWrite::new(io, NAREncoder);
        let stream = iter(test_data::empty_file_in_dir()).map(|e| Ok(e) as io::Result<NAREvent>);
        stream.forward(encoder).await.unwrap();

        let io = File::open(path).await.unwrap();
        let s = parse_nar(io).try_collect::<Vec<NAREvent>>().await.unwrap();
        assert_eq!(s, test_data::empty_file_in_dir());
    }

    #[test]
    fn proptest_encode_parse() {
        let r = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        proptest!(|(events in arb_nar_events(8, 256, 10))| {
            let mut ctx = ring::digest::Context::new(&ring::digest::SHA256);
            let mut size = 0;
            let mut buf = BytesMut::with_capacity(65_000);
            for item in events.iter() {
                size += item.encoded_size() as u64;
                item.encode_into(&mut buf);
                ctx.update(&buf);
                buf.clear()
            }
            let hash = ctx.finish();

            r.block_on(async {
                let dir = tempdir()?;
                let path = dir.path().join("encode_parse.nar");
                let stream = iter(events.clone().into_iter())
                    .map(|e| Ok(e) as io::Result<NAREvent> );

                let io = File::create(&path).await?;
                let mut ctx = ring::digest::Context::new(&ring::digest::SHA256);
                let mut nar_size = 0;
                let iio = InspectWriter::new(io, |buf| {
                    ctx.update(buf);
                    nar_size += buf.len() as u64;
                });
                let encoder = FramedWrite::new(iio, NAREncoder);
                stream.forward(encoder).await?;
                let nar_hash = ctx.finish();

                let io = File::open(&path).await?;
                let s = parse_nar(io)
                    .try_collect::<Vec<NAREvent>>().await?;
                pretty_prop_assert_eq!(s, events);

                pretty_prop_assert_eq!(fs::metadata(&path).await?.len(), size);
                pretty_prop_assert_eq!(nar_size, size);
                pretty_prop_assert_eq!(nar_hash.as_ref(), hash.as_ref());
                Ok(())
            })?;

        });
    }
}
