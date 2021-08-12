use std::io;
use std::sync::Arc;

use async_stream::try_stream;
use bytes::BytesMut;
use futures::Stream;
use log::trace;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;

use crate::AsyncSource;
use crate::OffsetReader;

use super::{FileType, NAREvent, NAR_VERSION_MAGIC_1};

pub fn parse_nar<R>(source: R) -> impl Stream<Item=io::Result<NAREvent>>
    where R: AsyncSource + AsyncRead + AsyncReadExt + Unpin
{
    parse_nar_ext(source, false)
}


pub fn parse_nar_ext<R>(source: R, skip_content: bool) -> impl Stream<Item=io::Result<NAREvent>>
    where R: AsyncRead + Unpin
{
    try_stream! {
        let mut source = OffsetReader::new(source);
        let start_pos = source.offset();
        match source.read_limited_string(NAR_VERSION_MAGIC_1.len()).await {
            Ok(magic) => {
                if magic != NAR_VERSION_MAGIC_1 {
                    Err(io::Error::new(
                        io::ErrorKind::InvalidData, 
                        format!("input doesn't look like a Nix archive {}!={}", magic, NAR_VERSION_MAGIC_1)))?;
                    return;
                }
                yield NAREvent::Magic(Arc::new(magic));
            },
            Err(err) => {
                if err.kind() == io::ErrorKind::UnexpectedEof {
                    Err(io::Error::new(
                        io::ErrorKind::InvalidData, 
                        "input doesn't look like a Nix archive. Unexpected EOF"))?;
                    return;        
                } else {
                    Err(err)?;
                    return;
                }
            }
        }

        let s = source.read_string().await?;
        if s != "(" {
            Err(io::Error::new(
                io::ErrorKind::InvalidData, 
                "expected open tag"))?;
            return;
        }

        let mut names = Vec::new();
        let mut prev_names : Vec<Option<String>> = Vec::new();
        let mut prev_name : Option<String> = None;
        let mut file_type = None;
        let mut executable = false;
        let mut size = 0;
        let mut got_target = false;
        let mut buf = BytesMut::with_capacity(65536);
        let cut_off = 65536 / 4;
        let mut depth = 0;
        loop {
            trace!("reading next item");
            let s = source.read_string().await?;
            trace!("read {}", s);
            if s == ")" {
                trace!("read end");
                if size == 0 && file_type == Some(FileType::Regular) {
                    trace!("{}Regular {}", " ".repeat(depth), executable);
                    yield NAREvent::RegularNode { offset: 0, size, executable };
                } else if file_type == Some(FileType::Directory) {
                    depth -= 1;
                    trace!("{}End Dir {}", " ".repeat(depth), names.len());
                    yield NAREvent::EndDirectory;
                } else if !got_target && file_type == Some(FileType::Symlink) {
                    Err(io::Error::new(
                        io::ErrorKind::InvalidData, 
                        "symlink missing target"))?;
                    break;
                }
                if names.is_empty() {
                    break;
                } else {
                    trace!("Pop names {}", names.len());
                    let s = source.read_string().await?;
                    if s != ")" {
                        Err(io::Error::new(
                            io::ErrorKind::InvalidData, 
                            format!("unknown field '{}'", s)))?;
                        return;        
                    }
                    depth -= 1;
                    trace!("{}End DirEntry {}", " ".repeat(depth), names.len());
                    yield NAREvent::EndDirectoryEntry;

                    names.pop();
                    prev_name = prev_names.pop().unwrap();
                    file_type = Some(FileType::Directory);
                    executable = false;
                    got_target = false;
                    size = 0;
                }
            } else if s == "type" {
                if file_type.is_some() {
                    Err(io::Error::new(
                        io::ErrorKind::InvalidData, 
                        "multiple type fields"))?;
                    break;
                }
                let t = source.read_string().await?;
                if t == "regular" {
                    file_type = Some(FileType::Regular);
                } else if t == "directory" {
                    trace!("{}Dir {}", " ".repeat(depth), names.len());
                    depth += 1;
                    yield NAREvent::Directory;
                    file_type = Some(FileType::Directory);
                } else if t == "symlink" {
                    file_type = Some(FileType::Symlink);
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::InvalidData, 
                        format!("unknown file type {}", t)))?;
                    break
                }
            } else if s == "executable" && file_type == Some(FileType::Regular) {
                trace!("executable");
                let c = source.read_string().await?;
                if c != "" {
                    Err(io::Error::new(
                        io::ErrorKind::InvalidData, 
                        "executable marker has non-empty value"))?;
                    break;
                }
                if size > 0 {
                    Err(io::Error::new(
                        io::ErrorKind::InvalidData, 
                        "executable marker after contents"))?;
                    break;
                }
                executable = true;
            } else if s == "contents" && file_type == Some(FileType::Regular) {
                size = source.read_u64_le().await?;
                if size > 0 {
                    let offset = source.offset() - start_pos;
                    trace!("{}Regular {} {} {} {}", " ".repeat(depth), size, offset, executable, names.len());
                    yield NAREvent::RegularNode { offset, size, executable };    
                }
                if skip_content {
                    source.drain_exact(size).await?;
                } else {
                    let mut index = 0;
                    while index < size {
                        if buf.capacity() - buf.len() < cut_off {
                            buf.reserve(cut_off);
                        }
                        trace!("Buf {} > {} - {} = {}", buf.capacity(), size, index, size - index);
                        if buf.capacity() as u64 > size - index {
                            trace!("Splitting off");
                            drop(buf.split_off((size - index) as usize));
                        }
                        trace!("Buf {} > {} - {} = {}", buf.capacity(), size, index, size - index);
                        trace!("Buf {} > {} - {} = {}", buf.capacity(), size, index, size - index);
                        source.read_buf(&mut buf).await?;
                        let data = buf.split().freeze();
                        let new_index = index + data.len() as u64;
                        yield NAREvent::Contents {
                            total: size,
                            index,
                            buf: data
                        };
                        index = new_index;
                    }
                }
                source.read_padding(size).await?;
            } else if s == "entry" && file_type == Some(FileType::Directory) {
                let s = source.read_string().await?;
                if s != "(" {
                    Err(io::Error::new(
                        io::ErrorKind::InvalidData, 
                        "expected open tag"))?;
                    return;
                }
                let mut name = None;
                loop {
                    let s = source.read_string().await?;
                    if s == ")" {
                        break;
                    } else if s == "name" {
                        trace!("read name");
                        if name.is_some() {
                            Err(io::Error::new(
                                io::ErrorKind::InvalidData, 
                                "multiple name fields"))?;
                            return;
                        }
                        let n = source.read_string().await?;
                        if n.is_empty() || n == "." || n == ".." || n.contains("/") {
                            Err(io::Error::new(
                                io::ErrorKind::InvalidData, 
                                format!("NAR contains invalid file name '{}'", n)))?;
                            return;
                        }
                        if let Some(p_name) = prev_name.as_ref() {
                            if &n <= p_name {
                                Err(io::Error::new(
                                    io::ErrorKind::InvalidData, 
                                    "NAR directory is not sorted"))?;
                                return;
                            }
                        }
                        prev_name = Some(n.clone());
                        name = Some(Arc::new(n));

                    } else if s == "node" {
                        if let Some(name) = name {
                            trace!("read node open");
                            let s = source.read_string().await?;
                            if s != "(" {
                                Err(io::Error::new(
                                    io::ErrorKind::InvalidData, 
                                    "expected open tag"))?;
                                return;
                            }
                            names.push(name.clone());
                            prev_names.push(prev_name);
                            prev_name = None;
                            file_type = None;
                            trace!("{}DirEntry {} {}", " ".repeat(depth), name, names.len());
                            depth += 1;
                            yield NAREvent::DirectoryEntry { name };
                            break;
                        } else {
                            Err(io::Error::new(
                                io::ErrorKind::InvalidData, 
                                "entry name missing"))?;
                            return;
                        }
                    } else {
                        Err(io::Error::new(
                            io::ErrorKind::InvalidData, 
                            format!("unknown field '{}'", s)))?;
                        return;
                    }
                }

            } else if s == "target" && file_type == Some(FileType::Symlink) {
                let target = Arc::new(source.read_string().await?);
                trace!("{}Symlink {}", " ".repeat(depth), names.len());
                yield NAREvent::SymlinkNode { target };
                got_target = true;
            } else {
                Err(io::Error::new(
                    io::ErrorKind::InvalidData, 
                    format!("unknown field '{}'", s)))?;
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures::TryStreamExt;
    use tokio::fs::File;
    use pretty_assertions::assert_eq;

    use crate::archive::test_data;

    use super::*;

    #[tokio::test]
    async fn test_parse_nar_dir() {
        let io = File::open("test-data/test-dir.nar").await.unwrap();
        let s = parse_nar(io)
            .try_collect::<Vec<NAREvent>>().await.unwrap();
        assert_eq!(s, test_data::dir_example());
    }

    #[tokio::test]
    async fn test_parse_nar_text() {
        let io = File::open("test-data/test-text.nar").await.unwrap();
        let s = parse_nar(io)
            .try_collect::<Vec<NAREvent>>().await.unwrap();
        let expected = vec! [
            NAREvent::Magic(Arc::new(NAR_VERSION_MAGIC_1.to_owned())),
            NAREvent::RegularNode { executable: false, size: 12, offset: 96 },
            NAREvent::Contents { total: 12, index: 0, buf: Bytes::from_static(b"Hello world!") },
        ];
        assert_eq!(s, expected);
    }

    #[tokio::test]
    async fn test_parse_nar_exec() {
        let io = File::open("test-data/test-exec.nar").await.unwrap();
        let s = parse_nar(io)
            .try_collect::<Vec<NAREvent>>().await.unwrap();
        let expected = vec! [
            NAREvent::Magic(Arc::new(NAR_VERSION_MAGIC_1.to_owned())),
            NAREvent::RegularNode { executable: true, size: 15, offset: 128 },
            NAREvent::Contents { total: 15, index: 0, buf: Bytes::from_static(b"Very cool stuff") },
        ];
        assert_eq!(s, expected);
    }
}