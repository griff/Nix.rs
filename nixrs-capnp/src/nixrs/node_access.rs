use std::ffi::{OsStr, OsString};
use std::io;
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::rc::Rc;
use std::{fs::Metadata, path::PathBuf};

use bstr::{ByteSlice as _, ByteVec as _};
use capnp::Error as CapError;
use capnp::capability::FromClientHook;
use capnp_rpc::{new_client, new_client_from_rc};
use nixrs::archive::CASE_HACK_SUFFIX;
use tokio::fs::{File, read_dir, read_link, symlink_metadata};
use tokio::io::{AsyncReadExt as _, AsyncSeekExt, BufReader, copy_buf};

use crate::capnp::nixrs_capnp::{blob, directory_access, file_access, node_access, node_handler};

pub async fn stream_access_to_handler(
    access: node_access::Client,
    handler: node_handler::Client,
) -> capnp::Result<()> {
    let mut stack = vec![vec![access].into_iter()];
    while let Some(mut cur_dir) = stack.pop() {
        while let Some(access) = cur_dir.next() {
            let node = access.node_request().send().promise.await?;
            let r = node.get()?.get_node()?;
            match r.which()? {
                crate::capnp::nixrs_capnp::node::Which::Directory(_) => {
                    let entries = access
                        .as_directory_request()
                        .send()
                        .pipeline
                        .get_directory()
                        .list_request()
                        .send()
                        .promise
                        .await?;
                    let dir = entries
                        .get()?
                        .get_list()?
                        .iter()
                        .collect::<capnp::Result<Vec<_>>>()?;
                    stack.push(cur_dir);
                    cur_dir = dir.into_iter();
                    let mut req = handler.start_directory_request();
                    req.get().set_name(r.get_name()?);
                    req.send().await?;
                }
                crate::capnp::nixrs_capnp::node::Which::File(file) => {
                    let mut req = handler.file_request();
                    let mut b = req.get();
                    b.set_executable(file.get_executable());
                    b.set_size(file.get_size());
                    b.set_name(r.get_name()?);
                    let res = req.send().promise.await?;
                    let r = res.get()?;
                    if r.has_write_to() {
                        let blob: blob::Client = access
                            .as_file_request()
                            .send()
                            .pipeline
                            .get_file()
                            .cast_to();
                        let mut req = blob.write_to_request();
                        req.get().set_stream(r.get_write_to()?);
                        req.send().promise.await?;
                    }
                }
                crate::capnp::nixrs_capnp::node::Which::Symlink(symlink) => {
                    let mut req = handler.symlink_request();
                    let mut b = req.get();
                    b.set_target(symlink.get_target()?);
                    b.set_name(r.get_name()?);
                    req.send().await?;
                }
            }
        }
        handler.finish_directory_request().send().await?;
    }
    handler.end_request().send().promise.await?;
    Ok(())
}

struct InnerPathNodeAccess {
    name: OsString,
    path: PathBuf,
    metadata: Metadata,
    use_case_hack: bool,
}

#[derive(Clone)]
pub struct PathNodeAccess {
    inner: Rc<InnerPathNodeAccess>,
}

impl PathNodeAccess {
    fn name(&self) -> capnp::Result<&[u8]> {
        <[u8]>::from_os_str(&self.inner.name)
            .ok_or_else(|| CapError::failed("missing file name or not valid UTF-8".into()))
    }
}

impl node_access::Server for PathNodeAccess {
    async fn node(
        self: Rc<Self>,
        _params: node_access::NodeParams,
        mut result: node_access::NodeResults,
    ) -> capnp::Result<()> {
        if self.inner.metadata.is_dir() {
            let mut nb = result.get().init_node();
            nb.set_name(self.name()?);
            nb.set_directory(());
            Ok(())
        } else if self.inner.metadata.is_symlink() {
            let me = self.clone();
            let mut nb = result.get().init_node();
            nb.set_name(me.name()?);
            let mut b = nb.init_symlink();
            let target = read_link(&me.inner.path).await?;
            let target = Vec::from_os_string(target.into_os_string()).map_err(|target_s| {
                io::Error::other(format!("target {target_s:?} not valid UTF-8"))
            })?;
            b.set_target(&target[..]);
            Ok(())
        } else if self.inner.metadata.is_file() {
            let mut nb = result.get().init_node();
            nb.set_name(self.name()?);
            let mut b = nb.init_file();
            #[cfg(unix)]
            {
                let mode = self.inner.metadata.permissions().mode();
                b.set_executable(mode & 0o100 == 0o100);
            }
            #[cfg(not(unix))]
            {
                b.set_executable(false);
            }
            b.set_size(self.inner.metadata.len());
            Ok(())
        } else {
            Err(capnp::Error::failed("node has invalid type".into()))
        }
    }

    async fn as_directory(
        self: Rc<Self>,
        _params: node_access::AsDirectoryParams,
        mut result: node_access::AsDirectoryResults,
    ) -> capnp::Result<()> {
        if self.inner.metadata.is_dir() {
            let client = new_client_from_rc(self.clone());
            result.get().set_directory(client);
            Ok(())
        } else {
            Err(capnp::Error::failed("node is not a directory".into()))
        }
    }

    async fn as_file(
        self: Rc<Self>,
        _params: node_access::AsFileParams,
        mut result: node_access::AsFileResults,
    ) -> capnp::Result<()> {
        if self.inner.metadata.is_file() {
            let client = new_client(PathFileAccess {
                path: self.inner.path.clone(),
                metadata: self.inner.metadata.clone(),
            });
            result.get().set_file(client);
            Ok(())
        } else {
            Err(capnp::Error::failed("node is not a file".into()))
        }
    }

    async fn get_name(
        self: Rc<Self>,
        _params: node_access::GetNameParams,
        mut result: node_access::GetNameResults,
    ) -> capnp::Result<()> {
        let file_name = self.name()?;
        result.get().set_name(file_name);
        Ok(())
    }

    async fn stream(
        self: Rc<Self>,
        params: node_access::StreamParams,
        _result: node_access::StreamResults,
    ) -> capnp::Result<()> {
        let access = new_client_from_rc(self.clone());
        let handler = params.get()?.get_handler()?;
        stream_access_to_handler(access, handler).await
    }
}

impl directory_access::Server for PathNodeAccess {
    async fn get_size(
        self: Rc<Self>,
        _params: directory_access::GetSizeParams,
        mut result: directory_access::GetSizeResults,
    ) -> capnp::Result<()> {
        result.get().set_size(self.inner.metadata.len());
        Ok(())
    }

    async fn get_extra(
        self: Rc<Self>,
        _params: directory_access::GetExtraParams,
        _result: directory_access::GetExtraResults,
    ) -> capnp::Result<()> {
        Ok(())
    }

    async fn get_extras(
        self: Rc<Self>,
        _params: directory_access::GetExtrasParams,
        mut result: directory_access::GetExtrasResults,
    ) -> capnp::Result<()> {
        result.get().init_extras(0);
        Ok(())
    }

    async fn list(
        self: Rc<Self>,
        _params: directory_access::ListParams,
        _result: directory_access::ListResults,
    ) -> capnp::Result<()> {
        let path = self.inner.path.clone();
        let use_case_hack = self.inner.use_case_hack;
        let mut rd = read_dir(&path).await?;
        let mut items = Vec::new();
        while let Some(entry) = rd.next_entry().await? {
            let path = entry.path();
            let metadata = entry.metadata().await?;
            let name = if use_case_hack {
                remove_case_hack_osstr(&entry.file_name())
                    .map(ToOwned::to_owned)
                    .unwrap_or_else(|| entry.file_name())
            } else {
                entry.file_name()
            };
            items.push(PathNodeAccess {
                inner: Rc::new(InnerPathNodeAccess {
                    path,
                    metadata,
                    name,
                    use_case_hack,
                }),
            });
        }
        items.sort_by(|a, b| a.inner.name.cmp(&b.inner.name));
        Ok(())
    }

    async fn lookup(
        self: Rc<Self>,
        params: directory_access::LookupParams,
        mut result: directory_access::LookupResults,
    ) -> capnp::Result<()> {
        let path = join_name(&self.inner.path, params.get()?.get_name()?)?;
        let use_case_hack = self.inner.use_case_hack;
        match symlink_metadata(&path).await {
            Ok(metadata) => {
                let name = path.file_name().unwrap().to_owned();
                let client = new_client(PathNodeAccess {
                    inner: Rc::new(InnerPathNodeAccess {
                        path,
                        metadata,
                        name,
                        use_case_hack,
                    }),
                });
                result.get().set_node(client);
                Ok(())
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                if use_case_hack {
                    let mut rd = read_dir(path.parent().unwrap()).await?;
                    let p_name = OsStr::from_bytes(params.get()?.get_name()?);
                    while let Some(entry) = rd.next_entry().await? {
                        if let Some(name) = remove_case_hack_osstr(&entry.file_name())
                            && name == p_name
                        {
                            let name = name.to_os_string();
                            let path = entry.path();
                            let metadata = entry.metadata().await?;
                            let client = new_client(PathNodeAccess {
                                inner: Rc::new(InnerPathNodeAccess {
                                    path,
                                    metadata,
                                    name,
                                    use_case_hack,
                                }),
                            });
                            result.get().set_node(client);
                            break;
                        }
                    }
                }
                Ok(())
            }
            Err(err) => Err(err.into()),
        }
    }
}

pub struct PathFileAccess {
    path: PathBuf,
    metadata: Metadata,
}

impl blob::Server for PathFileAccess {
    async fn get_size(
        self: Rc<Self>,
        _params: blob::GetSizeParams,
        mut result: blob::GetSizeResults,
    ) -> capnp::Result<()> {
        result.get().set_size(self.metadata.len());
        Ok(())
    }

    async fn write_to(
        self: Rc<Self>,
        params: blob::WriteToParams,
        _result: blob::WriteToResults,
    ) -> capnp::Result<()> {
        let r = params.get()?;
        let offset = r.get_start_at_offset();
        let stream = r.get_stream()?;
        let path = self.path.clone();
        let mut writer = capnp_rpc_tokio::stream::ByteStreamWriter::new(stream);
        let mut reader = BufReader::new(File::open(path).await?);
        reader.seek(io::SeekFrom::Start(offset)).await?;
        copy_buf(&mut reader, &mut writer).await?;
        Ok(())
    }

    async fn get_slice(
        self: Rc<Self>,
        params: blob::GetSliceParams,
        mut result: blob::GetSliceResults,
    ) -> capnp::Result<()> {
        let r = params.get()?;
        let offset = r.get_offset();
        let size = r.get_size();
        let path = self.path.clone();
        let mut buf = Vec::new();
        let mut reader = File::open(path).await?;
        reader.seek(io::SeekFrom::Start(offset)).await?;
        reader.take(size as u64).read_to_end(&mut buf).await?;
        result.get().set_data(&buf[..]);
        Ok(())
    }
}

impl file_access::Server for PathFileAccess {
    async fn get_executable(
        self: Rc<Self>,
        _params: file_access::GetExecutableParams,
        mut result: file_access::GetExecutableResults,
    ) -> capnp::Result<()> {
        let mut b = result.get();
        #[cfg(unix)]
        {
            let mode = self.metadata.permissions().mode();
            b.set_flag(mode & 0o100 == 0o100);
        }
        #[cfg(not(unix))]
        {
            b.set_flag(false);
        }
        Ok(())
    }

    async fn get_extra(
        self: Rc<Self>,
        _params: file_access::GetExtraParams,
        _result: file_access::GetExtraResults,
    ) -> capnp::Result<()> {
        Ok(())
    }

    async fn get_extras(
        self: Rc<Self>,
        _params: file_access::GetExtrasParams,
        mut result: file_access::GetExtrasResults,
    ) -> capnp::Result<()> {
        result.get().init_extras(0);
        Ok(())
    }
}

fn remove_case_hack_osstr(name: &OsStr) -> Option<&OsStr> {
    if let Some(n) = <[u8]>::from_os_str(name)
        && let Some(pos) = n.rfind(CASE_HACK_SUFFIX)
    {
        return Some(OsStr::from_bytes(&n[..pos]));
    }
    None
}

fn join_name(path: &Path, name: &[u8]) -> Result<PathBuf, io::Error> {
    if name.is_empty() {
        Ok(path.to_owned())
    } else {
        let name_os = name.to_os_str().map_err(|err| {
            let lossy = name.to_os_str_lossy();
            let path = path.join(lossy);
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid UTF-8 making path {} {err:?}", path.display()),
            )
        })?;
        Ok(path.join(name_os))
    }
}
