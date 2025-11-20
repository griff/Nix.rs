use std::ffi::{OsStr, OsString};
use std::io;
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::rc::Rc;
use std::{fs::Metadata, path::PathBuf};

use bstr::{ByteSlice as _, ByteVec as _};
use capnp::Error as CapError;
use capnp::capability::{FromClientHook, Promise};
use capnp_rpc::{new_client, pry};
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
    fn node(
        &mut self,
        _params: node_access::NodeParams,
        mut result: node_access::NodeResults,
    ) -> Promise<(), CapError> {
        if self.inner.metadata.is_dir() {
            let mut nb = result.get().init_node();
            nb.set_name(pry!(self.name()));
            nb.set_directory(());
            Promise::ok(())
        } else if self.inner.metadata.is_symlink() {
            let me = self.clone();
            Promise::from_future(async move {
                let mut nb = result.get().init_node();
                nb.set_name(me.name()?);
                let mut b = nb.init_symlink();
                let target = read_link(&me.inner.path).await?;
                let target = Vec::from_os_string(target.into_os_string()).map_err(|target_s| {
                    io::Error::other(format!("target {target_s:?} not valid UTF-8"))
                })?;
                b.set_target(&target[..]);
                Ok(())
            })
        } else if self.inner.metadata.is_file() {
            let mut nb = result.get().init_node();
            nb.set_name(pry!(self.name()));
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
            Promise::ok(())
        } else {
            Promise::err(CapError::failed("node has invalid type".into()))
        }
    }

    fn as_directory(
        &mut self,
        _params: node_access::AsDirectoryParams,
        mut result: node_access::AsDirectoryResults,
    ) -> Promise<(), CapError> {
        if self.inner.metadata.is_dir() {
            let client = new_client(self.clone());
            result.get().set_directory(client);
            Promise::ok(())
        } else {
            Promise::err(CapError::failed("node is not a directory".into()))
        }
    }

    fn as_file(
        &mut self,
        _params: node_access::AsFileParams,
        mut result: node_access::AsFileResults,
    ) -> Promise<(), CapError> {
        if self.inner.metadata.is_file() {
            let client = new_client(PathFileAccess {
                path: self.inner.path.clone(),
                metadata: self.inner.metadata.clone(),
            });
            result.get().set_file(client);
            Promise::ok(())
        } else {
            Promise::err(CapError::failed("node is not a file".into()))
        }
    }

    fn get_name(
        &mut self,
        _params: node_access::GetNameParams,
        mut result: node_access::GetNameResults,
    ) -> Promise<(), CapError> {
        let file_name = pry!(self.name());
        result.get().set_name(file_name);
        Promise::ok(())
    }

    fn stream(
        &mut self,
        params: node_access::StreamParams,
        _result: node_access::StreamResults,
    ) -> Promise<(), CapError> {
        let access = new_client(self.clone());
        Promise::from_future(async move {
            let handler = params.get()?.get_handler()?;
            stream_access_to_handler(access, handler).await
        })
    }
}

impl directory_access::Server for PathNodeAccess {
    fn get_size(
        &mut self,
        _params: directory_access::GetSizeParams,
        mut result: directory_access::GetSizeResults,
    ) -> Promise<(), CapError> {
        result.get().set_size(self.inner.metadata.len());
        Promise::ok(())
    }

    fn get_extra(
        &mut self,
        _params: directory_access::GetExtraParams,
        _result: directory_access::GetExtraResults,
    ) -> Promise<(), CapError> {
        Promise::ok(())
    }

    fn get_extras(
        &mut self,
        _params: directory_access::GetExtrasParams,
        mut result: directory_access::GetExtrasResults,
    ) -> Promise<(), CapError> {
        result.get().init_extras(0);
        Promise::ok(())
    }

    fn list(
        &mut self,
        _params: directory_access::ListParams,
        _result: directory_access::ListResults,
    ) -> Promise<(), CapError> {
        let path = self.inner.path.clone();
        let use_case_hack = self.inner.use_case_hack;
        Promise::from_future(async move {
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
        })
    }

    fn lookup(
        &mut self,
        params: directory_access::LookupParams,
        mut result: directory_access::LookupResults,
    ) -> Promise<(), CapError> {
        let path = pry!(join_name(
            &self.inner.path,
            pry!(pry!(params.get()).get_name())
        ));
        let use_case_hack = self.inner.use_case_hack;
        Promise::from_future(async move {
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
        })
    }
}

pub struct PathFileAccess {
    path: PathBuf,
    metadata: Metadata,
}

impl blob::Server for PathFileAccess {
    fn get_size(
        &mut self,
        _params: blob::GetSizeParams,
        mut result: blob::GetSizeResults,
    ) -> Promise<(), CapError> {
        result.get().set_size(self.metadata.len());
        Promise::ok(())
    }

    fn write_to(
        &mut self,
        params: blob::WriteToParams,
        _result: blob::WriteToResults,
    ) -> Promise<(), CapError> {
        let r = pry!(params.get());
        let offset = r.get_start_at_offset();
        let stream = pry!(r.get_stream());
        let path = self.path.clone();
        Promise::from_future(async move {
            let mut writer = capnp_rpc_tokio::stream::ByteStreamWriter::new(stream);
            let mut reader = BufReader::new(File::open(path).await?);
            reader.seek(io::SeekFrom::Start(offset)).await?;
            copy_buf(&mut reader, &mut writer).await?;
            Ok(())
        })
    }

    fn get_slice(
        &mut self,
        params: blob::GetSliceParams,
        mut result: blob::GetSliceResults,
    ) -> Promise<(), CapError> {
        let r = pry!(params.get());
        let offset = r.get_offset();
        let size = r.get_size();
        let path = self.path.clone();
        Promise::from_future(async move {
            let mut buf = Vec::new();
            let mut reader = File::open(path).await?;
            reader.seek(io::SeekFrom::Start(offset)).await?;
            reader.take(size as u64).read_to_end(&mut buf).await?;
            result.get().set_data(&buf[..]);
            Ok(())
        })
    }
}

impl file_access::Server for PathFileAccess {
    fn get_executable(
        &mut self,
        _params: file_access::GetExecutableParams,
        mut result: file_access::GetExecutableResults,
    ) -> Promise<(), CapError> {
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
        Promise::ok(())
    }

    fn get_extra(
        &mut self,
        _params: file_access::GetExtraParams,
        _result: file_access::GetExtraResults,
    ) -> Promise<(), CapError> {
        Promise::ok(())
    }

    fn get_extras(
        &mut self,
        _params: file_access::GetExtrasParams,
        mut result: file_access::GetExtrasResults,
    ) -> Promise<(), CapError> {
        result.get().init_extras(0);
        Promise::ok(())
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
