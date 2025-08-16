use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::{io, mem};

use capnp::Error;
use capnp::capability::Promise;
use capnp_rpc::pry;
use futures::TryFutureExt as _;
use pin_project_lite::pin_project;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::oneshot;

use crate::byte_stream_capnp::byte_stream;

pub fn from_cap_error(err: capnp::Error) -> io::Error {
    let kind = match err.kind {
        capnp::ErrorKind::Overloaded => io::ErrorKind::TimedOut,
        capnp::ErrorKind::Disconnected => io::ErrorKind::ConnectionAborted,
        _ => io::ErrorKind::Other,
    };
    io::Error::new(kind, format!("{err}"))
}

pin_project! {
    #[project = ByteStreamWriterProj]
    pub struct ByteStreamWriter {
        inner: crate::byte_stream_capnp::byte_stream::Client,
        #[pin]
        req: Option<Promise<(), Error>>,
        shutdown: bool,
    }
}

impl ByteStreamWriter {
    pub fn new(client: crate::byte_stream_capnp::byte_stream::Client) -> Self {
        Self {
            inner: client,
            req: None,
            shutdown: false,
        }
    }
}

impl ByteStreamWriterProj<'_> {
    fn poll_req(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        if let Some(req) = self.req.as_mut().as_pin_mut() {
            ready!(req.poll(cx)).map_err(from_cap_error)?;
        }
        self.req.set(None);
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for ByteStreamWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        let mut this = self.project();
        ready!(this.poll_req(cx))?;
        if *this.shutdown {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "connection shutdown",
            )));
        }
        let mut req = this.inner.write_request();
        req.get().set_bytes(buf);
        this.req.set(Some(req.send()));
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        let mut this = self.project();
        ready!(this.poll_req(cx))?;
        if *this.shutdown {
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "connection shutdown",
            )))
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        let mut this = self.project();
        ready!(this.poll_req(cx))?;
        if !*this.shutdown {
            *this.shutdown = true;
            let req = this.inner.end_request();
            this.req.set(Some(Promise::from_future(
                req.send().promise.map_ok(|_| ()),
            )));
        }
        this.poll_req(cx)
    }
}

struct BsWriteParams {
    params: byte_stream::WriteParams,
}

impl BsWriteParams {
    fn new(params: byte_stream::WriteParams) -> Result<BsWriteParams, capnp::Error> {
        {
            params.get()?.get_bytes()?;
        }
        Ok(BsWriteParams { params })
    }
}

impl AsRef<[u8]> for BsWriteParams {
    fn as_ref(&self) -> &[u8] {
        self.params.get().unwrap().get_bytes().unwrap()
    }
}

#[derive(Debug, Default)]
enum WriteState<W> {
    #[default]
    None,
    Writeable(W),
    Writing(oneshot::Receiver<W>),
    Finished,
}

impl<W> WriteState<W>
where
    W: AsyncWrite + Unpin + 'static,
{
    fn write(&mut self, data: BsWriteParams) -> Promise<(), capnp::Error> {
        match mem::take(self) {
            WriteState::Writeable(mut writer) => {
                let (c, rec) = oneshot::channel();
                let p = async move {
                    writer.write_all(data.as_ref()).await?;
                    c.send(writer)
                        .map_err(|_| capnp::Error::failed("Receiver dropped".into()))?;
                    Ok(())
                };
                *self = WriteState::Writing(rec);
                Promise::from_future(p)
            }
            WriteState::Writing(fut) => {
                let (c, rec) = oneshot::channel();
                let p = async move {
                    let mut writer: W = fut
                        .await
                        .map_err(|err| capnp::Error::failed(format!("{err}")))?;
                    writer.write_all(data.as_ref()).await?;
                    c.send(writer)
                        .map_err(|_| capnp::Error::failed("Receiver dropped".into()))?;
                    Ok(())
                };
                *self = WriteState::Writing(rec);
                Promise::from_future(p)
            }
            e => {
                *self = e;
                Promise::err(capnp::Error::failed("invalid state".to_string()))
            }
        }
    }

    fn end(&mut self) -> Promise<(), capnp::Error> {
        match mem::replace(self, WriteState::None) {
            WriteState::Writeable(mut writer) => {
                let p = async move {
                    writer.shutdown().await.map_err(capnp::Error::from)?;
                    Ok(())
                };
                *self = WriteState::Finished;
                Promise::from_future(p)
            }
            WriteState::Writing(rec) => {
                let p = async move {
                    let mut writer = rec
                        .await
                        .map_err(|err| capnp::Error::failed(format!("{err}")))?;
                    writer.shutdown().await.map_err(capnp::Error::from)?;
                    Ok(())
                };
                *self = WriteState::Finished;
                Promise::from_future(p)
            }
            e => {
                *self = e;
                Promise::err(capnp::Error::failed("invalid state".to_string()))
            }
        }
    }
}

pub struct ByteStreamWrap<W> {
    state: WriteState<W>,
}

impl<W> ByteStreamWrap<W> {
    pub fn new(writer: W) -> ByteStreamWrap<W> {
        ByteStreamWrap {
            state: WriteState::Writeable(writer),
        }
    }
}

impl<W> byte_stream::Server for ByteStreamWrap<W>
where
    W: AsyncWrite + Unpin + 'static,
{
    fn write(&mut self, params: byte_stream::WriteParams) -> Promise<(), capnp::Error> {
        let data = pry!(BsWriteParams::new(params));
        self.state.write(data)
    }
    fn end(
        &mut self,
        _: byte_stream::EndParams,
        _: byte_stream::EndResults,
    ) -> Promise<(), capnp::Error> {
        self.state.end()
    }
}
