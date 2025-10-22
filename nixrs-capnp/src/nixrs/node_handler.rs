use std::{
    pin::pin,
    task::{Poll, ready},
};

use bytes::Bytes;
use capnp::capability::Promise;
use capnp_rpc::{new_client, pry};
use capnp_rpc_tokio::stream::{ByteStreamWrap, ByteStreamWriter};
use futures::{Sink, SinkExt as _, Stream, TryFutureExt as _, TryStreamExt as _, channel::mpsc};
use nixrs::archive::{NarEvent, parse_nar};
use pin_project_lite::pin_project;
use tokio::io::{
    AsyncBufRead, AsyncRead, AsyncWriteExt as _, BufReader, ReadHalf, SimplexStream, copy_buf,
    simplex,
};

use crate::{DEFAULT_BUF_SIZE, capnp::nixrs_capnp::node_handler};

pin_project! {
    pub struct NodeHandlerSink {
        handler: node_handler::Client,
        closing: bool,
        #[pin]
        active_send: Option<Promise<(), capnp::Error>>,
    }
}

impl NodeHandlerSink {
    pub fn new(handler: node_handler::Client) -> NodeHandlerSink {
        NodeHandlerSink {
            handler,
            closing: false,
            active_send: None,
        }
    }

    fn poll_active(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<capnp::Result<()>> {
        let mut me = self.project();
        if let Some(active_send) = me.active_send.as_mut().as_pin_mut() {
            match active_send.poll(cx) {
                Poll::Ready(res) => {
                    *me.active_send = None;
                    Poll::Ready(res)
                }
                Poll::Pending => Poll::Pending,
            }
        } else {
            Poll::Ready(Ok(()))
        }
    }
}

impl<R> Sink<NarEvent<R>> for NodeHandlerSink
where
    R: AsyncBufRead + 'static,
{
    type Error = capnp::Error;

    fn poll_ready(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        if self.closing {
            return Poll::Ready(Err(capnp::Error::failed("Already closed".into())));
        }
        self.poll_active(cx)
    }

    fn start_send(
        mut self: std::pin::Pin<&mut Self>,
        item: NarEvent<R>,
    ) -> Result<(), Self::Error> {
        if self.active_send.is_some() {
            return Err(capnp::Error::failed("Already active send".into()));
        }
        let handler = self.handler.clone();
        self.active_send = Some(Promise::from_future(async move {
            match item {
                NarEvent::File {
                    name,
                    executable,
                    size,
                    reader,
                } => {
                    let mut req = handler.file_request();
                    let mut b = req.get();
                    b.set_name(&*name);
                    b.set_executable(executable);
                    b.set_size(size);
                    let res = req.send().promise.await?;
                    let r = res.get()?;
                    if r.has_write_to() {
                        let mut writer = ByteStreamWriter::new(r.get_write_to()?);
                        let mut reader = pin!(reader);
                        copy_buf(&mut reader, &mut writer).await?;
                        writer.shutdown().await?;
                    }
                }
                NarEvent::Symlink { name, target } => {
                    let mut req = handler.symlink_request();
                    let mut b = req.get();
                    b.set_name(&*name);
                    b.set_target(&*target);
                    req.send().await?;
                }
                NarEvent::StartDirectory { name } => {
                    let mut req = handler.start_directory_request();
                    req.get().set_name(&*name);
                    req.send().await?;
                }
                NarEvent::EndDirectory => {
                    handler.finish_directory_request().send().await?;
                }
            }
            Ok(())
        }));
        Ok(())
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        if self.closing {
            return Poll::Ready(Err(capnp::Error::failed("Already closed".into())));
        }
        self.poll_active(cx)
    }

    fn poll_close(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        ready!(self.as_mut().poll_active(cx))?;
        if !self.closing {
            self.closing = true;
            self.active_send = Some(Promise::from_future(
                self.handler.end_request().send().promise.map_ok(|_| ()),
            ));
            ready!(self.poll_active(cx))?;
        }
        Poll::Ready(Ok(()))
    }
}

pub async fn nar_to_handler<S, U, R>(stream: S, handler: node_handler::Client) -> capnp::Result<()>
where
    S: Stream<Item = U>,
    U: Into<capnp::Result<NarEvent<R>>>,
    R: AsyncBufRead + 'static,
{
    use futures::stream::StreamExt as _;
    let restorer = NodeHandlerSink::new(handler);
    stream.map(|item| item.into()).forward(restorer).await
}

pub async fn nar_reader_to_handler<R>(reader: R, handler: node_handler::Client) -> capnp::Result<()>
where
    R: AsyncRead + Unpin + 'static,
{
    nar_to_handler(parse_nar(reader).map_err(From::from), handler).await
}

type PushEvent = NarEvent<BufReader<ReadHalf<SimplexStream>>>;
pub fn nar_handler_channel(buffer: usize) -> (node_handler::Client, mpsc::Receiver<PushEvent>) {
    let (sender, receiver) = mpsc::channel(buffer);
    let push = NodeHandlerPush { sender };
    (new_client(push), receiver)
}

pub struct NodeHandlerPush {
    sender: mpsc::Sender<PushEvent>,
}

impl NodeHandlerPush {
    fn send_event(&self, event: PushEvent) -> Promise<(), capnp::Error> {
        let mut sender = self.sender.clone();
        Promise::from_future(async move {
            sender
                .send(event)
                .await
                .map_err(|err| capnp::Error::failed(err.to_string()))
        })
    }
}

impl node_handler::Server for NodeHandlerPush {
    fn symlink(&mut self, params: node_handler::SymlinkParams) -> Promise<(), capnp::Error> {
        let r = pry!(params.get());
        let name = Bytes::copy_from_slice(pry!(r.get_name()));
        let target = Bytes::copy_from_slice(pry!(r.get_target()));
        self.send_event(NarEvent::Symlink { name, target })
    }

    fn file(
        &mut self,
        params: node_handler::FileParams,
        mut result: node_handler::FileResults,
    ) -> Promise<(), capnp::Error> {
        let (reader, writer) = simplex(DEFAULT_BUF_SIZE);
        let reader = BufReader::new(reader);
        let writer = new_client(ByteStreamWrap::new(writer));
        result.get().set_write_to(writer);

        let r = pry!(params.get());
        let name = Bytes::copy_from_slice(pry!(r.get_name()));
        let size = r.get_size();
        let executable = r.get_executable();

        self.send_event(NarEvent::File {
            name,
            executable,
            size,
            reader,
        })
    }

    fn start_directory(
        &mut self,
        params: node_handler::StartDirectoryParams,
    ) -> Promise<(), capnp::Error> {
        let r = pry!(params.get());
        let name = Bytes::copy_from_slice(pry!(r.get_name()));
        self.send_event(NarEvent::StartDirectory { name })
    }

    fn finish_directory(
        &mut self,
        _: node_handler::FinishDirectoryParams,
    ) -> Promise<(), capnp::Error> {
        self.send_event(NarEvent::EndDirectory)
    }

    fn end(
        &mut self,
        _: node_handler::EndParams,
        _: node_handler::EndResults,
    ) -> Promise<(), capnp::Error> {
        self.sender.close_channel();
        Promise::ok(())
    }
}
