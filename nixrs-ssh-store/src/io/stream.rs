// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.
// Originally from microsoft/dev-tunnels

use super::read_buffer::ReadBuffer;
use std::io;
use std::pin::Pin;
use std::task::{ready, Poll};

use log::debug;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;

/// AsyncRead/AsyncWrite wrapper for SSH Channels
pub struct ChannelRead {
    incoming: mpsc::UnboundedReceiver<Vec<u8>>,
    readbuf: ReadBuffer,
}

impl ChannelRead {
    pub fn new() -> (Self, mpsc::UnboundedSender<Vec<u8>>) {
        let (w_tx, w_rx) = mpsc::unbounded_channel();
        (
            ChannelRead {
                incoming: w_rx,
                readbuf: ReadBuffer::default(),
            },
            w_tx,
        )
    }
}

/// AsyncRead/AsyncWrite wrapper for SSH Channels
pub struct ChannelWrite {
    outgoing: mpsc::UnboundedSender<Vec<u8>>,
    is_write_fut_valid: bool,
    write_fut: tokio_util::sync::ReusableBoxFuture<'static, Result<(), Vec<u8>>>,
}

impl ChannelWrite {
    pub fn new() -> (Self, mpsc::UnboundedReceiver<Vec<u8>>) {
        let (r_tx, r_rx) = mpsc::unbounded_channel();
        (
            ChannelWrite {
                outgoing: r_tx,
                is_write_fut_valid: false,
                write_fut: tokio_util::sync::ReusableBoxFuture::new(make_client_write_fut(None)),
            },
            r_rx,
        )
    }
}

/// Makes a future that writes to the russh handle. This general approach was
/// taken from https://docs.rs/tokio-util/0.7.3/tokio_util/sync/struct.PollSender.html
/// This is just like make_server_write_fut, but for clients (they don't share a trait...)
async fn make_client_write_fut(
    data: Option<(mpsc::UnboundedSender<Vec<u8>>, Vec<u8>)>,
) -> Result<(), Vec<u8>> {
    match data {
        Some((sender, data)) => sender.send(data).map_err(|e| e.0),
        None => unreachable!("this future should not be pollable in this state"),
    }
}

impl AsyncWrite for ChannelWrite {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        ready!(self.as_mut().poll_flush(cx))?;

        let outgoing = self.outgoing.clone();
        self.write_fut
            .set(make_client_write_fut(Some((outgoing, buf.to_vec()))));
        self.is_write_fut_valid = true;
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        if !self.is_write_fut_valid {
            return Poll::Ready(Ok(()));
        }

        match self.write_fut.poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(_)) => {
                self.is_write_fut_valid = false;
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(_)) => {
                self.is_write_fut_valid = false;
                debug!("ChannelStream AsyncWrite EOF");
                Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, "EOF")))
            }
        }
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        if let Err(err) = self.outgoing.send("".into()) {
            let err = format!("{err:?}");
            return Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, err)));
        }
        Poll::Ready(Ok(()))
    }
}

impl AsyncRead for ChannelRead {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if let Some((v, s)) = self.readbuf.take_data() {
            return self.readbuf.put_data(buf, v, s);
        }

        let x = self.incoming.poll_recv(cx);
        match x {
            Poll::Ready(Some(msg)) => self.readbuf.put_data(buf, msg, 0),
            Poll::Ready(None) => Poll::Ready(Ok(())),
            Poll::Pending => Poll::Pending,
        }
    }
}
