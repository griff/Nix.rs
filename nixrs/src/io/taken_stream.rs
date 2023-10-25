use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::oneshot;

#[derive(Debug)]
enum Inner<R> {
    Invalid,
    Available(R),
    Taken(oneshot::Receiver<R>),
}

#[derive(Debug)]
pub struct TakenStream<R> {
    inner: Arc<Mutex<Inner<R>>>,
}

impl<R> TakenStream<R>
where
    R: Unpin,
{
    pub fn new(reader: R) -> TakenStream<R> {
        TakenStream {
            inner: Arc::new(Mutex::new(Inner::Available(reader))),
        }
    }

    pub fn taker(&self) -> Taker<R> {
        Taker {
            inner: self.inner.clone(),
        }
    }

    fn poll_available<F, T>(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        poll_fn: F,
    ) -> Poll<io::Result<T>>
    where
        F: FnOnce(Pin<&mut R>, &mut Context<'_>) -> Poll<io::Result<T>>,
    {
        let mut guard = self.inner.lock().unwrap();
        loop {
            match std::mem::replace(&mut *guard, Inner::Invalid) {
                Inner::Invalid => panic!("TakenStream is invalid"),
                Inner::Taken(mut rec) => match Pin::new(&mut rec).poll(cx) {
                    Poll::Pending => {
                        *guard = Inner::Taken(rec);
                        return Poll::Pending;
                    }
                    Poll::Ready(Ok(reader)) => {
                        *guard = Inner::Available(reader);
                    }
                    Poll::Ready(Err(_)) => {
                        *guard = Inner::Taken(rec);
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::Other,
                            "TakenStream sender was dropped",
                        )));
                    }
                },
                Inner::Available(mut reader) => {
                    let res = poll_fn(Pin::new(&mut reader), cx);
                    *guard = Inner::Available(reader);
                    return res;
                }
            }
        }
    }
}

impl<R> AsyncRead for TakenStream<R>
where
    R: AsyncRead + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.poll_available(cx, |reader, cx| reader.poll_read(cx, buf))
        /*
        let mut guard = self.inner.lock().unwrap();
        loop {
            match std::mem::replace(&mut *guard, Inner::Invalid) {
                Inner::Invalid => panic!("TakenStream is invalid"),
                Inner::Taken(mut rec) => match Pin::new(&mut rec).poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(reader)) => {
                        *guard = Inner::Available(reader);
                    }
                    Poll::Ready(Err(_)) => {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::Other,
                            "TakenReader sender was dropped",
                        )));
                    }
                },
                Inner::Available(mut reader) => {
                    let res = Pin::new(&mut reader).poll_read(cx, buf);
                    *guard = Inner::Available(reader);
                    return res;
                }
            }
        }
         */
    }
}

impl<W> AsyncWrite for TakenStream<W>
where
    W: AsyncWrite + Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        self.poll_available(cx, |reader, cx| reader.poll_write(cx, buf))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        self.poll_available(cx, |reader, cx| reader.poll_flush(cx))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        self.poll_available(cx, |reader, cx| reader.poll_shutdown(cx))
    }
}

pub struct Taker<S> {
    inner: Arc<Mutex<Inner<S>>>,
}

impl<S> Clone for Taker<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<S> Taker<S> {
    pub fn take(&self) -> TakenGuard<S> {
        let mut guard = self.inner.lock().unwrap();
        match std::mem::replace(&mut *guard, Inner::Invalid) {
            Inner::Invalid => panic!("TakenReader is invalid"),
            Inner::Taken(mut rec) => {
                if let Ok(reader) = rec.try_recv() {
                    let (tx, rx) = oneshot::channel();
                    *guard = Inner::Taken(rx);
                    TakenGuard {
                        stream: Some(reader),
                        sender: Some(tx),
                    }
                } else {
                    panic!("Reader can only be taken once")
                }
            }
            Inner::Available(reader) => {
                let (tx, rx) = oneshot::channel();
                *guard = Inner::Taken(rx);
                TakenGuard {
                    stream: Some(reader),
                    sender: Some(tx),
                }
            }
        }
    }
}

pub struct TakenGuard<S> {
    stream: Option<S>,
    sender: Option<oneshot::Sender<S>>,
}

impl<S> Drop for TakenGuard<S> {
    fn drop(&mut self) {
        if let Some(reader) = self.stream.take() {
            if let Some(sender) = self.sender.take() {
                sender.send(reader).ok();
            }
        }
    }
}

impl<R> AsyncRead for TakenGuard<R>
where
    R: AsyncRead + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.stream.as_mut() {
            None => panic!("poll_read called after drop"),
            Some(r) => Pin::new(r).poll_read(cx, buf),
        }
    }
}

impl<W> AsyncWrite for TakenGuard<W>
where
    W: AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        match self.stream.as_mut() {
            None => panic!("poll_read called after drop"),
            Some(r) => Pin::new(r).poll_write(cx, buf),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        match self.stream.as_mut() {
            None => panic!("poll_read called after drop"),
            Some(r) => Pin::new(r).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        match self.stream.as_mut() {
            None => panic!("poll_read called after drop"),
            Some(r) => Pin::new(r).poll_shutdown(cx),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::from_utf8;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::spawn;

    use super::*;

    #[tokio::test]
    async fn read_data() {
        let (mut client, server) = tokio::io::duplex(64);
        let mut take_reader = TakenStream::new(server);

        client.write_all(b"ping").await.unwrap();

        let mut buf = [0u8; 4];
        take_reader.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ping");

        client.write_all(b"completed").await.unwrap();
        client.shutdown().await.unwrap();

        let mut buf = Vec::new();
        take_reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(&buf, b"completed");
    }

    #[tokio::test]
    async fn write_data() {
        let (mut client, server) = tokio::io::duplex(64);
        let mut take_writer = TakenStream::new(server);

        take_writer.write_all(b"ping").await.unwrap();

        let mut buf = [0u8; 4];
        client.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ping");

        take_writer.write_all(b"completed").await.unwrap();
        take_writer.shutdown().await.unwrap();

        let mut buf = Vec::new();
        client.read_to_end(&mut buf).await.unwrap();
        assert_eq!(&buf, b"completed");
    }

    #[tokio::test]
    async fn take_reader() {
        let (mut client, server) = tokio::io::duplex(64);
        let mut take_reader = TakenStream::new(server);

        client.write_all(b"ping").await.unwrap();
        let mut buf = [0u8; 4];
        take_reader.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ping");

        client.write_all(b"pong").await.unwrap();
        {
            let mut t = take_reader.taker().take();
            let mut buf = [0u8; 4];
            t.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"pong");
        }

        client.write_all(b"completed").await.unwrap();
        client.shutdown().await.unwrap();

        let mut buf = Vec::new();
        take_reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(&buf, b"completed");
    }

    #[tokio::test]
    async fn double_shutdown() {
        let (mut client, server) = tokio::io::duplex(64);
        let mut take_reader = TakenStream::new(server);

        client.write_all(b"ping").await.unwrap();
        let mut buf = [0u8; 4];
        take_reader.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ping");

        client.write_all(b"completed").await.unwrap();
        client.shutdown().await.unwrap();
        {
            let mut t = take_reader.taker().take();
            let mut buf = Vec::new();
            t.read_to_end(&mut buf).await.unwrap();
            assert_eq!(&buf, b"completed");
        }

        let mut buf = Vec::new();
        take_reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(&buf, b"");
    }

    #[tokio::test]
    async fn interrupted_forward() {
        let (mut client, server) = tokio::io::duplex(64);
        let mut take_reader = TakenStream::new(server);
        let taker = take_reader.taker();

        client.write_all(b"ping").await.unwrap();
        let mut buf = [0u8; 4];
        take_reader.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ping");

        let (tx, rx) = oneshot::channel();
        client.write_all(b"completed").await.unwrap();
        let join = spawn(async move {
            let mut buf2 = [0u8; 9];
            take_reader.read_exact(&mut buf2).await.unwrap();
            tx.send(()).unwrap();
            let mut buf = Vec::new();
            buf.extend_from_slice(&buf2);
            take_reader.read_to_end(&mut buf).await?;
            Ok(buf) as io::Result<_>
        });
        rx.await.unwrap();
        {
            let mut t = taker.take();
            client.write_all(b"log").await.unwrap();
            let mut buf = [0u8; 3];
            t.read_exact(&mut buf).await.unwrap();
            assert_eq!(from_utf8(&buf).unwrap(), "log");
        }
        client.write_all(b"...done").await.unwrap();
        client.shutdown().await.unwrap();
        let buf = join.await.unwrap().unwrap();
        assert_eq!(&buf, b"completed...done");
    }

    #[tokio::test]
    async fn double_take() {
        let (mut client, server) = tokio::io::duplex(64);
        let take_reader = TakenStream::new(server);
        let taker = take_reader.taker();

        {
            let mut t = taker.take();
            client.write_all(b"log").await.unwrap();
            let mut buf = [0u8; 3];
            t.read_exact(&mut buf).await.unwrap();
            assert_eq!(from_utf8(&buf).unwrap(), "log");
        }
        {
            let mut t = taker.take();
            client.write_all(b"log").await.unwrap();
            let mut buf = [0u8; 3];
            t.read_exact(&mut buf).await.unwrap();
            assert_eq!(from_utf8(&buf).unwrap(), "log");
        }
    }
}
