use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::{Mutex, Arc};
use std::task::{Poll, Context};

use tokio::io::AsyncRead;
use tokio::sync::oneshot;


enum Inner<R> {
    Invalid,
    Available(R),
    Taken(oneshot::Receiver<R>),
}

pub struct TakenReader<R> {
    inner: Arc<Mutex<Inner<R>>>,
}

impl<R> TakenReader<R> {
    pub fn new(reader: R) -> TakenReader<R> {
        TakenReader {
            inner: Arc::new(Mutex::new(Inner::Available(reader))),
        }
    }

    pub fn taker(&self) -> Taker<R> {
        Taker { inner: self.inner.clone() }
    }
}

impl<R> AsyncRead for TakenReader<R>
    where R: AsyncRead + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut guard = self.inner.lock().unwrap();
        loop {
            match std::mem::replace(&mut *guard, Inner::Invalid) {
                Inner::Invalid => panic!("TakenReader is invalid"),
                Inner::Taken(mut rec) => {
                    match Pin::new(&mut rec).poll(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Ok(reader)) => {
                            *guard = Inner::Available(reader);
                        },
                        Poll::Ready(Err(_)) => {
                            return Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, "TakenReader sender was dropped")));
                        }
                    }
                },
                Inner::Available(mut reader) => {
                    let res = Pin::new(&mut reader).poll_read(cx, buf);
                    *guard = Inner::Available(reader);
                    return res
                }
            }    
        }
    }
}

pub struct Taker<R> {
    inner: Arc<Mutex<Inner<R>>>,
}

impl<R> Clone for Taker<R> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

impl<R> Taker<R> {
    pub fn take(&self) -> TakenGuard<R> {
        let mut guard = self.inner.lock().unwrap();
        match std::mem::replace(&mut *guard, Inner::Invalid) {
            Inner::Invalid => panic!("TakenReader is invalid"),
            Inner::Taken(_) => panic!("Reader can only be taken once"),
            Inner::Available(reader) => {
                let (tx, rx) = oneshot::channel();
                *guard = Inner::Taken(rx);
                TakenGuard {
                    reader: Some(reader),
                    sender: Some(tx),
                }
            }
        }
    }
}

pub struct TakenGuard<R> {
    reader: Option<R>,
    sender: Option<oneshot::Sender<R>>,
}

impl<R> Drop for TakenGuard<R> {
    fn drop(&mut self) {
        if let Some(reader) = self.reader.take() {
            if let Some(sender) = self.sender.take() {
                sender.send(reader).ok();
            }
        }
    }
}

impl<R> AsyncRead for TakenGuard<R>
    where R: AsyncRead + Unpin
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.reader.as_mut() {
            None => panic!("poll_read called after drop"),
            Some(r) => {
                Pin::new(r).poll_read(cx, buf)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::from_utf8;

    use tokio::io::{AsyncWriteExt, AsyncReadExt};
    use tokio::spawn;

    use super::*;

    #[tokio::test]
    async fn read_data() {
        let (mut client, server) = tokio::io::duplex(64);
        let mut take_reader = TakenReader::new(server);

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
    async fn take_reader() {
        let (mut client, server) = tokio::io::duplex(64);
        let mut take_reader = TakenReader::new(server);

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
        let mut take_reader = TakenReader::new(server);

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
        let mut take_reader = TakenReader::new(server);
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
}