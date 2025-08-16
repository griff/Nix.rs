use std::{future::Future, task::Poll};

use futures::{
    FutureExt,
    future::{Fuse, FusedFuture},
};
use pin_project_lite::pin_project;
use tokio::{io::AsyncRead, sync::oneshot};

pub struct CancelToken(oneshot::Sender<()>);

impl CancelToken {
    pub fn cancel(self) {
        self.0.send(()).ok();
    }
}

pin_project! {
    pub struct CancelledReader<R> {
        #[pin]
        reader: R,
        #[pin]
        cancel: Fuse<oneshot::Receiver<()>>,
    }
}
impl<R> CancelledReader<R> {
    pub fn new(reader: R) -> (CancelledReader<R>, CancelToken) {
        let (tx, rx) = oneshot::channel();
        let cancel = rx.fuse();
        (CancelledReader { reader, cancel }, CancelToken(tx))
    }

    pub fn into_inner(self) -> R {
        self.reader
    }
}

impl<R: AsyncRead> AsyncRead for CancelledReader<R> {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.cancel.is_terminated() {
            return Poll::Ready(Ok(()));
        }
        let mut this = self.project();
        match this.reader.as_mut().poll_read(cx, buf) {
            Poll::Ready(Ok(_)) => Poll::Ready(Ok(())),
            Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
            Poll::Pending => match this.cancel.poll(cx) {
                Poll::Ready(_) => Poll::Ready(Ok(())),
                Poll::Pending => Poll::Pending,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
    use tokio::spawn;

    use super::*;

    #[tokio::test]
    async fn read_data() {
        let (mut client, server) = tokio::io::duplex(64);
        let (mut reader, _cancel) = CancelledReader::new(server);

        client.write_all(b"ping").await.unwrap();
        let mut buf = [0u8; 4];
        reader.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ping");

        client.write_all(b"completed").await.unwrap();
        client.shutdown().await.unwrap();

        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(&buf, b"completed");
    }

    #[tokio::test]
    async fn read_data2() {
        let (mut client, server) = tokio::io::duplex(64);
        let (mut reader, cancel) = CancelledReader::new(server);

        client.write_all(b"ping").await.unwrap();
        let mut buf = [0u8; 4];
        reader.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ping");

        let join = spawn(async move {
            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).await?;

            Ok((buf, reader.into_inner())) as io::Result<(Vec<u8>, DuplexStream)>
        });
        eprintln!("Cancel");
        cancel.cancel();
        eprintln!("Canceled");
        let (buf, mut reader) = join.await.unwrap().unwrap();
        assert_eq!(&buf, b"");

        client.write_all(b"completed").await.unwrap();
        client.shutdown().await.unwrap();

        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(&buf, b"completed");
    }
}
