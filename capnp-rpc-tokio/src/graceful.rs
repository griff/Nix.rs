use std::fmt;
use std::pin::Pin;
use std::task;

use pin_project_lite::pin_project;
use tokio::sync::watch;

pub trait GracefulConnection: Future<Output = capnp::Result<()>> + crate::private::Sealed {
    fn poll_graceful_shutdown(
        self: Pin<&mut Self>,
        cx: &mut task::Context,
    ) -> task::Poll<capnp::Result<()>>;
}

pub struct GracefulShutdown {
    tx: watch::Sender<()>,
}

pub struct Watcher {
    rx: watch::Receiver<()>,
}

impl GracefulShutdown {
    /// Create a new graceful shutdown helper.
    pub fn new() -> Self {
        let (tx, _) = watch::channel(());
        Self { tx }
    }

    pub fn watch<C: GracefulConnection>(&self, conn: C) -> impl Future<Output = capnp::Result<()>> {
        self.watcher().watch(conn)
    }

    pub fn watcher(&self) -> Watcher {
        let rx = self.tx.subscribe();
        Watcher { rx }
    }

    pub fn shutdown_background(self) {
        let Self { tx } = self;
        let _ = tx.send(());
    }

    pub async fn shutdown(self) {
        let Self { tx } = self;
        let _ = tx.send(());
        tx.closed().await;
    }

    pub fn count(&self) -> usize {
        self.tx.receiver_count()
    }
}

impl Default for GracefulShutdown {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for GracefulShutdown {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GracefulShutdown").finish()
    }
}

impl Watcher {
    pub fn watch<C: GracefulConnection>(self, conn: C) -> impl Future<Output = capnp::Result<()>> {
        let Self { mut rx } = self;
        GracefulConnectionFuture::new(conn, async move {
            let _ = rx.changed().await;
            // hold onto the rx until the watched future is completed
            rx
        })
    }
}

pin_project! {
    struct GracefulConnectionFuture<C: GracefulConnection, F: Future> {
        #[pin]
        conn: C,
        #[pin]
        cancel: F,
        // If cancelled, this is held until the inner conn is done.
        cancelled_guard: Option<F::Output>,
    }
}

impl<C: GracefulConnection, F: Future> GracefulConnectionFuture<C, F> {
    fn new(conn: C, cancel: F) -> Self {
        Self {
            conn,
            cancel,
            cancelled_guard: None,
        }
    }
}

impl<C: GracefulConnection, F: Future> fmt::Debug for GracefulConnectionFuture<C, F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GracefulConnectionFuture").finish()
    }
}

impl<C, F> Future for GracefulConnectionFuture<C, F>
where
    C: GracefulConnection,
    F: Future,
{
    type Output = capnp::Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        let mut this = self.project();
        if this.cancelled_guard.is_none()
            && let task::Poll::Ready(guard) = this.cancel.poll(cx)
        {
            *this.cancelled_guard = Some(guard);
        }
        if this.cancelled_guard.is_some() {
            match (
                this.conn.as_mut().poll(cx)?,
                this.conn.as_mut().poll_graceful_shutdown(cx)?,
            ) {
                (task::Poll::Ready(_), task::Poll::Ready(_)) => task::Poll::Ready(Ok(())),
                _ => task::Poll::Pending,
            }
        } else {
            this.conn.poll(cx)
        }
    }
}
