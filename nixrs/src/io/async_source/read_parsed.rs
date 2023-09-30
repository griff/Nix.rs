use std::future::Future;
use std::io;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use pin_project_lite::pin_project;
use tokio::io::AsyncRead;

use super::read_string::ReadString;
use super::StateParse;

pin_project! {
    #[derive(Debug)]
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct ReadParsed<R, S, T> {
        #[pin]
        inner: ReadString<R>,
        state: S,
        _result: PhantomData<T>,
    }
}

impl<R, S, T> ReadParsed<R, S, T> {
    pub fn new(src: R, state: S) -> ReadParsed<R, S, T>
    where
        S: StateParse<T>,
    {
        ReadParsed {
            inner: ReadString::new(src),
            state,
            _result: PhantomData,
        }
    }
}

impl<R, S, T> Future for ReadParsed<R, S, T>
where
    R: AsyncRead + Unpin,
    S: StateParse<T>,
    S::Err: From<io::Error>,
{
    type Output = Result<T, S::Err>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut me = self.project();
        let s = ready!(me.inner.as_mut().poll(cx))?;
        Poll::Ready(me.state.parse(&s))
    }
}
