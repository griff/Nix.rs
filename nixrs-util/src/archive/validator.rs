use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Sink;

use super::NAREvent;

enum Location {
    Init,
    Root,
    Contents(u64, u64),
    Directory,
    Entry,
}

pub struct NARValidateError {}

pub struct NARValidator {
    loc: Location,
    depth: usize,
}

impl Sink<NAREvent> for NARValidator {
    type Error = NARValidateError;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: NAREvent) -> Result<(), Self::Error> {
        todo!()
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}
