use std::cmp::min;
use std::fmt;
use std::io::Cursor;
use std::pin::pin;

use futures::{Sink, SinkExt};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt};
use tracing::{trace, warn};

use crate::daemon::de::NixRead;
use crate::daemon::ser::{NixWrite, NixWriter};
use crate::daemon::wire::logger::RawLogMessage;
use crate::daemon::{DaemonError, DaemonErrorKind, DaemonResult};
use crate::log::{LogMessage, Message, Verbosity};

pub async fn read_logs<R, S, E>(reader: R, sender: S) -> DaemonResult<()>
where
    R: NixRead + AsyncRead + fmt::Debug + Unpin + Send,
    DaemonError: From<<R as NixRead>::Error> + From<E>,
    S: Sink<LogMessage, Error = E>,
{
    ProcessStderr::new(reader).forward_logs(sender).await
}

pub struct ProcessStderr<R, W, SR> {
    reader: R,
    writer: Option<W>,
    source: Option<SR>,
}

impl<R> ProcessStderr<R, NixWriter<Cursor<Vec<u8>>>, Cursor<Vec<u8>>> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            writer: None,
            source: None,
        }
    }
}

impl<R, W, SR> ProcessStderr<R, W, SR> {
    pub fn with_source<NW, NSR>(self, writer: NW, source: NSR) -> ProcessStderr<R, NW, NSR> {
        ProcessStderr {
            reader: self.reader,
            writer: Some(writer),
            source: Some(source),
        }
    }
}

impl<R, W, SR> ProcessStderr<R, W, SR>
where
    W: NixWrite + AsyncWrite + fmt::Debug + Unpin + Send,
    DaemonError: From<<W as NixWrite>::Error>,
    SR: AsyncBufRead + Unpin + Send,
{
    async fn process_read(&mut self, len: usize) -> Result<(), DaemonError> {
        if let Some(source) = self.source.as_mut() {
            let buf = source.fill_buf().await?;
            let writer = self.writer.as_mut().unwrap();
            let len = min(len, buf.len());
            writer.write_slice(&buf[..len]).await?;
            writer.flush().await?;
            source.consume(len);
            Ok(())
        } else {
            Err(DaemonErrorKind::NoSourceForLoggerRead.into())
        }
    }
}

impl<R, W, SR> ProcessStderr<R, W, SR>
where
    R: NixRead + AsyncRead + fmt::Debug + Unpin + Send,
    W: NixWrite + AsyncWrite + fmt::Debug + Unpin + Send,
    DaemonError: From<<W as NixWrite>::Error> + From<<R as NixRead>::Error>,
    SR: AsyncBufRead + Unpin + Send,
{
    pub async fn forward_logs<S, E>(mut self, sender: S) -> DaemonResult<()>
    where
        S: Sink<LogMessage, Error = E>,
        DaemonError: From<E>,
    {
        let mut sink = pin!(sender);
        loop {
            trace!("Reading log message");
            let msg = self.reader.read_value::<RawLogMessage>().await?;
            trace!(?msg, "Got log message");
            match msg {
                RawLogMessage::Next(text) => {
                    sink.send(LogMessage::Message(Message {
                        text,
                        level: Verbosity::Error,
                    }))
                    .await?;
                }
                RawLogMessage::Result(result) => {
                    sink.send(LogMessage::Result(result)).await?;
                }
                RawLogMessage::StartActivity(act) => {
                    sink.send(LogMessage::StartActivity(act)).await?;
                }
                RawLogMessage::StopActivity(act) => {
                    sink.send(LogMessage::StopActivity(act)).await?;
                }
                RawLogMessage::Read(len) => {
                    self.process_read(len).await?;
                }
                RawLogMessage::Write(buf) => {
                    warn!("Got unexpected write message of {} bytes", buf.len());
                }
                RawLogMessage::Last => {
                    return Ok(());
                }
                RawLogMessage::Error(err) => {
                    return Err(err.into());
                }
            }
        }
    }
}
