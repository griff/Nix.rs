use std::io::Cursor;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tracing::{debug, error, info};

use crate::io::{AsyncSink, AsyncSource};
use crate::store::activity::{
    ActivityId, ActivityLogger, ActivityType, LoggerField, LoggerFieldType, ResultType,
};
use crate::store::daemon::{
    get_protocol_minor, STDERR_ERROR, STDERR_LAST, STDERR_NEXT, STDERR_READ, STDERR_RESULT,
    STDERR_START_ACTIVITY, STDERR_STOP_ACTIVITY, STDERR_WRITE,
};
use crate::store::error::Verbosity;
use crate::store::Error;

async fn read_fields<R: AsyncRead + Unpin>(mut source: R) -> Result<Vec<LoggerField>, Error> {
    let size = source.read_usize().await?;
    let mut ret = Vec::with_capacity(size);
    for _ in 0..size {
        let field_type: LoggerFieldType = source.read_enum().await?;
        match field_type {
            LoggerFieldType::Int => ret.push(LoggerField::Int(source.read_u64_le().await?)),
            LoggerFieldType::String => {
                ret.push(LoggerField::String(source.read_string().await?));
            }
            LoggerFieldType::Invalid(val) => {
                return Err(Error::UnsupportedFieldType(val));
            }
        }
    }
    Ok(ret)
}

pub struct ProcessStderr<R, W, SR, SW> {
    logger: ActivityLogger,
    daemon_version: u64,
    from: R,
    to: Option<W>,
    source: Option<SR>,
    sink: Option<SW>,
}
impl<R> ProcessStderr<R, Cursor<Vec<u8>>, Cursor<Vec<u8>>, Cursor<Vec<u8>>> {
    pub fn new(logger: ActivityLogger, daemon_version: u64, from: R) -> Self {
        ProcessStderr {
            logger,
            daemon_version,
            from,
            to: None,
            source: None,
            sink: None,
        }
    }
}

impl<R, W, SR, SW> ProcessStderr<R, W, SR, SW> {
    pub fn with_source<NW, NSR>(self, to: NW, source: NSR) -> ProcessStderr<R, NW, NSR, SW> {
        ProcessStderr {
            logger: self.logger,
            daemon_version: self.daemon_version,
            from: self.from,
            to: Some(to),
            source: Some(source),
            sink: self.sink,
        }
    }

    /*
    pub fn with_sink<NSW>(self, sink: NSW) -> ProcessStderr<R, W, SR, NSW> {
        ProcessStderr { daemon_version: self.daemon_version, from: self.from, to: self.to, source: self.source, sink: Some(sink) }
    }
     */

    pub async fn run(mut self) -> Result<(), Error>
    where
        R: AsyncRead + Unpin,
        W: AsyncWrite + Unpin,
        SR: AsyncRead + Unpin,
        SW: AsyncWrite + Unpin,
    {
        let mut buf = Vec::new();
        loop {
            let msg = self.from.read_u64_le().await?;
            match msg {
                STDERR_WRITE => {
                    debug!("Got STDERR_WRITE");
                    let s = self.from.read_string().await?;
                    if let Some(sink) = self.sink.as_mut() {
                        sink.write_all(s.as_bytes()).await?;
                    } else {
                        return Err(Error::NoSink);
                    }
                }
                STDERR_READ => {
                    debug!("Got STDERR_READ");
                    if let Some(source) = self.source.as_mut() {
                        let mut to = self.to.as_mut().unwrap();
                        let len = source.read_usize().await?;
                        if buf.capacity() < len {
                            buf.reserve(len);
                        }
                        let read = source.read(&mut buf).await?;
                        AsyncSink::write_buf(&mut to, &buf[0..read]).await?;
                        buf.clear();
                        to.flush().await?;
                    } else {
                        return Err(Error::NoSource);
                    }
                }
                STDERR_ERROR => {
                    debug!("Got STDERR_ERROR");
                    if get_protocol_minor!(self.daemon_version) >= 26 {
                        let error_type = self.from.read_string().await?;
                        assert_eq!(error_type, "Error");
                        let level: Verbosity = self.from.read_enum().await?;
                        let _name = self.from.read_string().await?; // Removed
                        let msg = self.from.read_string().await?;
                        let have_pos = self.from.read_usize().await?;
                        assert_eq!(have_pos, 0);
                        let nr_traces = self.from.read_usize().await?;
                        let mut traces = Vec::with_capacity(nr_traces);
                        for _ in 0..nr_traces {
                            let have_pos = self.from.read_usize().await?;
                            assert_eq!(have_pos, 0);
                            let trace = self.from.read_string().await?;
                            traces.push(trace);
                        }
                        return Err(Error::ErrorInfo { level, msg, traces });
                    } else {
                        let error = self.from.read_string().await?;
                        let status = self.from.read_u64_le().await?;
                        return Err(Error::Custom(status, error));
                    }
                }
                STDERR_NEXT => {
                    debug!("Got STDERR_NEXT");
                    let s = self.from.read_string().await?;
                    info!("Next {}", s.trim_end());
                }
                STDERR_START_ACTIVITY => {
                    debug!("Got STDERR_START_ACTIVITY");
                    let act: ActivityId = self.from.read_u64_le().await?;
                    let lvl: Verbosity = self.from.read_enum().await?;
                    let act_type: ActivityType = self.from.read_enum().await?;
                    let s = self.from.read_string().await?;
                    let fields = read_fields(&mut self.from).await?;
                    let parent: ActivityId = self.from.read_u64_le().await?;
                    self.logger
                        .start_activity(act, lvl, act_type, s, fields, parent);
                }
                STDERR_STOP_ACTIVITY => {
                    debug!("Got STDERR_STOP_ACTIVITY");
                    let act: ActivityId = self.from.read_u64_le().await?;
                    self.logger.stop_activity(act);
                }
                STDERR_RESULT => {
                    debug!("Got STDERR_RESULT");
                    let act: ActivityId = self.from.read_u64_le().await?;
                    let res_type: ResultType = self.from.read_enum().await?;
                    let fields = read_fields(&mut self.from).await?;
                    self.logger.result(act, res_type, fields);
                }
                STDERR_LAST => {
                    return Ok(());
                }
                _ => {
                    error!("Unknown message type {}", msg);
                    return Err(Error::UnknownMessageType(msg));
                }
            }
        }
    }
}
