use std::io::{self, Cursor};

use bytes::Bytes;
use futures::{FutureExt as _, SinkExt as _, StreamExt as _, stream::iter};
use tokio::io::AsyncRead;

use crate::archive::{NarEvent, NarWriter, parse_nar};

pub mod test_data;

pub async fn read_nar<R>(source: R) -> io::Result<test_data::TestNarEvents>
where
    R: AsyncRead + Unpin,
{
    use futures::stream::TryStreamExt as _;
    parse_nar(source)
        .and_then(NarEvent::read_file)
        .try_collect()
        .await
}

pub fn write_nar<'e, E>(events: E) -> Bytes
where
    E: IntoIterator<Item = &'e test_data::TestNarEvent>,
{
    let mut buf = Vec::new();
    let mut writer = NarWriter::new(Cursor::new(&mut buf));
    let mut stream = iter(events).map(Clone::clone).map(Ok);
    writer
        .send_all(&mut stream)
        .now_or_never()
        .expect("BUG: NarWriter blocks")
        .expect("BUG: NarWriter returned error");
    writer
        .close()
        .now_or_never()
        .expect("BUG: NarWriter close blocks")
        .expect("BUG: NarWriter close errors");
    buf.into()
}

impl<R> NarEvent<R>
where
    R: tokio::io::AsyncRead + Unpin,
{
    pub async fn read_file(self) -> std::io::Result<test_data::TestNarEvent> {
        use tokio::io::AsyncReadExt as _;
        match self {
            NarEvent::StartDirectory { name } => Ok(NarEvent::StartDirectory { name }),
            NarEvent::EndDirectory => Ok(NarEvent::EndDirectory),
            NarEvent::Symlink { name, target } => Ok(NarEvent::Symlink { name, target }),
            NarEvent::File {
                name,
                executable,
                size,
                mut reader,
            } => {
                let mut buf = Vec::new();
                reader.read_to_end(&mut buf).await?;
                let reader = std::io::Cursor::new(bytes::Bytes::from(buf));
                Ok(NarEvent::File {
                    name,
                    executable,
                    size,
                    reader,
                })
            }
        }
    }
}
