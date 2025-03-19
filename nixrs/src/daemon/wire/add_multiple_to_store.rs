use std::pin::pin;

use async_stream::try_stream;
use futures::{Stream, StreamExt};
use pin_project_lite::pin_project;
use tokio::io::{copy_buf, AsyncBufRead, AsyncRead, AsyncWrite};
use tracing::{debug, debug_span, instrument, trace, Instrument};

use crate::archive::NarReader;
use crate::daemon::wire::types2::ValidPathInfo;
use crate::daemon::DaemonResult;
use crate::io::{AsyncBufReadCompat, AsyncBytesRead, TakenReader};

use crate::daemon::de::NixRead;
use crate::daemon::ser::NixWrite;
use crate::daemon::types::AddToStoreItem;
use crate::daemon::{DaemonError, DaemonErrorKind};

#[instrument(level = "trace", skip_all)]
pub async fn write_add_multiple_to_store_stream<W, S, R>(
    mut writer: W,
    stream: S,
) -> Result<usize, DaemonError>
where
    W: NixWrite + AsyncWrite + Unpin,
    S: Stream<Item = Result<AddToStoreItem<R>, DaemonError>>,
    DaemonError: From<W::Error>,
    R: AsyncBufRead,
{
    let size = stream.size_hint().1.expect("Stream with size");
    trace!(size, "Write stream size");
    writer.write_value(&size).await?;
    let mut stream = pin!(stream.enumerate());
    let mut written = 0;
    while let Some((idx, item)) = stream.next().await {
        trace!(idx, size, written, "Write stream item");
        if idx >= size {
            return Err(
                DaemonErrorKind::Custom(format!("More than {} items in stream", size)).into(),
            );
        }
        let item = item?;
        let span = debug_span!("write_path_to_store", idx, size, ?item.info.path, ?item.info.info.nar_hash, ?item.info.info.nar_size, ?item.info.info);
        async {
            debug!(idx, size, "Item CA {:?}", item.info.info.ca);
            writer.write_value(&item.info).await?;
            debug!(idx, size, "Written file {} info", idx);
            let mut reader = pin!(item.reader);
            copy_buf(&mut reader, &mut writer).await?;
            debug!(idx, size, "Written file {} to writer", idx);
            //writer.flush().await?;
            //debug!(idx, size, "Flushed file {} to writer", idx);
            written += 1;
            Ok(()) as DaemonResult<()>
        }
        .instrument(span)
        .await?;
    }
    if written != size {
        return Err(DaemonErrorKind::Custom(format!(
            "Not enough items in stream: Expected {} got {}",
            size, written
        ))
        .into());
    }
    Ok(size)
}

pin_project! {
    pub struct SizedStream<S> {
        pub count: usize,
        #[pin]
        pub stream: S,
    }
}

impl<S: Stream> Stream for SizedStream<S> {
    type Item = S::Item;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.project().stream.poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(self.count))
    }
}

#[instrument(level = "trace", skip_all)]
pub async fn parse_add_multiple_to_store<'s, R>(
    mut source: R,
) -> Result<
    impl Stream<Item = Result<AddToStoreItem<impl AsyncBufRead>, DaemonError>> + Send + 's,
    DaemonError,
>
where
    R: NixRead + AsyncRead + AsyncBytesRead + Unpin + 's,
    DaemonError: From<<R as NixRead>::Error>,
{
    let count = source.read_number().await?;
    trace!(count, "Reading {} items to add to store", count);
    Ok(SizedStream {
        count: count as usize,
        stream: try_stream! {
            for idx in 0..count {
                let info : ValidPathInfo = source.read_value().await?;
                trace!(idx, count, %info.path, %info.info.nar_hash, %info.info.nar_size, "Reading {}", info.path);
                let (stealer, reader) = TakenReader::new(source);
                let reader = AsyncBufReadCompat::new(reader);
                let reader = NarReader::new(reader);
                let item = AddToStoreItem {
                    info, reader,
                };
                yield item;
                trace!(idx, count, "Looting reader");
                source = stealer.loot();
            }
            trace!(count, "Stream done");
        },
    })
}

#[cfg(test)]
mod test {
    use std::io::Cursor;

    use bytes::Bytes;
    use futures::stream::iter;
    use futures::{TryFutureExt as _, TryStreamExt as _};
    use nixrs_archive::{test_data, write_nar, NAREvent};
    use pretty_assertions::assert_eq;
    use rstest::rstest;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::try_join;
    use tokio_test::io::Builder;
    use tracing_test::traced_test;

    use super::*;
    use crate::daemon::de::NixReader;
    use crate::daemon::ser::NixWriter;
    use crate::daemon::{DaemonResult, UnkeyedValidPathInfo};
    use crate::hash::NarHash;
    use crate::io::DEFAULT_BUF_SIZE;

    #[tokio::test]
    async fn write_empty() {
        let mock = Builder::new().write(b"\0\0\0\0\0\0\0\0").build();
        let mut writer = NixWriter::new(mock);
        let list: Vec<DaemonResult<AddToStoreItem<Cursor<Vec<u8>>>>> = Vec::new();
        let stream = iter(list);
        write_add_multiple_to_store_stream(&mut writer, stream)
            .await
            .unwrap();
        writer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn read_empty() {
        let mock = Builder::new().read(b"\0\0\0\0\0\0\0\0\x01\x01\x01").build();
        let mut reader = NixReader::new(mock);
        {
            let stream = parse_add_multiple_to_store(&mut reader).await.unwrap();
            let mut stream = pin!(stream);
            assert_eq!((0, Some(0)), stream.size_hint());
            assert!(stream.try_next().await.unwrap().is_none());
        }

        let mut postfix = Vec::new();
        reader.read_to_end(&mut postfix).await.unwrap();
        assert_eq!(Bytes::from_static(b"\x01\x01\x01"), Bytes::from(postfix));
    }

    pub fn info_stream(
        infos: Vec<(ValidPathInfo, Vec<NAREvent>)>,
    ) -> impl Stream<Item = Result<AddToStoreItem<impl AsyncBufRead>, DaemonError>> {
        let infos_content: Vec<_> = infos
            .iter()
            .map(|(info, events)| (info.clone(), write_nar(events)))
            .collect();
        iter(infos_content.into_iter().map(|(info, content)| {
            Ok(AddToStoreItem {
                info: info.clone(),
                reader: Cursor::new(content.clone()),
            })
        }))
    }

    #[traced_test]
    #[tokio::test]
    #[rstest]
    #[case::one(
        vec![
            (
                ValidPathInfo {
                    path: "00000000000000000000000000000000-_".parse().unwrap(),
                    info: UnkeyedValidPathInfo {
                        deriver: Some("00000000000000000000000000000000-_.drv".parse().unwrap()),
                        nar_hash: NarHash::new(&[0u8; 32]),
                        references: vec!["00000000000000000000000000000000-_".parse().unwrap()],
                        registration_time: 0,
                        nar_size: 0,
                        ultimate: true,
                        signatures: vec![],
                        ca: None,
                    }
                },
                test_data::text_file(),
            ),
        ],
    )]
    #[case::multiple(
        vec![
            (
                ValidPathInfo {
                    path: "00000000000000000000000000000000-_".parse().unwrap(),
                    info: UnkeyedValidPathInfo {
                        deriver: Some("00000000000000000000000000000000-_.drv".parse().unwrap()),
                        nar_hash: NarHash::new(&[0u8; 32]),
                        references: vec!["00000000000000000000000000000000-_".parse().unwrap()],
                        registration_time: 0,
                        nar_size: 0,
                        ultimate: true,
                        signatures: vec![],
                        ca: None,
                    }
                },
                test_data::text_file(),
            ),
            (
                ValidPathInfo {
                    path: "00000000000000000000000000000011-_".parse().unwrap(),
                    info: UnkeyedValidPathInfo {
                        deriver: Some("00000000000000000000000000000022-_.drv".parse().unwrap()),
                        nar_hash: NarHash::new(&[1u8; 32]),
                        references: vec!["00000000000000000000000000000000-_".parse().unwrap()],
                        registration_time: 0,
                        nar_size: 200,
                        ultimate: true,
                        signatures: vec![],
                        ca: None,
                    }
                },
                test_data::dir_example()
            ),
        ],
    )]
    async fn test_read_written(#[case] infos: Vec<(ValidPathInfo, Vec<NAREvent>)>) {
        use futures::FutureExt as _;
        use nixrs_archive::read_nar;
        use tokio::io::simplex;

        let stream = info_stream(infos.clone());
        let (reader, writer) = simplex(DEFAULT_BUF_SIZE);
        let mut writer = NixWriter::new(writer);
        let mut reader = NixReader::new(reader);
        {
            let mut b_writer = &mut writer;
            let w = async move {
                let size = write_add_multiple_to_store_stream(&mut b_writer, stream).await?;
                b_writer.flush().await?;
                Ok(size)
            };
            let stream = parse_add_multiple_to_store(&mut reader).and_then(|stream| {
                stream
                    .and_then(|item| async move {
                        Ok((item.info, read_nar(item.reader).boxed().await?))
                    })
                    .try_collect::<Vec<(ValidPathInfo, Vec<NAREvent>)>>()
            });
            let (size, actual_infos) = try_join!(w, stream).unwrap();
            assert_eq!(infos, actual_infos);
            assert_eq!(infos.len(), size);
        }

        writer.write_slice(b"Hello World!!!").await.unwrap();
        writer.shutdown().await.unwrap();
        assert_eq!(
            Bytes::from_static(b"Hello World!!!"),
            reader.read_bytes().await.unwrap()
        );
    }
    // Partial read
    // Proptest
}
