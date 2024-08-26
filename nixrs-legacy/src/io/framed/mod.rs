pub mod framed_sink;
pub mod framed_source;

#[cfg(test)]
mod tests {
    use ::proptest::arbitrary::any;
    use ::proptest::proptest;
    use futures::future::join;
    use proptest::prop_assert_eq;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use crate::hash;
    use crate::io::{FramedSink, FramedSource};

    proptest! {
        #[test]
        fn proptest_copy_data(
            data in any::<Vec<u8>>(),
            read_buf_size in 1usize..500_000usize,
            duplex_buf_size in 1usize..500_000usize,
            iterations in 0usize..200usize,
         )
         {
            eprintln!("Data {}, read_buf={}, duplex_buf={}, iterations={}", data.len(), read_buf_size, duplex_buf_size, iterations);
            let r = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();

            let (reader, writer) = tokio::io::duplex(duplex_buf_size);
            let mut reader = FramedSource::new(reader);
            let mut writer = FramedSink::new(writer);
            let write_fut = async {
                let mut d1 = hash::Context::new(hash::Algorithm::SHA256);
                for _i in 0..iterations {
                    //eprintln!("write {}", i);
                    d1.update(&data);
                    writer.write_all(&data).await?;
                }
                //eprintln!("written {}", iterations * data.len());
                writer.flush().await?;
                writer.shutdown().await?;
                //eprintln!("shutdown");
                Ok(d1.finish()) as std::io::Result<hash::Hash>
            };
            let read_fut = async {
                let mut d2 = hash::Context::new(hash::Algorithm::SHA256);
                let mut buf = Vec::with_capacity(read_buf_size);
                //let mut total_read = 0;
                loop {
                    let read = reader.read_buf(&mut buf).await?;
                    //eprintln!("Read {} {} {} {}", total_read, read, buf.len(), buf.capacity());
                    if read == 0 {
                        break;
                    }
                    //total_read += read;
                    d2.update(&buf);
                    buf.clear();
                }
                Ok(d2.finish()) as std::io::Result<hash::Hash>
            };
            let fut = join(write_fut, read_fut);
            let (d1, d2) = r.block_on(fut);
            let d1 = d1.unwrap();
            let d2 = d2.unwrap();
            prop_assert_eq!(d1, d2);
            /*
            match r.block_on(fut) {
                (d1, d2) => prop_assert_eq!(d1, d2),
                Ok((d1, d2)) => prop_assert_eq!(d1, d2),
                Err(err) => prop_assert()
            }
             */

         }
    }
}
