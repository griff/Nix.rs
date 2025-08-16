use clap::Args as OtherArgs;
use nixrs::daemon::wire::{FramedReader, FramedWriter};
use nixrs::io::DEFAULT_BUF_SIZE;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, simplex};

#[derive(Clone, OtherArgs)]
pub struct Args {
    count: usize,
}

pub async fn run_command(app: crate::App, args: Args) {
    let (reader, mut writer) = simplex(DEFAULT_BUF_SIZE);
    let mut reader = BufReader::new(reader);
    let f_reader = FramedReader::new(&mut reader);
    let f_writer = FramedWriter::new(&mut writer);
    app.send_stream(args.count, f_writer, f_reader)
        .await
        .unwrap();
    eprintln!("Streams done. Writing after");
    writer.write_all(b"after stream").await.unwrap();
    writer.shutdown().await.unwrap();
    eprintln!("Reading after");
    let mut s = String::new();
    reader.read_to_string(&mut s).await.unwrap();
}
