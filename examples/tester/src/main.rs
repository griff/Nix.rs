use std::{path::PathBuf, pin::pin};

use clap::{Parser, Subcommand};
use futures::channel::mpsc;
use futures::{SinkExt as _, Stream as _, StreamExt as _};
use nixrs::daemon::client::DaemonClient;
use nixrs::daemon::de::NixReader;
use nixrs::daemon::ser::NixWriter;
use nixrs::daemon::wire::{
    SizedStream, parse_add_multiple_to_store, types2::ValidPathInfo,
    write_add_multiple_to_store_stream,
};
use nixrs::daemon::{AddToStoreItem, DaemonError, DaemonResult, DaemonStore as _};
use nixrs::hash::HashSink;
use nixrs::io::DEFAULT_BUF_SIZE;
use nixrs::store_path::StorePath;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt as _, BufReader, copy_buf, simplex};
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::try_join;
use tracing::Level;
use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt as _};

mod commands;

fn default_daemon() -> PathBuf {
    "/nix/var/nix/daemon-socket/socket".into()
}

#[derive(Parser)]
struct App {
    #[arg(long, default_value_os_t = default_daemon())]
    daemon: PathBuf,
    #[arg(long, short, default_value_t = Level::WARN)]
    log_level: Level,
    #[command(subcommand)]
    command: Command,
}

impl App {
    pub fn init_logger(&self) {
        let filter = tracing_subscriber::filter::Targets::new()
            .with_default(self.log_level)
            //.with_target("nixrs", Level::TRACE)
            //.with_target("nixrs::archive", Level::INFO)
            .with_target("nixrs::io", Level::INFO)
            .with_target("nixrs::daemon::se", Level::INFO)
            .with_target("nixrs::daemon::de", Level::INFO)
            .with_target("nixrs_archive", Level::INFO)
            //.with_target("nixrs_ssh_store", Level::TRACE)
            .with_target("nixrs_ssh_store::io::data_write", Level::DEBUG)
            .with_target("thrussh", Level::INFO)
            .with_target("tokio_util::codec", Level::INFO)
            .with_target("h2", Level::INFO)
            .with_target("opentelemetry_sdk", Level::INFO)
            .with_target("opentelemetry-otlp", Level::INFO)
            .with_target("tower::buffer::worker", Level::INFO);
        let layer = tracing_subscriber::fmt::layer()
            .with_file(true)
            .with_line_number(true)
            .with_writer(std::io::stderr);

        tracing_subscriber::registry()
            .with(filter)
            .with(layer)
            .init();
    }

    pub async fn client(&self) -> DaemonResult<DaemonClient<OwnedReadHalf, OwnedWriteHalf>> {
        DaemonClient::builder()
            .connect_unix(self.daemon.clone())
            .await
    }

    pub async fn send_stream<R, W>(&self, count: usize, writer: W, reader: R) -> DaemonResult<()>
    where
        W: AsyncWrite + Send + Unpin,
        R: AsyncRead + Send + Unpin,
    {
        let mut client = self.client().await?;
        eprintln!("Query all valid paths");
        let mut paths: Vec<StorePath> = client.query_all_valid_paths().await?.into_iter().collect();
        eprintln!("Found {} paths", paths.len());
        paths.truncate(count);

        let (mut info_sender, info_receiver) = mpsc::channel(50);
        let (mut sender, receiver) = mpsc::channel(1);
        let send_paths = async {
            for path in paths {
                if let Some(info) = client.query_path_info(&path).await? {
                    let (reader, mut writer) = simplex(DEFAULT_BUF_SIZE);
                    let reader = BufReader::new(reader);
                    let info = ValidPathInfo {
                        info,
                        path: path.clone(),
                    };
                    info_sender
                        .send(info.clone())
                        .await
                        .map_err(DaemonError::custom)?;
                    sender
                        .send(Ok(AddToStoreItem { info, reader }))
                        .await
                        .map_err(DaemonError::custom)?;
                    eprintln!("Sent path {path}");
                    let mut reader = client.nar_from_path(&path).await?;
                    copy_buf(&mut reader, &mut writer).await?;
                    eprintln!("Sent nar {path}");
                    writer.shutdown().await?;
                    eprintln!("Shutdown writer {path}");
                }
            }
            sender.close_channel();
            eprintln!("Send paths done");
            Ok(()) as DaemonResult<()>
        };
        let writer = NixWriter::new(writer);
        let stream = SizedStream {
            count,
            stream: receiver,
        };
        let write = async {
            write_add_multiple_to_store_stream(writer, stream).await?;
            eprintln!("Write stream done");
            Ok(()) as DaemonResult<()>
        };
        let reader = NixReader::new(reader);
        let read_stream = parse_add_multiple_to_store(reader);
        let compare_stream = async {
            let read_stream = read_stream.await?;
            let mut read_stream = pin!(read_stream);
            let mut info_receiver = pin!(info_receiver);
            eprintln!("Read Stream: {:?}", read_stream.size_hint());
            let mut idx = 0;
            while let Some(mut item) = read_stream.next().await.transpose()? {
                eprintln!("{}: Read info for {}", idx, item.info.path);
                let expected_info = info_receiver.next().await.unwrap();
                if item.info != expected_info {
                    eprintln!(
                        "{}: Info doesn't match {:?} != {:?}",
                        idx, expected_info, item.info
                    );
                } else {
                    let mut sink = HashSink::new(nixrs::hash::Algorithm::SHA256);
                    eprintln!("{}: Reading NAR for {}", idx, item.info.path);
                    copy_buf(&mut item.reader, &mut sink).await?;
                    eprintln!("{}: Read NAR for {}", idx, item.info.path);
                    let (size, hash) = sink.finish();
                    if size != expected_info.info.nar_size {
                        eprintln!(
                            "{}: Unmatched size {} != {}",
                            idx, expected_info.info.nar_size, size
                        );
                    }
                    if hash != expected_info.info.nar_hash.into() {
                        eprintln!(
                            "{}: Unmatched hash {} != {}",
                            idx, expected_info.info.nar_hash, hash
                        );
                    }
                }
                idx += 1;
            }
            eprintln!("Compare stream done");
            Ok(()) as DaemonResult<()>
        };
        try_join!(send_paths, write, compare_stream).map(|_| ())
    }
}

#[derive(Clone, Subcommand)]
enum Command {
    AddMultipleToStore(commands::add_multiple_to_store::Args),
    Framed(commands::framed::Args),
}

#[tokio::main]
async fn main() {
    let app = App::parse();
    app.init_logger();
    match app.command.clone() {
        Command::AddMultipleToStore(args) => {
            commands::add_multiple_to_store::run_command(app, args).await
        }
        Command::Framed(args) => commands::framed::run_command(app, args).await,
    }
}
