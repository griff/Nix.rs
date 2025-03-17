use clap::Args as OtherArgs;
use nixrs::io::DEFAULT_BUF_SIZE;
use tokio::io::simplex;

#[derive(Clone, OtherArgs)]
pub struct Args {
    count: usize,
}

pub async fn run_command(app: crate::App, args: Args) {
    let (reader, writer) = simplex(DEFAULT_BUF_SIZE);
    app.send_stream(args.count, writer, reader).await.unwrap();
}
