use std::env;
use std::fs::OpenOptions;
use std::process::exit;

//use nixrs_store::store::LegacyLocalStore;
use simplelog as slog;
use simplelog::LevelFilter;
use simplelog::TermLogger;
use simplelog::WriteLogger;
use slog::CombinedLogger;

mod cached_store;
use cached_store::CachedStore;

pub fn main() {
    let _ = CombinedLogger::init(vec![
        TermLogger::new(
            LevelFilter::Trace,
            slog::Config::default(),
            slog::TerminalMode::Stderr,
            slog::ColorChoice::Never,
        ),
        WriteLogger::new(
            LevelFilter::Trace,
            slog::Config::default(),
            OpenOptions::new()
                .append(true)
                .create(true)
                .open("nix-cache.log")
                .unwrap(),
        ),
    ]);

    let mut write_allowed = false;
    for argument in env::args().skip(1) {
        if argument == "--write" {
            write_allowed = true;
        } else {
            eprintln!("unknown flag '{}'", argument);
            exit(1);
        }
    }
    let source = tokio::io::stdin();
    let out = tokio::io::stdout();
    let res = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async move {
            let store = CachedStore::connect(write_allowed).await?;
            nixrs_store::nix_store::serve(source, out, store, write_allowed).await
        });

    if let Err(e) = res {
        eprintln!("Nix.rs Error: {}", e);
        exit(e.exit_code() as i32);
    }
}
