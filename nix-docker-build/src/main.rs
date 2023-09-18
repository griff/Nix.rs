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
            LevelFilter::Debug,
            slog::Config::default(),
            slog::TerminalMode::Stderr,
            slog::ColorChoice::Never,
        ),
        WriteLogger::new(
            LevelFilter::Debug,
            slog::Config::default(),
            OpenOptions::new()
                .append(true)
                .create(true)
                .open("nix-cache.log")
                .unwrap(),
        ),
    ]);

    let mut args = env::args().skip(1);
    let store_uri = args
        .next()
        .expect("expected first argument to be local store URI");
    let docker_bin = args
        .next()
        .expect("second argument should be docker binary");
    let nix_store_bin = args
        .next()
        .expect("third argument should be nix-store binary");
    let mut write_allowed = false;
    for argument in args {
        if argument == "--write" {
            write_allowed = true;
        } else {
            eprintln!("unknown flag '{}'", argument);
            exit(1);
        }
    }
    let source = tokio::io::stdin();
    let out = tokio::io::stdout();
    let build_log = tokio::io::stderr();
    let res = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async move {
            let store =
                CachedStore::connect(store_uri, docker_bin, nix_store_bin, write_allowed).await?;
            nixrs_store::legacy_worker::server::run(source, out, store, build_log, write_allowed)
                .await
        });

    if let Err(e) = res {
        eprintln!("Nix.rs Error: {}", e);
        exit(e.exit_code() as i32);
    }
}
