use std::path::PathBuf;

use log::{error, info};
use nixrs_store::Error;
use nixrs_store::Store;
use nixrs_util::hash;

/// Verify whether the contents of the given store path have not changed.
pub async fn verify_path<S: Store>(mut store: S, paths: &[PathBuf]) -> Result<(), Error> {
    let store_dir = store.store_dir();
    let mut ret = Ok(());
    for path in paths {
        let store_path = store_dir.follow_links_to_store_path(path).await?;
        let sp_s = store_dir.print_path(&store_path);
        info!("checking path '{}'...", sp_s);
        let info = store.query_path_info(&store_path).await?;
        let mut sink = hash::HashSink::new(info.nar_hash.algorithm());
        store.nar_from_path(&store_path, &mut sink).await?;
        let (_size, current) = sink.finish();
        if current != info.nar_hash {
            error!(
                "path '{}' was modified! expected hash '{}', got '{}'",
                sp_s, info.nar_hash, current
            );
            ret = Err(Error::Custom(1, "some modified paths".into()));
        }
    }
    ret
}
