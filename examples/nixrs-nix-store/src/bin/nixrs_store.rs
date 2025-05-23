use std::path::PathBuf;

use nixrs_legacy::store::legacy_worker::LegacyStoreClient;
use nixrs_nix_store::verify_path::verify_path;

pub fn main() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let res = runtime.block_on(async move {
        let store = LegacyStoreClient::connect(true).await?;
        verify_path(
            store,
            &[PathBuf::from(
                "/nix/store/050cxaj0ydhlhgn6f783aah9isg95xiv-autoreconf-hook.drv",
            )],
        )
        .await
    });

    res.unwrap();
}
