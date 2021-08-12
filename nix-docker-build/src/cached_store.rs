use async_trait::async_trait;
use tokio::process::{ChildStdin, ChildStdout};

use nixrs_store::Error;
use nixrs_store::copy_paths;
use nixrs_store::{LegacyLocalStore, LegacyStoreBuilder, Store, StoreDir, StorePathSet};
use nixrs_store::{BuildSettings, BuildResult, StorePath, SubstituteFlag, ValidPathInfo};
use nixrs_store::{BasicDerivation, CheckSignaturesFlag, DerivedPath, RepairFlag};


pub struct CachedStore {
    cache: LegacyLocalStore<ChildStdout,ChildStdin>,
    write_allowed: bool,
}

impl CachedStore {
    pub async fn connect(write_allowed: bool) -> Result<CachedStore, Error> {
        let mut b = LegacyStoreBuilder::new("/run/current-system/sw/bin/nix-store");
        b.command_mut()
            .env("NIX_REMOTE", "file:///Users/bro/Documents/Maven-Group/Nix.rs/test-store3")
            .arg("--serve");
        b.host("cache");
        if write_allowed {
            b.command_mut().arg("--write");
        }
        let cache = b.connect().await?;
        Ok(CachedStore { cache, write_allowed })
    }
}

#[async_trait(?Send)]
impl Store for CachedStore {
    fn store_dir(&self) -> StoreDir {
        self.cache.store_dir()
    }

    async fn legacy_query_valid_paths(&mut self, paths: &StorePathSet, lock: bool, maybe_substitute: SubstituteFlag) -> Result<StorePathSet, Error> {
        self.cache.legacy_query_valid_paths(paths, lock, maybe_substitute).await
    }

    async fn query_valid_paths(&mut self, paths: &StorePathSet, maybe_substitute: SubstituteFlag) -> Result<StorePathSet, Error> {
        self.cache.query_valid_paths(paths, maybe_substitute).await
    }

    fn add_temp_root(&self, path: &StorePath) {
        self.cache.add_temp_root(path)
    }

    async fn query_path_info(&mut self, path: &StorePath) -> Result<ValidPathInfo, Error> {
        self.cache.query_path_info(path).await
    }

    async fn nar_from_path<W:tokio::io::AsyncWrite + Unpin>(&mut self, path: &StorePath, sink: W) -> Result<(), Error> {
        self.cache.nar_from_path(path, sink).await
    }

    async fn export_paths<W:tokio::io::AsyncWrite + Unpin>(&mut self, paths: &StorePathSet, sink: W) -> Result<(), Error> {
        self.cache.export_paths(paths, sink).await
    }

    async fn import_paths<R:tokio::io::AsyncRead + Unpin>(&mut self, source: R) -> Result<(), Error> {
        self.cache.import_paths(source).await
    }

    async fn add_to_store<R:tokio::io::AsyncRead + Unpin>(&mut self, info: &ValidPathInfo, source: R,
        repair: RepairFlag, check_sigs: CheckSignaturesFlag) -> Result<(), Error> {
        self.cache.add_to_store(info, source, repair, check_sigs).await
    }

    async fn query_closure(&mut self, paths: &StorePathSet, include_outputs: bool) -> Result<StorePathSet, Error> {
        self.cache.query_closure(paths, include_outputs).await
    }

    async fn build_paths(&mut self, _drv_paths: &[DerivedPath], _settings: &BuildSettings) -> Result<(), Error> {
        unimplemented!("Unsupported operation 'build_paths'");
    }

    async fn build_derivation(&mut self, drv_path: &StorePath, drv: &BasicDerivation, settings: &BuildSettings) -> Result<BuildResult, Error> {
        let store_dir = self.store_dir();
        let inputs = self.cache.query_closure(&drv.input_srcs, false).await?;
        let mut b = LegacyStoreBuilder::new("/usr/local/bin/docker");
        b.command_mut()
            .args(&["run", "-i", "--tmpfs", "/nix/store:exec", "griff/nix-static", "nix-store", "--serve"]);
        b.host("builder");
        if self.write_allowed {
            b.command_mut().arg("--write");
        }
        
        let mut builder = b.connect().await?;

        copy_paths(&mut self.cache, &mut builder, &inputs).await?;
        let result = builder.build_derivation(drv_path, drv, settings).await?;

        if result.success() {
            let mut missing_paths = StorePathSet::new();
            let output_paths = drv.outputs_and_opt_paths(&store_dir)?;
            for (_output_name, (_, store_path)) in output_paths {
                let path = store_path.expect("We should have just built the path");
                if !self.cache.is_valid_path(&path).await? {
                    missing_paths.insert(path);
                }
            }
            if !missing_paths.is_empty() {
                copy_paths(&mut builder, &mut self.cache, &missing_paths).await?;
            }
        }

        Ok(result)
    }
}