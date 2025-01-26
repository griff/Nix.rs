use std::fmt;

use async_trait::async_trait;
use nixrs_legacy::path_info::ValidPathInfo;
use nixrs_legacy::store::legacy_worker::{LegacyStore, LegacyStoreBuilder, LegacyStoreClient};
use nixrs_legacy::store::{copy_paths, BuildMode};
use nixrs_legacy::store::{
    BasicDerivation, BuildResult, CheckSignaturesFlag, DerivedPath, Error, RepairFlag, Store,
    SubstituteFlag,
};
use nixrs_legacy::store_path::{StoreDir, StoreDirProvider, StorePath, StorePathSet};
use tokio::io::{BufReader, BufWriter};
use tokio::process::{ChildStdin, ChildStdout};

pub struct CachedStore {
    cache: LegacyStoreClient<BufReader<ChildStdout>, BufWriter<ChildStdin>>,
    builder: Option<LegacyStoreClient<BufReader<ChildStdout>, BufWriter<ChildStdin>>>,
    write_allowed: bool,
    docker_bin: String,
}

impl CachedStore {
    pub async fn connect(
        store_uri: String,
        docker_bin: String,
        nix_store_bin: String,
        write_allowed: bool,
    ) -> Result<CachedStore, Error> {
        let mut b = LegacyStoreBuilder::new(nix_store_bin);
        b.command_mut().env("NIX_REMOTE", store_uri).arg("--serve");
        b.host("cache");
        if write_allowed {
            b.command_mut().arg("--write");
        }
        let cache = b.connect().await?;
        Ok(CachedStore {
            builder: None,
            cache,
            write_allowed,
            docker_bin,
        })
    }
}

impl StoreDirProvider for CachedStore {
    fn store_dir(&self) -> StoreDir {
        self.cache.store_dir()
    }
}

#[async_trait]
impl Store for CachedStore {
    async fn query_path_info(&mut self, path: &StorePath) -> Result<Option<ValidPathInfo>, Error> {
        if let Some(builder) = self.builder.as_mut() {
            match builder.query_path_info(path).await {
                Ok(Some(ret)) => Ok(Some(ret)),
                Ok(None) => self.cache.query_path_info(path).await,
                Err(err) => Err(err),
            }
        } else {
            self.cache.query_path_info(path).await
        }
    }
    async fn nar_from_path<W: tokio::io::AsyncWrite + fmt::Debug + Send + Unpin>(
        &mut self,
        path: &StorePath,
        sink: W,
    ) -> Result<(), Error> {
        if let Some(builder) = self.builder.as_mut() {
            if builder.query_path_info(path).await?.is_some() {
                builder.nar_from_path(path, sink).await
            } else {
                self.cache.nar_from_path(path, sink).await
            }
        } else {
            self.cache.nar_from_path(path, sink).await
        }
    }

    async fn add_to_store<R: tokio::io::AsyncRead + fmt::Debug + Send + Unpin>(
        &mut self,
        info: &ValidPathInfo,
        source: R,
        repair: RepairFlag,
        check_sigs: CheckSignaturesFlag,
    ) -> Result<(), Error> {
        self.cache
            .add_to_store(info, source, repair, check_sigs)
            .await
    }

    async fn build_paths(
        &mut self,
        _drv_paths: &[DerivedPath],
        _build_mode: BuildMode,
    ) -> Result<(), Error> {
        Err(Error::Misc("Unsupported operation 'build_paths'".into()))
    }

    async fn build_derivation(
        &mut self,
        drv_path: &StorePath,
        drv: &BasicDerivation,
        build_mode: BuildMode,
    ) -> Result<BuildResult, Error> {
        //let store_dir = self.store_dir();
        let inputs = self.cache.query_closure(&drv.input_srcs, false).await?;
        let mut b = LegacyStoreBuilder::new(&self.docker_bin);
        b.command_mut().args([
            "run",
            "-i",
            "--network",
            "none",
            "--tmpfs",
            "/nix/store:exec",
        ]);
        for input in inputs.iter() {
            b.command_mut()
                .arg("-v")
                .arg(format!("/nix/store/{}:/nix/store/{}", input, input));
        }
        b.command_mut()
            .args(["griff/nix-static", "nix-store", "--serve"]);
        b.host("builder");
        if self.write_allowed {
            b.command_mut().arg("--write");
        }

        let mut builder = b.connect().await?;

        copy_paths(&mut self.cache, &mut builder, &inputs).await?;
        let result = builder.build_derivation(drv_path, drv, build_mode).await?;

        if result.success() {
            /*
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
             */
            self.builder = Some(builder);
        }

        Ok(result)
    }
}

#[async_trait]
impl LegacyStore for CachedStore {
    /*
    async fn query_path_infos(&mut self, paths: &StorePathSet) -> Result<BTreeSet<ValidPathInfo>, Error> {
        if let Some(builder) = self.builder.as_mut() {
            match builder.query_path_info(path).await {
                Ok(ret) => Ok(ret),
                Err(Error::InvalidPath(_)) => self.cache.query_path_info(path).await,
                Err(err) => Err(err),
            }
        } else {
            self.cache.query_path_info(path).await
        }
    }
     */

    async fn query_valid_paths_locked(
        &mut self,
        paths: &StorePathSet,
        lock: bool,
        maybe_substitute: SubstituteFlag,
    ) -> Result<StorePathSet, Error> {
        if let Some(builder) = self.builder.as_mut() {
            let mut ret = builder
                .query_valid_paths_locked(paths, lock, maybe_substitute)
                .await?;
            let mut local = self
                .cache
                .query_valid_paths_locked(paths, lock, maybe_substitute)
                .await?;
            ret.append(&mut local);
            Ok(ret)
        } else {
            self.cache
                .query_valid_paths_locked(paths, lock, maybe_substitute)
                .await
        }
    }

    async fn export_paths<W: tokio::io::AsyncWrite + fmt::Debug + Send + Unpin>(
        &mut self,
        paths: &StorePathSet,
        sink: W,
    ) -> Result<(), Error> {
        self.cache.export_paths(paths, sink).await
    }

    async fn import_paths<R: tokio::io::AsyncRead + fmt::Debug + Send + Unpin>(
        &mut self,
        source: R,
    ) -> Result<(), Error> {
        self.cache.import_paths(source).await
    }

    async fn query_closure(
        &mut self,
        paths: &StorePathSet,
        include_outputs: bool,
    ) -> Result<StorePathSet, Error> {
        self.cache.query_closure(paths, include_outputs).await
    }
}
