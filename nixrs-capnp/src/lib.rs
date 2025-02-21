use std::future::{ready, Future};

use ::capnp::Error;
use futures::TryFutureExt as _;
use nixrs::daemon::{DaemonError, DaemonErrorKind, LocalLoggerResult, LogMessage};
use nixrs::store_path::{StorePath, StorePathError, StorePathHash, StorePathName, StorePathSet};
use tokio::io::AsyncWrite;

#[allow(clippy::needless_lifetimes, clippy::extra_unused_type_parameters)]
pub mod capnp {
    pub mod byte_stream_capnp {
        include!(concat!(env!("OUT_DIR"), "/byte_stream_capnp.rs"));
    }

    pub mod nix_daemon_capnp {
        include!(concat!(env!("OUT_DIR"), "/nix_daemon_capnp.rs"));
    }

    pub mod nixrs_capnp {
        include!(concat!(env!("OUT_DIR"), "/nixrs_capnp.rs"));
    }
}

pub struct CapnpResult<F> {
    promise: F,
}

impl<F, T> LocalLoggerResult<T, DaemonError> for CapnpResult<F>
where
    F: Future<Output = Result<T, Error>>,
    T: 'static,
{
    async fn next(&mut self) -> Option<Result<LogMessage, DaemonError>> {
        None
    }

    async fn result(self) -> Result<T, DaemonError> {
        match self.promise.await {
            Ok(v) => Ok(v),
            Err(err) => Err(DaemonErrorKind::Custom(err.to_string()).into()),
        }
    }
}

pub struct CapnpStore {
    store: capnp::nix_daemon_capnp::nix_daemon::Client,
}

impl nixrs::daemon::LocalDaemonStore for CapnpStore {
    fn trust_level(&self) -> nixrs::daemon::TrustLevel {
        nixrs::daemon::TrustLevel::Trusted
    }

    fn set_options<'a>(
        &'a mut self,
        options: &'a nixrs::daemon::ClientOptions,
    ) -> impl nixrs::daemon::LocalLoggerResult<(), nixrs::daemon::DaemonError> + 'a {
        CapnpResult {
            promise: async move {
                let mut req = self.store.set_options_request();
                let mut c_options = req.get().init_options();
                c_options.set_keep_failed(options.keep_failed);
                c_options.set_keep_going(options.keep_going);
                c_options.set_try_fallback(options.try_fallback);
                c_options.set_verbosity(options.verbosity.into());
                c_options.set_max_build_jobs(options.max_build_jobs);
                c_options.set_max_silent_time(options.max_silent_time as u64);
                c_options.set_verbose_build(options.verbose_build.into());
                c_options.set_build_cores(options.build_cores);
                c_options.set_use_substitutes(options.use_substitutes);
                if !options.other_settings.is_empty() {
                    let other = c_options.init_other_settings();
                    let mut entries = other.init_entries(options.other_settings.len() as u32);
                    for (index, (k, v)) in options.other_settings.iter().enumerate() {
                        let mut entry = entries.reborrow().get(index as u32);
                        entry.set_key(k)?;
                        entry.set_value(&v[..])?;
                    }
                }
                req.send().promise.await?;
                Ok(())
            },
        }
    }

    fn is_valid_path<'a>(
        &'a mut self,
        path: &'a nixrs::store_path::StorePath,
    ) -> impl nixrs::daemon::LocalLoggerResult<bool, nixrs::daemon::DaemonError> + 'a {
        let mut req = self.store.is_valid_path_request();
        let params = req.get();
        let mut c_path = params.init_path();
        c_path.set_hash(path.hash().as_ref());
        c_path.set_name(path.name().as_ref());
        CapnpResult {
            promise: req.send().promise.and_then(|resp| async move {
                let r = resp.get()?;
                Ok(r.get_valid())
            }),
        }
    }

    fn query_valid_paths<'a>(
        &'a mut self,
        paths: &'a nixrs::store_path::StorePathSet,
        substitute: bool,
    ) -> impl nixrs::daemon::LocalLoggerResult<
        nixrs::store_path::StorePathSet,
        nixrs::daemon::DaemonError,
    > + 'a {
        let mut req = self.store.query_valid_paths_request();
        let mut params = req.get();
        let mut c_paths = params.reborrow().init_paths(paths.len() as u32);
        for (index, path) in paths.iter().enumerate() {
            let mut c_path = c_paths.reborrow().get(index as u32);
            c_path.set_hash(path.hash().as_ref());
            c_path.set_name(path.name().as_ref());
        }
        params.set_substitute(substitute);
        CapnpResult {
            promise: req.send().promise.and_then(|resp| async move {
                let r = resp.get()?;
                let mut ret = StorePathSet::new();
                if r.has_valid_set() {
                    let set = r.get_valid_set()?;
                    for c_path in set.iter() {
                        let c_hash = c_path.get_hash()?;
                        let c_name = c_path.get_name()?.to_str()?;
                        let name = c_name
                            .parse::<StorePathName>()
                            .map_err(|err| Error::failed(err.to_string()))?;
                        let hash: StorePathHash = c_hash
                            .try_into()
                            .map_err(|err: StorePathError| Error::failed(err.to_string()))?;
                        let path: StorePath = (hash, name).into();
                        ret.insert(path);
                    }
                }
                Ok(ret)
            }),
        }
    }

    fn query_path_info<'a>(
        &'a mut self,
        _path: &'a nixrs::store_path::StorePath,
    ) -> impl nixrs::daemon::LocalLoggerResult<
        Option<nixrs::daemon::UnkeyedValidPathInfo>,
        nixrs::daemon::DaemonError,
    > + 'a {
        CapnpResult {
            promise: ready(Ok(None)),
        }
    }

    fn nar_from_path<'s, 'p, 'r, W>(
        &'s mut self,
        _path: &'p nixrs::store_path::StorePath,
        _sink: W,
    ) -> impl nixrs::daemon::LocalLoggerResult<(), nixrs::daemon::DaemonError> + 'r
    where
        W: AsyncWrite + Unpin + 'r,
        's: 'r,
        'p: 'r,
    {
        CapnpResult {
            promise: ready(Ok(())),
        }
    }
}
