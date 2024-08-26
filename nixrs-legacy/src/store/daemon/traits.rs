use std::fmt;

use async_trait::async_trait;
use tokio::io::AsyncRead;
use tracing::warn;

use crate::store::{BuildMode, CheckSignaturesFlag, DerivedPath, Error, RepairFlag, Store};
use crate::store_path::{StorePath, StorePathSet};

use super::TrustedFlag;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct QueryMissingResult {
    pub will_build: StorePathSet,
    pub will_substitute: StorePathSet,
    pub unknown: StorePathSet,
    pub download_size: u64,
    pub nar_size: u64,
}

#[async_trait]
pub trait DaemonStore: Store {
    fn is_trusted_client(&self) -> Option<TrustedFlag>;
    async fn set_options(&mut self) -> Result<(), Error>;
    async fn is_valid_path(&mut self, path: &StorePath) -> Result<bool, Error>;

    async fn add_multiple_to_store<R: AsyncRead + fmt::Debug + Send + Unpin>(
        &mut self,
        source: R,
        repair: RepairFlag,
        check_sigs: CheckSignaturesFlag,
    ) -> Result<(), Error>;

    /// Given a set of paths that are to be built, return the set of
    /// derivations that will be built, and the set of output paths that
    /// will be substituted.
    async fn query_missing(&mut self, targets: &[DerivedPath])
        -> Result<QueryMissingResult, Error>;
    async fn substitute_paths(&mut self, paths: &StorePathSet) -> Result<(), Error> {
        let mut paths2 = Vec::new();
        for path in paths {
            if path.is_derivation() {
                paths2.push(DerivedPath::Opaque(path.clone()));
            }
        }
        let res = self.query_missing(&paths2).await?;
        if res.will_substitute.is_empty() {
            let ret = async {
                let mut subs = Vec::new();
                for p in res.will_substitute {
                    subs.push(DerivedPath::Opaque(p));
                }
                self.build_paths(&subs, BuildMode::Normal).await
            }
            .await;
            if let Err(err) = ret {
                warn!("{}", err);
            }
        }
        Ok(())
    }
}

macro_rules! deref_daemon_store {
    () => {
        fn is_trusted_client(&self) -> Option<TrustedFlag> {
            (**self).is_trusted_client()
        }

        #[must_use]
        #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
        fn set_options<'life0, 'async_trait>(
            &'life0 mut self,
        ) -> ::core::pin::Pin<
            Box<
                dyn ::core::future::Future<Output = Result<(), Error>>
                    + ::core::marker::Send
                    + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            Self: 'async_trait,
        {
            (**self).set_options()
        }

        #[must_use]
        #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
        fn is_valid_path<'life0, 'life1, 'async_trait>(
            &'life0 mut self,
            path: &'life1 StorePath,
        ) -> ::core::pin::Pin<
            Box<
                dyn ::core::future::Future<Output = Result<bool, Error>>
                    + ::core::marker::Send
                    + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            (**self).is_valid_path(path)
        }

        #[must_use]
        #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
        fn add_multiple_to_store<'life0, 'async_trait, R>(
            &'life0 mut self,
            source: R,
            repair: RepairFlag,
            check_sigs: CheckSignaturesFlag,
        ) -> ::core::pin::Pin<
            Box<
                dyn ::core::future::Future<Output = Result<(), Error>>
                    + ::core::marker::Send
                    + 'async_trait,
            >,
        >
        where
            R: 'async_trait + AsyncRead + fmt::Debug + Send + Unpin,
            'life0: 'async_trait,
            Self: 'async_trait,
        {
            (**self).add_multiple_to_store(source, repair, check_sigs)
        }

        #[must_use]
        #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
        fn query_missing<'life0, 'life1, 'async_trait>(
            &'life0 mut self,
            targets: &'life1 [DerivedPath],
        ) -> ::core::pin::Pin<
            Box<
                dyn ::core::future::Future<Output = Result<QueryMissingResult, Error>>
                    + ::core::marker::Send
                    + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            (**self).query_missing(targets)
        }
    };
}

impl<T: ?Sized + DaemonStore + Unpin + Send> DaemonStore for Box<T> {
    deref_daemon_store!();
}

impl<T: ?Sized + DaemonStore + Unpin + Send> DaemonStore for &mut T {
    deref_daemon_store!();
}
