use std::fmt;

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::store::{Error, Store, SubstituteFlag};
use crate::store_path::StorePathSet;

#[async_trait]
pub trait LegacyStore: Store {
    /*
    async fn query_path_infos(
        &mut self,
        paths: &StorePathSet
    ) -> Result<BTreeSet<ValidPathInfo>, Error>;
     */
    async fn query_valid_paths_locked(
        &mut self,
        paths: &StorePathSet,
        lock: bool,
        maybe_substitute: SubstituteFlag,
    ) -> Result<StorePathSet, Error>;
    async fn export_paths<SW: AsyncWrite + fmt::Debug + Send + Unpin>(
        &mut self,
        paths: &StorePathSet,
        mut sink: SW,
    ) -> Result<(), Error>;
    async fn import_paths<SR: AsyncRead + fmt::Debug + Send + Unpin>(
        &mut self,
        mut source: SR,
    ) -> Result<(), Error>;
    async fn query_closure(
        &mut self,
        paths: &StorePathSet,
        include_outputs: bool,
    ) -> Result<StorePathSet, Error>;
}

macro_rules! deref_legacy_store {
    () => {
        #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
        fn query_valid_paths_locked<'life0, 'life1, 'async_trait>(
            &'life0 mut self,
            paths: &'life1 StorePathSet,
            lock: bool,
            maybe_substitute: SubstituteFlag,
        ) -> ::core::pin::Pin<
            Box<
                dyn ::core::future::Future<Output = Result<StorePathSet, Error>>
                    + ::core::marker::Send
                    + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            (**self).query_valid_paths_locked(paths, lock, maybe_substitute)
        }

        #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
        fn export_paths<'life0, 'life1, 'async_trait, SW>(
            &'life0 mut self,
            paths: &'life1 StorePathSet,
            sink: SW,
        ) -> ::core::pin::Pin<
            Box<
                dyn ::core::future::Future<Output = Result<(), Error>>
                    + ::core::marker::Send
                    + 'async_trait,
            >,
        >
        where
            SW: 'async_trait + AsyncWrite + fmt::Debug + Send + Unpin,
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            (**self).export_paths(paths, sink)
        }

        #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
        fn import_paths<'life0, 'async_trait, SR>(
            &'life0 mut self,
            source: SR,
        ) -> ::core::pin::Pin<
            Box<
                dyn ::core::future::Future<Output = Result<(), Error>>
                    + ::core::marker::Send
                    + 'async_trait,
            >,
        >
        where
            SR: 'async_trait + AsyncRead + fmt::Debug + Send + Unpin,
            'life0: 'async_trait,
            Self: 'async_trait,
        {
            (**self).import_paths(source)
        }

        #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
        fn query_closure<'life0, 'life1, 'async_trait>(
            &'life0 mut self,
            paths: &'life1 StorePathSet,
            include_outputs: bool,
        ) -> ::core::pin::Pin<
            Box<
                dyn ::core::future::Future<Output = Result<StorePathSet, Error>>
                    + ::core::marker::Send
                    + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            (**self).query_closure(paths, include_outputs)
        }
    };
}

impl<T: ?Sized + LegacyStore + Unpin + Send> LegacyStore for Box<T> {
    deref_legacy_store!();
}

impl<T: ?Sized + LegacyStore + Unpin + Send> LegacyStore for &mut T {
    deref_legacy_store!();
}
