use std::fmt;
use std::time::SystemTime;

use async_trait::async_trait;
use futures::future::try_join;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tracing::debug;
use nixrs_io::flag_enum;

use super::topo_sort_paths_slow;
use super::{BasicDerivation, DerivedPath, DrvOutputs, Error, RepairFlag};
use crate::num_enum::num_enum;
use crate::path_info::ValidPathInfo;
use crate::store_path::{StoreDirProvider, StorePath, StorePathSet};

/* Magic header of exportPath() output (obsolete). */
pub const EXPORT_MAGIC: u64 = 0x4558494e;

num_enum! {
    #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
    pub enum BuildMode {
        Unknown(u64),
        Normal = 0,
        Repair = 1,
        Check = 2,
    }
}

flag_enum! {
    #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
    pub enum CheckSignaturesFlag {
        CheckSigs = true,
        NoCheckSigs = false,
    }
}

flag_enum! {
    #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
    pub enum SubstituteFlag {
        NoSubstitute = false,
        Substitute = true,
    }
}

num_enum! {
    #[derive(Debug, Default, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Hash)]
    pub enum BuildStatus {
        Unsupported(u64),
        Built = 0,
        Substituted = 1,
        AlreadyValid = 2,
        PermanentFailure = 3,
        InputRejected = 4,
        OutputRejected = 5,
        TransientFailure = 6, // possibly transient
        CachedFailure = 7, // no longer used
        TimedOut = 8,
        #[default]
        MiscFailure = 9,
        DependencyFailed = 10,
        LogLimitExceeded = 11,
        NotDeterministic = 12
    }
}
impl BuildStatus {
    pub fn success(&self) -> bool {
        matches!(
            self,
            BuildStatus::Built | BuildStatus::Substituted | BuildStatus::AlreadyValid
        )
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct BuildResult {
    pub status: BuildStatus,
    pub error_msg: String,

    /// How many times this build was performed.
    pub times_built: u64,

    /// If timesBuilt > 1, whether some builds did not produce the same
    /// result. (Note that 'isNonDeterministic = false' does not mean
    /// the build is deterministic, just that we don't have evidence of
    /// non-determinism.)
    pub is_non_deterministic: bool,

    pub built_outputs: DrvOutputs,

    /// The start time of the build (or one of the rounds, if it was repeated).
    pub start_time: SystemTime,
    /// The stop time of the build (or one of the rounds, if it was repeated).
    pub stop_time: SystemTime,
}

impl BuildResult {
    pub fn new(status: BuildStatus, error_msg: String) -> BuildResult {
        BuildResult {
            status,
            error_msg,
            times_built: 0,
            is_non_deterministic: false,
            built_outputs: DrvOutputs::new(),
            start_time: SystemTime::UNIX_EPOCH,
            stop_time: SystemTime::UNIX_EPOCH,
        }
    }
    pub fn success(&self) -> bool {
        self.status.success()
    }
}

pub async fn copy_paths<S, D>(
    src_store: &mut S,
    dst_store: &mut D,
    store_paths: &StorePathSet,
) -> Result<(), Error>
where
    S: Store,
    D: Store + Send,
{
    copy_paths_full(
        src_store,
        dst_store,
        store_paths,
        RepairFlag::NoRepair,
        CheckSignaturesFlag::CheckSigs,
        SubstituteFlag::NoSubstitute,
    )
    .await
}

pub async fn copy_paths_full<S, D>(
    src_store: &mut S,
    dst_store: &mut D,
    store_paths: &StorePathSet,
    repair: RepairFlag,
    check_sigs: CheckSignaturesFlag,
    substitute: SubstituteFlag,
) -> Result<(), Error>
where
    S: Store,
    D: Store + Send,
{
    let valid = dst_store.query_valid_paths(store_paths, substitute).await?;

    let missing: StorePathSet = store_paths.difference(&valid).cloned().collect();

    let sorted = topo_sort_paths_slow(src_store, &missing).await?;
    for store_path in sorted {
        if dst_store.query_path_info(&store_path).await?.is_none() {
            copy_store_path(src_store, dst_store, &store_path, repair, check_sigs).await?;
        }
    }
    Ok(())
}

pub async fn copy_store_path<S, D>(
    src_store: &mut S,
    dst_store: &mut D,
    store_path: &StorePath,
    repair: RepairFlag,
    check_sigs: CheckSignaturesFlag,
) -> Result<(), Error>
where
    S: Store,
    D: Store,
{
    debug!("Copying path {}", store_path);
    let mut info = src_store
        .query_path_info(store_path)
        .await?
        .ok_or(Error::InvalidPath(store_path.to_string()))?;

    // recompute store path on the chance dstStore does it differently
    if info.ca.is_some() && info.references.is_empty() {
        let path = dst_store.store_dir().make_fixed_output_path_from_ca(
            info.path.name.name(),
            &info.content_address_with_references().unwrap(),
        )?;
        if dst_store.store_dir() == src_store.store_dir() {
            assert_eq!(info.path, path);
        }
        info.path = path;
    }

    if info.ultimate {
        info.ultimate = false;
    }
    let (sink, source) = tokio::io::duplex(64_000);
    try_join(
        src_store.nar_from_path(store_path, sink),
        dst_store.add_to_store(&info, source, repair, check_sigs),
    )
    .await?;
    /*
    auto source = sinkToSource([&](Sink & sink) {
        LambdaSink progressSink([&](std::string_view data) {
            total += data.size();
            act.progress(total, info->narSize);
        });
        TeeSink tee { sink, progressSink };
        srcStore->narFromPath(storePath, tee);
    }, [&]() {
           throw EndOfFile("NAR for '%s' fetched from '%s' is incomplete", srcStore->printStorePath(storePath), srcStore->getUri());
    });

    dstStore->addToStore(*info, *source, repair, checkSigs);
     */
    Ok(())
}

#[async_trait]
pub trait Store: StoreDirProvider {
    async fn query_valid_paths(
        &mut self,
        paths: &StorePathSet,
        _maybe_substitute: SubstituteFlag,
    ) -> Result<StorePathSet, Error> {
        let mut ret = StorePathSet::new();
        for path in paths.iter() {
            if self.query_path_info(path).await?.is_some() {
                ret.insert(path.clone());
            }
        }
        Ok(ret)
    }

    async fn query_path_info(&mut self, path: &StorePath) -> Result<Option<ValidPathInfo>, Error>;

    /// Export path from the store
    async fn nar_from_path<W: AsyncWrite + fmt::Debug + Send + Unpin>(
        &mut self,
        path: &StorePath,
        sink: W,
    ) -> Result<(), Error>;

    /// Import a path into the store.
    async fn add_to_store<R: AsyncRead + fmt::Debug + Send + Unpin>(
        &mut self,
        info: &ValidPathInfo,
        source: R,
        repair: RepairFlag,
        check_sigs: CheckSignaturesFlag,
    ) -> Result<(), Error>;

    async fn build_derivation(
        &mut self,
        drv_path: &StorePath,
        drv: &BasicDerivation,
        build_mode: BuildMode,
    ) -> Result<BuildResult, Error> {
        let _ = (drv_path, drv, build_mode);
        Err(Error::UnsupportedOperation("build_derivation".into()))
    }

    async fn build_paths(
        &mut self,
        drv_paths: &[DerivedPath],
        build_mode: BuildMode,
    ) -> Result<(), Error> {
        let _ = (drv_paths, build_mode);
        Err(Error::UnsupportedOperation("build_paths".into()))
    }
}

macro_rules! deref_store {
    () => {
        fn query_valid_paths<'life0, 'life1, 'async_trait>(
            &'life0 mut self,
            paths: &'life1 StorePathSet,
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
            Self: ::core::marker::Send + 'async_trait,
        {
            (**self).query_valid_paths(paths, maybe_substitute)
        }

        fn query_path_info<'life0, 'life1, 'async_trait>(
            &'life0 mut self,
            path: &'life1 StorePath,
        ) -> ::core::pin::Pin<
            Box<
                dyn ::core::future::Future<Output = Result<Option<ValidPathInfo>, Error>>
                    + ::core::marker::Send
                    + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            (**self).query_path_info(path)
        }

        fn nar_from_path<'life0, 'life1, 'async_trait, W>(
            &'life0 mut self,
            path: &'life1 StorePath,
            sink: W,
        ) -> ::core::pin::Pin<
            Box<
                dyn ::core::future::Future<Output = Result<(), Error>>
                    + ::core::marker::Send
                    + 'async_trait,
            >,
        >
        where
            W: 'async_trait + AsyncWrite + fmt::Debug + Send + Unpin,
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            (**self).nar_from_path(path, sink)
        }

        fn add_to_store<'life0, 'life1, 'async_trait, R>(
            &'life0 mut self,
            info: &'life1 ValidPathInfo,
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
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            (**self).add_to_store(info, source, repair, check_sigs)
        }

        fn build_derivation<'life0, 'life1, 'life2, 'async_trait>(
            &'life0 mut self,
            drv_path: &'life1 StorePath,
            drv: &'life2 BasicDerivation,
            build_mode: BuildMode,
        ) -> ::core::pin::Pin<
            Box<
                dyn ::core::future::Future<Output = Result<BuildResult, Error>>
                    + ::core::marker::Send
                    + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            'life2: 'async_trait,
            Self: ::core::marker::Send + 'async_trait,
        {
            (**self).build_derivation(drv_path, drv, build_mode)
        }
        fn build_paths<'life0, 'life1, 'async_trait>(
            &'life0 mut self,
            drv_paths: &'life1 [DerivedPath],
            build_mode: BuildMode,
        ) -> ::core::pin::Pin<
            Box<
                dyn ::core::future::Future<Output = Result<(), Error>>
                    + ::core::marker::Send
                    + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: ::core::marker::Send + 'async_trait,
        {
            (**self).build_paths(drv_paths, build_mode)
        }
    };
}

impl<T: ?Sized + Store + Unpin + Send> Store for Box<T> {
    deref_store!();
}

impl<T: ?Sized + Store + Unpin + Send> Store for &mut T {
    deref_store!();
}

#[cfg(any(test, feature = "test"))]
pub mod proptest {
    use std::time::Duration;

    use super::*;
    use crate::{proptest::arb_system_time, store::realisation::proptest::arb_drv_outputs};
    use ::proptest::prelude::*;

    impl Arbitrary for BuildMode {
        type Parameters = ();
        type Strategy = BoxedStrategy<BuildMode>;

        fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
            use BuildMode::*;
            prop_oneof![
                1 => (13u64..500u64).prop_map(Unknown),
                50 => Just(Normal),
                5 => Just(Repair),
                5 => Just(Check),
            ]
            .boxed()
        }
    }

    impl Arbitrary for BuildStatus {
        type Parameters = ();
        type Strategy = BoxedStrategy<BuildStatus>;

        fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
            use BuildStatus::*;
            prop_oneof![
                1 => (13u64..500u64).prop_map(Unsupported),
                50 => Just(Built),
                5 => Just(Substituted),
                5 => Just(AlreadyValid),
                5 => Just(PermanentFailure),
                5 => Just(InputRejected),
                5 => Just(OutputRejected),
                5 => Just(TransientFailure), // possibly transient
                5 => Just(TimedOut),
                5 => Just(MiscFailure),
                5 => Just(DependencyFailed),
                5 => Just(LogLimitExceeded),
                5 => Just(NotDeterministic)
            ]
            .boxed()
        }
    }

    impl Arbitrary for BuildResult {
        type Parameters = ();
        type Strategy = BoxedStrategy<BuildResult>;
        fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
            arb_build_result().boxed()
        }
    }

    prop_compose! {
        pub fn arb_build_result()
        (
            status in any::<BuildStatus>(),
            error_msg in any::<String>(),
            times_built in 0u64..50u64,
            is_non_deterministic in ::proptest::bool::ANY,
            built_outputs in arb_drv_outputs(0..10),
            start_time in arb_system_time(),
            duration_secs in 0u64..604_800u64,
        ) -> BuildResult
        {
            let stop_time = start_time + Duration::from_secs(duration_secs);
            BuildResult {
                status, error_msg, times_built, is_non_deterministic,
                built_outputs, start_time, stop_time,
            }
        }
    }
}
