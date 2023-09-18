use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf, MAIN_SEPARATOR};
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use futures::future::try_join;
use log::{debug, trace};
use nixrs_util::io::{StateParse, StatePrint};
use nixrs_util::path::absolute_path_from_current;
use nixrs_util::path::clean_path;
use nixrs_util::path::resolve_link;
use nixrs_util::{flag_enum, hash, num_enum};
use tokio::fs;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;

use super::content_address::{ContentAddress, FileIngestionMethod};
use super::RepairFlag;
use super::{BasicDerivation, DrvOutputs, StorePath, StorePathSet, ValidPathInfo};
use super::{BuildSettings, DerivedPath, ParseStorePathError, ReadStorePathError};
use crate::Error;

/* Magic header of exportPath() output (obsolete). */
pub const EXPORT_MAGIC: u64 = 0x4558494e;

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
    #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Hash)]
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
        MiscFailure = 9,
        DependencyFailed = 10,
        LogLimitExceeded = 11,
        NotDeterministic = 12
    }
}
impl BuildStatus {
    pub fn success(&self) -> bool {
        match self {
            BuildStatus::Built | BuildStatus::Substituted | BuildStatus::AlreadyValid => true,
            _ => false,
        }
    }
}

impl Default for BuildStatus {
    fn default() -> Self {
        BuildStatus::MiscFailure
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

struct DisplayStorePath<'a> {
    store_dir: &'a StoreDir,
    path: &'a StorePath,
}

impl<'a> fmt::Display for DisplayStorePath<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}{}", self.store_dir, MAIN_SEPARATOR, self.path)
    }
}

/// Store directory.
/// Since the [`StorePath`] abstraction is only a hash and a name we need this
/// to convert the path to a full store path string.
///
/// ```
/// use nixrs_store::StoreDir;
/// let store = StoreDir::new("/nix/store").unwrap();
/// let path = store.parse_path("/nix/store/55xkmqns51sw7nrgykp5vnz36w4fr3cw-nix-2.1.3").unwrap();
/// ```
///
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StoreDir(Arc<PathBuf>, Arc<String>);
impl StoreDir {
    /// Create a new StoreDir from given path.
    /// This can fail if the path contains non-UTF-8 characters and therefore can't be
    /// converted to a [`String`].
    pub fn new<P: Into<PathBuf>>(path: P) -> Result<StoreDir, ParseStorePathError> {
        let path = path.into();
        let path_s = path
            .to_str()
            .ok_or_else(|| ParseStorePathError::BadStorePath(path.clone()))?
            .to_string();
        Ok(StoreDir(Arc::new(path), Arc::new(path_s)))
    }

    /// Get [`str`] representation of this StoreDir.
    ///
    /// ```
    /// # use nixrs_store::StoreDir;
    /// let store = StoreDir::new("/nix/store").unwrap();
    /// assert_eq!("/nix/store", store.to_str());
    /// ```
    pub fn to_str(&self) -> &str {
        self.1.as_ref()
    }

    /// Returns an object that implements [`Display`] for printing a [`StorePath`] complete
    /// with the full path.
    ///
    /// ```
    /// # use nixrs_store::StoreDir;
    /// let store = StoreDir::new("/nix/store").unwrap();
    /// let path = store.parse_path("/nix/store/55xkmqns51sw7nrgykp5vnz36w4fr3cw-nix-2.1.3").unwrap();
    /// println!("{}", store.display_path(&path));
    /// ```
    ///
    /// [`Display`]: fmt::Display
    pub fn display_path<'a>(&'a self, path: &'a StorePath) -> impl fmt::Display + 'a {
        DisplayStorePath {
            store_dir: self,
            path,
        }
    }

    /// Returns a [`String`] with the full path for the provided [`StorePath`].
    ///
    /// ```
    /// # use nixrs_store::StoreDir;
    /// let store = StoreDir::new("/nix/store").unwrap();
    /// let path = store.parse_path("/nix/store/55xkmqns51sw7nrgykp5vnz36w4fr3cw-nix-2.1.3").unwrap();
    /// assert_eq!("/nix/store/55xkmqns51sw7nrgykp5vnz36w4fr3cw-nix-2.1.3", store.print_path(&path));
    /// ```
    pub fn print_path(&self, path: &StorePath) -> String {
        self.display_path(path).to_string()
    }

    /// Parses a string `s` to a [`StorePath`].
    ///
    /// ```
    /// # use nixrs_store::StoreDir;
    /// let store = StoreDir::new("/nix/store").unwrap();
    /// let path = store.parse_path("/nix/store/55xkmqns51sw7nrgykp5vnz36w4fr3cw-nix-2.1.3").unwrap();
    /// assert_eq!("55xkmqns51sw7nrgykp5vnz36w4fr3cw-nix-2.1.3", format!("{}", path));
    /// ```
    pub fn parse_path(&self, s: &str) -> Result<StorePath, ParseStorePathError> {
        StorePath::new(Path::new(s), self)
    }

    fn make_type(
        &self,
        mut path_type: String,
        references: &StorePathSet,
        has_self_reference: bool,
    ) -> String {
        for reference in references {
            path_type.push_str(":");
            path_type.push_str(&self.print_path(reference));
        }
        if has_self_reference {
            path_type.push_str(":self");
        }
        path_type
    }

    pub fn make_store_path_str(
        &self,
        path_type: &str,
        hash: &str,
        name: &str,
    ) -> Result<StorePath, ParseStorePathError> {
        let s = format!("{}:{}:{}:{}", path_type, hash, self, name);
        StorePath::from_hash(&hash::digest(hash::Algorithm::SHA256, &s), name)
    }

    pub fn make_store_path(
        &self,
        path_type: &str,
        hash: hash::Hash,
        name: &str,
    ) -> Result<StorePath, ParseStorePathError> {
        self.make_store_path_str(path_type, &format!("{:x}", hash), name)
    }

    pub fn make_fixed_output_path(
        &self,
        method: FileIngestionMethod,
        hash: hash::Hash,
        name: &str,
        references: &StorePathSet,
        has_self_reference: bool,
    ) -> Result<StorePath, ParseStorePathError> {
        if let (hash::Algorithm::SHA256, FileIngestionMethod::Recursive) =
            (hash.algorithm(), method)
        {
            self.make_store_path(
                &self.make_type("source".into(), references, has_self_reference),
                hash,
                name,
            )
        } else {
            assert!(references.is_empty());
            let hash = hash::digest(
                hash::Algorithm::SHA256,
                &format!("fixed:out:{:#}{:x}:", method, hash),
            );
            trace!("Output hash {:x}", hash);
            self.make_store_path("output:out", hash, name)
        }
    }

    pub fn make_fixed_output_path_from_ca(
        &self,
        name: &str,
        ca: ContentAddress,
        references: &StorePathSet,
        has_self_reference: bool,
    ) -> Result<StorePath, ParseStorePathError> {
        use ContentAddress::*;
        match ca {
            TextHash(hash) => self.make_text_path(name, hash, references),
            FixedOutputHash(fsh) => self.make_fixed_output_path(
                fsh.method,
                fsh.hash,
                name,
                references,
                has_self_reference,
            ),
        }
    }

    pub fn make_text_path(
        &self,
        name: &str,
        hash: hash::Hash,
        references: &StorePathSet,
    ) -> Result<StorePath, ParseStorePathError> {
        assert_eq!(hash.algorithm(), hash::Algorithm::SHA256);
        // Stuff the references (if any) into the type.  This is a bit
        // hacky, but we can't put them in `s' since that would be
        // ambiguous.
        let path_type = self.make_type("text".into(), references, false);
        self.make_store_path(&path_type, hash, name)
    }

    fn strip_store_path<'a>(&self, path: &'a Path) -> Result<Cow<'a, Path>, &'a Path> {
        if !path.is_absolute() {
            return Err(path);
        }
        let clean = clean_path(path);
        if let Cow::Owned(o) = clean {
            match o.strip_prefix(&self) {
                Err(_) => Err(path),
                Ok(p) if p == Path::new("") => Err(path),
                Ok(p) => Ok(Cow::Owned(p.into())),
            }
        } else {
            match path.strip_prefix(&self) {
                Err(_) => Err(path),
                Ok(p) if p == Path::new("") => Err(path),
                Ok(p) => Ok(Cow::Borrowed(p)),
            }
        }
    }

    /// Checks that the suplied path is in this store.
    ///
    /// ```
    /// # use nixrs_store::StoreDir;
    /// let store = StoreDir::new("/nix/store").unwrap();
    /// assert_eq!(true, store.is_in_store("/nix/store/55xkmqns51sw7nrgykp5vnz36w4fr3cw-nix-2.1.3"));
    /// assert_eq!(true, store.is_in_store("/nix/store/55xkmqns51sw7nrgykp5vnz36w4fr3cw-nix-2.1.3/etc/"));
    /// assert_eq!(false, store.is_in_store("/nix/store/"));
    /// assert_eq!(false, store.is_in_store("/var/lib/"));
    /// ```
    pub fn is_in_store<P: AsRef<Path>>(&self, path: P) -> bool {
        match self.strip_store_path(path.as_ref()) {
            Err(_) => false,
            Ok(_) => true,
        }
    }

    /// Convert a `path` in this store to a [`StorePath`].
    /// This will fail when the given path is not in this store or when the path
    /// is not a valid store path.
    ///
    /// # Examples
    ///
    /// Basic store path with no extra path elements
    ///
    /// ```
    /// # use std::path::Path;
    /// # use nixrs_store::StoreDir;
    /// let store = StoreDir::new("/nix/store").unwrap();
    /// let p = Path::new("/nix/store/55xkmqns51sw7nrgykp5vnz36w4fr3cw-nix-2.1.3");
    /// let (path, rest) = store.to_store_path(p).unwrap();
    ///
    /// assert_eq!("55xkmqns51sw7nrgykp5vnz36w4fr3cw-nix-2.1.3", path.to_string());
    /// assert_eq!(Path::new(""), rest);
    /// ```
    ///
    /// Path that points to a file inside the store:
    ///
    /// ```
    /// # use std::path::Path;
    /// # use nixrs_store::StoreDir;
    /// let store = StoreDir::new("/nix/store").unwrap();
    /// let p = Path::new("/nix/store/55xkmqns51sw7nrgykp5vnz36w4fr3cw-nix-2.1.3/etc/init/nix-daemon.conf");
    /// let (path, rest) = store.to_store_path(p).unwrap();
    /// assert_eq!("55xkmqns51sw7nrgykp5vnz36w4fr3cw-nix-2.1.3", path.to_string());
    /// assert_eq!(Path::new("etc/init/nix-daemon.conf"), rest);
    /// ```
    ///
    /// Path outside the store:
    ///
    /// ```
    /// # use std::path::Path;
    /// # use nixrs_store::StoreDir;
    /// let store = StoreDir::new("/nix/store").unwrap();
    /// let res = store.to_store_path(Path::new("/var/local/lib"));
    /// assert!(res.is_err());
    /// ```
    pub fn to_store_path<'p>(
        &self,
        path: &'p Path,
    ) -> Result<(StorePath, Cow<'p, Path>), ParseStorePathError> {
        match self.strip_store_path(path) {
            Err(p) => Err(ParseStorePathError::NotInStore(p.into())),
            Ok(p) => match p {
                Cow::Owned(o) => {
                    let mut c = o.components();
                    let base_name = c.next().unwrap().as_os_str();
                    let store_path = StorePath::new_from_base_name(
                        base_name
                            .to_str()
                            .ok_or_else(|| ParseStorePathError::BadStorePath(path.into()))?,
                    )?;
                    let after = c.as_path();
                    Ok((store_path, Cow::Owned(after.into())))
                }
                Cow::Borrowed(b) => {
                    let mut c = b.components();
                    let base_name = c.next().unwrap().as_os_str();
                    let store_path = StorePath::new_from_base_name(
                        base_name
                            .to_str()
                            .ok_or_else(|| ParseStorePathError::BadStorePath(path.into()))?,
                    )?;
                    let after = c.as_path();
                    Ok((store_path, Cow::Borrowed(after)))
                }
            },
        }
    }

    /// Follow a chain of symlinks until we either end up with a path in this store
    /// or return an error.
    pub async fn follow_links_to_store(&self, path: &Path) -> Result<PathBuf, ReadStorePathError> {
        let mut path = absolute_path_from_current(path)?.into_owned();
        while !self.is_in_store(&path) {
            let m = fs::symlink_metadata(&path).await?;
            if !m.file_type().is_symlink() {
                break;
            }
            path = resolve_link(&path).await?;
        }
        if !self.is_in_store(&path) {
            Err(ReadStorePathError::BadStorePath(
                ParseStorePathError::NotInStore(path),
            ))
        } else {
            Ok(path)
        }
    }

    /// Like [`follow_links_to_store`] but returns a [`StorePath`].
    ///
    /// [`follow_links_to_store`]: #method.follow_links_to_store
    pub async fn follow_links_to_store_path(
        &self,
        path: &Path,
    ) -> Result<StorePath, ReadStorePathError> {
        let path = self.follow_links_to_store(path).await?;
        Ok(self.to_store_path(&path)?.0)
    }
}

impl StateParse<StorePath> for StoreDir {
    type Err = ReadStorePathError;

    fn parse(&self, s: &str) -> Result<StorePath, Self::Err> {
        Ok(self.parse_path(s)?)
    }
}

impl StatePrint<StorePath> for StoreDir {
    fn print(&self, path: &StorePath) -> String {
        self.print_path(path)
    }
}

impl AsRef<str> for StoreDir {
    fn as_ref(&self) -> &str {
        self.to_str()
    }
}

impl AsRef<Path> for StoreDir {
    fn as_ref(&self) -> &Path {
        self.0.as_ref()
    }
}

impl fmt::Display for StoreDir {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_str())
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

// TODO: Rewrite as parallel process when we support sync store
// This is also a super thrown together implementation that was done
// at the end of the project and after a 20h coding session.
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

    let missing: StorePathSet = store_paths.difference(&valid).map(|s| s.clone()).collect();

    let mut paths_map = BTreeMap::new();
    for path in store_paths.iter() {
        paths_map.insert(path.clone(), path.clone());
    }
    let dst_store_dir = dst_store.store_dir();
    let src_store_dir = src_store.store_dir();
    let mut refs = BTreeMap::new();
    let mut rrefs: BTreeMap<StorePath, StorePathSet> = BTreeMap::new();
    let mut roots = StorePathSet::new();
    for store_path in missing {
        if let Some(info) = src_store.query_path_info(&store_path).await? {
            let mut store_path_for_dst = store_path.clone();
            if info.ca.is_some() && info.references.is_empty() {
                store_path_for_dst = dst_store_dir.make_fixed_output_path_from_ca(
                    store_path.name.name(),
                    info.ca.unwrap(),
                    &StorePathSet::new(),
                    false,
                )?;
                if dst_store_dir == src_store_dir {
                    assert_eq!(store_path_for_dst, store_path)
                }
                if store_path_for_dst != store_path {
                    debug!(
                        "replaced path '{}' to '{}' for substituter '{}'",
                        src_store_dir.print_path(&store_path),
                        dst_store_dir.print_path(&store_path_for_dst),
                        "local"
                    );
                }
            }
            paths_map.insert(store_path.clone(), store_path_for_dst);
            if !dst_store.query_path_info(&store_path).await?.is_some() {
                let mut edges = info.references;
                edges.remove(&store_path);
                if edges.is_empty() {
                    roots.insert(store_path.clone());
                } else {
                    for m in edges.iter() {
                        rrefs
                            .entry(m.clone())
                            .or_default()
                            .insert(store_path.clone());
                    }
                    refs.insert(store_path, edges);
                }
            }
        }
    }
    let mut sorted = Vec::new();
    while !roots.is_empty() {
        let n = roots.iter().next().unwrap().to_owned();
        roots.remove(&n);
        sorted.push(n.clone());
        if let Some(edges) = rrefs.get(&n) {
            for m in edges {
                if let Some(references) = refs.get_mut(m) {
                    references.remove(&n);
                    if references.is_empty() {
                        roots.insert(m.clone());
                    }
                }
            }
        }
    }
    /*
    try_join_all(sorted.into_iter().map(|store_path| {
        let mut dst_store = dst_store.clone();
        let mut src_store = src_store.clone();
        async move {
            if !dst_store.is_valid_path(&store_path).await? {
                copy_store_path(&mut src_store, &mut dst_store, &store_path, repair, check_sigs).await?;
            }
            Ok(()) as Result<(), Error>
        }
    })).await?;
     */
    for store_path in sorted {
        if !dst_store.query_path_info(&store_path).await?.is_some() {
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
    let mut info = src_store
        .query_path_info(store_path)
        .await?
        .ok_or(Error::InvalidPath(store_path.to_string()))?;

    // recompute store path on the chance dstStore does it differently
    if info.ca.is_some() && info.references.is_empty() {
        let path = dst_store.store_dir().make_fixed_output_path_from_ca(
            info.path.name.name(),
            info.ca.unwrap(),
            &StorePathSet::new(),
            false,
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
        src_store.nar_from_path(&store_path, sink),
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

pub trait StoreDirProvider {
    /// Root path of this store
    fn store_dir(&self) -> StoreDir;
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
    async fn nar_from_path<W: AsyncWrite + Send + Unpin>(
        &mut self,
        path: &StorePath,
        sink: W,
    ) -> Result<(), Error>;

    /// Import a path into the store.
    async fn add_to_store<R: AsyncRead + Send + Unpin>(
        &mut self,
        info: &ValidPathInfo,
        source: R,
        repair: RepairFlag,
        check_sigs: CheckSignaturesFlag,
    ) -> Result<(), Error>;

    async fn build_derivation<W: AsyncWrite + Send + Unpin>(
        &mut self,
        drv_path: &StorePath,
        drv: &BasicDerivation,
        settings: &BuildSettings,
        build_log: W,
    ) -> Result<BuildResult, Error>;

    async fn build_paths<W: AsyncWrite + Send + Unpin>(
        &mut self,
        drv_paths: &[DerivedPath],
        settings: &BuildSettings,
        build_log: W,
    ) -> Result<(), Error>;
}

#[cfg(any(test, feature = "test"))]
pub mod proptest {
    use std::time::Duration;

    use super::*;
    use ::proptest::prelude::*;
    use nixrs_util::proptest::arb_system_time;

    impl Arbitrary for BuildStatus {
        type Parameters = ();
        type Strategy = BoxedStrategy<BuildStatus>;

        fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
            use BuildStatus::*;
            prop_oneof![
                1 => (13u64..500u64).prop_map(|v| Unsupported(v) ),
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
            built_outputs in any::<DrvOutputs>(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use ::proptest::{arbitrary::any, prop_assert_eq, proptest};
    use nixrs_util::hash;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_store_dir_display() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        assert_eq!(format!("{}", store_dir), "/nix/store");
        assert_eq!(store_dir.to_str(), "/nix/store");
        let s: &str = store_dir.as_ref();
        assert_eq!(s, "/nix/store");
    }

    #[test]
    fn test_store_dir_as_ref() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let s: &Path = store_dir.as_ref();
        assert_eq!(s, Path::new("/nix/store"));
    }

    #[test]
    fn test_store_dir_parse() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let p = store_dir
            .parse_path("/nix/store/7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3")
            .unwrap();
        assert_eq!(p.name.as_ref(), "konsole-18.12.3");
        assert_eq!(
            p.hash.as_ref(),
            [
                0x9f, 0x76, 0x49, 0x20, 0xf6, 0x5d, 0xe9, 0x71, 0xc4, 0xca, 0x46, 0x21, 0xab, 0xff,
                0x9b, 0x44, 0xef, 0x87, 0x0f, 0x3c
            ]
        );
        let p: StorePath = store_dir
            .parse("/nix/store/7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3")
            .unwrap();
        assert_eq!(
            p.to_string(),
            "7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3"
        );
    }

    #[test]
    fn test_store_dir_print() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let p = store_dir
            .parse_path("/nix/store/7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3")
            .unwrap();
        assert_eq!(
            store_dir.print_path(&p),
            "/nix/store/7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3"
        );
        assert_eq!(
            StatePrint::print(&store_dir, &p),
            "/nix/store/7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3"
        );
        assert_eq!(
            p.print(&store_dir),
            "/nix/store/7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3"
        );
    }

    #[test]
    fn test_store_dir_parse_not_in_store() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let path = "/niv/store/7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3";
        let p = store_dir.parse_path(path);
        assert_eq!(p, Err(ParseStorePathError::NotInStore(PathBuf::from(path))));
    }

    #[test]
    fn test_store_dir_make_output_path() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let d = hash::digest(hash::Algorithm::SHA256, "source:sha256:248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1:/nix/store:konsole-18.12.3");
        let p = StorePath::from_hash(&d, "konsole-18.12.3").unwrap();
        let hash = "sha256:248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"
            .parse::<hash::Hash>()
            .unwrap();
        let p2 = store_dir
            .make_store_path("source", hash, "konsole-18.12.3")
            .unwrap();
        assert_eq!(p2, p);
        assert_eq!(
            format!("{}", p2),
            "1w01xxn8f7s9s4n65ry6rwd7x9awf04s-konsole-18.12.3"
        );
    }

    #[test]
    fn test_store_dir_make_fixed_output_path() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let d = hash::digest(hash::Algorithm::SHA256, "source:sha256:248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1:/nix/store:konsole-18.12.3");
        let p = StorePath::from_hash(&d, "konsole-18.12.3").unwrap();

        let hash = "sha256:248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"
            .parse::<hash::Hash>()
            .unwrap();
        let p2 = store_dir
            .make_fixed_output_path(
                FileIngestionMethod::Recursive,
                hash,
                "konsole-18.12.3",
                &StorePathSet::new(),
                false,
            )
            .unwrap();
        assert_eq!(p2, p);
        assert_eq!(
            format!("{}", p2),
            "1w01xxn8f7s9s4n65ry6rwd7x9awf04s-konsole-18.12.3"
        );
    }

    #[test]
    fn test_store_dir_make_fixed_output_path2() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let d = hash::digest(hash::Algorithm::SHA256, "source:/nix/store/7h7qgvs4kgzsn8a6rb274saxyqh4jxlz-konsole-18.12.3.drv:/nix/store/ldhh7c134ap5swsm86rqnc0i7cinqvrc-my-terminal:self:sha256:248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1:/nix/store:konsole-18.12.3");
        let p = StorePath::from_hash(&d, "konsole-18.12.3").unwrap();

        let hash = "sha256:248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"
            .parse::<hash::Hash>()
            .unwrap();
        let mut set = StorePathSet::new();
        set.insert(
            StorePath::new_from_base_name("ldhh7c134ap5swsm86rqnc0i7cinqvrc-my-terminal").unwrap(),
        );
        set.insert(
            StorePath::new_from_base_name("7h7qgvs4kgzsn8a6rb274saxyqh4jxlz-konsole-18.12.3.drv")
                .unwrap(),
        );
        let p2 = store_dir
            .make_fixed_output_path(
                FileIngestionMethod::Recursive,
                hash,
                "konsole-18.12.3",
                &set,
                true,
            )
            .unwrap();
        assert_eq!(p2, p);
        assert_eq!(
            format!("{}", p2),
            "k7jq5x1vj193x2317ypwsl4k9h0kvra2-konsole-18.12.3"
        );
    }

    #[test]
    fn test_store_dir_make_fixed_output_path_flat() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let d = hash::digest(
            hash::Algorithm::SHA256,
            "fixed:out:sha256:248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1:",
        );
        let d = hash::digest(
            hash::Algorithm::SHA256,
            &format!("output:out:{:x}:/nix/store:konsole-18.12.3", d),
        );
        let p = StorePath::from_hash(&d, "konsole-18.12.3").unwrap();

        let hash = "sha256:248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"
            .parse::<hash::Hash>()
            .unwrap();

        let p2 = store_dir
            .make_fixed_output_path(
                FileIngestionMethod::Flat,
                hash,
                "konsole-18.12.3",
                &StorePathSet::new(),
                false,
            )
            .unwrap();
        assert_eq!(p2, p);
        assert_eq!(
            format!("{}", p2),
            "jw8chmp9sf8f7pw684cszp6pa2zmn0bx-konsole-18.12.3"
        );
    }

    #[test]
    fn test_store_dir_make_fixed_output_path_sha1() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let d = hash::digest(
            hash::Algorithm::SHA256,
            "fixed:out:r:sha1:84983e441c3bd26ebaae4aa1f95129e5e54670f1:",
        );
        let d = hash::digest(
            hash::Algorithm::SHA256,
            &format!("output:out:{:x}:/nix/store:konsole-18.12.3", d),
        );
        let p = StorePath::from_hash(&d, "konsole-18.12.3").unwrap();

        let hash = "sha1:84983e441c3bd26ebaae4aa1f95129e5e54670f1"
            .parse::<hash::Hash>()
            .unwrap();

        let p2 = store_dir
            .make_fixed_output_path(
                FileIngestionMethod::Recursive,
                hash,
                "konsole-18.12.3",
                &StorePathSet::new(),
                false,
            )
            .unwrap();
        assert_eq!(p2, p);
        assert_eq!(
            format!("{}", p2),
            "ww9d58nz1xsl5ck0vcpc99h23l1y2hln-konsole-18.12.3"
        );
    }

    #[test]
    fn test_store_dir_is_in_store() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        assert!(!store_dir.is_in_store("/"));
        assert!(!store_dir.is_in_store("/test"));
        assert!(!store_dir.is_in_store("/nix"));
        assert!(!store_dir.is_in_store("/nix/store"));
        assert!(!store_dir.is_in_store("/nix/store.rs"));
        assert!(!store_dir.is_in_store("/nix/store/"));
        assert!(!store_dir.is_in_store("/nix/store/.."));
        assert!(!store_dir.is_in_store("/nix/store/test/.."));
        assert!(!store_dir.is_in_store("/nix/store/test/../../hello"));

        assert!(store_dir.is_in_store("/nix/store/test/../hello"));
        assert!(store_dir.is_in_store("/nix/test/../store/test"));
        assert!(store_dir.is_in_store("/nix/store/test"));
        assert!(store_dir.is_in_store("/nix/store/test/"));
        assert!(store_dir.is_in_store("/nix/store/test/also"));
        assert!(store_dir.is_in_store("/nix/store/test/also/"));
        assert!(store_dir.is_in_store("/nix/store/a"));
    }

    #[test]
    fn test_store_dir_to_store_path_error() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        assert_eq!(
            store_dir.to_store_path(Path::new("/")),
            Err(ParseStorePathError::NotInStore("/".into()))
        );
        assert_eq!(
            store_dir.to_store_path(Path::new("/test")),
            Err(ParseStorePathError::NotInStore("/test".into()))
        );
        assert_eq!(
            store_dir.to_store_path(Path::new("/nix")),
            Err(ParseStorePathError::NotInStore("/nix".into()))
        );
        assert_eq!(
            store_dir.to_store_path(Path::new("/nix/store")),
            Err(ParseStorePathError::NotInStore("/nix/store".into()))
        );
        assert_eq!(
            store_dir.to_store_path(Path::new("/nix/store.rs")),
            Err(ParseStorePathError::NotInStore("/nix/store.rs".into()))
        );
        assert_eq!(
            store_dir.to_store_path(Path::new("/nix/store/")),
            Err(ParseStorePathError::NotInStore("/nix/store/".into()))
        );
        assert_eq!(
            store_dir.to_store_path(Path::new("/nix/store/..")),
            Err(ParseStorePathError::NotInStore("/nix/store/..".into()))
        );
        assert_eq!(
            store_dir.to_store_path(Path::new(
                "/nix/store/ww9d58nz1xsl5ck0vcpc99h23l1y2hln-konsole-18.12.3/.."
            )),
            Err(ParseStorePathError::NotInStore(
                "/nix/store/ww9d58nz1xsl5ck0vcpc99h23l1y2hln-konsole-18.12.3/..".into()
            ))
        );
        assert_eq!(
            store_dir.to_store_path(Path::new(
                "/nix/store/ww9d58nz1xsl5ck0vcpc99h23l1y2hln-konsole-18.12.3/../../hello"
            )),
            Err(ParseStorePathError::NotInStore(
                "/nix/store/ww9d58nz1xsl5ck0vcpc99h23l1y2hln-konsole-18.12.3/../../hello".into()
            ))
        );
    }

    #[test]
    fn test_store_dir_to_store_path() {
        let store_dir = StoreDir::new("/nix/store").unwrap();

        let sp = StorePath::new_from_base_name("ww9d58nz1xsl5ck0vcpc99h23l1y2hln-konsole-18.12.3")
            .unwrap();
        assert_eq!(
            store_dir
                .to_store_path(Path::new(
                    "/nix/store/ww9d58nz1xsl5ck0vcpc99h23l1y2hln-konsole-18.12.3"
                ))
                .unwrap(),
            (sp.clone(), Cow::Borrowed(Path::new("")))
        );
        assert_eq!(
            store_dir
                .to_store_path(Path::new(
                    "/nix/store/test/../ww9d58nz1xsl5ck0vcpc99h23l1y2hln-konsole-18.12.3"
                ))
                .unwrap(),
            (sp.clone(), Cow::Borrowed(Path::new("")))
        );
        assert_eq!(
            store_dir
                .to_store_path(Path::new(
                    "/nix/store/ww9d58nz1xsl5ck0vcpc99h23l1y2hln-konsole-18.12.3/"
                ))
                .unwrap(),
            (sp.clone(), Cow::Borrowed(Path::new("")))
        );
        assert_eq!(
            store_dir
                .to_store_path(Path::new(
                    "/nix/store/ww9d58nz1xsl5ck0vcpc99h23l1y2hln-konsole-18.12.3/also"
                ))
                .unwrap(),
            (sp.clone(), Cow::Borrowed(Path::new("also")))
        );
        assert_eq!(
            store_dir
                .to_store_path(Path::new(
                    "/nix/store/ww9d58nz1xsl5ck0vcpc99h23l1y2hln-konsole-18.12.3/also/"
                ))
                .unwrap(),
            (sp.clone(), Cow::Borrowed(Path::new("also/")))
        );
        assert_eq!(
            store_dir
                .to_store_path(Path::new(
                    "/nix/store/ww9d58nz1xsl5ck0vcpc99h23l1y2hln-konsole-18.12.3/more/../also/"
                ))
                .unwrap(),
            (sp.clone(), Cow::Borrowed(Path::new("also/")))
        );
        let sp = StorePath::new_from_base_name("ww7d58nz1xsl5ck0vcpc99h23l1y2hln-a").unwrap();
        assert_eq!(
            store_dir
                .to_store_path(Path::new("/nix/store/ww7d58nz1xsl5ck0vcpc99h23l1y2hln-a"))
                .unwrap(),
            (sp, Cow::Borrowed(Path::new("")))
        );
    }

    proptest! {
        #[test]
        fn test_string_parse(path in any::<StorePath>()) {
            let store_dir = StoreDir::new("/nix/store").unwrap();
            let s = store_dir.print_path(&path);
            let parsed = store_dir.parse_path(&s).unwrap();
            prop_assert_eq!(path, parsed);
        }
    }
}
