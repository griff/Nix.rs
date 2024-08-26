use std::cell::RefCell;
use std::collections::BTreeMap;
use std::future::Future;
use std::num::ParseIntError;
use std::str::ParseBoolError;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pin_project_lite::pin_project;
use thiserror::Error;

use super::error::Verbosity;

#[derive(Debug, Error, Clone)]
pub enum ParseSettingError {
    #[error("{0}")]
    ParseBool(
        #[source]
        #[from]
        ParseBoolError,
    ),
    #[error("{0}")]
    ParseInt(
        #[source]
        #[from]
        ParseIntError,
    ),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BuildSettings {
    /// Whether to keep temporary directories of failed builds.
    pub keep_failed: bool,

    /// Whether to keep building derivations when another build fails.
    pub keep_going: bool,

    /// If set to `true`, Nix will fall back to building from source if a
    /// binary substitute fails. This is equivalent to the `--fallback`
    /// flag. The default is `false`.
    pub try_fallback: bool,

    /// This option defines the maximum number of jobs that Nix will try to
    /// build in parallel. The default is `1`. The special value `auto`
    /// causes Nix to use the number of CPUs in your system. `0` is useful
    /// when using remote builders to prevent any local builds (except for
    /// `preferLocalBuild` derivation attribute which executes locally
    /// regardless). It can be overridden using the `--max-jobs` (`-j`)
    /// command line switch.
    pub max_build_jobs: u64,

    /// Whether to show build log output in real time.
    pub verbose_build: bool,

    /// Sets the value of the `NIX_BUILD_CORES` environment variable in the
    /// invocation of builders. Builders can use this variable at their
    /// discretion to control the maximum amount of parallelism. For
    /// instance, in Nixpkgs, if the derivation attribute
    /// `enableParallelBuilding` is set to `true`, the builder passes the
    /// `-jN` flag to GNU Make. It can be overridden using the `--cores`
    /// command line switch and defaults to `1`. The value `0` means that
    /// the builder should use all available CPU cores in the system.
    pub build_cores: u64,

    /// suppress msgs > this.
    /// This is used when talking using the daemon protocol to set the
    /// verbosity of the other end.
    pub verbosity: Verbosity,

    /// If set to `true` (the default), Nix will write the build log of a
    /// derivation (i.e. the standard output and error of its builder) to
    /// the directory `/nix/var/log/nix/drvs`. The build log can be
    /// retrieved using the command `nix-store -l path`.
    pub keep_log: bool,

    /// If set to `true` (default), Nix will use binary substitutes if
    /// available. This option can be disabled to force building from
    /// source.
    pub use_substitutes: bool,

    /// This option defines the maximum number of seconds that a builder can
    /// go without producing any data on standard output or standard error.
    /// This is useful (for instance in an automated build system) to catch
    /// builds that are stuck in an infinite loop, or to catch remote builds
    /// that are hanging due to network problems. It can be overridden using
    /// the `--max-silent-time` command line switch.
    ///
    /// The value `0` means that there is no timeout. This is also the
    /// default.
    pub max_silent_time: Duration,

    /// This option defines the maximum number of seconds that a builder can
    /// run. This is useful (for instance in an automated build system) to
    /// catch builds that are stuck in an infinite loop but keep writing to
    /// their standard output or standard error. It can be overridden using
    /// the `--timeout` command line switch.
    ///
    /// The value `0` means that there is no timeout. This is also the
    /// default.
    pub build_timeout: Duration,

    /// This option defines the maximum number of bytes that a builder can
    /// write to its stdout/stderr. If the builder exceeds this limit, it’s
    /// killed. A value of `0` (the default) means that there is no limit.
    pub max_log_size: u64,

    /// If true, enable the execution of the `diff-hook` program.
    ///
    /// When using the Nix daemon, `run-diff-hook` must be set in the
    /// `nix.conf` configuration file, and cannot be passed at the command
    /// line.
    pub run_diff_hook: bool,

    /// Unknown settings
    pub unknown: BTreeMap<String, String>,
}

impl BuildSettings {
    const fn const_default() -> BuildSettings {
        BuildSettings {
            keep_failed: false,
            keep_going: false,
            try_fallback: false,
            max_build_jobs: 1,
            verbose_build: true,
            build_cores: 0,
            verbosity: Verbosity::Info,
            keep_log: true,
            use_substitutes: true,
            max_silent_time: Duration::from_secs(0),
            build_timeout: Duration::from_secs(0),
            max_log_size: 0,
            run_diff_hook: false,
            unknown: BTreeMap::new(),
        }
    }

    pub fn set(&mut self, map: BTreeMap<String, String>) -> Result<(), ParseSettingError> {
        for (k, v) in map.into_iter() {
            match k.as_ref() {
                "keep-failed" => self.keep_failed = v.parse()?,
                "keep-going" => self.keep_going = v.parse()?,
                "fallback" => self.try_fallback = v.parse()?,
                "build-fallback" => self.try_fallback = v.parse()?,
                "max-jobs" => self.max_build_jobs = v.parse()?,
                "build-max-jobs" => self.max_build_jobs = v.parse()?,
                "cores" => self.build_cores = v.parse()?,
                "build-cores" => self.build_cores = v.parse()?,
                "keep-build-log" => self.keep_log = v.parse()?,
                "build-keep-log" => self.keep_log = v.parse()?,
                "substitute" => self.use_substitutes = v.parse()?,
                "build-use-substitutes" => self.use_substitutes = v.parse()?,
                "max-silent-time" => {
                    let secs: u64 = v.parse()?;
                    self.max_silent_time = Duration::from_secs(secs)
                }
                "build-max-silent-time" => {
                    let secs: u64 = v.parse()?;
                    self.max_silent_time = Duration::from_secs(secs)
                }
                "timeout" => {
                    let secs: u64 = v.parse()?;
                    self.build_timeout = Duration::from_secs(secs)
                }
                "build-timeout" => {
                    let secs: u64 = v.parse()?;
                    self.build_timeout = Duration::from_secs(secs)
                }
                "max-build-log-size" => self.max_log_size = v.parse()?,
                "build-max-log-size" => self.max_log_size = v.parse()?,
                "run-diff-hook" => self.run_diff_hook = v.parse()?,
                _ => {
                    self.unknown.insert(k, v);
                }
            }
        }
        Ok(())
    }

    pub fn get_all(&self, map: &mut BTreeMap<String, String>) {
        map.insert("keep-failed".into(), self.keep_failed.to_string());
        map.insert("keep-going".into(), self.keep_going.to_string());
        map.insert("fallback".into(), self.try_fallback.to_string());
        map.insert("max-jobs".into(), self.max_build_jobs.to_string());
        map.insert("cores".into(), self.build_cores.to_string());
        map.insert("keep-build-log".into(), self.keep_log.to_string());
        map.insert("substitute".into(), self.use_substitutes.to_string());
        map.insert(
            "max-silent-time".into(),
            self.max_silent_time.as_secs().to_string(),
        );
        map.insert("timeout".into(), self.build_timeout.as_secs().to_string());
        map.insert("max-build-log-size".into(), self.max_log_size.to_string());
        map.insert("run-diff-hook".into(), self.run_diff_hook.to_string());
        map.extend(self.unknown.iter().map(|(k, v)| (k.clone(), v.clone())));
    }
}

impl Default for BuildSettings {
    /// Returns the current default settings
    fn default() -> Self {
        get_settings(|default| default.clone())
    }
}

#[derive(Debug, Clone)]
pub struct DefaultSettings(Arc<Mutex<BuildSettings>>);

impl DefaultSettings {
    pub fn new(settings: BuildSettings) -> Self {
        Self(Arc::new(Mutex::new(settings)))
    }
}

impl Default for DefaultSettings {
    fn default() -> Self {
        get_default(|default| {
            default
                .cloned()
                .unwrap_or_else(|| DefaultSettings::new(NONE.clone()))
        })
    }
}

impl From<BuildSettings> for DefaultSettings {
    fn from(value: BuildSettings) -> Self {
        DefaultSettings::new(value)
    }
}

thread_local! {
    static CURRENT_STATE: State = State {
        default: RefCell::new(None),
    };
}

static NONE: BuildSettings = BuildSettings::const_default();

struct State {
    default: RefCell<Option<DefaultSettings>>,
}

#[derive(Debug)]
pub struct DefaultGuard(Option<DefaultSettings>);

pub fn with_default<T>(settings: &DefaultSettings, f: impl FnOnce() -> T) -> T {
    // When this guard is dropped, the default dispatcher will be reset to the
    // prior default. Using this (rather than simply resetting after calling
    // `f`) ensures that we always reset to the prior dispatcher even if `f`
    // panics.
    let _guard = set_default(settings);
    f()
}

pub fn set_default(settings: &DefaultSettings) -> DefaultGuard {
    // When this guard is dropped, the default dispatcher will be reset to the
    // prior default. Using this ensures that we always reset to the prior
    // dispatcher even if the thread calling this function panics.
    State::set_default(settings.clone())
}

pub fn get_default<T, F>(mut f: F) -> T
where
    F: FnMut(Option<&DefaultSettings>) -> T,
{
    CURRENT_STATE
        .try_with(|state| f(state.default.borrow().as_ref()))
        .unwrap_or_else(|_| f(None))
}

pub fn get_settings<T, F>(mut f: F) -> T
where
    F: FnMut(&BuildSettings) -> T,
{
    CURRENT_STATE
        .try_with(|state| {
            if let Some(settings) = state.default.borrow().as_ref() {
                if let Ok(inner) = settings.0.lock() {
                    return f(&inner);
                }
            }
            f(&NONE)
        })
        .unwrap_or_else(|_| f(&NONE))
}

pub fn get_mut_settings<T, F>(mut f: F) -> T
where
    F: FnMut(Option<&mut BuildSettings>) -> T,
{
    CURRENT_STATE
        .try_with(|state| {
            if let Some(settings) = state.default.borrow().as_ref() {
                if let Ok(mut inner) = settings.0.lock() {
                    return f(Some(&mut inner));
                }
            }
            f(None)
        })
        .unwrap_or_else(|_| f(None))
}

impl State {
    /// Replaces the current default dispatcher on this thread with the provided
    /// dispatcher.Any
    ///
    /// Dropping the returned `ResetGuard` will reset the default dispatcher to
    /// the previous value.
    #[inline]
    fn set_default(new_settings: DefaultSettings) -> DefaultGuard {
        let prior = CURRENT_STATE
            .try_with(|state| state.default.replace(Some(new_settings)))
            .ok()
            .flatten();
        DefaultGuard(prior)
    }
}

impl Drop for DefaultGuard {
    #[inline]
    fn drop(&mut self) {
        // Replace the dispatcher and then drop the old one outside
        // of the thread-local context. Dropping the dispatch may
        // lead to the drop of a subscriber which, in the process,
        // could then also attempt to access the same thread local
        // state -- causing a clash.
        let prev = CURRENT_STATE.try_with(|state| state.default.replace(self.0.take()));
        drop(prev)
    }
}

pin_project! {
    pub struct WithDefaultSettings<F> {
        settings: DefaultSettings,
        #[pin]
        inner: F,
    }
}

impl<F> Future for WithDefaultSettings<F>
where
    F: Future,
{
    type Output = F::Output;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.project();
        let settings = this.settings;
        let future = this.inner;
        with_default(settings, || future.poll(cx))
    }
}

pub trait WithSettings: Sized {
    fn with_settings<S>(self, settings: S) -> WithDefaultSettings<Self>
    where
        S: Into<DefaultSettings>;
    fn with_current_settings(self) -> WithDefaultSettings<Self>;
}

impl<T: Sized> WithSettings for T {
    fn with_settings<S>(self, settings: S) -> WithDefaultSettings<Self>
    where
        S: Into<DefaultSettings>,
    {
        WithDefaultSettings {
            settings: settings.into(),
            inner: self,
        }
    }
    fn with_current_settings(self) -> WithDefaultSettings<Self> {
        let settings: DefaultSettings = Default::default();
        self.with_settings(settings)
    }
}

#[cfg(any(test, feature = "test"))]
pub mod proptest {
    use super::*;
    use crate::proptest::arb_duration;
    use proptest::prelude::*;

    impl Arbitrary for BuildSettings {
        type Parameters = ();
        type Strategy = BoxedStrategy<BuildSettings>;

        fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
            arb_settings().boxed()
        }
    }

    prop_compose! {
        pub fn arb_settings()
        (
            max_silent_time in arb_duration(),
            build_timeout in arb_duration(),
            max_log_size in any::<u64>(),
            keep_failed in ::proptest::bool::ANY
        ) -> BuildSettings
        {
            BuildSettings {
                max_silent_time, build_timeout, max_log_size, keep_failed,
                keep_log: false,
                use_substitutes: false,
                run_diff_hook: true,
                ..Default::default()
            }
        }
    }
}
