use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BuildSettings {
    //TODO: verbosity: Verbosity,

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

    /// How many times to repeat builds to check whether they are
    /// deterministic. The default value is 0. If the value is non-zero,
    /// every build is repeated the specified number of times. If the
    /// contents of any of the runs differs from the previous ones and
    /// `enforce-determinism` is true, the build is rejected and the
    /// resulting store paths are not registered as “valid” in Nix’s
    /// database.
    pub build_repeat: u64,

    /// Whether to fail if repeated builds produce different output. See `repeat`.
    pub enforce_determinism: bool,

    /// If true, enable the execution of the `diff-hook` program.
    ///
    /// When using the Nix daemon, `run-diff-hook` must be set in the
    /// `nix.conf` configuration file, and cannot be passed at the command
    /// line.
    pub run_diff_hook: bool,

    /// When buildRepeat > 0 and verboseBuild == true, whether to print
    /// repeated builds (i.e. builds other than the first one) to
    /// stderr. Hack to prevent Hydra logs from being polluted.
    pub print_repeated_builds: bool,
}

impl Default for BuildSettings {
    fn default() -> Self {
        BuildSettings {
            keep_log: true,
            use_substitutes: true,
            max_silent_time: Duration::from_secs(0),
            build_timeout: Duration::from_secs(0),
            max_log_size: 0,
            build_repeat: 0,
            enforce_determinism: true,
            run_diff_hook: false,
            print_repeated_builds: true,
        }
    }
}

#[cfg(any(test, feature="test"))]
pub mod proptest {
    use ::proptest::prelude::*;
    use super::*;
    use nixrs_util::proptest::arb_duration;

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
            build_repeat in any::<u8>(),
            enforce_determinism in ::proptest::bool::ANY
        ) -> BuildSettings
        {
            BuildSettings {
                max_silent_time, build_timeout, max_log_size, enforce_determinism,
                build_repeat: build_repeat as u64,
                keep_log: false,
                use_substitutes: false,
                run_diff_hook: true,
                print_repeated_builds: false,
                ..Default::default()
            }
        }
    }
}