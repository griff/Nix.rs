use std::collections::BTreeSet;

pub mod archive;
pub mod base32;
mod closure;
mod flag_enum;
pub mod hash;
pub mod io;
pub mod num_enum;
pub mod path;

pub use closure::compute_closure;

pub type StringSet = BTreeSet<String>;

#[macro_export]
macro_rules! string_set {
    [] => { StringSet::new()};
    [$e:expr] => {{
        let mut ret = StringSet::new();
        ret.insert(($e).to_string());
        ret
    }};
    [$e:expr$(,$e2:expr)+$(,)?] => {{
        let mut ret = StringSet::new();
        ret.insert(($e).to_string());
        $(
            ret.insert(($e2).to_string());
        )+
        ret
    }}
}

#[macro_export]
macro_rules! ready {
    ($e:expr) => {
        match $e {
            std::task::Poll::Ready(t) => t,
            std::task::Poll::Pending => {
                return std::task::Poll::Pending;
            }
        }
    };
}

#[cfg(any(test, feature = "test"))]
pub mod proptest {
    use std::{
        path::PathBuf,
        time::{Duration, SystemTime},
    };

    use ::proptest::prelude::*;

    pub fn arb_filename() -> impl Strategy<Value = String> {
        "[a-zA-Z 0-9.?=+]+".prop_filter("Not cur and parent dir", |s| s != "." && s != "..")
    }
    /*
    pub fn arb_filename() -> impl Strategy<Value=String> {
        "[^!/\\r\\n\u{0}\\pC]+"
            .prop_filter("Not cur and parent dir", |s| s != "." && s != ".." )
    }
    */
    pub fn arb_file_component() -> impl Strategy<Value = String> {
        "[a-zA-Z 0-9.?=+]+"
    }
    prop_compose! {
        pub fn arb_path()(prefix in "[a-zA-Z 0-9.?=+][a-zA-Z 0-9.?=+/]{0,250}", last in arb_filename()) -> PathBuf
        {
            let mut ret = PathBuf::from(prefix);
            ret.push(last);
            ret
        }
    }
    prop_compose! {
        pub fn arb_system_time()(secs in arb_duration()) -> SystemTime
        {
            SystemTime::UNIX_EPOCH + secs
        }
    }
    prop_compose! {
        pub fn arb_duration()(secs in ::proptest::num::i32::ANY) -> Duration
        {
            Duration::from_secs((secs as i64).abs() as u64)
        }
    }

    #[macro_export]
    macro_rules! pretty_prop_assert_eq {
        ($left:expr , $right:expr,) => ({
            $crate::assert_eq!($left, $right)
        });
        ($left:expr , $right:expr) => ({
            match (&($left), &($right)) {
                (left_val, right_val) => {
                    ::proptest::prop_assert!(*left_val == *right_val,
                        "assertion failed: `(left == right)`\
                              \n\
                              \n{}\
                              \n",
                              ::pretty_assertions::Comparison::new(left_val, right_val))
                }
            }
        });
        ($left:expr , $right:expr, $($arg:tt)*) => ({
            match (&($left), &($right)) {
                (left_val, right_val) => {
                    ::proptest::prop_assert!(*left_val == *right_val,
                        "assertion failed: `(left == right)`: {}\
                              \n\
                              \n{}\
                              \n",
                               format_args!($($arg)*),
                               ::pretty_assertions::Comparison::new(left_val, right_val))
                }
            }
        });
    }
}
