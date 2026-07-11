use std::{path::PathBuf, time::Duration};

use ::proptest::prelude::*;

use crate::ByteString;

#[cfg(feature = "archive")]
pub mod archive;
#[cfg(any(feature = "daemon", feature = "daemon-serde"))]
#[cfg_attr(docsrs, doc(cfg(feature = "daemon")))]
pub mod daemon;
pub mod derivation;
mod derived_path;
mod hash;
#[cfg(feature = "internal")]
pub mod helpers;
#[cfg(not(feature = "internal"))]
pub(crate) mod helpers;
pub mod log;
pub mod realisation;
pub mod signature;
pub mod store_path;

pub fn arb_filename() -> impl Strategy<Value = String> {
    "[.][a-zA-Z 0-9?=+]|[.][a-zA-Z 0-9.?=+]{2,}|[a-zA-Z 0-9?=+][a-zA-Z 0-9.?=+]*"
        .prop_filter("Not cur and parent dir", |s| s != "." && s != "..")
}

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

pub fn arb_url_path() -> impl Strategy<Value = String> {
    proptest::collection::vec(arb_filename(), 0..50).prop_map(|components| components.join("/"))
}

prop_compose! {
    pub fn arb_byte_string()(data in any::<Vec<u8>>()) -> ByteString {
        ByteString::from(data)
    }
}

prop_compose! {
    pub fn arb_system_time()(secs in arb_duration()) -> Duration
    {
        secs
    }
}

prop_compose! {
    pub fn arb_duration()(secs in ::proptest::num::i32::ANY) -> Duration
    {
        Duration::from_secs((secs as i64).unsigned_abs())
    }
}

pub fn arb_hostname() -> impl Strategy<Value = String> {
    "[a-zA-Z0-9_]+"
}

pub fn arb_dns_hostname() -> impl Strategy<Value = String> {
    proptest::collection::vec(arb_hostname(), 0..5).prop_map(|segments| segments.join("."))
}

pub fn arb_http_uri() -> impl Strategy<Value = http::Uri> {
    // FUTUREWORK: This is very naive and could be massively improved
    (arb_dns_hostname(), arb_url_path()).prop_filter_map("invalid url", |(host, path)| {
        let uri_s = format!("https://{host}/{path}");
        uri_s.parse::<http::Uri>().ok()
    })
}

#[macro_export]
macro_rules! pretty_prop_assert_eq {
    ($left:expr , $right:expr,) => ({
        $crate::pretty_prop_assert_eq!($left, $right)
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
