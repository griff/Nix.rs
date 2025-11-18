// SPDX-FileCopyrightText: 2024 griff
//
// SPDX-License-Identifier: EUPL-1.2 or MIT
//#![deny(unused_crate_dependencies)]

use serde::Serialize;

extern crate self as nixrs;

#[cfg(feature = "archive")]
pub mod archive;
pub mod base32;
#[cfg(any(feature = "daemon", feature = "daemon-serde"))]
pub mod daemon;
pub mod derivation;
pub mod derived_path;
pub mod hash;
#[cfg(feature = "internal")]
pub mod io;
#[cfg(all(
    not(feature = "internal"),
    any(feature = "archive", feature = "daemon-serde")
))]
#[allow(unused_imports, dead_code)]
pub(crate) mod io;
pub mod log;
pub mod profile;
pub mod realisation;
pub mod signature;
pub mod store_path;
#[cfg(any(test, feature = "test"))]
pub mod test;
#[cfg(feature = "internal")]
pub mod wire;
#[cfg(not(feature = "internal"))]
pub(crate) mod wire;

pub type ByteString = bytes::Bytes;

pub(crate) fn serialize_byte_string<S>(value: &ByteString, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    match std::str::from_utf8(value) {
        Ok(s) => s.serialize(serializer),
        Err(_) => value.serialize(serializer),
    }
}

#[doc(hidden)]
pub mod exports {
    pub use tracing::trace;
}
