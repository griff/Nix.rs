// SPDX-FileCopyrightText: 2024 griff
//
// SPDX-License-Identifier: EUPL-1.2 or MIT
//#![deny(unused_crate_dependencies)]

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
#[cfg(not(feature = "internal"))]
#[allow(unused_imports, dead_code)]
pub(crate) mod io;
pub mod realisation;
pub mod signature;
pub mod store_path;
#[cfg(feature = "internal")]
pub mod wire;
#[cfg(not(feature = "internal"))]
#[allow(dead_code)]
pub(crate) mod wire;

pub type ByteString = bytes::Bytes;

#[doc(hidden)]
pub mod exports {
    pub use tracing::trace;
}
