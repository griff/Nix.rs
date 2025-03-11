// SPDX-FileCopyrightText: 2024 griff
//
// SPDX-License-Identifier: EUPL-1.2 or MIT
//#![deny(unused_crate_dependencies)]

extern crate self as nixrs;

#[cfg(feature = "archive")]
pub mod archive;
pub mod base32;
pub mod daemon;
pub mod hash;
pub(crate) mod io;
pub mod store_path;
pub(crate) mod wire;

#[doc(hidden)]
pub mod exports {
    pub use tracing::trace;
}
