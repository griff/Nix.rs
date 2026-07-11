use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::ByteString;
use crate::store_path::{StorePath, StorePathSet};

use super::DerivationOutputs;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BasicDerivation {
    pub drv_path: StorePath,
    pub outputs: DerivationOutputs,
    pub input_srcs: StorePathSet,
    #[serde(serialize_with = "crate::serialize_byte_string")]
    pub platform: ByteString,
    #[serde(serialize_with = "crate::serialize_byte_string")]
    pub builder: ByteString,
    pub args: Vec<ByteString>,
    pub env: BTreeMap<ByteString, ByteString>,
}
