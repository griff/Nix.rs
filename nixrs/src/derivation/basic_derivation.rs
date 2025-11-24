use std::collections::BTreeMap;

use crate::ByteString;
use crate::store_path::{StorePath, StorePathSet};

use super::DerivationOutputs;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BasicDerivation {
    pub drv_path: StorePath,
    pub outputs: DerivationOutputs,
    pub input_srcs: StorePathSet,
    pub platform: ByteString,
    pub builder: ByteString,
    pub args: Vec<ByteString>,
    pub env: BTreeMap<ByteString, ByteString>,
}
