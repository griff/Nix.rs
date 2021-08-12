use std::collections::HashSet;
use std::io::{Cursor, Read};
use std::path::PathBuf;

use crate::StorePath;
use serde::{Deserialize, Serialize};

//mod s3;
#[allow(unused)]
fn nar_info_file_for(path: &StorePath) -> String {
    format!("{}.narinfo", path.hash)
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
enum Compression {
    None,
    XZ,
    BZip2,
}

impl Default for Compression {
    fn default() -> Compression {
        Compression::XZ
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Config {
    //const Setting<std::string> compression{(StoreConfig*) this, "xz", "compression", "NAR compression method ('xz', 'bzip2', or 'none')"};
    /// NAR compression method ('xz', 'bzip2', or 'none')
    #[serde(default)]
    compression: Compression,

    //const Setting<bool> writeNARListing{(StoreConfig*) this, false, "write-nar-listing", "whether to write a JSON file listing the files in each NAR"};
    /// whether to write a JSON file listing the files in each NAR
    #[serde(rename = "write-nar-listing")]
    #[serde(default)]
    write_nar_listing: bool,

    //const Setting<bool> writeDebugInfo{(StoreConfig*) this, false, "index-debug-info", "whether to index DWARF debug info files by build ID"};
    /// whether to index DWARF debug info files by build ID
    #[serde(rename = "index-debug-info")]
    #[serde(default)]
    index_debug_info: bool,

    //const Setting<Path> secretKeyFile{(StoreConfig*) this, "", "secret-key", "path to secret key used to sign the binary cache"};
    /// path to secret key used to sign the binary cache
    #[serde(rename = "secret-key")]
    #[serde(default)]
    secret_key: Option<PathBuf>,

    //const Setting<Path> localNarCache{(StoreConfig*) this, "", "local-nar-cache", "path to a local cache of NARs"};
    /// path to a local cache of NARs
    #[serde(rename = "local-nar-cache")]
    #[serde(default)]
    local_nar_cache: Option<PathBuf>,

    //const Setting<bool> parallelCompression{(StoreConfig*) this, false, "parallel-compression",
    //    "enable multi-threading compression, available for xz only currently"};
    /// enable multi-threading compression, available for xz only currently
    #[serde(rename = "parallel-compression")]
    #[serde(default)]
    parallel_compression: bool,
}

pub trait BinaryCache {
    fn uri_schemes(&self) -> HashSet<String>;
    fn file_exists(&self, path: &StorePath) -> bool;
    fn upsert_file<R: Read>(&self, path: &StorePath, stream: R, mime_type: &str);
    fn upsert_file_data(&self, path: &StorePath, data: &[u8], mime_type: &str) {
        let stream = Cursor::new(data);
        self.upsert_file(path, stream, mime_type)
    }
    /// Dump the contents of the specified file to a sink.
    fn get_file(&self, path: &StorePath) -> Vec<u8>;
    fn query_all_valid_paths(&self) -> HashSet<StorePath>;
    /*
    fn isValidPathUncached(&self, storePath: & StorePath) -> bool {
        // FIXME: this only checks whether a .narinfo with a matching hash
        // part exists. So ‘f4kb...-foo’ matches ‘f4kb...-bar’, even
        // though they shouldn't. Not easily fixed.
        return fileExists(narInfoFileFor(storePath));
    }
    */
}
