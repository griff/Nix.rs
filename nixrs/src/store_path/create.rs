//!
//! ```EBNF
//! type = path_type, { ':', reference }, [ ':self']
//! store_path = path_type, ':', store_path_hash, ':', store_dir, ':', name
//! store_path_hash = 'sha256:', digest
//!
//! fingerprint = type, ':sha256', inner_digest, ':', store_dir, ':', name
//!
//! fixed_output_path_from_ca = text_path | fixed_output_path
//! text_path = 'text', { ':', reference }, ':sha256', text_digest, ':', store_dir, ':', name
//! source_path = 'source', { ':', reference }, [ ':self'], ':sha256', nar_digest, ':', store_dir, ':', name
//! fixed_path = 'output:out:', ':sha256', fixed_out_hash, ':', store_dir, ':', name
//! fixed_output_path = source_path | fixed_path
//! fixed_out_hash = digest(fixed_out_hash_input)
//! fixed_out_hash_input = 'fixed:out:', ingestion_method, fixed_output_hash
//! ingestion_method = '' | 'r:'
//! fixed_output_hash = algorithm, ':', base16
//! ```
//!

use super::{ContentAddress, FixedOutput, StoreDirDisplay, StorePathSet};
use crate::hash;

pub struct Fingerprint<N> {
    pub path_type: PathType,
    pub name: N,
}

impl<N> StoreDirDisplay for Fingerprint<N>
where
    N: std::fmt::Display,
{
    fn fmt(
        &self,
        store_dir: &super::StoreDir,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(
            f,
            "{}:{}:{}",
            store_dir.display(&self.path_type),
            store_dir,
            self.name
        )
    }
}

pub enum PathType {
    Text {
        references: StorePathSet,
        digest: hash::Sha256,
    },
    Source {
        references: StorePathSet,
        self_ref: bool,
        digest: hash::Sha256,
    },
    FixedOutput(FixedOutput),
}

impl From<ContentAddress> for PathType {
    fn from(value: ContentAddress) -> Self {
        match value {
            ContentAddress::Text(digest) => PathType::Text {
                references: StorePathSet::new(),
                digest,
            },
            ContentAddress::Fixed(fo) if fo.is_source() => {
                let digest = fo.hash.try_into().unwrap();
                PathType::Source {
                    references: StorePathSet::new(),
                    self_ref: false,
                    digest,
                }
            }
            ContentAddress::Fixed(fo) => PathType::FixedOutput(fo),
        }
    }
}

impl StoreDirDisplay for PathType {
    fn fmt(
        &self,
        store_dir: &super::StoreDir,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        match self {
            PathType::Text { references, digest } => {
                f.write_str("text")?;
                for path in references {
                    write!(f, ":{}", store_dir.display(path))?
                }
                write!(f, ":sha256:{digest:x}")
            }
            PathType::Source {
                references,
                self_ref,
                digest,
            } => {
                f.write_str("source")?;
                for path in references {
                    write!(f, ":{}", store_dir.display(path))?
                }
                if *self_ref {
                    f.write_str(":self")?;
                }
                write!(f, ":sha256:{digest:x}")
            }
            PathType::FixedOutput(fo) => {
                let digest = fo.fod_digest();
                write!(f, "output:out:sha256:{digest:x}")
            }
        }
    }
}
