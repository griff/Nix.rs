use std::time::SystemTime;

use bytes::Bytes;
use nixrs_legacy::hash::{self, Algorithm};
use nixrs_legacy::path_info::ValidPathInfo;
use nixrs_legacy::signature::{ParseSignatureError, Signature, SignatureSet};
use nixrs_legacy::store::Error;
use nixrs_legacy::store_path::{
    ContentAddressMethod, FileIngestionMethod, ParseStorePathError, StorePath, StorePathSet,
};
use tvix_castore::proto;
use tvix_store::proto::nar_info::ca::Hash;
use tvix_store::proto::{self as store_proto, PathInfo};

pub fn valid_path_info_from_path_info(info: PathInfo) -> Result<ValidPathInfo, Error> {
    if let PathInfo {
        narinfo: Some(narinfo),
        node: Some(proto::Node { node: Some(node) }),
        ..
    } = info
    {
        let name_b = match node {
            proto::node::Node::Directory(dir) => dir.name.clone(),
            proto::node::Node::File(file) => file.name.clone(),
            proto::node::Node::Symlink(symlink) => symlink.name.clone(),
        };
        let name_s = std::str::from_utf8(&name_b).map_err(|_| Error::BadNarInfo)?;
        let path = StorePath::new_from_base_name(name_s)?;
        let references = narinfo
            .reference_names
            .iter()
            .map(|s| StorePath::new_from_base_name(s))
            .collect::<Result<StorePathSet, ParseStorePathError>>()?;
        let sigs = narinfo
            .signatures
            .iter()
            .map(|s| Signature::from_parts(&s.name, &s.data))
            .collect::<Result<SignatureSet, ParseSignatureError>>()?;
        let ret = ValidPathInfo {
            path,
            deriver: None,
            nar_size: narinfo.nar_size,
            nar_hash: hash::Hash::from_slice(hash::Algorithm::SHA256, &narinfo.nar_sha256)?,
            references,
            sigs,
            registration_time: SystemTime::UNIX_EPOCH,
            ultimate: false,
            ca: None,
        };
        Ok(ret)
    } else {
        Err(Error::BadNarInfo)
    }
}

pub fn path_info_from_valid_path_info(info: &ValidPathInfo, mut node: proto::Node) -> PathInfo {
    let references = info
        .references
        .iter()
        .map(|p| Bytes::from(p.hash.to_vec()))
        .collect();
    let reference_names = info.references.iter().map(|p| p.to_string()).collect();
    let signatures = info
        .sigs
        .iter()
        .map(|s| store_proto::nar_info::Signature {
            name: s.name().to_string(),
            data: s.signature_bytes().to_vec().into(),
        })
        .collect();
    let name: Bytes = info.path.to_string().as_bytes().to_vec().into();
    match &mut node.node {
        Some(proto::node::Node::Directory(ref mut dir)) => {
            dir.name = name;
        }
        Some(proto::node::Node::File(ref mut file)) => {
            file.name = name;
        }
        Some(proto::node::Node::Symlink(symlink)) => {
            symlink.name = name;
        }
        _ => {}
    }
    let ca = info.ca.as_ref().map(|info_ca| {
        use ContentAddressMethod::*;
        use FileIngestionMethod::*;
        let r#type = match (info_ca.method, info_ca.hash.algorithm()) {
            (Text, _) => Hash::TextSha256,
            (Fixed(Flat), Algorithm::MD5) => Hash::FlatMd5,
            (Fixed(Flat), Algorithm::SHA1) => Hash::FlatSha1,
            (Fixed(Flat), Algorithm::SHA256) => Hash::FlatSha256,
            (Fixed(Flat), Algorithm::SHA512) => Hash::FlatSha512,
            (Fixed(Recursive), Algorithm::MD5) => Hash::NarMd5,
            (Fixed(Recursive), Algorithm::SHA1) => Hash::NarSha1,
            (Fixed(Recursive), Algorithm::SHA256) => Hash::NarSha256,
            (Fixed(Recursive), Algorithm::SHA512) => Hash::NarSha512,
        }
        .into();
        store_proto::nar_info::Ca {
            r#type,
            digest: info_ca.hash.as_ref().to_vec().into(),
        }
    });
    let deriver = if let Some(info_deriver) = info.deriver.as_ref() {
        let name = info_deriver.name_from_drv().to_string();
        Some(store_proto::StorePath {
            digest: info_deriver.hash.as_ref().to_vec().into(),
            name,
        })
    } else {
        None
    };
    PathInfo {
        node: Some(node),
        references,
        narinfo: Some(store_proto::NarInfo {
            nar_size: info.nar_size,
            nar_sha256: info.nar_hash.data().to_vec().into(),
            ca,
            deriver,
            reference_names,
            signatures,
        }),
    }
}
