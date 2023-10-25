use std::time::SystemTime;

use bytes::Bytes;
use nixrs::hash;
use nixrs::path_info::ValidPathInfo;
use nixrs::signature::{ParseSignatureError, Signature, SignatureSet};
use nixrs::store::Error;
use nixrs::store_path::{ParseStorePathError, StorePath, StorePathSet};
use tvix_castore::proto;
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
            .map(|s| StorePath::new_from_base_name(&s))
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
    let ca = if let Some(info_ca) = info.ca.as_ref() {
        let mut ca = store_proto::nar_info::Ca::default();
        ca.digest = info_ca.hash.as_ref().to_vec().into();
        Some(ca)
    } else {
        None
    };
    let deriver = if let Some(info_deriver) = info.deriver.as_ref() {
        let name = info_deriver.name_from_drv();
        let mut path = store_proto::StorePath::default();
        path.digest = info_deriver.hash.as_ref().to_vec().into();
        path.name = name.to_string();
        Some(path)
    } else {
        None
    };
    let mut ret = PathInfo::default();
    ret.node = Some(node);
    ret.references = references;
    ret.narinfo = Some(store_proto::NarInfo {
        nar_size: info.nar_size,
        nar_sha256: info.nar_hash.data().to_vec().into(),
        ca,
        deriver,
        reference_names,
        signatures,
    });
    ret
}
