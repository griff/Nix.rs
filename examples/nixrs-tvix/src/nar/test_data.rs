use bytes::Bytes;
use tvix_castore::proto::{self, DirectoryNode, FileNode, SymlinkNode};
use tvix_castore::B3Digest;

pub fn text_file() -> (proto::Node, (B3Digest, Bytes)) {
    let contents = Bytes::from_static(b"Hello world!");
    let digest: B3Digest = blake3::hash(&contents).as_bytes().into();
    let root_node = proto::FileNode {
        name: b"".to_vec().into(),
        digest: digest.clone().into(),
        size: 12,
        executable: false,
    };
    (
        proto::Node {
            node: Some(proto::node::Node::File(root_node)),
        },
        (digest, contents),
    )
}

pub fn exec_file() -> (proto::Node, (B3Digest, Bytes)) {
    let contents = Bytes::from_static(b"Very cool stuff");
    let digest: B3Digest = blake3::hash(&contents).as_bytes().into();
    let root_node = proto::FileNode {
        name: b"".to_vec().into(),
        digest: digest.clone().into(),
        size: 15,
        executable: true,
    };
    (
        proto::Node {
            node: Some(proto::node::Node::File(root_node)),
        },
        (digest, contents),
    )
}

pub fn empty_file() -> (proto::Node, (B3Digest, Bytes)) {
    let contents = Bytes::from_static(b"");
    let digest: B3Digest = blake3::hash(&contents).as_bytes().into();
    let root_node = proto::FileNode {
        name: b"".to_vec().into(),
        digest: digest.clone().into(),
        size: 0,
        executable: false,
    };
    (
        proto::Node {
            node: Some(proto::node::Node::File(root_node)),
        },
        (digest, contents),
    )
}

#[allow(clippy::type_complexity)]
pub fn empty_file_in_dir() -> (
    proto::Node,
    Vec<(B3Digest, proto::Directory)>,
    Vec<(B3Digest, Bytes)>,
) {
    let (_, (empty_digest, empty_file)) = empty_file();
    let root = proto::Directory {
        directories: vec![],
        files: vec![FileNode {
            name: b"a=?.0.aA".to_vec().into(),
            digest: empty_digest.clone().into(),
            size: 0,
            executable: false,
        }],
        symlinks: vec![],
    };

    let root_node = DirectoryNode {
        name: b"".to_vec().into(),
        digest: root.digest().into(),
        size: root.size(),
    };
    (
        proto::Node {
            node: Some(proto::node::Node::Directory(root_node)),
        },
        vec![(root.digest(), root)],
        vec![(empty_digest, empty_file)],
    )
}

#[allow(clippy::type_complexity)]
pub fn dir_example() -> (
    proto::Node,
    Vec<(B3Digest, proto::Directory)>,
    Vec<(B3Digest, Bytes)>,
) {
    let (_, (empty_digest, empty_file)) = empty_file();
    let deep = proto::Directory {
        directories: vec![],
        files: vec![FileNode {
            name: b"empty.keep".to_vec().into(),
            digest: empty_digest.clone().into(),
            size: 0,
            executable: false,
        }],
        symlinks: vec![
            SymlinkNode {
                name: b"loop".to_vec().into(),
                target: b"../deep".to_vec().into(),
            },
            SymlinkNode {
                name: b"test".to_vec().into(),
                target: b"/etc/ssh/sshd_config".to_vec().into(),
            },
        ],
    };

    let deep_file = Bytes::from_static(b"Very cool stuff");
    let deep_digest: B3Digest = blake3::hash(&deep_file).as_bytes().into();
    let more = proto::Directory {
        directories: vec![DirectoryNode {
            name: b"deep".to_vec().into(),
            digest: deep.digest().into(),
            size: deep.size(),
        }],
        files: vec![FileNode {
            name: b"Deep".to_vec().into(),
            digest: deep_digest.clone().into(),
            size: 15,
            executable: true,
        }],
        symlinks: vec![],
    };
    let dir = proto::Directory {
        directories: vec![DirectoryNode {
            name: b"more".to_vec().into(),
            digest: more.digest().into(),
            size: more.size(),
        }],
        files: vec![],
        symlinks: vec![],
    };

    let testing_file = Bytes::from_static(b"Hello world!");
    let testing_digest: B3Digest = blake3::hash(&testing_file).as_bytes().into();
    let root = proto::Directory {
        directories: vec![DirectoryNode {
            name: b"dir".to_vec().into(),
            digest: dir.digest().into(),
            size: dir.size(),
        }],
        files: vec![FileNode {
            name: b"testing.txt".to_vec().into(),
            digest: testing_digest.clone().into(),
            size: 12,
            executable: false,
        }],
        symlinks: vec![],
    };
    let root_node = DirectoryNode {
        name: b"".to_vec().into(),
        digest: root.digest().into(),
        size: root.size(),
    };
    (
        proto::Node {
            node: Some(proto::node::Node::Directory(root_node)),
        },
        vec![
            (deep.digest(), deep),
            (more.digest(), more),
            (dir.digest(), dir),
            (root.digest(), root),
        ],
        vec![
            (empty_digest, empty_file),
            (deep_digest, deep_file),
            (testing_digest, testing_file),
        ],
    )
}
