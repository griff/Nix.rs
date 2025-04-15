use std::{
    fs::{create_dir_all, set_permissions, write, Permissions},
    io,
    os::unix::fs::PermissionsExt as _,
    path::Path,
    sync::Arc,
};

use bytes::Bytes;

use crate::CASE_HACK_SUFFIX;

use super::{NAREvent, NAR_VERSION_MAGIC_1};

pub fn text_file() -> Vec<NAREvent> {
    vec![
        NAREvent::Magic(Arc::new(NAR_VERSION_MAGIC_1.to_owned())),
        NAREvent::RegularNode {
            executable: false,
            size: 12,
            offset: 96,
        },
        NAREvent::Contents {
            total: 12,
            index: 0,
            buf: Bytes::from_static(b"Hello world!"),
        },
    ]
}

pub fn exec_file() -> Vec<NAREvent> {
    vec![
        NAREvent::Magic(Arc::new(NAR_VERSION_MAGIC_1.to_owned())),
        NAREvent::RegularNode {
            executable: true,
            size: 15,
            offset: 128,
        },
        NAREvent::Contents {
            total: 15,
            index: 0,
            buf: Bytes::from_static(b"Very cool stuff"),
        },
    ]
}

pub fn empty_file() -> Vec<NAREvent> {
    vec![
        NAREvent::Magic(Arc::new(NAR_VERSION_MAGIC_1.to_owned())),
        NAREvent::RegularNode {
            executable: false,
            size: 0,
            offset: 0,
        },
    ]
}

pub fn empty_file_in_dir() -> Vec<NAREvent> {
    vec![
        NAREvent::Magic(Arc::new(NAR_VERSION_MAGIC_1.to_owned())),
        NAREvent::Directory,
        NAREvent::DirectoryEntry {
            name: Bytes::from_static(b"a=?.0.aA"),
        },
        NAREvent::RegularNode {
            executable: false,
            size: 0,
            offset: 0,
        },
        NAREvent::EndDirectoryEntry,
        NAREvent::EndDirectory,
    ]
}

pub fn empty_dir() -> Vec<NAREvent> {
    vec![
        NAREvent::Magic(Arc::new(NAR_VERSION_MAGIC_1.to_owned())),
        NAREvent::Directory,
        NAREvent::EndDirectory,
    ]
}

pub fn empty_dir_in_dir() -> Vec<NAREvent> {
    vec![
        NAREvent::Magic(Arc::new(NAR_VERSION_MAGIC_1.to_owned())),
        NAREvent::Directory,
        NAREvent::DirectoryEntry {
            name: Bytes::from_static(b"empty"),
        },
        NAREvent::Directory,
        NAREvent::EndDirectory,
        NAREvent::EndDirectoryEntry,
        NAREvent::EndDirectory,
    ]
}

pub fn symlink() -> Vec<NAREvent> {
    vec![
        NAREvent::Magic(Arc::new(NAR_VERSION_MAGIC_1.to_owned())),
        NAREvent::SymlinkNode {
            target: Bytes::from_static(b"../deep"),
        },
    ]
}

pub fn dir_example() -> Vec<NAREvent> {
    /*
    Offsets:

    NAR_VERSION_MAGIC_1 // 24

    "(", // 16
    "type", // 16
    "directory", // 24
    // Size -> offset: 56 -> 80

    "entry", // 16
    "(" // 16
    "name" // 16
    "dir" // 16
    "node" // 16
    // Size -> offset: 80 -> 160

    "(" // 16
    "type" // 16
    "directory" // 24
    // Size -> offset: 56 -> 216

    "entry", // 16
    "(" // 16
    "name" // 16
    "more" // 16
    "node" // 16
    // Size -> offset: 80 -> 296

    "(" // 16
    "type" // 16
    "directory" // 24
    // Size -> offset: 56 -> 352

    "entry" // 16
    "("// 16
    "name" // 16
    "Deep" // 16
    "node" // 16
    // Size -> offset: 80 -> 432

    "(" // 16
    "type" // 16
    "regular" // 16
    "executable", // 24
    "" // 8
    "contents", // 16
    8
    // Size -> offset: 104 -> 536

    */
    vec![
        NAREvent::Magic(Arc::new(NAR_VERSION_MAGIC_1.to_owned())),
        NAREvent::Directory,
        NAREvent::DirectoryEntry {
            name: Bytes::from_static(b"dir"),
        },
        NAREvent::Directory,
        NAREvent::DirectoryEntry {
            name: Bytes::from_static(b"more"),
        },
        NAREvent::Directory,
        NAREvent::DirectoryEntry {
            name: Bytes::from_static(b"Deep"),
        },
        NAREvent::RegularNode {
            executable: true,
            size: 15,
            offset: 536,
        },
        NAREvent::Contents {
            total: 15,
            index: 0,
            buf: Bytes::from_static(b"Very cool stuff"),
        },
        NAREvent::EndDirectoryEntry,
        NAREvent::DirectoryEntry {
            name: Bytes::from_static(b"deep"),
        },
        NAREvent::Directory,
        NAREvent::DirectoryEntry {
            name: Bytes::from_static(b"empty.keep"),
        },
        NAREvent::RegularNode {
            executable: false,
            size: 0,
            offset: 0,
        },
        NAREvent::EndDirectoryEntry,
        NAREvent::DirectoryEntry {
            name: Bytes::from_static(b"loop"),
        },
        NAREvent::SymlinkNode {
            target: Bytes::from_static(b"../deep"),
        },
        NAREvent::EndDirectoryEntry,
        NAREvent::DirectoryEntry {
            name: Bytes::from_static(b"test"),
        },
        NAREvent::SymlinkNode {
            target: Bytes::from_static(b"/etc/ssh/sshd_config"),
        },
        NAREvent::EndDirectoryEntry,
        NAREvent::EndDirectory,
        NAREvent::EndDirectoryEntry,
        NAREvent::EndDirectory,
        NAREvent::EndDirectoryEntry,
        NAREvent::EndDirectory,
        NAREvent::EndDirectoryEntry,
        NAREvent::DirectoryEntry {
            name: Bytes::from_static(b"testing.txt"),
        },
        NAREvent::RegularNode {
            executable: false,
            size: 12,
            offset: 1568,
        },
        NAREvent::Contents {
            total: 12,
            index: 0,
            buf: Bytes::from_static(b"Hello world!"),
        },
        NAREvent::EndDirectoryEntry,
        NAREvent::EndDirectory,
    ]
}

pub fn create_dir_example<P>(path: P, case_hack: bool) -> io::Result<()>
where
    P: AsRef<Path>,
{
    let path = path.as_ref();
    create_dir_all(path)?;
    let more = path.join("dir").join("more");
    create_dir_all(&more)?;
    let deep_file = more.join("Deep");
    write(&deep_file, b"Very cool stuff")?;
    let permissions = Permissions::from_mode(0o700);
    set_permissions(deep_file, permissions)?;

    let deep = if case_hack {
        more.join(format!("deep{}{}", CASE_HACK_SUFFIX, 1))
    } else {
        more.join("deep")
    };
    create_dir_all(&deep)?;
    write(deep.join("empty.keep"), b"")?;
    std::os::unix::fs::symlink("../deep", deep.join("loop"))?;
    std::os::unix::fs::symlink("/etc/ssh/sshd_config", deep.join("test"))?;
    write(path.join("testing.txt"), b"Hello world!")?;
    Ok(())
}
