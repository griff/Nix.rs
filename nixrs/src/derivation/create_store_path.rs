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

use std::borrow::Cow;
use std::fmt;

use crate::derivation::OutputName;
use crate::hash;
use crate::store_path::{
    ContentAddress, FixedOutput, StoreDir, StoreDirDisplay, StorePath, StorePathName,
    StorePathNameRef,
};

pub trait StorePathCreate {
    fn make_store_path_fingerprint<'d, 'r, D, DI, R, RI>(
        &self,
        fingerprint: Fingerprint<'_, 'd, 'r, '_, DI, RI>,
    ) -> StorePath
    where
        D: StoreDirDisplay + 'd,
        &'d DI: IntoIterator<Item = &'d D>,
        R: StoreDirDisplay + 'r,
        &'r RI: IntoIterator<Item = &'r R>;
    fn make_store_path_from_ca(&self, name: StorePathName, ca: ContentAddress) -> StorePath;
}

impl StorePathCreate for StoreDir {
    fn make_store_path_fingerprint<'d, 'r, D, DI, R, RI>(
        &self,
        fingerprint: Fingerprint<'_, 'd, 'r, '_, DI, RI>,
    ) -> StorePath
    where
        D: StoreDirDisplay + 'd,
        &'d DI: IntoIterator<Item = &'d D>,
        R: StoreDirDisplay + 'r,
        &'r RI: IntoIterator<Item = &'r R>,
    {
        let finger_print_s = self.display(&fingerprint);
        StorePath::from_hash(
            &hash::Sha256::digest_display(finger_print_s),
            fingerprint.name.into_owned(),
        )
    }

    fn make_store_path_from_ca(&self, name: StorePathName, ca: ContentAddress) -> StorePath {
        let path_type = ca.into();
        let fingerprint = Fingerprint {
            name: Cow::Owned(name),
            path_type,
        };
        self.make_store_path_fingerprint(fingerprint)
    }
}

pub struct Fingerprint<'n, 'd, 'r, 'o, DI, RI> {
    pub path_type: StorePathType<'d, 'r, 'o, DI, RI>,
    pub name: Cow<'n, StorePathNameRef>,
}

impl<'n, 'd, 'r, 'o, D, DI, R, RI> StoreDirDisplay for Fingerprint<'n, 'd, 'r, 'o, DI, RI>
where
    D: StoreDirDisplay + 'd,
    &'d DI: IntoIterator<Item = &'d D>,
    R: StoreDirDisplay + 'r,
    &'r RI: IntoIterator<Item = &'r R>,
{
    fn fmt(&self, store_dir: &StoreDir, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}:{}:{}",
            store_dir.display(&self.path_type),
            store_dir,
            self.name
        )
    }
}

pub enum StorePathType<'d, 'r, 'o, DI, RI> {
    Text {
        drv_references: &'d DI,
        references: &'r RI,
        digest: hash::Sha256,
    },
    Output {
        output_name: OutputName,
        digest: hash::Sha256,
    },
    Source {
        references: &'r RI,
        self_ref: bool,
        digest: hash::Sha256,
    },
    FixedOutput(FixedOutput),
    FixedOutputPath {
        output_hash: FixedOutput,
        output_path: &'o StorePath,
    },
}

impl<'d, 'r, DI, RI> StorePathType<'d, 'r, '_, DI, RI> {
    pub fn text_with_references(
        drv_references: &'d DI,
        references: &'r RI,
        digest: hash::Sha256,
    ) -> Self {
        StorePathType::Text {
            drv_references,
            references,
            digest,
        }
    }
}

impl StorePathType<'static, 'static, '_, [StorePath; 0], [StorePath; 0]> {
    pub fn text(digest: hash::Sha256) -> Self {
        StorePathType::Text {
            drv_references: &[],
            references: &[],
            digest,
        }
    }

    pub fn output(output_name: OutputName, digest: hash::Sha256) -> Self {
        Self::Output {
            output_name,
            digest,
        }
    }

    pub fn fixed_output(fo: FixedOutput) -> Self {
        Self::FixedOutput(fo)
    }
}
impl<'o> StorePathType<'static, 'static, 'o, [StorePath; 0], [StorePath; 0]> {
    pub fn fixed_output_path(output_hash: FixedOutput, output_path: &'o StorePath) -> Self {
        Self::FixedOutputPath {
            output_hash,
            output_path,
        }
    }
}

impl<'r, RI> StorePathType<'_, 'r, '_, [StorePath; 0], RI> {
    pub fn source(references: &'r RI, self_ref: bool, digest: hash::Sha256) -> Self {
        StorePathType::Source {
            references,
            self_ref,
            digest,
        }
    }
}
impl From<ContentAddress> for StorePathType<'_, '_, '_, [StorePath; 0], [StorePath; 0]> {
    fn from(value: ContentAddress) -> Self {
        match value {
            ContentAddress::Text(digest) => StorePathType::Text {
                drv_references: &[],
                references: &[],
                digest,
            },
            ContentAddress::Fixed(fo) if fo.is_source() => {
                let digest = fo.hash.try_into().unwrap();
                StorePathType::Source {
                    references: &[],
                    self_ref: false,
                    digest,
                }
            }
            ContentAddress::Fixed(fo) => StorePathType::FixedOutput(fo),
        }
    }
}

impl<'d, 'r, D, DI, R, RI> StoreDirDisplay for StorePathType<'d, 'r, '_, DI, RI>
where
    D: StoreDirDisplay + 'd,
    &'d DI: IntoIterator<Item = &'d D>,
    R: StoreDirDisplay + 'r,
    &'r RI: IntoIterator<Item = &'r R>,
{
    fn fmt(&self, store_dir: &StoreDir, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorePathType::Text {
                drv_references,
                references,
                digest,
            } => {
                f.write_str("text")?;
                for path in *drv_references {
                    write!(f, ":{}", store_dir.display(path))?
                }
                for path in *references {
                    write!(f, ":{}", store_dir.display(path))?
                }
                write!(f, ":sha256:{digest:x}")
            }
            StorePathType::Output {
                output_name,
                digest,
            } => {
                write!(f, "output:{output_name}:sha256:{digest:x}")
            }
            StorePathType::Source {
                references,
                self_ref,
                digest,
            } => {
                f.write_str("source")?;
                for path in *references {
                    write!(f, ":{}", store_dir.display(path))?
                }
                if *self_ref {
                    f.write_str(":self")?;
                }
                write!(f, ":sha256:{digest:x}")
            }
            StorePathType::FixedOutput(fo) => {
                let digest = fo.fod_digest();
                write!(f, "output:out:sha256:{digest:x}")
            }
            StorePathType::FixedOutputPath {
                output_hash,
                output_path,
            } => {
                let fod_display = output_hash.fod_output_display(output_path);
                let digest = hash::Sha256::digest_display(store_dir.display(&fod_display));
                write!(f, "output:out:sha256:{digest:x}")
            }
        }
    }
}

#[cfg(test)]
mod unittests {
    use std::str::FromStr;
    use std::sync::LazyLock;

    use rstest::rstest;

    use super::*;
    use crate::hash::Sha256;
    use crate::hash::fmt::Any;
    use crate::store_path::{StoreDir, StorePathName};

    static FOOBAR_TEXT: LazyLock<Vec<StorePath>> = LazyLock::new(|| {
        vec![StorePath::from_str("cszvawc8k9mw1v9ci9c2ldh37qwdm6as-konsole-18.12.1").unwrap()]
    });

    #[rstest]
    // > nix eval -E 'builtins.toFile "konsole-18.12.1" "foobar"'
    // "/nix/store/cszvawc8k9mw1v9ci9c2ldh37qwdm6as-konsole-18.12.1"
    #[case::text(
        StorePathType::text(Sha256::digest("foobar")),
        "konsole-18.12.1",
        None,
        "text:sha256:c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2:/nix/store:konsole-18.12.1",
        "cszvawc8k9mw1v9ci9c2ldh37qwdm6as-konsole-18.12.1"
    )]
    // > nix eval -E 'builtins.toFile "konsole-18.12.2" "foobar ${builtins.toFile "konsole-18.12.1" "foobar"}"'
    // "/nix/store/vp67yq903rcv35n9zh7m93qinzynknay-konsole-18.12.2"
    // > echo -n "foobar /nix/store/cszvawc8k9mw1v9ci9c2ldh37qwdm6as-konsole-18.12.1" | sha256
    // 9abc4a9b9a937d4e7729a3c2f8e482b29fcef81e8e7b97bd787806693895395a
    #[case::text_with_references(
        StorePathType::text_with_references(
            &[] as &[StorePath; 0],
            &*FOOBAR_TEXT,
            Sha256::digest("foobar /nix/store/cszvawc8k9mw1v9ci9c2ldh37qwdm6as-konsole-18.12.1")
        ),
        "konsole-18.12.2",
        None,
        "text:/nix/store/cszvawc8k9mw1v9ci9c2ldh37qwdm6as-konsole-18.12.1:sha256:9abc4a9b9a937d4e7729a3c2f8e482b29fcef81e8e7b97bd787806693895395a:/nix/store:konsole-18.12.2",
        "vp67yq903rcv35n9zh7m93qinzynknay-konsole-18.12.2"
    )]
    // > nix store prefetch-file --executable --json --name konsole-18.12.3 --hash-type sha256 file:///nix/store/cszvawc8k9mw1v9ci9c2ldh37qwdm6as-konsole-18.12.1
    // {"hash":"sha256-xtrbRCC4nmXqdfVmX74A3i7nY44VOiFkT+a3vsYdWhw=","storePath":"/nix/store/bykd2g42ajgyq42f0y71m8hmknp4ljwn-konsole-18.12.3"}
    // > nix hash to-base16 sha256-xtrbRCC4nmXqdfVmX74A3i7nY44VOiFkT+a3vsYdWhw=
    // c6dadb4420b89e65ea75f5665fbe00de2ee7638e153a21644fe6b7bec61d5a1c
    #[case::source(
        StorePathType::source(
            &[] as &[StorePath; 0],
            false,
            "sha256-xtrbRCC4nmXqdfVmX74A3i7nY44VOiFkT+a3vsYdWhw=".parse::<Any<Sha256>>().unwrap().into_hash(),
        ),
        "konsole-18.12.3",
        None,
        "source:sha256:c6dadb4420b89e65ea75f5665fbe00de2ee7638e153a21644fe6b7bec61d5a1c:/nix/store:konsole-18.12.3",
        "bykd2g42ajgyq42f0y71m8hmknp4ljwn-konsole-18.12.3"
    )]
    // > nix store prefetch-file --executable --json --name konsole-18.12.4 --hash-type sha1 file:///nix/store/cszvawc8k9mw1v9ci9c2ldh37qwdm6as-konsole-18.12.1
    // {"hash":"sha1-AlCd9v11bzCK7+WrZvA1DFHNJKY=","storePath":"/nix/store/m53si5mibflysj2fanf9lr7khsrpfrsg-konsole-18.12.4"}
    // > nix hash to-base16 sha1-AlCd9v11bzCK7+WrZvA1DFHNJKY=
    // 02509df6fd756f308aefe5ab66f0350c51cd24a6
    // > echo -n "fixed:out:r:sha1:02509df6fd756f308aefe5ab66f0350c51cd24a6:" | sha256
    // 293646cb6b49dcf1b64e3b3b77d4b42848401fca9191337973c40684ed184c37
    #[case::fixed_output(
        StorePathType::fixed_output(FixedOutput::recursive("sha1-AlCd9v11bzCK7+WrZvA1DFHNJKY=".parse::<Any<hash::Hash>>().unwrap().into_hash())),
        "konsole-18.12.4",
        Some("fixed:out:r:sha1:02509df6fd756f308aefe5ab66f0350c51cd24a6:"),
        "output:out:sha256:293646cb6b49dcf1b64e3b3b77d4b42848401fca9191337973c40684ed184c37:/nix/store:konsole-18.12.4",
        "m53si5mibflysj2fanf9lr7khsrpfrsg-konsole-18.12.4"
    )]
    // > nix store prefetch-file --json --name konsole-18.12.5 --hash-type sha256 file:///nix/store/cszvawc8k9mw1v9ci9c2ldh37qwdm6as-konsole-18.12.1
    // {"hash":"sha256-w6uP8Tcg6K2QR905Rms8iXTlksL6OD1KOWBxTK7wxPI=","storePath":"/nix/store/ixs204nprlf6pfkdrf8pnjifk19sc2d6-konsole-18.12.5"}
    // > nix hash to-base16 sha256-w6uP8Tcg6K2QR905Rms8iXTlksL6OD1KOWBxTK7wxPI=
    // c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2
    // > echo -n "fixed:out:sha256:c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2:" | sha256
    // ecde46689503a72ce9abd659d2a1b9bf944905ce2d519d6a09fa3ab0542feaba
    #[case::fixed_flat_output(
        StorePathType::fixed_output(FixedOutput::flat("sha256-w6uP8Tcg6K2QR905Rms8iXTlksL6OD1KOWBxTK7wxPI=".parse::<Any<hash::Hash>>().unwrap().into_hash())),
        "konsole-18.12.5",
        Some("fixed:out:sha256:c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2:"),
        "output:out:sha256:ecde46689503a72ce9abd659d2a1b9bf944905ce2d519d6a09fa3ab0542feaba:/nix/store:konsole-18.12.5",
        "ixs204nprlf6pfkdrf8pnjifk19sc2d6-konsole-18.12.5"
    )]
    fn test_make_store_path_fingerprint<'d, 'r, 'o, D, DI, R, RI>(
        #[case] path_type: StorePathType<'d, 'r, 'o, DI, RI>,
        #[case] name: StorePathName,
        #[case] fod_s: Option<&str>,
        #[case] fingerprint_s: &str,
        #[case] final_path: StorePath,
    ) where
        D: StoreDirDisplay + 'd,
        &'d DI: IntoIterator<Item = &'d D>,
        R: StoreDirDisplay + 'r,
        &'r RI: IntoIterator<Item = &'r R>,
    {
        let expected_hash = hash::Sha256::digest(fingerprint_s);
        let expected_path = StorePath::from_hash(&expected_hash, name.clone());
        let store_dir = StoreDir::default();
        if let Some(fod_s) = fod_s {
            match path_type {
                StorePathType::FixedOutput(fo) => {
                    assert_eq!(fod_s, fo.fod_display().to_string());
                }
                StorePathType::FixedOutputPath {
                    output_hash,
                    output_path,
                } => {
                    assert_eq!(
                        fod_s,
                        store_dir
                            .display(&output_hash.fod_output_display(output_path))
                            .to_string()
                    );
                }
                _ => {}
            }
            let hash = hash::Sha256::digest(fod_s);
            let actual_fingerprint_s = format!("output:out:sha256:{hash:x}:{store_dir}:{name}");
            assert_eq!(actual_fingerprint_s, fingerprint_s);
        }
        let fingerprint = Fingerprint {
            name: Cow::Owned(name),
            path_type,
        };
        let actual_fingerprint_s = store_dir.display(&fingerprint).to_string();
        assert_eq!(
            actual_fingerprint_s, fingerprint_s,
            "fingerprint does not match"
        );
        let actual_path = store_dir.make_store_path_fingerprint(fingerprint);
        assert_eq!(expected_path, actual_path, "expected path not right");
        assert_eq!(final_path, actual_path, "final path not right");
    }

    #[rstest]
    // nix eval -E 'builtins.toFile "konsole-18.12.3" "foobar"'
    #[case::text(
        ContentAddress::Text(Sha256::digest("foobar")),
        "konsole-18.12.1",
        None,
        "text:sha256:c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2:/nix/store:konsole-18.12.1",
        "cszvawc8k9mw1v9ci9c2ldh37qwdm6as-konsole-18.12.1"
    )]
    #[case::source(
        ContentAddress::fixed_recursive("sha256:248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1".parse::<Any<hash::Hash>>().unwrap().into()),
        "konsole-18.12.3",
        None,
        "source:sha256:248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1:/nix/store:konsole-18.12.3",
        "1w01xxn8f7s9s4n65ry6rwd7x9awf04s-konsole-18.12.3"
    )]
    #[case::fixed_output(
        ContentAddress::fixed_recursive("sha1:84983e441c3bd26ebaae4aa1f95129e5e54670f1".parse::<Any<hash::Hash>>().unwrap().into()),
        "konsole-18.12.3",
        Some("fixed:out:r:sha1:84983e441c3bd26ebaae4aa1f95129e5e54670f1:"),
        "output:out:sha256:5341f5afdd0fb724c8f7eae0e346de5bb151a00422d47ae683aed85cd78f7120:/nix/store:konsole-18.12.3",
        "ww9d58nz1xsl5ck0vcpc99h23l1y2hln-konsole-18.12.3"
    )]
    #[case::fixed_flat_output(
        ContentAddress::fixed_flat("sha256:248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1".parse::<Any<hash::Hash>>().unwrap().into()),
        "konsole-18.12.3",
        Some("fixed:out:sha256:248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1:"),
        "output:out:sha256:e55d6c8c9a08e91f15d5344612c42305702f04f08c487a7aff0b56c4c4add3e7:/nix/store:konsole-18.12.3",
        "jw8chmp9sf8f7pw684cszp6pa2zmn0bx-konsole-18.12.3"
    )]
    fn test_make_store_path_from_ca(
        #[case] ca: ContentAddress,
        #[case] name: StorePathName,
        #[case] inner_print: Option<&str>,
        #[case] fingerprint: &str,
        #[case] final_path: StorePath,
    ) {
        let expected_hash = hash::Sha256::digest(fingerprint);
        let expected_path = StorePath::from_hash(&expected_hash, name.clone());
        let store_dir = StoreDir::default();
        if let Some(print) = inner_print {
            let hash = hash::Sha256::digest(print);
            let actual_fingerprint = format!("output:out:sha256:{hash:x}:{store_dir}:{name}");
            assert_eq!(actual_fingerprint, fingerprint);
        }
        let actual_path = store_dir.make_store_path_from_ca(name, ca);
        assert_eq!(expected_path, actual_path);
        assert_eq!(final_path, actual_path);
    }
}
