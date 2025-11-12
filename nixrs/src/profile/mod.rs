use std::ffi::OsString;
use std::fmt::{self, Write};
use std::io;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use tokio::fs::{read_dir, read_link, rename, symlink, symlink_metadata};

use crate::store_path::{HasStoreDir, StorePath};

/// Parse profile link base name of the format `<profile name>-<number>-link`
pub fn parse_name(profile_name: &str, name: &str) -> Option<u64> {
    let rest = name.strip_prefix(profile_name)?;
    let rest = rest.strip_prefix('-')?;
    let num = rest.strip_suffix("-link")?;
    num.parse().ok()
}

pub fn make_name<P: Into<OsString>>(profile: P, generation: u64) -> PathBuf {
    let mut ret = profile.into();
    write!(ret, "-{generation}-link").expect("fmt::Write for OsString returns no errors");
    ret.into()
}

async fn replace_link<P: AsRef<Path>, Q: AsRef<Path>>(original: P, link: Q) -> io::Result<()> {
    let original = original.as_ref();
    let link = link.as_ref();
    let mut n = 0;
    let file = link.file_name().expect("path to have file name");
    let mut name = OsString::new();
    let mut tmp_path = link.to_path_buf();
    loop {
        write!(name, ".{n}_").expect("fmt::Write on OsString returns no errors");
        name.push(file);
        tmp_path.set_file_name(&name);
        match symlink(original, &tmp_path).await {
            Ok(_) => {
                rename(&tmp_path, link).await?;
                break;
            }
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
                name.clear();
                n += 1;
            }
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

#[non_exhaustive]
pub struct Generation<'p, R> {
    pub number: u64,
    //pub path: PathBuf,
    pub creation_time: SystemTime,
    profile: &'p Profile<R>,
}

impl<R> Generation<'_, R> {
    pub fn path(&self) -> PathBuf {
        make_name(self.profile.profile.clone(), self.number)
    }
    pub fn file_name(&self) -> String {
        format!("{}-{}-link", self.profile.profile_name(), self.number)
    }
    pub async fn switch(&self) -> io::Result<()> {
        let original = self.file_name();
        /*
        let mut path: &Path = self.path().as_ref();
        if self.profile.profile.parent() == path.parent() && path.file_name().is_some() {
            path = path.file_name().unwrap().as_ref();
        }
         */
        replace_link(original, &self.profile.profile).await?;
        Ok(())
    }

    pub fn delete(&self) -> io::Result<()> {
        Ok(())
    }
}
impl<R: ProfileRoots> Generation<'_, R> {
    pub async fn store_path(&self) -> io::Result<StorePath> {
        let link = read_link(self.path()).await?;
        let full_store_path = link.to_str().ok_or(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("generation symlink {} is not valid UTF-8", link.display()),
        ))?;
        self.profile
            .roots
            .store_dir()
            .parse(full_store_path)
            .map_err(|err| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "generation symlink {} is not valid store path {err:?}",
                        link.display()
                    ),
                )
            })
    }
}

impl<R> fmt::Debug for Generation<'_, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Generation")
            .field("number", &self.number)
            .field("creation_time", &self.creation_time)
            .field("profile", &self.profile)
            .finish()
    }
}

impl<R> PartialEq for Generation<'_, R> {
    fn eq(&self, other: &Self) -> bool {
        self.number == other.number
            && self.creation_time == other.creation_time
            && self.profile == other.profile
    }
}

impl<R> Eq for Generation<'_, R> {}

pub struct Profile<R> {
    profile_name: String,
    profile: PathBuf,
    roots: R,
}
impl<R> Profile<R> {
    pub fn new<P: Into<PathBuf>>(profile: P, roots: R) -> io::Result<Self> {
        let profile = profile.into();
        if profile.parent().is_none() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "profile path does not have a parent directory",
            ));
        }
        if let Some(profile_name) = profile
            .file_name()
            .and_then(|n| n.to_str())
            .map(|n| n.to_string())
        {
            Ok(Self {
                profile,
                roots,
                profile_name,
            })
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "profile path does not have a file name that is UTF-8",
            ))
        }
    }

    pub fn profile_name(&self) -> &str {
        &self.profile_name
    }

    pub fn profile_dir(&self) -> &Path {
        self.profile
            .parent()
            .expect("profile most have parent directory")
    }

    pub async fn current_generation(&self) -> io::Result<Option<Generation<'_, R>>> {
        let profile_name = self.profile_name();
        match read_link(&self.profile).await {
            Ok(path) => {
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    if let Some(number) = parse_name(profile_name, name) {
                        let creation_time = symlink_metadata(&path).await?.modified()?;
                        return Ok(Some(Generation {
                            number,
                            creation_time,
                            profile: self,
                        }));
                    }
                }
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }
        Ok(None)
    }

    pub async fn get_generation(&self, number: u64) -> io::Result<Generation<'_, R>> {
        let path = make_name(self.profile.clone(), number);
        let creation_time = symlink_metadata(&path).await?.modified()?;
        Ok(Generation {
            number,
            creation_time,
            profile: self,
        })
    }

    pub async fn list_generations(&self) -> io::Result<Vec<Generation<'_, R>>> {
        let mut ret = Vec::new();
        let profile_name = self.profile_name();
        let profile_dir = self.profile_dir();
        let mut dir = read_dir(profile_dir).await?;
        while let Some(entry) = dir.next_entry().await? {
            if let Some(number) = entry
                .file_name()
                .to_str()
                .and_then(|name| parse_name(profile_name, name))
            {
                //let path = entry.path();
                let creation_time = entry.metadata().await?.modified()?;
                ret.push(Generation {
                    number,
                    creation_time,
                    profile: self,
                });
            }
        }
        ret.sort_by_key(|g| g.number);
        Ok(ret)
    }
}

impl<R: ProfileRoots> Profile<R> {
    pub async fn create_generation(&self, out_path: &StorePath) -> io::Result<Generation<'_, R>> {
        let mut generations = self.list_generations().await?;
        let mut number = 1;
        if let Some(old) = generations.pop() {
            number = old.number + 1;
            let full_path: OsString = self.roots.store_dir().display(out_path).to_string().into();
            if read_link(old.path()).await? == full_path {
                return Ok(old);
            }
        }
        let path = make_name(self.profile.clone(), number);
        self.roots.make_gc_symlink(&path, out_path).await?;
        let creation_time = symlink_metadata(&path).await?.modified()?;
        Ok(Generation {
            number,
            creation_time,
            profile: self,
        })
    }
}

impl<R> fmt::Debug for Profile<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Profile")
            .field("profile", &self.profile)
            .finish()
    }
}

impl<R> PartialEq for Profile<R> {
    fn eq(&self, other: &Self) -> bool {
        self.profile == other.profile
    }
}

impl<R> Eq for Profile<R> {}

pub trait ProfileRoots: HasStoreDir {
    fn make_gc_symlink(
        &self,
        link: &Path,
        target: &StorePath,
    ) -> impl Future<Output = io::Result<()>>;
}

#[cfg(test)]
mod unittests {
    use std::fs::create_dir_all;

    use nixrs::store_path::StoreDir;
    use tempfile::Builder;

    use super::*;

    struct DummyRoots(StoreDir);
    impl HasStoreDir for DummyRoots {
        fn store_dir(&self) -> &StoreDir {
            &self.0
        }
    }

    impl ProfileRoots for DummyRoots {
        async fn make_gc_symlink(&self, link: &Path, target: &StorePath) -> io::Result<()> {
            let original = self.0.display(target).to_string();
            replace_link(original, link).await
        }
    }

    #[tokio::test]
    pub async fn list_generations() {
        let dir = Builder::new()
            .prefix("test_list_generations")
            .tempdir()
            .unwrap();
        let profiles = dir.path().join("profiles");
        create_dir_all(&profiles).unwrap();
        let store = dir.path().join("store");
        create_dir_all(&store).unwrap();
        let roots = DummyRoots(StoreDir::new(&store).unwrap());
        let profile = Profile::new(profiles.join("system"), roots).unwrap();

        assert_eq!(profile.list_generations().await.unwrap(), vec![]);

        let full_store_path = store.join("fj1n3w8wvvz72ihlrxwdpm3siq7lhd7v-depot-3p-sources.txt");
        std::fs::write(&full_store_path, "dummy").unwrap();
        symlink(&full_store_path, profiles.join("junk-22-link"))
            .await
            .unwrap();
        symlink("junk-22-link", profiles.join("junk"))
            .await
            .unwrap();
        std::fs::write(profiles.join("extra"), "dummy").unwrap();
        symlink(&full_store_path, profiles.join("system-1-link"))
            .await
            .unwrap();
        symlink(&full_store_path, profiles.join("system-55-link"))
            .await
            .unwrap();
        symlink("system-55-link", profiles.join("system"))
            .await
            .unwrap();
        let list = profile.list_generations().await.unwrap();
        assert_eq!(
            list.iter().map(|g| g.number).collect::<Vec<_>>(),
            vec![1, 55]
        );
        assert_eq!(
            "fj1n3w8wvvz72ihlrxwdpm3siq7lhd7v-depot-3p-sources.txt"
                .parse::<StorePath>()
                .unwrap(),
            list.last().unwrap().store_path().await.unwrap()
        );
        assert_eq!("system-55-link", list.last().unwrap().file_name());
    }
}
