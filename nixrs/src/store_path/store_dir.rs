use std::borrow::Cow;
use std::fmt;
use std::path::{Path, PathBuf, MAIN_SEPARATOR};
use std::sync::Arc;

use tokio::fs;
use tracing::trace;

use super::content_address::FixedOutputInfo;
use super::{
    ContentAddressWithReferences, FileIngestionMethod, ParseStorePathError, ReadStorePathError,
    StorePath, StoreReferences, TextInfo,
};
use crate::hash;
use crate::io::{StateParse, StatePrint};
use crate::path::absolute_path_from_current;
use crate::path::clean_path;
use crate::path::resolve_link;

struct DisplayStorePath<'a> {
    store_dir: &'a StoreDir,
    path: &'a StorePath,
}

impl<'a> fmt::Display for DisplayStorePath<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}{}", self.store_dir, MAIN_SEPARATOR, self.path)
    }
}

/// Store directory.
/// Since the [`StorePath`] abstraction is only a hash and a name we need this
/// to convert the path to a full store path string.
///
/// ```
/// use nixrs::store_path::StoreDir;
/// let store = StoreDir::new("/nix/store").unwrap();
/// let path = store.parse_path("/nix/store/55xkmqns51sw7nrgykp5vnz36w4fr3cw-nix-2.1.3").unwrap();
/// ```
///
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StoreDir(Arc<PathBuf>, Arc<String>);
impl StoreDir {
    /// Create a new StoreDir from given path.
    /// This can fail if the path contains non-UTF-8 characters and therefore can't be
    /// converted to a [`String`].
    pub fn new<P: Into<PathBuf>>(path: P) -> Result<StoreDir, ParseStorePathError> {
        let path = path.into();
        let path_s = path
            .to_str()
            .ok_or_else(|| ParseStorePathError::BadStorePath(path.clone()))?
            .to_string();
        Ok(StoreDir(Arc::new(path), Arc::new(path_s)))
    }

    /// Get [`str`] representation of this StoreDir.
    ///
    /// ```
    /// # use nixrs::store_path::StoreDir;
    /// let store = StoreDir::new("/nix/store").unwrap();
    /// assert_eq!("/nix/store", store.to_str());
    /// ```
    pub fn to_str(&self) -> &str {
        self.1.as_ref()
    }

    /// Returns an object that implements [`Display`] for printing a [`StorePath`] complete
    /// with the full path.
    ///
    /// ```
    /// # use nixrs::store_path::StoreDir;
    /// let store = StoreDir::new("/nix/store").unwrap();
    /// let path = store.parse_path("/nix/store/55xkmqns51sw7nrgykp5vnz36w4fr3cw-nix-2.1.3").unwrap();
    /// println!("{}", store.display_path(&path));
    /// ```
    ///
    /// [`Display`]: fmt::Display
    pub fn display_path<'a>(&'a self, path: &'a StorePath) -> impl fmt::Display + 'a {
        DisplayStorePath {
            store_dir: self,
            path,
        }
    }

    /// Returns a [`String`] with the full path for the provided [`StorePath`].
    ///
    /// ```
    /// # use nixrs::store_path::StoreDir;
    /// let store = StoreDir::new("/nix/store").unwrap();
    /// let path = store.parse_path("/nix/store/55xkmqns51sw7nrgykp5vnz36w4fr3cw-nix-2.1.3").unwrap();
    /// assert_eq!("/nix/store/55xkmqns51sw7nrgykp5vnz36w4fr3cw-nix-2.1.3", store.print_path(&path));
    /// ```
    pub fn print_path(&self, path: &StorePath) -> String {
        self.display_path(path).to_string()
    }

    /// Parses a string `s` to a [`StorePath`].
    ///
    /// ```
    /// # use nixrs::store_path::StoreDir;
    /// let store = StoreDir::new("/nix/store").unwrap();
    /// let path = store.parse_path("/nix/store/55xkmqns51sw7nrgykp5vnz36w4fr3cw-nix-2.1.3").unwrap();
    /// assert_eq!("55xkmqns51sw7nrgykp5vnz36w4fr3cw-nix-2.1.3", format!("{}", path));
    /// ```
    pub fn parse_path(&self, s: &str) -> Result<StorePath, ParseStorePathError> {
        StorePath::new(Path::new(s), self)
    }

    fn make_type(&self, mut path_type: String, references: &StoreReferences) -> String {
        for reference in references.others.iter() {
            path_type.push(':');
            path_type.push_str(&self.print_path(reference));
        }
        if references.self_ref {
            path_type.push_str(":self");
        }
        path_type
    }

    pub fn make_store_path_str(
        &self,
        path_type: &str,
        hash: &str,
        name: &str,
    ) -> Result<StorePath, ParseStorePathError> {
        let s = format!("{}:{}:{}:{}", path_type, hash, self, name);
        StorePath::from_hash(&hash::digest(hash::Algorithm::SHA256, s), name)
    }

    pub fn make_store_path(
        &self,
        path_type: &str,
        hash: hash::Hash,
        name: &str,
    ) -> Result<StorePath, ParseStorePathError> {
        self.make_store_path_str(path_type, &format!("{:x}", hash), name)
    }

    pub fn make_fixed_output_path(
        &self,
        name: &str,
        info: &FixedOutputInfo,
    ) -> Result<StorePath, ParseStorePathError> {
        if let (hash::Algorithm::SHA256, FileIngestionMethod::Recursive) =
            (info.hash.algorithm(), info.method)
        {
            self.make_store_path(
                &self.make_type("source".into(), &info.references),
                info.hash,
                name,
            )
        } else {
            assert!(info.references.is_empty());
            let hash = hash::digest(
                hash::Algorithm::SHA256,
                format!("fixed:out:{:#}{:x}:", info.method, info.hash),
            );
            trace!("Output hash {:x}", hash);
            self.make_store_path("output:out", hash, name)
        }
    }

    pub fn make_fixed_output_path_from_ca(
        &self,
        name: &str,
        ca: &ContentAddressWithReferences,
    ) -> Result<StorePath, ParseStorePathError> {
        use ContentAddressWithReferences::*;
        match ca {
            Text(info) => self.make_text_path(name, info),
            Fixed(info) => self.make_fixed_output_path(name, info),
        }
    }

    pub fn make_text_path(
        &self,
        name: &str,
        info: &TextInfo,
    ) -> Result<StorePath, ParseStorePathError> {
        assert_eq!(info.hash.algorithm(), hash::Algorithm::SHA256);
        // Stuff the references (if any) into the type.  This is a bit
        // hacky, but we can't put them in `s' since that would be
        // ambiguous.
        let path_type = self.make_type(
            "text".into(),
            &StoreReferences {
                others: info.references.clone(),
                self_ref: false,
            },
        );
        self.make_store_path(&path_type, info.hash, name)
    }

    fn strip_store_path<'a>(&self, path: &'a Path) -> Result<Cow<'a, Path>, &'a Path> {
        if !path.is_absolute() {
            return Err(path);
        }
        let clean = clean_path(path);
        if let Cow::Owned(o) = clean {
            match o.strip_prefix(self) {
                Err(_) => Err(path),
                Ok(p) if p == Path::new("") => Err(path),
                Ok(p) => Ok(Cow::Owned(p.into())),
            }
        } else {
            match path.strip_prefix(self) {
                Err(_) => Err(path),
                Ok(p) if p == Path::new("") => Err(path),
                Ok(p) => Ok(Cow::Borrowed(p)),
            }
        }
    }

    /// Checks that the suplied path is in this store.
    ///
    /// ```
    /// # use nixrs::store_path::StoreDir;
    /// let store = StoreDir::new("/nix/store").unwrap();
    /// assert_eq!(true, store.is_in_store("/nix/store/55xkmqns51sw7nrgykp5vnz36w4fr3cw-nix-2.1.3"));
    /// assert_eq!(true, store.is_in_store("/nix/store/55xkmqns51sw7nrgykp5vnz36w4fr3cw-nix-2.1.3/etc/"));
    /// assert_eq!(false, store.is_in_store("/nix/store/"));
    /// assert_eq!(false, store.is_in_store("/var/lib/"));
    /// ```
    pub fn is_in_store<P: AsRef<Path>>(&self, path: P) -> bool {
        self.strip_store_path(path.as_ref()).is_ok()
    }

    /// Convert a `path` in this store to a [`StorePath`].
    /// This will fail when the given path is not in this store or when the path
    /// is not a valid store path.
    ///
    /// # Examples
    ///
    /// Basic store path with no extra path elements
    ///
    /// ```
    /// # use std::path::Path;
    /// # use nixrs::store_path::StoreDir;
    /// let store = StoreDir::new("/nix/store").unwrap();
    /// let p = Path::new("/nix/store/55xkmqns51sw7nrgykp5vnz36w4fr3cw-nix-2.1.3");
    /// let (path, rest) = store.to_store_path(p).unwrap();
    ///
    /// assert_eq!("55xkmqns51sw7nrgykp5vnz36w4fr3cw-nix-2.1.3", path.to_string());
    /// assert_eq!(Path::new(""), rest);
    /// ```
    ///
    /// Path that points to a file inside the store:
    ///
    /// ```
    /// # use std::path::Path;
    /// # use nixrs::store_path::StoreDir;
    /// let store = StoreDir::new("/nix/store").unwrap();
    /// let p = Path::new("/nix/store/55xkmqns51sw7nrgykp5vnz36w4fr3cw-nix-2.1.3/etc/init/nix-daemon.conf");
    /// let (path, rest) = store.to_store_path(p).unwrap();
    /// assert_eq!("55xkmqns51sw7nrgykp5vnz36w4fr3cw-nix-2.1.3", path.to_string());
    /// assert_eq!(Path::new("etc/init/nix-daemon.conf"), rest);
    /// ```
    ///
    /// Path outside the store:
    ///
    /// ```
    /// # use std::path::Path;
    /// # use nixrs::store_path::StoreDir;
    /// let store = StoreDir::new("/nix/store").unwrap();
    /// let res = store.to_store_path(Path::new("/var/local/lib"));
    /// assert!(res.is_err());
    /// ```
    pub fn to_store_path<'p>(
        &self,
        path: &'p Path,
    ) -> Result<(StorePath, Cow<'p, Path>), ParseStorePathError> {
        match self.strip_store_path(path) {
            Err(p) => Err(ParseStorePathError::NotInStore(p.into())),
            Ok(p) => match p {
                Cow::Owned(o) => {
                    let mut c = o.components();
                    let base_name = c.next().unwrap().as_os_str();
                    let store_path = StorePath::new_from_base_name(
                        base_name
                            .to_str()
                            .ok_or_else(|| ParseStorePathError::BadStorePath(path.into()))?,
                    )?;
                    let after = c.as_path();
                    Ok((store_path, Cow::Owned(after.into())))
                }
                Cow::Borrowed(b) => {
                    let mut c = b.components();
                    let base_name = c.next().unwrap().as_os_str();
                    let store_path = StorePath::new_from_base_name(
                        base_name
                            .to_str()
                            .ok_or_else(|| ParseStorePathError::BadStorePath(path.into()))?,
                    )?;
                    let after = c.as_path();
                    Ok((store_path, Cow::Borrowed(after)))
                }
            },
        }
    }

    /// Follow a chain of symlinks until we either end up with a path in this store
    /// or return an error.
    pub async fn follow_links_to_store(&self, path: &Path) -> Result<PathBuf, ReadStorePathError> {
        let mut path = absolute_path_from_current(path)?.into_owned();
        while !self.is_in_store(&path) {
            let m = fs::symlink_metadata(&path).await?;
            if !m.file_type().is_symlink() {
                break;
            }
            path = resolve_link(&path).await?;
        }
        if !self.is_in_store(&path) {
            Err(ReadStorePathError::BadStorePath(
                ParseStorePathError::NotInStore(path),
            ))
        } else {
            Ok(path)
        }
    }

    /// Like [`follow_links_to_store`] but returns a [`StorePath`].
    ///
    /// [`follow_links_to_store`]: #method.follow_links_to_store
    pub async fn follow_links_to_store_path(
        &self,
        path: &Path,
    ) -> Result<StorePath, ReadStorePathError> {
        let path = self.follow_links_to_store(path).await?;
        Ok(self.to_store_path(&path)?.0)
    }
}

impl Default for StoreDir {
    fn default() -> Self {
        StoreDir::new("/nix/store").unwrap()
    }
}

impl StateParse<StorePath> for StoreDir {
    type Err = ReadStorePathError;

    fn parse(&self, s: &str) -> Result<StorePath, Self::Err> {
        Ok(self.parse_path(s)?)
    }
}

impl StatePrint<StorePath> for StoreDir {
    fn print(&self, path: &StorePath) -> String {
        self.print_path(path)
    }
}

impl AsRef<str> for StoreDir {
    fn as_ref(&self) -> &str {
        self.to_str()
    }
}

impl AsRef<Path> for StoreDir {
    fn as_ref(&self) -> &Path {
        self.0.as_ref()
    }
}

impl fmt::Display for StoreDir {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_str())
    }
}

pub trait StoreDirProvider {
    /// Root path of this store
    fn store_dir(&self) -> StoreDir;
}

impl<T: ?Sized + StoreDirProvider> StoreDirProvider for Box<T> {
    fn store_dir(&self) -> StoreDir {
        (**self).store_dir()
    }
}

impl<T: ?Sized + StoreDirProvider> StoreDirProvider for &T {
    fn store_dir(&self) -> StoreDir {
        (**self).store_dir()
    }
}

impl<T: ?Sized + StoreDirProvider> StoreDirProvider for &mut T {
    fn store_dir(&self) -> StoreDir {
        (**self).store_dir()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{hash, store_path::StorePathSet};
    use ::proptest::{arbitrary::any, prop_assert_eq, proptest};
    use pretty_assertions::assert_eq;

    #[test]
    fn test_store_dir_display() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        assert_eq!(format!("{}", store_dir), "/nix/store");
        assert_eq!(store_dir.to_str(), "/nix/store");
        let s: &str = store_dir.as_ref();
        assert_eq!(s, "/nix/store");
    }

    #[test]
    fn test_store_dir_as_ref() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let s: &Path = store_dir.as_ref();
        assert_eq!(s, Path::new("/nix/store"));
    }

    #[test]
    fn test_store_dir_parse() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let p = store_dir
            .parse_path("/nix/store/7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3")
            .unwrap();
        assert_eq!(p.name.as_ref(), "konsole-18.12.3");
        assert_eq!(
            p.hash.as_ref(),
            [
                0x9f, 0x76, 0x49, 0x20, 0xf6, 0x5d, 0xe9, 0x71, 0xc4, 0xca, 0x46, 0x21, 0xab, 0xff,
                0x9b, 0x44, 0xef, 0x87, 0x0f, 0x3c
            ]
        );
        let p: StorePath = store_dir
            .parse("/nix/store/7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3")
            .unwrap();
        assert_eq!(
            p.to_string(),
            "7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3"
        );
    }

    #[test]
    fn test_store_dir_print() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let p = store_dir
            .parse_path("/nix/store/7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3")
            .unwrap();
        assert_eq!(
            store_dir.print_path(&p),
            "/nix/store/7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3"
        );
        assert_eq!(
            StatePrint::print(&store_dir, &p),
            "/nix/store/7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3"
        );
        assert_eq!(
            p.print(&store_dir),
            "/nix/store/7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3"
        );
    }

    #[test]
    fn test_store_dir_parse_not_in_store() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let path = "/niv/store/7h7qgvs4kgzsn8a6rb273saxyqh4jxlz-konsole-18.12.3";
        let p = store_dir.parse_path(path);
        assert_eq!(p, Err(ParseStorePathError::NotInStore(PathBuf::from(path))));
    }

    #[test]
    fn test_store_dir_make_output_path() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let d = hash::digest(hash::Algorithm::SHA256, "source:sha256:248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1:/nix/store:konsole-18.12.3");
        let p = StorePath::from_hash(&d, "konsole-18.12.3").unwrap();
        let hash = "sha256:248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"
            .parse::<hash::Hash>()
            .unwrap();
        let p2 = store_dir
            .make_store_path("source", hash, "konsole-18.12.3")
            .unwrap();
        assert_eq!(p2, p);
        assert_eq!(
            format!("{}", p2),
            "1w01xxn8f7s9s4n65ry6rwd7x9awf04s-konsole-18.12.3"
        );
    }

    #[test]
    fn test_store_dir_make_fixed_output_path() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let d = hash::digest(hash::Algorithm::SHA256, "source:sha256:248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1:/nix/store:konsole-18.12.3");
        let p = StorePath::from_hash(&d, "konsole-18.12.3").unwrap();

        let hash = "sha256:248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"
            .parse::<hash::Hash>()
            .unwrap();
        let info = FixedOutputInfo {
            method: FileIngestionMethod::Recursive,
            hash,
            references: StoreReferences::new(),
        };
        let p2 = store_dir
            .make_fixed_output_path("konsole-18.12.3", &info)
            .unwrap();
        assert_eq!(p2, p);
        assert_eq!(
            format!("{}", p2),
            "1w01xxn8f7s9s4n65ry6rwd7x9awf04s-konsole-18.12.3"
        );
    }

    #[test]
    fn test_store_dir_make_fixed_output_path2() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let d = hash::digest(hash::Algorithm::SHA256, "source:/nix/store/7h7qgvs4kgzsn8a6rb274saxyqh4jxlz-konsole-18.12.3.drv:/nix/store/ldhh7c134ap5swsm86rqnc0i7cinqvrc-my-terminal:self:sha256:248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1:/nix/store:konsole-18.12.3");
        let p = StorePath::from_hash(&d, "konsole-18.12.3").unwrap();

        let hash = "sha256:248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"
            .parse::<hash::Hash>()
            .unwrap();
        let mut set = StorePathSet::new();
        set.insert(
            StorePath::new_from_base_name("ldhh7c134ap5swsm86rqnc0i7cinqvrc-my-terminal").unwrap(),
        );
        set.insert(
            StorePath::new_from_base_name("7h7qgvs4kgzsn8a6rb274saxyqh4jxlz-konsole-18.12.3.drv")
                .unwrap(),
        );
        let references = StoreReferences {
            others: set,
            self_ref: true,
        };
        let info = FixedOutputInfo {
            method: FileIngestionMethod::Recursive,
            hash,
            references,
        };
        let p2 = store_dir
            .make_fixed_output_path("konsole-18.12.3", &info)
            .unwrap();
        assert_eq!(p2, p);
        assert_eq!(
            format!("{}", p2),
            "k7jq5x1vj193x2317ypwsl4k9h0kvra2-konsole-18.12.3"
        );
    }

    #[test]
    fn test_store_dir_make_fixed_output_path_flat() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let d = hash::digest(
            hash::Algorithm::SHA256,
            "fixed:out:sha256:248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1:",
        );
        let d = hash::digest(
            hash::Algorithm::SHA256,
            &format!("output:out:{:x}:/nix/store:konsole-18.12.3", d),
        );
        let p = StorePath::from_hash(&d, "konsole-18.12.3").unwrap();

        let hash = "sha256:248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"
            .parse::<hash::Hash>()
            .unwrap();

        let info = FixedOutputInfo {
            method: FileIngestionMethod::Flat,
            hash,
            references: StoreReferences::new(),
        };
        let p2 = store_dir
            .make_fixed_output_path("konsole-18.12.3", &info)
            .unwrap();
        assert_eq!(p2, p);
        assert_eq!(
            format!("{}", p2),
            "jw8chmp9sf8f7pw684cszp6pa2zmn0bx-konsole-18.12.3"
        );
    }

    #[test]
    fn test_store_dir_make_fixed_output_path_sha1() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let d = hash::digest(
            hash::Algorithm::SHA256,
            "fixed:out:r:sha1:84983e441c3bd26ebaae4aa1f95129e5e54670f1:",
        );
        let d = hash::digest(
            hash::Algorithm::SHA256,
            &format!("output:out:{:x}:/nix/store:konsole-18.12.3", d),
        );
        let p = StorePath::from_hash(&d, "konsole-18.12.3").unwrap();

        let hash = "sha1:84983e441c3bd26ebaae4aa1f95129e5e54670f1"
            .parse::<hash::Hash>()
            .unwrap();

        let info = FixedOutputInfo {
            method: FileIngestionMethod::Recursive,
            hash,
            references: StoreReferences::new(),
        };
        let p2 = store_dir
            .make_fixed_output_path("konsole-18.12.3", &info)
            .unwrap();
        assert_eq!(p2, p);
        assert_eq!(
            format!("{}", p2),
            "ww9d58nz1xsl5ck0vcpc99h23l1y2hln-konsole-18.12.3"
        );
    }

    #[test]
    fn test_store_dir_is_in_store() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        assert!(!store_dir.is_in_store("/"));
        assert!(!store_dir.is_in_store("/test"));
        assert!(!store_dir.is_in_store("/nix"));
        assert!(!store_dir.is_in_store("/nix/store"));
        assert!(!store_dir.is_in_store("/nix/store.rs"));
        assert!(!store_dir.is_in_store("/nix/store/"));
        assert!(!store_dir.is_in_store("/nix/store/.."));
        assert!(!store_dir.is_in_store("/nix/store/test/.."));
        assert!(!store_dir.is_in_store("/nix/store/test/../../hello"));

        assert!(store_dir.is_in_store("/nix/store/test/../hello"));
        assert!(store_dir.is_in_store("/nix/test/../store/test"));
        assert!(store_dir.is_in_store("/nix/store/test"));
        assert!(store_dir.is_in_store("/nix/store/test/"));
        assert!(store_dir.is_in_store("/nix/store/test/also"));
        assert!(store_dir.is_in_store("/nix/store/test/also/"));
        assert!(store_dir.is_in_store("/nix/store/a"));
    }

    #[test]
    fn test_store_dir_to_store_path_error() {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        assert_eq!(
            store_dir.to_store_path(Path::new("/")),
            Err(ParseStorePathError::NotInStore("/".into()))
        );
        assert_eq!(
            store_dir.to_store_path(Path::new("/test")),
            Err(ParseStorePathError::NotInStore("/test".into()))
        );
        assert_eq!(
            store_dir.to_store_path(Path::new("/nix")),
            Err(ParseStorePathError::NotInStore("/nix".into()))
        );
        assert_eq!(
            store_dir.to_store_path(Path::new("/nix/store")),
            Err(ParseStorePathError::NotInStore("/nix/store".into()))
        );
        assert_eq!(
            store_dir.to_store_path(Path::new("/nix/store.rs")),
            Err(ParseStorePathError::NotInStore("/nix/store.rs".into()))
        );
        assert_eq!(
            store_dir.to_store_path(Path::new("/nix/store/")),
            Err(ParseStorePathError::NotInStore("/nix/store/".into()))
        );
        assert_eq!(
            store_dir.to_store_path(Path::new("/nix/store/..")),
            Err(ParseStorePathError::NotInStore("/nix/store/..".into()))
        );
        assert_eq!(
            store_dir.to_store_path(Path::new(
                "/nix/store/ww9d58nz1xsl5ck0vcpc99h23l1y2hln-konsole-18.12.3/.."
            )),
            Err(ParseStorePathError::NotInStore(
                "/nix/store/ww9d58nz1xsl5ck0vcpc99h23l1y2hln-konsole-18.12.3/..".into()
            ))
        );
        assert_eq!(
            store_dir.to_store_path(Path::new(
                "/nix/store/ww9d58nz1xsl5ck0vcpc99h23l1y2hln-konsole-18.12.3/../../hello"
            )),
            Err(ParseStorePathError::NotInStore(
                "/nix/store/ww9d58nz1xsl5ck0vcpc99h23l1y2hln-konsole-18.12.3/../../hello".into()
            ))
        );
    }

    #[test]
    fn test_store_dir_to_store_path() {
        let store_dir = StoreDir::new("/nix/store").unwrap();

        let sp = StorePath::new_from_base_name("ww9d58nz1xsl5ck0vcpc99h23l1y2hln-konsole-18.12.3")
            .unwrap();
        assert_eq!(
            store_dir
                .to_store_path(Path::new(
                    "/nix/store/ww9d58nz1xsl5ck0vcpc99h23l1y2hln-konsole-18.12.3"
                ))
                .unwrap(),
            (sp.clone(), Cow::Borrowed(Path::new("")))
        );
        assert_eq!(
            store_dir
                .to_store_path(Path::new(
                    "/nix/store/test/../ww9d58nz1xsl5ck0vcpc99h23l1y2hln-konsole-18.12.3"
                ))
                .unwrap(),
            (sp.clone(), Cow::Borrowed(Path::new("")))
        );
        assert_eq!(
            store_dir
                .to_store_path(Path::new(
                    "/nix/store/ww9d58nz1xsl5ck0vcpc99h23l1y2hln-konsole-18.12.3/"
                ))
                .unwrap(),
            (sp.clone(), Cow::Borrowed(Path::new("")))
        );
        assert_eq!(
            store_dir
                .to_store_path(Path::new(
                    "/nix/store/ww9d58nz1xsl5ck0vcpc99h23l1y2hln-konsole-18.12.3/also"
                ))
                .unwrap(),
            (sp.clone(), Cow::Borrowed(Path::new("also")))
        );
        assert_eq!(
            store_dir
                .to_store_path(Path::new(
                    "/nix/store/ww9d58nz1xsl5ck0vcpc99h23l1y2hln-konsole-18.12.3/also/"
                ))
                .unwrap(),
            (sp.clone(), Cow::Borrowed(Path::new("also/")))
        );
        assert_eq!(
            store_dir
                .to_store_path(Path::new(
                    "/nix/store/ww9d58nz1xsl5ck0vcpc99h23l1y2hln-konsole-18.12.3/more/../also/"
                ))
                .unwrap(),
            (sp.clone(), Cow::Borrowed(Path::new("also/")))
        );
        let sp = StorePath::new_from_base_name("ww7d58nz1xsl5ck0vcpc99h23l1y2hln-a").unwrap();
        assert_eq!(
            store_dir
                .to_store_path(Path::new("/nix/store/ww7d58nz1xsl5ck0vcpc99h23l1y2hln-a"))
                .unwrap(),
            (sp, Cow::Borrowed(Path::new("")))
        );
    }

    proptest! {
        #[test]
        fn proptest_string_parse(path in any::<StorePath>()) {
            let store_dir = StoreDir::new("/nix/store").unwrap();
            let s = store_dir.print_path(&path);
            let parsed = store_dir.parse_path(&s).unwrap();
            prop_assert_eq!(path, parsed);
        }
    }
}
