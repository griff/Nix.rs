use std::borrow::Cow;
use std::io;
use std::path::{Path, PathBuf, Component};

use smallvec::SmallVec;
use tokio::fs;

pub async fn resolve_link<P:AsRef<Path>>(path: P) -> io::Result<PathBuf> {
    let path = path.as_ref();
    let target = fs::read_link(path).await?;
    if let Some(dir) = path.parent() {
        Ok(absolute_path_buf(target, dir))
    } else {
        absolute_path_buf_from_current(target)
    }
}

pub fn absolute_path_buf_from_current(path: PathBuf) -> io::Result<PathBuf> {
    match absolute_path_from_current(&path)? {
        Cow::Borrowed(_) => Ok(path),
        Cow::Owned(o) => Ok(o)
    } 
}

pub fn absolute_path_from_current(path: &Path) -> io::Result<Cow<Path>> {
    if path.is_absolute() {
        return Ok(clean_path(path))
    }
    let base = std::env::current_dir()?;
    Ok(absolute_path(path, &base))
}

pub fn absolute_path_buf(path: PathBuf, base: &Path) -> PathBuf {
    match absolute_path(&path, base) {
        Cow::Borrowed(_) => path,
        Cow::Owned(o) => o,
    }
}

pub fn absolute_path<'p>(path: &'p Path, base: &Path) -> Cow<'p, Path> {
    if path.is_absolute() {
        return clean_path(path)
    }
    Cow::Owned(clean_path_buf(base.join(path)))
}

pub fn clean_path_buf(path: PathBuf) -> PathBuf {
    match clean_path(&path) {
        Cow::Borrowed(_) => path,
        Cow::Owned(o)  => o
    }
}

pub fn clean_path(path: &Path) -> Cow<Path> {
    let mut out = SmallVec::<[Component; 20]>::new();
    let mut no_changes = true;
    for comp in path.components() {
        match comp {
            Component::CurDir => {
                no_changes = false;
            },
            Component::ParentDir => match out.last() {
                Some(Component::RootDir) => {
                    no_changes = false;
                },
                Some(Component::Normal(_)) => {
                    no_changes = false;
                    out.pop();
                },
                None
                | Some(Component::CurDir)
                | Some(Component::ParentDir)
                | Some(Component::Prefix(_)) => out.push(comp),
                
            },
            comp => out.push(comp),
        }
    }
    if no_changes {
        Cow::Borrowed(path)
    } else if !out.is_empty() {
        let ret : PathBuf = out.iter().collect();
        Cow::Owned(ret)
    } else {
        Cow::Owned(PathBuf::from("."))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use tempfile::Builder;

    #[test]
    fn test_clean_path() {
        assert_eq!(clean_path(Path::new("/nix/store")),
            Cow::Borrowed(Path::new("/nix/store")));
        assert_eq!(clean_path(Path::new("/../nix/store")),
            Cow::<Path>::Owned(PathBuf::from("/nix/store")));
        assert_eq!(clean_path(Path::new("/nix/./store")),
            Cow::<Path>::Owned(PathBuf::from("/nix/store")));
        assert_eq!(clean_path(Path::new("/nix/./../store")),
            Cow::<Path>::Owned(PathBuf::from("/store")));
        assert_eq!(clean_path(Path::new("/nix/item/part/../../store")),
            Cow::<Path>::Owned(PathBuf::from("/nix/store")));
        assert_eq!(clean_path(Path::new("/nix/item/../part/../store")),
            Cow::<Path>::Owned(PathBuf::from("/nix/store")));
        /*
        //assert_eq!(clean_path(Path::new("c:/test")),
        //    Cow::Borrowed(Path::new("c:/test")));

        assert_eq!(clean_path(Path::new("c:../nix/store")),
            Cow::Borrowed(Path::new("/nix/store")));
        assert_eq!(clean_path(Path::new("c:../nix/store")),
            Cow::Borrowed(Path::new("/nix/store")));
        */
        assert_eq!(clean_path(Path::new("../nix/store")),
            Cow::Borrowed(Path::new("../nix/store")));
        assert_eq!(clean_path(Path::new("../../nix/store")),
            Cow::Borrowed(Path::new("../../nix/store")));
        assert_eq!(clean_path(Path::new("./test")),
            Cow::Borrowed(Path::new("test")));
        assert_eq!(clean_path(Path::new("./.")),
            Cow::Borrowed(Path::new(".")));
    }

    #[test]
    fn test_clean_path_buf() {
        assert_eq!(clean_path_buf(PathBuf::from("/nix/store")),
            PathBuf::from("/nix/store"));
        assert_eq!(clean_path_buf(PathBuf::from("/nix/../store")),
            PathBuf::from("/store"));
    }

    #[test]
    fn test_absolute_path() {
        assert_eq!(absolute_path(Path::new("/nix/store"), Path::new("/test")),
            Cow::Borrowed(Path::new("/nix/store")));
        assert_eq!(absolute_path(Path::new("./nix/../store"), Path::new("test")),
            Cow::Borrowed(Path::new("test/store")));
        assert_eq!(absolute_path(Path::new("./nix/../store"), Path::new("/test")),
            Cow::Borrowed(Path::new("/test/store")));
    }

    #[test]
    fn test_absolute_path_buf() {
        assert_eq!(absolute_path_buf(PathBuf::from("/nix/store"), Path::new("/test")),
            PathBuf::from("/nix/store"));
        assert_eq!(absolute_path_buf(PathBuf::from("store"), Path::new("/test")),
            PathBuf::from("/test/store"));
    }

    #[test]
    fn test_absolute_path_from_current() -> io::Result<()> {
        let base = std::env::current_dir()?;

        assert_eq!(absolute_path_from_current(Path::new("/nix/store"))?,
            Cow::Borrowed(Path::new("/nix/store")));
        assert_eq!(absolute_path_from_current(Path::new("./nix/../store"))?,
            Cow::<Path>::Owned(base.join("store")));
        Ok(())
    }

    #[test]
    fn test_absolute_path_buf_from_current() -> io::Result<()> {
        let base = std::env::current_dir()?;

        assert_eq!(absolute_path_buf_from_current(PathBuf::from("/nix/store"))?,
            PathBuf::from("/nix/store"));
        assert_eq!(absolute_path_buf_from_current(PathBuf::from("./nix/../store"))?,
            base.join("store"));
        Ok(())
    }

    #[tokio::test]
    async fn test_resolve_link() -> io::Result<()> {
        let dir = Builder::new().prefix("test_resolve_link").tempdir()?;
        let path = dir.path().join("output");
        fs::symlink("/nix/store", &path).await?;
        let ret = resolve_link(path).await?;
        assert_eq!(ret, PathBuf::from("/nix/store"));

        let path = dir.path().join("relative");
        fs::symlink("./test", &path).await?;
        let ret = resolve_link(path).await?;
        assert_eq!(ret, dir.path().join("test"));

        let path = dir.path().join("relative2");
        fs::symlink("test", &path).await?;
        let ret = resolve_link(path).await?;
        assert_eq!(ret, dir.path().join("test"));

        let path = dir.path().join("relative3");
        fs::symlink("./test/more/../other", &path).await?;
        let ret = resolve_link(path).await?;
        assert_eq!(ret, dir.path().join("test/other"));

        let path = dir.path().join("relative4");
        fs::symlink("../test", &path).await?;
        let ret = resolve_link(path).await?;
        assert_eq!(ret, dir.path().parent().unwrap().join("test"));

        Ok(())
    }
}