use std::collections::{btree_map::Entry, BTreeMap};

use super::{Error, Store, StorePath, StorePathSet};
use crate::compute_closure;

pub async fn compute_fs_closure<S>(
    store: S,
    start_paths: StorePathSet,
    include_derivers: bool,
) -> Result<StorePathSet, Error>
where
    S: Store + Clone,
{
    /*
    let query_deps = if flip_direction {
        compute_closure(
            start_paths,
            move |path: &StorePath| {
                let path = path.clone();
                let mut store = store.clone();
                Box::pin(async move {
                    let mut res = StorePathSet::new();
                    let info = store.query_path_info(&path).await?;
                    for reference in info.references {
                        if reference != path {
                            res.insert(reference);
                        }
                    }
                    if include_derivers {
                        if let Some(deriver) = info.deriver {
                            if store.is_valid_path(&deriver).await? {
                                res.insert(deriver);
                            }
                        }
                    }
                    Ok(res)
                })
            }
        ).await
    } else {
        |path: &StorePath, fut: Box<dyn Future<Output=ValidPathInfo>>| async {
            let res = StorePathSet::new();
            let info = fut.await;
            for reference in &info.references {
                res.insert(reference);
            }
            res
        }
    };
    if (flipDirection)
        queryDeps = [&](const StorePath& path,
                        std::future<ref<const ValidPathInfo>> & fut) {
            StorePathSet res;
            StorePathSet referrers;
            queryReferrers(path, referrers);
            for (auto& ref : referrers)
                if (ref != path)
                    res.insert(ref);

            if (includeOutputs)
                for (auto& i : queryValidDerivers(path))
                    res.insert(i);

            if (includeDerivers && path.isDerivation())
                for (auto& [_, maybeOutPath] : queryPartialDerivationOutputMap(path))
                    if (maybeOutPath && isValidPath(*maybeOutPath))
                        res.insert(*maybeOutPath);
            return res;
        };
    else
        queryDeps = [&](const StorePath& path,
                        std::future<ref<const ValidPathInfo>> & fut) {
            StorePathSet res;
            auto info = fut.get();
            for (auto& ref : info->references)
                if (ref != path)
                    res.insert(ref);

            if (includeOutputs && path.isDerivation())
                for (auto& [_, maybeOutPath] : queryPartialDerivationOutputMap(path))
                    if (maybeOutPath && isValidPath(*maybeOutPath))
                        res.insert(*maybeOutPath);

            if (includeDerivers && info->deriver && isValidPath(*info->deriver))
                res.insert(*info->deriver);
            return res;
        };

     */
    compute_closure(start_paths, move |path: &StorePath| {
        let path = path.clone();
        let mut store = store.clone();
        Box::pin(async move {
            let mut res = StorePathSet::new();
            let info = store
                .query_path_info(&path)
                .await?
                .ok_or(Error::InvalidPath(path.to_string()))?;
            for reference in info.references {
                if reference != path {
                    res.insert(reference);
                }
            }
            if include_derivers {
                if let Some(deriver) = info.deriver {
                    if store.query_path_info(&deriver).await?.is_some() {
                        res.insert(deriver);
                    }
                }
            }
            Ok(res)
        })
    })
    .await
}

pub async fn compute_fs_closure_slow<S>(
    store: &mut S,
    start_paths: &StorePathSet,
    include_derivers: bool,
) -> Result<StorePathSet, Error>
where
    S: Store,
{
    let mut res = StorePathSet::new();
    let mut pending = Vec::with_capacity(start_paths.len());
    for path in start_paths {
        let mut edges = StorePathSet::new();
        let info = store
            .query_path_info(&path)
            .await?
            .ok_or(Error::InvalidPath(path.to_string()))?;
        for reference in info.references {
            if reference != *path {
                edges.insert(reference);
            }
        }
        if include_derivers {
            if let Some(deriver) = info.deriver {
                if store.query_path_info(&deriver).await?.is_some() {
                    edges.insert(deriver);
                }
            }
        }
        pending.push(edges);
        res.insert(path.clone());
    }
    while !pending.is_empty() {
        let edges = pending.pop().unwrap();
        for edge in edges {
            if res.insert(edge.clone()) {
                let mut edges = StorePathSet::new();
                let info = store
                    .query_path_info(&edge)
                    .await?
                    .ok_or(Error::InvalidPath(edge.to_string()))?;
                for reference in info.references {
                    if reference != edge {
                        edges.insert(reference);
                    }
                }
                if include_derivers {
                    if let Some(deriver) = info.deriver {
                        if store.query_path_info(&deriver).await?.is_some() {
                            edges.insert(deriver);
                        }
                    }
                }
                pending.push(edges);
            }
        }
    }
    Ok(res)
}

pub async fn topo_sort_paths_slow<S: Store>(
    store: &mut S,
    store_paths: &StorePathSet,
) -> Result<Vec<StorePath>, Error> {
    let mut refs = BTreeMap::new();
    let mut rrefs: BTreeMap<StorePath, StorePathSet> = BTreeMap::new();
    let mut roots = StorePathSet::new();
    for store_path in store_paths.iter() {
        if let Some(info) = store.query_path_info(&store_path).await? {
            let mut edges = info.references;
            edges.remove(&store_path);
            let edges: StorePathSet = edges.intersection(&store_paths).cloned().collect();
            if edges.is_empty() {
                roots.insert(store_path.clone());
            } else {
                for m in edges.iter() {
                    rrefs
                        .entry(m.clone())
                        .or_default()
                        .insert(store_path.clone());
                }
                refs.insert(store_path, edges);
            }
        }
    }
    let mut sorted = Vec::with_capacity(store_paths.len());
    while !roots.is_empty() {
        let n = roots.pop_first().unwrap();
        sorted.push(n.clone());
        if let Some(edges) = rrefs.get(&n) {
            for m in edges {
                if let Entry::Occupied(mut oci) = refs.entry(m) {
                    let references = oci.get_mut();
                    references.remove(&n);
                    if references.is_empty() {
                        oci.remove_entry();
                        roots.insert(m.clone());
                    }
                }
            }
        }
    }
    if refs.is_empty() {
        Ok(sorted)
    } else {
        Err(Error::CycleDetected)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, time::SystemTime};

    use assert_matches::assert_matches;
    use tokio::io::AsyncWrite;

    use super::*;
    use crate::store::{
        path::STORE_PATH_HASH_BYTES, Error, Store, StoreDirProvider, StorePath, StorePathSet,
        ValidPathInfo,
    };

    macro_rules! store_path {
        ($l:expr) => {{
            let mut hash = [0u8; STORE_PATH_HASH_BYTES];
            let b = $l.repeat(STORE_PATH_HASH_BYTES);
            hash.copy_from_slice(&b);
            let name = std::str::from_utf8($l).unwrap();
            $crate::store::StorePath::from_parts(hash, name).unwrap()
        }};
    }

    #[macro_export]
    macro_rules! set_clone {
        () => { std::collections::BTreeSet::new() };
        ($($x:expr),+ $(,)?) => {{
            let mut ret = std::collections::BTreeSet::new();
            $(
                ret.insert($x.clone());
            )+
            ret
        }};
    }

    macro_rules! graph {
        ($($a:expr => [$($x:expr),* $(,)?]),+ $(,)?) => {{
            let mut ret = std::collections::BTreeMap::new();
            $(
                ret.insert($a.clone(), set_clone![$($x ,)*]);
            )+
            ret
        }};
    }

    #[derive(Clone)]
    struct QueryStore {
        references: BTreeMap<StorePath, StorePathSet>,
    }

    impl StoreDirProvider for QueryStore {
        fn store_dir(&self) -> crate::store::StoreDir {
            crate::store::StoreDir::default()
        }
    }

    #[async_trait::async_trait]
    impl Store for QueryStore {
        async fn query_path_info(
            &mut self,
            path: &StorePath,
        ) -> Result<Option<ValidPathInfo>, Error> {
            if let Some(refs) = self.references.get(path) {
                let info = ValidPathInfo {
                    path: path.clone(),
                    deriver: None,
                    nar_size: 0,
                    nar_hash: "sha256:ungWv48Bz+pBQUDeXa4iI7ADYaOWF3qctBD/YfIAFa0="
                        .parse()
                        .unwrap(),
                    references: refs.clone(),
                    sigs: Default::default(),
                    registration_time: SystemTime::now(),
                    ultimate: false,
                    ca: None,
                };
                Ok(Some(info))
            } else {
                Ok(None)
            }
        }

        /// Export path from the store
        async fn nar_from_path<W: AsyncWrite + Send + Unpin>(
            &mut self,
            _path: &StorePath,
            _sink: W,
        ) -> Result<(), Error> {
            Err(Error::UnsupportedOperation("nar_from_path".into()))
        }

        /// Import a path into the store.
        async fn add_to_store<R: tokio::io::AsyncRead + Send + Unpin>(
            &mut self,
            _info: &ValidPathInfo,
            _source: R,
            _repair: crate::store::RepairFlag,
            _check_sigs: crate::store::CheckSignaturesFlag,
        ) -> Result<(), Error> {
            Err(Error::UnsupportedOperation("nar_from_path".into()))
        }
    }

    #[tokio::test]
    async fn test_closure() {
        let a = store_path!(b"a");
        let b = store_path!(b"b");
        let c = store_path!(b"c");
        let d = store_path!(b"d");
        let e = store_path!(b"e");
        let f = store_path!(b"f");
        let g = store_path!(b"g");
        let references = graph! {
            a => [b, c, g],
            b => [a], // Loops back to A
            c => [f], // Indirect reference
            d => [a], // Not reachable, but has backreferences
            e => [], // Just not reachable
            f => [],
            g => [g] // Self reference
        };
        let expected_closure = set_clone! {a, b, c, f, g};

        let store = QueryStore { references };
        let actual = compute_fs_closure(store, set_clone![a], false)
            .await
            .unwrap();
        assert_eq!(expected_closure, actual);
    }

    #[tokio::test]
    async fn test_closure_no_loops() {
        let a = store_path!(b"a");
        let b = store_path!(b"b");
        let c = store_path!(b"c");
        let d = store_path!(b"d");
        let e = store_path!(b"e");
        let f = store_path!(b"f");
        let g = store_path!(b"g");
        let references = graph! {
            a => [b, c, g],
            b => [],
            c => [f], // Indirect reference
            d => [a], // Not reachable, but has backreferences
            e => [], // Just not reachable
            f => [],
            g => [g] // Self reference
        };
        let expected_closure = set_clone! {a, b, c, f, g};

        let store = QueryStore { references };
        let actual = compute_fs_closure(store, set_clone![a], false)
            .await
            .unwrap();
        assert_eq!(expected_closure, actual);
    }

    #[tokio::test]
    async fn test_closure_slow() {
        let a = store_path!(b"a");
        let b = store_path!(b"b");
        let c = store_path!(b"c");
        let d = store_path!(b"d");
        let e = store_path!(b"e");
        let f = store_path!(b"f");
        let g = store_path!(b"g");
        let references = graph! {
            a => [b, c, g],
            b => [a], // Loops back to A
            c => [f], // Indirect reference
            d => [a], // Not reachable, but has backreferences
            e => [], // Just not reachable
            f => [],
            g => [g] // Self reference
        };
        let expected_closure = set_clone! {a, b, c, f, g};

        let mut store = QueryStore { references };
        let actual = compute_fs_closure_slow(&mut store, &set_clone![a], false)
            .await
            .unwrap();
        assert_eq!(expected_closure, actual);
    }

    #[tokio::test]
    async fn test_closure_no_loops_slow() {
        let a = store_path!(b"a");
        let b = store_path!(b"b");
        let c = store_path!(b"c");
        let d = store_path!(b"d");
        let e = store_path!(b"e");
        let f = store_path!(b"f");
        let g = store_path!(b"g");
        let references = graph! {
            a => [b, c, g],
            b => [],
            c => [f], // Indirect reference
            d => [a], // Not reachable, but has backreferences
            e => [], // Just not reachable
            f => [],
            g => [g] // Self reference
        };
        let expected_closure = set_clone! {a, b, c, f, g};

        let mut store = QueryStore { references };
        let actual = compute_fs_closure_slow(&mut store, &set_clone![a], false)
            .await
            .unwrap();
        assert_eq!(expected_closure, actual);
    }

    macro_rules! vec_clone {
        () => {Vec::new()};
        ($($x:expr),+ $(,)?) => {{
            vec![$($x.clone() ,)+]
        }};
    }

    #[tokio::test]
    async fn test_topo_sort_cycle() {
        let a = store_path!(b"a");
        let b = store_path!(b"b");
        let c = store_path!(b"c");
        let d = store_path!(b"d");
        let e = store_path!(b"e");
        let f = store_path!(b"f");
        let g = store_path!(b"g");
        let references = graph! {
            a => [b, c, g],
            b => [a], // Loops back to A
            c => [f], // Indirect reference
            d => [a], // Not reachable, but has backreferences
            e => [], // Just not reachable
            f => [],
            g => [g] // Self reference
        };
        let mut store = QueryStore { references };
        let actual = topo_sort_paths_slow(&mut store, &set_clone! {a, b, c, f, g})
            .await
            .unwrap_err();
        assert_matches!(actual, Error::CycleDetected);
    }

    #[tokio::test]
    async fn test_topo_sort() {
        let a = store_path!(b"a");
        let b = store_path!(b"b");
        let c = store_path!(b"c");
        let d = store_path!(b"d");
        let e = store_path!(b"e");
        let f = store_path!(b"f");
        let g = store_path!(b"g");
        let references = graph! {
            a => [b, c, g],
            b => [],
            c => [f], // Indirect reference
            d => [a], // Not reachable, but has backreferences
            e => [], // Just not reachable
            f => [],
            g => [g] // Self reference
        };
        let mut store = QueryStore { references };
        let expected = vec_clone![b, f, c, g, a];
        let actual = topo_sort_paths_slow(&mut store, &set_clone! {a, b, c, f, g})
            .await
            .unwrap();
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn test_topo_sort_full_closure() {
        let a = store_path!(b"a");
        let b = store_path!(b"b");
        let c = store_path!(b"c");
        let d = store_path!(b"d");
        let e = store_path!(b"e");
        let f = store_path!(b"f");
        let g = store_path!(b"g");
        let references = graph! {
            a => [b, c, g],
            b => [],
            c => [f], // Indirect reference
            d => [a], // Not reachable, but has backreferences
            e => [], // Just not reachable
            f => [],
            g => [g] // Self reference
        };
        let mut store = QueryStore { references };
        let expected = vec_clone![b, f, c, g, a];
        let actual = topo_sort_paths_slow(&mut store, &set_clone! {a, b, c, f, g})
            .await
            .unwrap();
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn test_topo_sort_missing_leaf() {
        let a = store_path!(b"a");
        let b = store_path!(b"b");
        let c = store_path!(b"c");
        let d = store_path!(b"d");
        let e = store_path!(b"e");
        let f = store_path!(b"f");
        let g = store_path!(b"g");
        let references = graph! {
            a => [b, c, g],
            b => [],
            c => [f], // Indirect reference
            d => [a], // Not reachable, but has backreferences
            e => [], // Just not reachable
            f => [],
            g => [g] // Self reference
        };
        let mut store = QueryStore { references };
        let expected = vec_clone![f, c, g, a];
        let actual = topo_sort_paths_slow(&mut store, &set_clone! {a, c, f, g})
            .await
            .unwrap();
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn test_topo_sort_indirect() {
        let a = store_path!(b"a");
        let b = store_path!(b"b");
        let c = store_path!(b"c");
        let d = store_path!(b"d");
        let e = store_path!(b"e");
        let f = store_path!(b"f");
        let g = store_path!(b"g");
        let references = graph! {
            a => [b, c, g],
            b => [],
            c => [f], // Indirect reference
            d => [a], // Not reachable, but has backreferences
            e => [], // Just not reachable
            f => [],
            g => [g] // Self reference
        };
        let mut store = QueryStore { references };
        let expected = vec_clone![b, f, g, a];
        let actual = topo_sort_paths_slow(&mut store, &set_clone! {a, b, f, g})
            .await
            .unwrap();
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn test_topo_sort_indirect_disjoint() {
        let a = store_path!(b"a");
        let b = store_path!(b"b");
        let c = store_path!(b"c");
        let d = store_path!(b"d");
        let e = store_path!(b"e");
        let f = store_path!(b"f");
        let g = store_path!(b"g");
        let h = store_path!(b"h");
        let references = graph! {
            a => [b, c, g],
            b => [],
            c => [f], // Indirect reference
            d => [a], // Not reachable, but has backreferences
            e => [], // Just not reachable
            f => [h],
            g => [g], // Self reference
            h => [],
        };
        let mut store = QueryStore { references };
        let expected = vec_clone![b, f, g, a];
        let actual = topo_sort_paths_slow(&mut store, &set_clone! {a, b, f, g})
            .await
            .unwrap();
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn test_topo_sort_indirect_disjoint2() {
        let a = store_path!(b"a");
        let b = store_path!(b"b");
        let c = store_path!(b"c");
        let d = store_path!(b"d");
        let e = store_path!(b"e");
        let f = store_path!(b"f");
        let g = store_path!(b"g");
        let h = store_path!(b"h");
        let references = graph! {
            a => [b, c, g],
            b => [],
            c => [f], // Indirect reference
            d => [a], // Not reachable, but has backreferences
            e => [], // Just not reachable
            f => [h],
            g => [g], // Self reference
            h => [],
        };
        let mut store = QueryStore { references };
        // Is this correct? Nix does it like this but is it right?
        let expected = vec_clone![b, g, a, h, f];
        let actual = topo_sort_paths_slow(&mut store, &set_clone! {a, b, f, h, g})
            .await
            .unwrap();
        assert_eq!(actual, expected);
    }
}
