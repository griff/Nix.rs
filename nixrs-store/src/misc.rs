use nixrs_util::compute_closure;

use crate::{Error, StorePath, StorePathSet, Store};

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
            let info = store.query_path_info(&path).await?
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
