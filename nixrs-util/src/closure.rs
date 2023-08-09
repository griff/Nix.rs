use std::collections::BTreeSet;
use std::future::Future;

use futures::future::select_all;


pub async fn compute_closure<E, T, F, Fut>(
    start_elts: BTreeSet<T>,
    get_edges_async: F,
) -> Result<BTreeSet<T>, E>
    where T: Ord + Clone,
          F: Fn(&T) -> Fut,
          Fut: Future<Output=Result<BTreeSet<T>, E>> + Unpin
{
    let mut res = BTreeSet::new();
    let mut pending = Vec::with_capacity(start_elts.len());
    for start_elt in start_elts {
        let p = get_edges_async(&start_elt);
        pending.push(p);
    }
    while !pending.is_empty() {
        let (edges, _, mut new_pending) = select_all(pending).await;
        for edge in edges? {
            if res.insert(edge.clone()) {
                let p = get_edges_async(&edge);
                new_pending.push(p);
            }
        }
        pending = new_pending;
    }

    Ok(res)
}