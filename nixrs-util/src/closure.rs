use std::collections::BTreeSet;
use std::future::Future;

use futures::future::select_all;

pub async fn compute_closure<E, T, F, Fut>(
    start_elts: BTreeSet<T>,
    get_edges_async: F,
) -> Result<BTreeSet<T>, E>
where
    T: Ord + Clone,
    F: Fn(&T) -> Fut,
    Fut: Future<Output = Result<BTreeSet<T>, E>> + Unpin,
{
    let mut res = BTreeSet::new();
    let mut pending = Vec::with_capacity(start_elts.len());
    for start_elt in start_elts {
        let p = get_edges_async(&start_elt);
        pending.push(p);
        res.insert(start_elt);
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

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        convert::Infallible,
        future::ready,
    };

    use crate::compute_closure;

    #[macro_export]
    macro_rules! set {
        () => { BTreeSet::new() };
        ($($x:expr),+ $(,)?) => {{
            let mut ret = BTreeSet::new();
            $(
                ret.insert($x);
            )+
            ret
        }};
    }

    #[tokio::test]
    async fn test_closure() {
        let mut test_graph = BTreeMap::new();
        test_graph.insert("A", set!["B", "C", "G"]);
        test_graph.insert("B", set!["A"]); // Loops back to A

        test_graph.insert("C", set! { "F" }); // Indirect reference
        test_graph.insert("D", set! { "A" }); // Not reachable, but has backreferences
        test_graph.insert("E", set! {}); // Just not reachable
        test_graph.insert("F", set! {});
        test_graph.insert("G", set! { "G" }); // Self reference

        let expected_closure = set! {"A", "B", "C", "F", "G"};
        let ret = compute_closure(set! {"A"}, |current_node| {
            ready(Ok(test_graph.get(current_node).unwrap().clone()) as Result<_, Infallible>)
        })
        .await
        .unwrap();
        assert_eq!(ret, expected_closure);
    }

    #[tokio::test]
    async fn test_closure_no_loops() {
        let mut test_graph = BTreeMap::new();
        test_graph.insert("A", set!["B", "C", "G"]);
        test_graph.insert("B", set![]);

        test_graph.insert("C", set! { "F" }); // Indirect reference
        test_graph.insert("D", set! { "A" }); // Not reachable, but has backreferences
        test_graph.insert("E", set! {}); // Just not reachable
        test_graph.insert("F", set! {});
        test_graph.insert("G", set! { "G" }); // Self reference

        let expected_closure = set! {"A", "B", "C", "F", "G"};
        let ret = compute_closure(set! {"A"}, |current_node| {
            ready(Ok(test_graph.get(current_node).unwrap().clone()) as Result<_, Infallible>)
        })
        .await
        .unwrap();
        assert_eq!(ret, expected_closure);
    }
}
