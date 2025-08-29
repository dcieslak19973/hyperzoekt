use crate::repo_index::types::RepoIndexService;
use petgraph::algo::page_rank;
use petgraph::graph::Graph;
use petgraph::prelude::NodeIndex;
use petgraph::Directed;

pub fn compute_pagerank(svc: &mut RepoIndexService) {
    let n = svc.entities.len();
    if n == 0 {
        return;
    }

    // Extract weights & params
    let crate::repo_index::indexer::types::RankWeights {
        call: w_call,
        import: w_import,
        containment: w_contain,
        damping,
        iterations,
    } = svc.rank_weights;

    // Build a directed graph with `n` nodes
    let mut g: Graph<(), f64, Directed> = Graph::with_capacity(n, 0);
    let mut idxs: Vec<NodeIndex> = Vec::with_capacity(n);
    for _ in 0..n {
        idxs.push(g.add_node(()));
    }

    // Add weighted edges according to the configured weights
    for src in 0..n {
        let mut edges: Vec<(usize, f32)> = Vec::new();
        if let Some(calls) = svc.call_edges.get(src) {
            for &t in calls {
                edges.push((t as usize, w_call));
            }
        }
        if let Some(imps) = svc.import_edges.get(src) {
            for &t in imps {
                edges.push((t as usize, w_import));
            }
        }
        if let Some(children) = svc.containment_children.get(src) {
            for &t in children {
                edges.push((t as usize, w_contain));
            }
        }
        if let Some(Some(p)) = svc.containment_parent.get(src) {
            edges.push((*p as usize, w_contain));
        }

        // If there are edges, add them directly to the graph with the weight.
        // petgraph's page_rank will use edge weights via the `edge_weight` function.
        for (t_idx, w) in edges {
            g.add_edge(idxs[src], idxs[t_idx], w as f64);
        }
    }

    // If damping is zero, the rank is uniform (teleport-only). Short-circuit
    // to match previous behavior/tests.
    if damping == 0.0 {
        let v = 1.0f32 / n as f32;
        for ent in svc.entities.iter_mut() {
            ent.rank = v;
        }
        return;
    }

    // petgraph::algo::page_rank expects an adjacency representation; it supports
    // a `node_weight` and `edge_weight` selectors. We can call the basic API.
    // The function returns a vector of ranks keyed by node order.
    let ranks = page_rank(&g, damping as f64, iterations);

    // page_rank returns a Vec<f64> indexed by the node order we created
    let mut sum = 0.0f32;
    for (i, v) in ranks.iter().enumerate().take(n) {
        let r = *v as f32;
        if let Some(ent) = svc.entities.get_mut(i) {
            ent.rank = r;
        }
        sum += r;
    }

    // Normalize ranks to sum to 1.0 (page_rank may already normalize but ensure it)
    if sum > 0.0 {
        for ent in svc.entities.iter_mut() {
            ent.rank /= sum;
        }
    }
}
