# HiRAG Hierarchical Clustering Implementation

## Overview

HyperZoekt now implements a full hierarchical clustering system inspired by the HiRAG paper (https://arxiv.org/html/2503.10150v3). Instead of just building a two-level hierarchy (entities → clusters), the system now builds multiple layers of clusters until a termination condition is met.

## Architecture

### Layer 0: Entity Layer
- Base layer consisting of individual code entities (functions, classes, etc.)
- Each entity has an embedding vector and source content
- Built by `build_first_layer_for_repo()` or `build_first_layer()`

### Layer 1, 2, 3, ... N: Meta-Cluster Layers
- Each layer clusters the previous layer's clusters
- Each meta-cluster has:
  - A centroid (average of child cluster centroids)
  - An LLM-generated summary (summarizing child cluster summaries)
  - Member list (stable_ids of child clusters)
  - Repository provenance (aggregated from children)

### Termination Conditions

The hierarchical clustering stops when **any** of these conditions are met:

1. **Too few clusters**: Layer N has fewer than `min_clusters` clusters (default: 3)
   - No point clustering 2 items into 2 clusters
   
2. **No reduction**: The branching factor k would result in almost no hierarchy reduction
   - Example: 10 clusters → 9 clusters is pointless
   
3. **Max depth reached**: Hit the `HZ_HIRAG_MAX_DEPTH` limit (default: 24)
   - Safety guard against infinite loops or degenerate cases

4. **No valid clusters**: Layer N has no clusters with valid centroids

## Configuration

### Environment Variables

- **`HZ_HIRAG_MAX_DEPTH`** (default: 24)
  - Hard limit on number of layers to prevent infinite loops
  - Set to a lower value for faster builds or testing
  - Example: `export HZ_HIRAG_MAX_DEPTH=10`

### Job Parameters

When enqueueing a HiRAG job (via Redis), you can specify:

```json
{
  "repo": "hyperzoekt",
  "commit": "abc123",
  "k": 0,
  "min_clusters": 3
}
```

- **`k`**: Branching factor for clustering (0 = auto, typically sqrt(N))
- **`min_clusters`**: Stop when a layer has fewer than this many clusters (default: 3)

## Functions

### New: `build_hierarchical_layers()`

```rust
pub async fn build_hierarchical_layers(
    conn: &SurrealConnection,
    repo: Option<&str>,
    k: usize,
    min_clusters_for_next_layer: usize,
) -> Result<()>
```

Main function for building all hierarchical layers. This:
1. Starts from layer 0 (already built by `build_first_layer_for_repo`)
2. Iteratively builds layer N+1 from layer N clusters
3. Stops when termination condition is met

### Legacy: `build_second_layer()`

Now a thin wrapper around `build_hierarchical_layers()` that only builds layer 1.
Kept for backwards compatibility.

## Cluster ID Format

Clusters are identified by stable IDs following this pattern:

- **Global clusters**: `hirag::layer{N}::{cluster_id}`
  - Example: `hirag::layer0::5`, `hirag::layer1::2`

- **Per-repo clusters**: `hirag::layer{N}::{cluster_id}::repo::{repo_name}`
  - Example: `hirag::layer0::5::repo::hyperzoekt`

## Database Schema

Each cluster is stored as a `hirag_cluster` record with:

```rust
{
  "id": "hirag::layer1::5::repo::hyperzoekt",
  "stable_id": "hirag::layer1::5::repo::hyperzoekt",
  "stable_label": "layer1-cluster-5",
  "label": "layer1-cluster-5",
  "summary": "LLM-generated summary of child clusters...",
  "members": ["hirag::layer0::12::repo::hyperzoekt", ...],
  "centroid": [0.1, 0.2, ..., 0.768],
  "centroid_len": 768,
  "member_repos": ["hyperzoekt"],
  "layer": 1
}
```

## Example Hierarchy

For a repository with 2,757 entities:

```
Layer 0: 2,757 entities (functions, classes, etc.)
         ↓ (k=53, sqrt(2757) ≈ 52.5)
Layer 1: 53 clusters (groups of ~52 entities each)
         ↓ (k=7, sqrt(53) ≈ 7.3)
Layer 2: 7 meta-clusters (groups of ~7-8 clusters each)
         ↓ (k=3, sqrt(7) ≈ 2.6)
Layer 3: 3 meta-meta-clusters
         ↓ (STOP: min_clusters=3 reached)
```

## Usage

### Via HiRAG Worker

The worker automatically builds all hierarchical layers after building layer 0:

```bash
# Enqueue a job (via Redis)
redis-cli LPUSH hirag_jobs '{"repo":"hyperzoekt","k":0,"min_clusters":3}'
```

The worker will:
1. Build layer 0 using `build_first_layer_for_repo()`
2. Build all higher layers using `build_hierarchical_layers()`
3. Stop when termination condition is met

### Programmatically

```rust
use hyperzoekt::hirag;
use hyperzoekt::db::connection::connect;

// Connect to SurrealDB
let conn = connect(&None, &None, &None, "zoekt", "repos").await?;

// Build layer 0
hirag::build_first_layer_for_repo(&conn, "hyperzoekt", None, 0).await?;

// Build all hierarchical layers (layer 1, 2, 3, ...)
hirag::build_hierarchical_layers(&conn, Some("hyperzoekt"), 0, 3).await?;
```

## Querying Clusters

### Get all layer 1 clusters

```sql
SELECT * FROM hirag_cluster WHERE id CONTAINS 'layer1';
```

### Get all clusters for a specific repo

```sql
SELECT * FROM hirag_cluster 
WHERE member_repos CONTAINS 'hyperzoekt';
```

### Get cluster hierarchy depth

```sql
SELECT layer, count() as num_clusters 
FROM hirag_cluster 
GROUP BY layer 
ORDER BY layer;
```

## Performance Considerations

### Branching Factor (k)

- **Too small** (k=2): Creates deep, narrow trees → many layers, slower retrieval
- **Too large** (k=100): Creates shallow, wide trees → minimal hierarchy benefit
- **Auto (k=0)**: Uses sqrt(N) heuristic, generally optimal for balanced trees

### Min Clusters

- **Too small** (min=2): May create unnecessary deep layers
- **Too large** (min=10): May stop too early, missing useful hierarchy
- **Default (min=3)**: Good balance for most cases

### Max Depth

- **Safety guard**: Prevents infinite loops in degenerate cases
- **Default (24)**: Extremely conservative, allows for very deep hierarchies
- **Practical**: Most repos will terminate at 3-5 layers based on min_clusters

## Debugging

### Check layer distribution

```bash
# Via SurrealDB
surreal sql --conn http://localhost:8000 \
  --user root --pass root \
  --ns zoekt --db repos \
  "SELECT layer, count() FROM hirag_cluster GROUP BY layer ORDER BY layer;"
```

### View cluster summaries

```bash
# Layer 1 summaries
surreal sql "SELECT label, summary FROM hirag_cluster WHERE id CONTAINS 'layer1' LIMIT 10;"
```

### Monitor worker logs

```bash
docker logs -f hyperzoekt-hirag-worker
```

Look for:
- `Building layer N from layer M clusters`
- `Layer N clustering complete, created K clusters`
- `Hierarchical clustering complete, built N layers total`
- Termination reason logs

## Future Enhancements

1. **Similarity-based termination**: Stop when cluster cohesion is high enough
2. **GMM clustering**: Replace k-means with Gaussian Mixture Models (per HiRAG paper)
3. **Bridge-level relationships**: Add cross-cluster relationships for graph-based retrieval
4. **Adaptive branching**: Vary k per layer based on cluster density
5. **Incremental updates**: Update only affected layers when entities change
