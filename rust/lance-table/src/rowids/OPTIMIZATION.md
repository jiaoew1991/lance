# RowIdIndex Build Optimization

## Problem

When `enable_stable_row_id` is enabled, the first `take_rows()` call triggers a full `RowIdIndex` build. On large datasets this cold start was extremely slow:

| Dataset | Rows | Fragments | Cold start (before) |
|---------|------|-----------|-------------------|
| 968M rows | 968,938,257 | 3,540 | 18.9 seconds |
| 4.26B rows | 4,261,682,117 | 18,243 | 220 seconds (3.7 min) |

## Root Causes

### 1. O(total_rows) segment expansion in `decompose_sequence`

**File:** `rust/lance-table/src/rowids/index.rs`

`decompose_sequence` builds the row_id-to-address mapping for each fragment. The original code expanded every `U64Segment` element-by-element:

```rust
segment.iter()          // Range(0..273711) expands to 273711 individual u64s
    .enumerate()
    .filter_map(...)    // checks deletion_vector.contains() for each row
    .collect::<Vec<>>() // allocates Vec of all active (row_id, address) pairs
```

Then it split the pairs into two `Vec<u64>` and called `U64Segment::from_iter()` to re-compress back into a segment.

For a `Range(0..273711)` with no deletions, this means:

- Iterate 273,711 values
- Check deletion vector 273,711 times (all pass)
- Allocate and fill a `Vec<(u64, u64)>` with 273,711 entries
- Split into two `Vec<u64>`, each with 273,711 entries
- `from_iter` re-discovers that both are contiguous ranges

**The input is a Range, the output is a Range, but 273K iterations happen in between.**

Across 18,243 fragments averaging 233K rows each, this totals **4.26 billion iterations** with ~32 GB of temporary allocations.

### 2. O(N²) fragment lookup in `load_row_id_index`

**File:** `rust/lance/src/dataset/rowids.rs`

`load_row_id_index` converts row_id sequences into `FragmentRowIdIndex` structs. The original code did:

```rust
try_join_all(sequences.into_iter().map(|(fragment_id, sequence)| {
    async move {
        let fragments = dataset.get_fragments();          // returns all N fragments
        let fragment = fragments.iter()
            .find(|f| f.id() as u32 == fragment_id);      // O(N) linear search
        fragment.get_deletion_vector().await               // called even without deletion file
    }
}))
```

Three compounding issues:

- **O(N) linear search × N fragments = O(N²):** For 18,243 fragments, this is 333 million comparisons.
- **`try_join_all` spawns all N futures at once:** Overwhelms the async runtime scheduler with 18K concurrent tasks.
- **`get_deletion_vector()` called unconditionally:** Even fragments with no deletion file pay the async overhead.

## Solution

### Fix 1: O(1) fast path for Range segments without deletions

When a fragment has no deletions and its row_id sequence is a `Range`, we construct the index chunk directly without iterating:

```rust
U64Segment::Range(range) => {
    let row_id_segment = U64Segment::Range(range.clone());
    let address_segment = U64Segment::Range(start_address..start_address + len);
    // Done in O(1), no iteration needed
}
```

This reduces `decompose_sequence` from O(total_rows) to O(num_fragments) for the common case.

### Fix 2: HashMap lookup + conditional deletion vector loading

```rust
// O(1) lookup via HashMap instead of O(N) linear search
let fragment_map: HashMap<u32, &FileFragment> = fragments.iter()
    .map(|f| (f.id() as u32, f)).collect();

// buffer_unordered instead of try_join_all for controlled concurrency
futures::stream::iter(sequences.into_iter().map(|(fragment_id, sequence)| {
    let fragment = fragment_map.get(&fragment_id).expect("Fragment should exist");
    // Skip async deletion vector load when there's no deletion file
    if has_deletion_file {
        fragment.get_deletion_vector().await
    } else {
        DeletionVector::default()
    }
}))
.buffer_unordered(io_parallelism())
```

## Results

| Dataset | Before | After | Speedup |
|---------|--------|-------|---------|
| 968M rows, 3,540 fragments | 18.9s | 150ms | **126x** |
| 4.26B rows, 18,243 fragments | 220s | 89ms | **2,471x** |

All existing tests pass. Correctness verified against real S3 datasets with:

- Boundary row_ids (first/last row of each fragment)
- 1,000 random row_ids across the full range
- Data consistency between `take()` and `take_rows()`
- Out-of-range row_id handling
