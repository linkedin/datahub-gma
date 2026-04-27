# Skip Redundant Aspect Null/Deleted Checks in Filter Queries

## Problem

`parseIndexFilter()` in `SQLIndexFilterUtils` unconditionally adds two WHERE conditions for every non-URN aspect
criterion:

```sql
<aspect_column> IS NOT NULL
JSON_EXTRACT(<aspect_column>, '$.gma_deleted') IS NULL
```

These conditions force MySQL to perform row lookups on the JSON column for every matching row, preventing index-only
scans. This affects all filter and groupBy queries across all entity types.

For example, a DatasetAssetService filter query:

```sql
SELECT COUNT(urn) FROM metadata_entity_dataset
WHERE a_status IS NOT NULL                                    -- forces row lookup
AND JSON_EXTRACT(a_status, '$.gma_deleted') IS NULL           -- forces JSON parse per row
AND i_status$removed = 'false'
AND i_urn$platform$platformName = 'kafka'
AND deleted_ts IS NULL
```

With a covering index on `(platformName, removed, deleted_ts)`, removing the two redundant conditions allows MySQL to
satisfy the query entirely from the index (0.07s vs 0.50s).

## Why These Checks Are Redundant When `pathParams` Is Present

When a criterion includes a path filter (e.g. `i_status$removed = 'false'`), the generated column comparison already
excludes rows where the aspect is NULL or soft-deleted.

The `gma_deleted` mechanism **overwrites** the entire aspect JSON with `{"gma_deleted":{...}}` (not a merge — the old
aspect data is completely replaced). The generated column is derived via `JSON_EXTRACT` from the aspect JSON, so it
evaluates to NULL after deletion. NULL fails all SQL comparisons.

| Row state                    | Aspect column                      | Generated column | Matches any comparison?         |
| ---------------------------- | ---------------------------------- | ---------------- | ------------------------------- |
| Aspect exists, removed=false | `{"aspect":{"removed":false},...}` | `'false'`        | YES                             |
| Aspect exists, removed=true  | `{"aspect":{"removed":true},...}`  | `'true'`         | Depends on filter value         |
| Aspect soft-deleted          | `{"gma_deleted":{...}}`            | NULL             | NO (NULL fails all comparisons) |
| Aspect never written         | NULL                               | NULL             | NO (NULL fails all comparisons) |

This holds for all SQL comparison operators: `=`, `<`, `>`, `>=`, `<=`, `IN`, `LIKE`, `JSON_CONTAINS`, `JSON_SEARCH`.

## When The Checks Are Still Needed

1. **Bare criteria without `pathParams`** — e.g. `{"aspect": "com.linkedin.common.Status"}` with no path filter. There
   is no generated column comparison, so the `IS NOT NULL` and `gma_deleted` checks are the only mechanism to exclude
   null/deleted aspects. These remain unchanged.

2. **pathParams present but generated column does not exist** — schema mismatch where the virtual column or expression
   index hasn't been created. The path filter is skipped and we fall back to the null/deleted checks.

## Change

In `parseIndexFilter()`, the null/deleted checks are now only added when there is no effective path filter for the
criterion:

- **pathParams present AND generated column exists** -> skip null/deleted checks (generated column handles exclusion)
- **pathParams present BUT generated column missing** -> keep null/deleted checks (fallback)
- **pathParams absent** -> keep null/deleted checks (bare criterion)

## Performance Impact

### DatasetAssetService (broad scan, 77K rows) — metagalaxy_stg with covering index

| Query             | Old SQL | New SQL | Improvement             |
| ----------------- | ------- | ------- | ----------------------- |
| COUNT             | 0.50s   | 0.07s   | **7x faster**           |
| DATA (LIMIT 1000) | 0.06s   | 0.05s   | ~same (LIMIT caps work) |
| Row count         | 77,589  | 77,589  | identical               |

EXPLAIN comparison:

|          | Old         | New         |
| -------- | ----------- | ----------- |
| filtered | 45%         | 100%        |
| Extra    | Using where | Using index |

### MlModelInstanceAssetService (narrow IN query, 18K rows) — ai_metadata_prod

| Query     | Old SQL | New SQL | Improvement |
| --------- | ------- | ------- | ----------- |
| COUNT     | 0.52s   | 0.51s   | ~same       |
| Row count | 18,780  | 18,780  | identical   |

Narrow queries with small IN lists show no improvement (the composite index already narrows the result set, JSON
overhead is negligible) but also **no regression**.

### Why both code change and covering index are needed

| Scenario                      | COUNT time (dataset, EI)                    |
| ----------------------------- | ------------------------------------------- |
| Old code, old index           | 0.50s                                       |
| Old code + covering index     | 0.50s (JSON checks still force row lookups) |
| New code + old index          | 0.50s (deleted_ts still forces row lookups) |
| **New code + covering index** | **0.07s** (pure index scan)                 |

Neither fix alone achieves the improvement. The code change removes the JSON conditions that prevent index-only scans.
The covering index ensures `deleted_ts IS NULL` can also be resolved from the index.
