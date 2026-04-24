# Skip Redundant Aspect Null/Deleted Checks in Filter Queries

## Problem

`parseIndexFilter()` in `SQLIndexFilterUtils` unconditionally adds two WHERE conditions for every non-URN aspect
criterion:

```sql
a_status IS NOT NULL
JSON_EXTRACT(a_status, '$.gma_deleted') IS NULL
```

These conditions force MySQL to perform row lookups on the JSON column for every matching row, preventing index-only
scans. This makes filter COUNT queries 7-10x slower than necessary.

For example, kafkasyncer's filter query:

```sql
SELECT COUNT(urn) FROM metadata_entity_dataset
WHERE a_status IS NOT NULL                                    -- forces row lookup
AND JSON_EXTRACT(a_status, '$.gma_deleted') IS NULL           -- forces JSON parse per row
AND i_status$removed = 'false'
AND i_urn$platform$platformName = 'kafka'
AND deleted_ts IS NULL
```

Takes ~0.5s on EI and ~1.2s on prod. Without the two redundant conditions, the same query takes ~0.07s on EI (with a
covering index on `platformName, removed, deleted_ts`).

## Why These Checks Are Redundant When `pathParams` Is Present

When a criterion includes a path filter (e.g. `i_status$removed = 'false'`), the generated column comparison already
excludes rows where the aspect is NULL or soft-deleted:

| Row state                    | `a_status` column                  | Generated column `i_status$removed` | Matches `= 'false'`?            |
| ---------------------------- | ---------------------------------- | ----------------------------------- | ------------------------------- |
| Aspect exists, removed=false | `{"aspect":{"removed":false},...}` | `'false'`                           | YES                             |
| Aspect exists, removed=true  | `{"aspect":{"removed":true},...}`  | `'true'`                            | NO                              |
| Aspect soft-deleted          | `{"gma_deleted":{...}}`            | NULL                                | NO (NULL fails all comparisons) |
| Aspect never written         | NULL                               | NULL                                | NO (NULL fails all comparisons) |

The `gma_deleted` mechanism **overwrites** the entire aspect JSON with `{"gma_deleted":{...}}`. This means the generated
column (derived via `JSON_EXTRACT` from the aspect JSON) evaluates to NULL after deletion, and NULL fails all SQL
comparisons (=, <, >, IN, LIKE, etc.).

## When The Checks Are Still Needed

For bare criteria **without** `pathParams` — e.g. `{"aspect": "com.linkedin.common.Status"}` with no path filter — there
is no generated column comparison. The `IS NOT NULL` and `gma_deleted` checks are the only mechanism to exclude
null/deleted aspects. These remain unchanged.

Also, when `pathParams` is present but the generated column does not exist (schema mismatch), the path filter is skipped
and we fall back to the null/deleted checks.

## Change

In `parseIndexFilter()`, the null/deleted checks are now only added when there is no effective path filter for the
criterion:

- **pathParams present AND generated column exists** → skip null/deleted checks (generated column handles exclusion)
- **pathParams present BUT generated column missing** → keep null/deleted checks (fallback)
- **pathParams absent** → keep null/deleted checks (bare criterion)

## Performance Impact

Tested on `metagalaxy_stg` (EI) with a covering index `(platformName, removed, deleted_ts)`:

| Query                                               | Time  | EXPLAIN                    |
| --------------------------------------------------- | ----- | -------------------------- |
| With `a_status IS NOT NULL` + `JSON_EXTRACT` checks | 0.47s | 45% filtered, Using where  |
| Without checks (path filter only)                   | 0.07s | 100% filtered, Using index |

On prod (no covering index yet), removing the checks alone doesn't help (0.86s → 0.86s) because the existing
`idx_platform_status` index doesn't cover `deleted_ts`. The covering index (separate PR in metadata-graph-assets) is
also required.

Both changes together: **~1.2s → ~0.07s per COUNT query** on prod.
