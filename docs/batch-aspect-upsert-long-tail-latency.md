# Batch Aspect Upsert: Long-Tail Latency Analysis

## Summary

Production latency data from the `metadata-graph-assets` (MGA) batch-upsert ramp shows that
`BatchUpsert=true` wins decisively at higher aspect counts (`CountBucket >= 2`), but is flat or
slightly *slower* than the existing per-aspect `dao.add()` path at `CountBucket = 1` — most
visibly on `dataset` and `datasetinstance`, which together dominate MGA's write volume.

This doc captures the production evidence and a code-level root-cause pass through
`datahub-gma`'s batch-upsert implementation, comparing it against the existing single-aspect
`add()`/`addWithOptimisticLocking()` path it's meant to replace.

## Production evidence

Latency comparison (`Mga.Asset.Update`, call-weighted average, ms; `F` = per-aspect path,
`T` = batch-upsert path), sampled over a 72h window:

| EntityType | Bucket | Calls (F) | Calls (T) | CW Avg (F) ms | CW Avg (T) ms | Avg % Δ |
|---|---|---|---|---|---|---|
| dataset | 1 | 53,665,724 | 975,262 | 2.74 | 1.27 | -53.65% |
| datasetinstance | 1 | 108,304,383 | 4,533,395 | 2.58 | 3.84 | **+48.84%** |
| datasetinstance | 2 | 947 | 321 | 6.99 | 14.46 | **+106.87%** |
| jiraissue | 1 | 18,486 | 17,922 | 0.99 | 1.06 | +7.07% |
| samzajobinstance | 1 | 65,329 | 75,363 | 1.65 | 1.75 | +6.06% |

For comparison, entities with higher aspect counts see the batch path win by a wide, consistent
margin (e.g. `mlmodel/6`: 90.50ms → 10.00ms, `mlmodel/7`: 138.00ms → 6.00ms). Median improvement
across all non-dataset/datasetinstance entities is -3.77% at bucket=1 vs. -39.83% at bucket>=2 —
the benefit scales with aspect count, and single-aspect writes see little to no benefit (and in
`dataset`/`datasetinstance`'s case, a regression).

This matches the hypothesis that batch-upsert carries fixed per-call overhead that only pays for
itself once there are multiple aspects to amortize it across. `dataset` and `datasetinstance` are
overwhelmingly single-aspect writes in production (buckets 1-2 make up the vast majority of
their call volume), so they're the entities most exposed to this fixed cost.

## Code-level comparison

Traced both code paths in `dao-impl/ebean-dao`:

- **Existing single-aspect path**: `EbeanLocalAccess.addWithOptimisticLocking()` (called via
  `BaseLocalDAO.aspectUpdateHelper()` → `getLatest()` → `addCommon()`)
- **Batch-upsert path**: `EbeanLocalAccess.batchUpsert()` (called via
  `BaseLocalDAO.addManyBatchInternal()`)

None of the overhead below is per-call reflection in the classic sense — column-name resolution
is cached (`ModelUtils.ASPECT_ALIAS_CACHE`, `SchemaValidatorUtil`'s column cache). The gap is a
combination of small, avoidable object/collection allocations and duplicated work, stacked on
every batch-upsert call, that the single-aspect path never pays.

### 1. Old-value read: generic batch-get vs. targeted point read

- Single-aspect: `aspectUpdateHelper()` → `getLatest()` → `queryLatest()` — a direct read by
  primary key.
- Batch: `addManyBatchInternal()` → `batchGetOldValuesWithExtraInfo()`
  (`BaseLocalDAO.java:902`) → `getWithExtraInfo(Set<AspectKey>)` →
  `EbeanLocalAccess.batchGetUnion()`. Even for a single aspect this:
  - builds a `Set<AspectKey>` via `.stream().map().collect(Collectors.toSet())`,
  - **constructs a second, duplicate `AspectKey` per lambda** just to look the result back up
    (`BaseLocalDAO.java` inside `batchGetOldValuesWithExtraInfo`, right after the query),
  - groups aspect classes into a `HashMap`, builds per-aspect-class SQL via
    `Collectors.toMap(...)`, and merges results into a `LinkedHashMap<SqlRow, Class>` before
    converting back via `EBeanDAOUtils.readSqlRows`.

  All of this generic multi-key machinery runs for N=1 where `queryLatest()` would do a single
  targeted read.

### 2. SQL construction: fixed template vs. hand-assembled per call

- Single-aspect (`SQLStatementUtils.createAspectUpsertSql`): one `getAspectColumnName()` lookup
  + **one** `String.format()` call against a small, fixed constant (`SQL_UPSERT_ASPECT_TEMPLATE`,
  ~3 placeholders).
- Batch (`EbeanLocalAccess.prepareMultiColumnInsert`, line 889, +
  `buildOnDuplicateKeyForUpsert`, line 1024):
  - Extracts `classNames` via `aspectLambdas.stream().map(...).collect(Collectors.toList())`
    (`EbeanLocalAccess.java:919`) — stream/lambda allocation even for a single aspect.
  - Builds the INSERT/VALUES clause with two separate `StringBuilder`s, calling
    `getAspectColumnName()` in the loop.
  - `buildOnDuplicateKeyForUpsert()` **re-derives `classNames` a second time**
    (`EbeanLocalAccess.java:1028`, another stream+collect) and **calls `getAspectColumnName()`
    again for the same aspects** — column-name resolution happens twice per aspect instead of
    once.
  - The fully-assembled SQL string (INSERT clause + VALUES clause + full ON DUPLICATE KEY
    clause, now containing real column names and bind placeholders) is passed through
    `String.format(insertStatement, tableName)` — this makes `String.format` scan the *entire*
    composed SQL text hunting for `%s`, instead of the tiny fixed template the single-aspect path
    uses. This scales with statement length even when there's only one aspect.
  - Neither path caches SQL templates today (`SQLStatementUtils` has no `computeIfAbsent`/static
    cache) — this isn't a regression from removed caching, the batch path's assembly is just
    structurally heavier by construction.

### 3. Extra boilerplate/validation unique to the batch path, even at N=1

- `batchUpsert()` (line 279) first unpacks `updateContexts` into two new parallel `ArrayList`s
  (`aspectValues`, `aspectUpdateLambdas`) via a manual loop — an allocation the single-aspect path
  skips entirely.
- `prepareMultiColumnInsert()` then does a `size()` equality check plus an
  `aspectValues.forEach(...)` null-check pass — a full traversal + lambda allocation for
  validation that the single-aspect path does inline via one `if (newValue == null)` branch.

### Net picture

Buckets >= 2 win because the fixed overhead above amortizes over more aspects (and multiple
single-aspect round-trips/statements are collapsed into one). Bucket 1 pays the same fixed cost
with nothing to amortize it against — exactly matching the production data, where `dataset/1` and
`datasetinstance/1` (MGA's highest-volume, mostly single-aspect writes) show the smallest or
negative improvement.

## Suggested follow-ups

In priority order:

1. **Special-case N=1 in `batchUpsert()`/`addManyBatchInternal()`**: fall through to the existing
   single-aspect `add()` path instead of the generic batch machinery. This would likely close
   most of the gap for `dataset`/`datasetinstance` bucket-1, which is MGA's dominant-volume case.
2. **De-duplicate `classNames` derivation**: compute it once in `batchUpsert()`/
   `prepareMultiColumnInsert()` and pass it into `buildOnDuplicateKeyFor*()` instead of
   re-deriving it — removes one redundant stream pass and N redundant `getAspectColumnName()`
   calls per write.
3. **Avoid the double `AspectKey` construction** in `batchGetOldValuesWithExtraInfo()` — build
   the class-keyed map directly from the same `AspectKey` instances used for the query instead of
   re-constructing them.
4. **Avoid the whole-string `String.format` scan**: replace
   `String.format(insertStatement, tableName)` with a targeted substitution (e.g. building the
   table name into the `StringBuilder` at the right position) so formatting cost doesn't scale
   with the size of the already-assembled SQL text.

## Appendix: raw production data

Full per-entity, per-bucket latency and call-count tables (batch-upsert vs. per-aspect), plus
median/weighted-average improvement rollups, are tracked in the companion analysis spreadsheet
(MGA Batch-Upsert Latency Analysis) rather than duplicated here.
