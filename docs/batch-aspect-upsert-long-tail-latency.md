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

### 1. Old-value read: same SQL, heavier Java wrapping

In new schema (what MGA runs), the single-aspect path's `getLatest()` and the batch path's
`batchGetOldValuesWithExtraInfo()` both funnel into the *same* `EbeanLocalAccess.batchGetUnion()`,
which reads **only the specific aspect column(s) for the given urn** — not all aspects — so the
read SQL is essentially identical for N=1 and is not where the gap comes from. The difference is
the extra Java wrapping the batch path stacks around that identical read: building a
`Set<AspectKey>` via streams, **constructing a second duplicate `AspectKey` per lambda** to look
results back up, and grouping through a `HashMap`/`Collectors.toMap`/`LinkedHashMap<SqlRow, Class>`
pipeline that the single-aspect call skips.

### 2. SQL construction: fixed template vs. hand-assembled per call

The single-aspect path (`SQLStatementUtils.createAspectUpsertSql`) does one `getAspectColumnName()`
lookup and one `String.format()` against a small fixed template, whereas the batch path
(`prepareMultiColumnInsert` + `buildOnDuplicateKeyForUpsert`) **derives `classNames` twice and
resolves `getAspectColumnName()` twice per aspect**, assembling the statement with two
`StringBuilder`s. The fully-composed SQL is then passed through `String.format(insertStatement,
tableName)`, forcing a scan of the *entire* assembled string for `%s` rather than the tiny fixed
template.

### 3. Extra boilerplate/validation unique to the batch path

`batchUpsert()` unpacks `updateContexts` into two parallel `ArrayList`s via a manual loop, then
`prepareMultiColumnInsert()` adds a `size()` check and an `aspectValues.forEach(...)` null-check
pass — allocations and traversals the single-aspect path replaces with a single inline
`if (newValue == null)` branch.

## Why not do anything now

On review, no immediate change is warranted. The overwhelming, wide-margin improvement the batch
path delivers across the non-`dataset`-bucket-1 slices (frequently 40-90%+ latency reductions at
higher aspect counts) far outweighs the ~0.1-0.2 ms (100-200 microsecond) regression seen on
`dataset`/`datasetinstance` bucket-1 — even accounting for the fact that `dataset` bucket-1 is the
single most common operation by call volume. The regression is small in absolute terms, the net
effect across the workload is strongly positive, and the overheads above are hypotheses that
haven't yet been confirmed as the dominant cost. This is documented for future reference rather
than as an action item.

## Suggested follow-ups

Look into the hypotheses above — validate with allocation profiling and/or a JMH microbenchmark
(N=1 batch vs. single-aspect path) to confirm which of the overheads actually dominate before
committing to a fix.
