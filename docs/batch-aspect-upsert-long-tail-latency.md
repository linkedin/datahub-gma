# Batch Aspect Upsert: Long-Tail Latency Analysis

## Summary

Latency observations from the asset service's batch-upsert rollout show that `BatchUpsert=true` wins decisively at
higher aspect counts (multiple aspects written per call), but is flat or slightly _slower_ than the existing per-aspect
`dao.add()` path when only a single aspect is written per call — the case that dominates write volume for single-aspect
entities.

This doc captures a code-level root-cause pass through `datahub-gma`'s batch-upsert implementation, comparing it against
the existing single-aspect `add()`/`addWithOptimisticLocking()` path it's meant to replace.

## Code-level comparison

Traced both code paths in `dao-impl/ebean-dao`:

- **Existing single-aspect path**: `EbeanLocalAccess.addWithOptimisticLocking()` (called via
  `BaseLocalDAO.aspectUpdateHelper()` → `getLatest()` → `addCommon()`)
- **Batch-upsert path**: `EbeanLocalAccess.batchUpsert()` (called via `BaseLocalDAO.addManyBatchInternal()`)

None of the overhead below is per-call reflection in the classic sense — column-name resolution is cached
(`ModelUtils.ASPECT_ALIAS_CACHE`, `SchemaValidatorUtil`'s column cache). The gap is a combination of small, avoidable
object/collection allocations and duplicated work, stacked on every batch-upsert call, that the single-aspect path never
pays.

### 1. Old-value read: same SQL, heavier Java wrapping

In new schema, the single-aspect path's `getLatest()` and the batch path's `batchGetOldValuesWithExtraInfo()` both
funnel into the _same_ `EbeanLocalAccess.batchGetUnion()`, which reads **only the specific aspect column(s) for the
given urn** — not all aspects — so the read SQL is essentially identical for N=1 and is not where the gap comes from.
The difference is the extra Java wrapping the batch path stacks around that identical read: building a `Set<AspectKey>`
via streams, **constructing a second duplicate `AspectKey` per lambda** to look results back up, and grouping through a
`HashMap`/`Collectors.toMap`/`LinkedHashMap<SqlRow, Class>` pipeline that the single-aspect call skips.

### 2. SQL construction: fixed template vs. hand-assembled per call

The single-aspect path (`SQLStatementUtils.createAspectUpsertSql`) does one `getAspectColumnName()` lookup and one
`String.format()` against a small fixed template, whereas the batch path (`prepareMultiColumnInsert` +
`buildOnDuplicateKeyForUpsert`) **derives `classNames` twice and resolves `getAspectColumnName()` twice per aspect**,
assembling the statement with two `StringBuilder`s. The fully-composed SQL is then passed through
`String.format(insertStatement, tableName)`, forcing a scan of the _entire_ assembled string for `%s` rather than the
tiny fixed template.

### 3. Extra boilerplate/validation unique to the batch path

`batchUpsert()` unpacks `updateContexts` into two parallel `ArrayList`s via a manual loop, then
`prepareMultiColumnInsert()` adds a `size()` check and an `aspectValues.forEach(...)` null-check pass — allocations and
traversals the single-aspect path replaces with a single inline `if (newValue == null)` branch.

## Why not do anything now

On review, no immediate change is warranted. The batch path delivers a wide-margin latency improvement across
multi-aspect writes that far outweighs the small regression seen on single-aspect writes — even accounting for the fact
that single-aspect writes are the more common operation by volume. The regression is small in absolute terms, the net
effect across the workload is strongly positive, and the overheads above are hypotheses that haven't yet been confirmed
as the dominant cost. This is documented for future reference rather than as an action item.

## Suggested follow-ups

Look into the hypotheses above — validate with allocation profiling and/or a JMH microbenchmark (N=1 batch vs.
single-aspect path) to confirm which of the overheads actually dominate before committing to a fix.
