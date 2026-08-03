# DAO Usage Event Instrumentation

## Context

datahub-gma provides the DAO layer for metadata storage. To support usage analytics — which callers read and write which
entities and aspects — we need one usage event per successful data-access operation (read / write / delete).

This RFC describes the interface, value type, and instrumentation decorator that live in the datahub-gma kernel. The
kernel defines the usage contract and a no-op default; service layers provide the real emitter backed by their eventing
infrastructure. Mirrors the existing `BaseDaoBenchmarkMetrics` instrumentation.

---

## Design

### `BaseDaoUsageEmitter` interface

Located in `dao-api/.../tracking/BaseDaoUsageEmitter.java`.

- `emit(operationType, entityType, sourceOperation, actorUrn, impersonatorUrn, targets)`
- `isEnabled()` — for short-circuiting when disabled

Passes only plain strings and `DaoUsageTarget`, so the kernel stays free of any concrete event-schema dependency.
Contract: implementations MUST be fire-and-forget — asynchronous, non-blocking, and never throwing back into the caller.
Same kernel-interface / service-implementation split as `BaseDaoBenchmarkMetrics`.

### `DaoUsageTarget` value type

Located in `dao-api/.../tracking/DaoUsageTarget.java`. Holds `{String urn, List<String> aspects}` — one per distinct URN
touched by an operation. `aspects` may be empty — a whole-entity `DELETE_ALL`, or a `create` with no aspect values. Do
not infer the operation kind from an empty `aspects` list; `operationType` is the authoritative discriminator.

### `NoOpDaoUsageEmitter`

Located in `dao-api/.../tracking/NoOpDaoUsageEmitter.java`. All methods no-op; `isEnabled()` returns `false`. The
default when no emitter is configured. Follows the `NoOpDaoBenchmarkMetrics` pattern.

### `UsageTrackingEbeanLocalAccess` decorator

Located in `dao-impl/ebean-dao/.../UsageTrackingEbeanLocalAccess.java`.

Implements `IEbeanLocalAccess<URN>`, delegates every call to the real implementation first, and emits only **after** a
successful delegate call — wrapped so emission can never change the return value or propagate an exception. When
`isEnabled()` returns `false`, delegation is direct with zero overhead. Entity type comes from `urn.getEntityType()` on
each operation. Test-mode, backfill, and internal read-before-write operations are skipped.

### `EbeanLocalDAO.setUsageEmitter()`

A setter that wraps the internal `_localAccess` with the decorator. No-op when `_localAccess` is `null` (OLD_SCHEMA_ONLY
mode). Composes with `setBenchmarkMetrics` — both decorate `_localAccess`. Consistent with existing setter patterns.

### `DaoReadContext` (internal-read filter)

Located in `dao-api/.../tracking/DaoReadContext.java`. A thread-local marker that lets the decorator distinguish a
genuine consumer read from a read the DAO issues internally — the read-before-write every write/delete performs
(`queryLatest`, `batchGetOldValuesWithExtraInfo`) and the old→new backfill reads (`backfill`, `backfillEntityTables`,
`backfillLocalRelationships`). Each is wrapped with
`try (DaoReadContext.Scope ignored = DaoReadContext.markInternalRead())`, and the decorator skips read emission while
the marker is set. The `Scope` restores the previous marker state on close rather than clearing unconditionally, so
nested internal reads compose safely; mark → read → restore runs synchronously on one thread, so the marker cannot leak.
Consumer reads never set it and are still emitted.

### `DaoUsageBuffer` (commit-gated write emission)

Located in `dao-api/.../tracking/DaoUsageBuffer.java`. A thread-local buffer that holds write/delete emissions until the
enclosing transaction commits: the decorator sits below the transaction boundary, so emitting inline would report writes
that later roll back and re-report them on every retry. `EbeanLocalDAO.runInTransactionWithRetry` brackets the
transaction with `enter()` / `exit()`; writes `record()` into the open frame (non-transactional paths run immediately,
reads are never buffered). A depth counter flushes only at the outermost commit (Ebean nests via savepoints), a rollback
discards the frame, and each retry truncates back to its entry mark so a retried write is reported once. The buffer is
armed only once an emitter is installed, so it stays zero-overhead otherwise, and `enter()` / `exit()` must stay paired
in a `finally` or an unexited frame leaks.

---

## Events Emitted

One event per successful operation, with fields: `operationType` (`READ` / `WRITE` / `DELETE` / `DELETE_ALL`),
`entityType`, `sourceOperation` (the DAO method), `actorUrn`, `impersonatorUrn`, and `targets` (`[{urn, aspects[]}]`).

### Operation mapping

| Operation                          | operationType                     | targets                           | caller            |
| ---------------------------------- | --------------------------------- | --------------------------------- | ----------------- |
| `batchGetUnion`                    | `READ`                            | grouped per URN                   | none              |
| `list(aspectClass, urn, ...)`      | `READ`                            | single URN                        | none              |
| `add` / `addWithOptimisticLocking` | `WRITE` (value) / `DELETE` (null) | single URN                        | audit-stamp actor |
| `create`                           | `WRITE`                           | single URN, aspects from values   | audit-stamp actor |
| `batchUpsert`                      | `WRITE`                           | single URN, aspects from contexts | audit-stamp actor |
| `softDeleteAsset`                  | `DELETE_ALL`                      | single URN, empty aspects         | none              |

Aspect names use the aspect class simple name. **Not emitted:** global `list`, `listUrns`, `exists`, `countAggregate`,
`batchSoftDeleteAssets`, `readDeletionInfoBatch`, `ensureSchemaUpToDate`, and config methods — none are organic
single-entity data access. Test-mode, backfill, and internal read-before-write operations are also skipped (see
`DaoReadContext`).

---

## Known limitations (v1)

The decorator observes only what reaches storage, and only through the new-schema access path. These caveats affect how
much to trust v1 numbers; none affect write-side data or the safety of the instrumentation.

- **Reads have no caller.** Writes carry an `AuditStamp`; reads do not, so every read event has `actorUrn = null`.
- **Only new/dual-schema DAOs are instrumented.** The decorator wraps the new-schema access layer, so an
  `OLD_SCHEMA_ONLY` DAO emits nothing, and historical (non-latest-version) reads served from the legacy path are not
  emitted.
- **Read counts are a lower bound.** Internal read-before-writes and backfill reads are excluded via `DaoReadContext`,
  and the uninstrumented legacy/historical paths above are not captured at all, so recorded read volume undercounts real
  reads — treat it as a lower bound, especially for distinct consumers.
- **Writes reflect committed change, not attempts.** Emission is gated on affected-row count (`> 0`) and buffered until
  the enclosing transaction commits (see `DaoUsageBuffer`), so a rolled-back write is never reported and a retried write
  is reported once rather than once per attempt. A no-op upsert (zero affected rows) does not emit.

Read-side caller attribution is a proposed follow-up. Until it lands, treat read events as anonymous and do not use them
for consumer-dependency decisions.

---

## Usage

Service layers inject their emitter via the setter:

```java
BaseDaoUsageEmitter emitter = createYourUsageEmitter();
dao.setUsageEmitter(emitter);
```

When no emitter is configured, the DAO operates normally with zero instrumentation overhead and no events.
