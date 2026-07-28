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
touched by an operation. `aspects` is empty for a whole-entity delete.

### `NoOpDaoUsageEmitter`

Located in `dao-api/.../tracking/NoOpDaoUsageEmitter.java`. All methods no-op; `isEnabled()` returns `false`. The
default when no emitter is configured. Follows the `NoOpDaoBenchmarkMetrics` pattern.

### `UsageTrackingEbeanLocalAccess` decorator

Located in `dao-impl/ebean-dao/.../UsageTrackingEbeanLocalAccess.java`.

Implements `IEbeanLocalAccess<URN>`, delegates every call to the real implementation first, and emits only **after** a
successful delegate call — wrapped so emission can never change the return value or propagate an exception. When
`isEnabled()` returns `false`, delegation is direct with zero overhead. Entity type is derived from the URN class simple
name at construction (same as `InstrumentedEbeanLocalAccess`). Test-mode and backfill operations are skipped.

### `EbeanLocalDAO.setUsageEmitter()`

A setter that wraps the internal `_localAccess` with the decorator. No-op when `_localAccess` is `null` (OLD_SCHEMA_ONLY
mode). Composes with `setBenchmarkMetrics` — both decorate `_localAccess`. Consistent with existing setter patterns.

### `DaoReadContext` (internal read-before-write filter)

Located in `dao-api/.../tracking/DaoReadContext.java`. A thread-local boolean flag that lets the decorator distinguish a
genuine consumer read from the DAO's own internal read-before-write.

Every aspect write/delete performs a read-modify-write: it reads the current value before writing. In new-schema mode
that internal read flows through the same decorator as consumer reads and would otherwise be counted as one. The write
paths set the flag around that internal read (cleared in a `finally`) — `queryLatest` for single-aspect
add/update/delete and `batchGetOldValuesWithExtraInfo` for `batchUpsert` — and the decorator skips `batchGetUnion`
emission while the flag is set. The set → read → clear happens synchronously on one thread with no async boundary, so
the flag is reliable and cannot leak. Consumer reads take a different entry path, never set the flag, and are still
emitted.

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
- **Read counts are an upper bound on volume.** Internal read-before-writes are excluded via `DaoReadContext`, but the
  uninstrumented legacy/historical paths above still make read counts incomplete — treat them as a lower bound on
  distinct consumers.
- **Writes reflect committed change, not attempts.** Emission is gated on affected-row count (`> 0`) and happens inside
  the write transaction, so a rolled-back or retried write can over-count and a no-op upsert does not emit.

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
