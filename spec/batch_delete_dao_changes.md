# Batch Deletion DAO Support

**PR scope:** datahub-gma only **Project:** META-23501 — Metadata Graph Stale Metadata Cleanup Phase 2

---

## Why

Stale metadata cleanup jobs need to soft-delete entities in bulk. The existing `softDeleteAsset()` method operates on a
single URN. This change adds two new DAO operations that work on a batch of URNs in exactly two DB round-trips,
regardless of batch size.

The DAO layer is intentionally kept simple: pure SQL, no business logic, no Kafka. The consuming service
(`metadata-graph-assets`) handles validation, Kafka archival, and per-URN result reporting.

---

## What Was Added

### `EntityDeletionInfo` (new, `dao-api`)

An immutable value object returned by the batch read operation. Contains the fields a caller needs to determine whether
each entity is eligible for deletion:

- `deletedTs` — whether the entity is already soft-deleted
- `statusRemoved` — whether the entity's Status aspect has `removed = true`
- `statusLastModifiedOn` — when the Status was last changed (for retention window checks)
- `aspectColumns` — all aspect column values as raw JSON strings, for use by the service layer's Kafka archival step

Presence in the returned map means the entity exists. Absence means it was not found.

### Two new `IEbeanLocalAccess` methods

**`readDeletionInfoBatch`** — reads deletion-relevant fields for a list of URNs in a single SELECT (explicit column
list: `urn`, `deleted_ts`, and all `a_*` aspect columns — index columns are excluded). The Status aspect column name is
resolved internally via `SQLSchemaUtils.getAspectColumnName()`. Returns a map of URN → `EntityDeletionInfo` for all URNs
found. URNs not present in the DB are simply absent from the result — the caller treats absence as "not found."

**`batchSoftDeleteAssets`** — soft-deletes a list of URNs in a single `UPDATE`. The Status aspect column name is
resolved internally, and is used in the WHERE clause guard conditions (not already deleted, Status.removed = true,
lastmodifiedon before cutoff) as defense-in-depth against race conditions between the SELECT and UPDATE.

### Layered implementation (no logic in `EbeanLocalAccess`)

- **`SQLStatementUtils`**: two new factory methods that build the SELECT and UPDATE statements
- **`EBeanDAOUtils`**: two new methods that parse `SqlRow` results into `EntityDeletionInfo`. Status fields are
  extracted using `RecordUtils.toDataMap()` (the same Pegasus data framework used throughout the codebase — no manual
  JSON parsing).

### `InstrumentedEbeanLocalAccess`

Both new methods are wired through the existing `instrument()` decorator, consistent with every other method on this
class.

---

## Intended Usage

```
caller → readDeletionInfoBatch(urns)      // 1 SELECT
         → validate each URN, partition into eligible / skipped
         → archive eligible to Kafka      // service layer responsibility
         → batchSoftDeleteAssets(eligible, cutoff)  // 1 UPDATE
```

The two methods are designed to be called together in sequence. The `aspectColumns` field in `EntityDeletionInfo`
carries the full entity state needed for Kafka archival between the two calls.

---

## Tests

### `EbeanLocalAccessTest` — integration tests against embedded MariaDB

Tests verify actual SQL execution and result correctness end-to-end. Each test inserts rows directly via raw SQL to
control `a_status` and `deleted_ts` precisely, then asserts on the returned `EntityDeletionInfo` values or on DB state
after the UPDATE.

**`readDeletionInfoBatch`:**

- Happy path: returns correct `statusRemoved`, `statusLastModifiedOn`, `deletedTs`, and `aspectColumns` for found URNs
- Empty input: returns empty map
- URNs not in DB: absent from result map
- Mixed found/not-found: only found URNs in result
- Already soft-deleted URN: `deletedTs` is non-null in result

**`batchSoftDeleteAssets`:**

- Happy path: eligible URNs are soft-deleted, returns correct affected row count
- Empty input: returns 0
- `Status.removed = false`: guard clause blocks deletion
- Retention window not met (recent `lastmodifiedon`): guard clause blocks deletion
- Already soft-deleted (`deleted_ts` set): guard clause blocks re-deletion
- Mixed batch: only eligible URNs are deleted; ineligible ones are untouched

The test SQL schema (both `ebean-local-access-create-all.sql` files) was extended with an `a_status` column on
`metadata_entity_foo` to support these tests.

### `InstrumentedEbeanLocalAccessTest` — mock-based unit tests

Verifies that `InstrumentedEbeanLocalAccess` correctly delegates both new methods to the underlying `IEbeanLocalAccess`
and records latency via `BaseDaoBenchmarkMetrics`. No database required.

---

## Design Constraints

- **DAO layer is Kafka-free.** No archival, no event publishing. The shared library stays generic.
- **Exactly 2 DB calls per batch.** No per-URN queries.
- **Guard clauses in the UPDATE.** Even if a caller skips the SELECT validation, the UPDATE will not soft-delete
  entities that don't meet all safety conditions.
- **No hardcoded column names.** The Status aspect column name is resolved internally via
  `SQLSchemaUtils.getAspectColumnName()`, consistent with how all other DAO methods resolve column names. Entity types
  that map Status to a different column (e.g. `a_foo_bar`) work without changes.
- **Batch size limit.** Both methods reject batches exceeding 2000 URNs as defense-in-depth against overwhelming the DB.
