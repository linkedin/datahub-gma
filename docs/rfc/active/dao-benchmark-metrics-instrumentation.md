# DAO Benchmark Metrics Instrumentation

## Context

datahub-gma provides the DAO layer for metadata storage. To support performance
benchmarking across different database backends, we need per-DAO-operation latency
histograms, operation counts, and error counts.

This RFC describes the interface and instrumentation decorator that live in the
datahub-gma kernel. The kernel defines the metrics contract and a no-op default;
service layers provide the real implementation backed by their metrics infrastructure.

---

## Design

### `BaseDaoBenchmarkMetrics` interface

Located in `dao-api/.../tracking/BaseDaoBenchmarkMetrics.java`.

Three methods:

- `recordOperationLatency(String operationType, String entityType, long latencyMs)`
- `recordOperationError(String operationType, String entityType, String exceptionClass)`
- `isEnabled()` — for short-circuiting when disabled

Follows the same pattern as `BaseTrackingManager` in the same package: interface in the
kernel, real implementation injected by the service layer.

### `NoOpDaoBenchmarkMetrics`

Located in `dao-api/.../tracking/NoOpDaoBenchmarkMetrics.java`.

All methods are no-ops. `isEnabled()` returns `false`. Used as the default when no
metrics backend is configured. Follows the `DummyTrackingManager` pattern.

### `InstrumentedEbeanLocalAccess` decorator

Located in `dao-impl/ebean-dao/.../InstrumentedEbeanLocalAccess.java`.

Implements `IEbeanLocalAccess<URN>`, wraps each method with timing via
`System.nanoTime()`, delegates to the real implementation. Records latency on both
success and error (via `finally` block); records error class on failure, then re-throws.

When `isEnabled()` returns `false`, delegation is direct with zero overhead.

Entity type is extracted from the URN class simple name at construction time (once,
not per-call): `urnClass.getSimpleName().replace("Urn", "").toLowerCase()`.

### `EbeanLocalDAO.setBenchmarkMetrics()`

A setter on `EbeanLocalDAO` that wraps the internal `_localAccess` with an
`InstrumentedEbeanLocalAccess` decorator. No-op when `_localAccess` is `null`
(OLD_SCHEMA_ONLY mode). Keeps the wrapping logic internal to the DAO, consistent
with existing setter patterns.

---

## Metrics Emitted

Three metrics are emitted per DAO call:

| Metric                                           | Type      | Description                                                |
| ------------------------------------------------ | --------- | ---------------------------------------------------------- |
| `dao.benchmark.<entity>.<op>.latency`            | Histogram | Wall-clock latency of the operation                        |
| `dao.benchmark.<entity>.<op>.count`              | Counter   | Number of times the operation was called                   |
| `dao.benchmark.<entity>.<op>.error.<exception>`  | Counter   | Number of failures, broken down by exception class         |

**Dimensions:**

- **entity** — derived from URN class name. `DatasetUrn` → `dataset`,
  `CorpUserUrn` → `corpuser`, etc.
- **operation** — the method name on `IEbeanLocalAccess`

### Instrumented Operations (9)

| Operation                  | Description                         |
| -------------------------- | ----------------------------------- |
| `add`                      | Upsert an aspect                    |
| `addWithOptimisticLocking` | Upsert with compare-and-swap        |
| `create`                   | Create entity with multiple aspects  |
| `batchGetUnion`            | Batch read aspects by keys          |
| `list` (2 overloads)      | Paginate over aspect versions        |
| `listUrns`                 | Paginate over URNs by filter        |
| `softDeleteAsset`          | Soft-delete an entity               |
| `countAggregate`           | Aggregated count by group           |
| `exists`                   | Check if entity exists              |

**Not instrumented:** `setUrnPathExtractor` (config) and `ensureSchemaUpToDate`
(admin) — neither are data-path operations.

### Example Metric Names

```
dao.benchmark.dataset.add.latency
dao.benchmark.dataset.add.count
dao.benchmark.dataset.add.error.SQLException
dao.benchmark.corpuser.batchGetUnion.latency
```

---

## Usage

Service layers inject their metrics implementation via the setter:

```java
BaseDaoBenchmarkMetrics metrics = createYourMetricsImpl();
dao.setBenchmarkMetrics(metrics);
```

When no metrics are configured, the DAO operates normally with zero instrumentation
overhead.
