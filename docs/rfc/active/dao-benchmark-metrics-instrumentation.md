# DAO Benchmark Metrics Instrumentation Plan

## Context

Metadata Graph (MG) is evaluating a MySQL → TiDB migration. We need per-DAO-operation latency histograms, operation counts, and error counts to establish a MySQL baseline. datahub-gma is the open-source kernel (no LinkedIn internal deps allowed); metadata-graph-assets is the LinkedIn service that consumes it. The plan uses an interface in the kernel + real impl in the service, following the existing `BaseTrackingManager` / `DummyTrackingManager` precedent.

The metrics pipeline: `BaseDaoBenchmarkMetrics` (interface) → `DropwizardDaoBenchmarkMetrics` (impl) → `BasicMetricsCollector` (from metrics-collector-util) → `InGraphsMetricsClient` → `SensorRegistry` → InGraphs → Observe. This is the same proven pipeline used by metadata-store (PR #3217).

## PRs

- **datahub-gma** (Phase 1): https://github.com/linkedin/datahub-gma/pull/601
- **metadata-graph-assets** (Phase 2+3): https://github.com/linkedin-multiproduct/metadata-graph-assets/pull/760

datahub-gma must land first since metadata-graph-assets depends on the new interface.

---

## Metrics Emitted

Three metrics are emitted per DAO call:

| Metric | Type | What it tells you |
|--------|------|-------------------|
| `dao.benchmark.<entity>.<op>.latency` | Histogram | How long the call took (auto-computes p50/p75/p95/p99/max) |
| `dao.benchmark.<entity>.<op>.count` | Counter | How many times this call happened (throughput) |
| `dao.benchmark.<entity>.<op>.error.<exception>` | Counter | How many times this call failed, by exception type |

**Dimensions:**
- **entity** — derived from the URN class name at construction time. `DatasetUrn` → `dataset`, `CorpUserUrn` → `corpuser`, `AzkabanFlowUrn` → `azkabanflow`, etc.
- **operation** — the method name on `IEbeanLocalAccess`

### Instrumented Operations (9)

| Operation | What it does |
|-----------|-------------|
| `add` | Upsert an aspect |
| `addWithOptimisticLocking` | Upsert with compare-and-swap |
| `create` | Create entity with multiple aspects |
| `batchGetUnion` | Batch read aspects by keys |
| `list` (2 overloads) | Paginate over aspect versions |
| `listUrns` | Paginate over URNs by filter |
| `softDeleteAsset` | Soft-delete an entity |
| `countAggregate` | Aggregated count by group |
| `exists` | Check if entity exists |

**Not instrumented:** `setUrnPathExtractor` (config) and `ensureSchemaUpToDate` (admin) — neither are real data operations.

### Example Metric Names
```
dao.benchmark.dataset.add.latency
dao.benchmark.dataset.add.count
dao.benchmark.dataset.add.error.SQLException
dao.benchmark.corpuser.batchGetUnion.latency
dao.benchmark.azkabanflow.listUrns.count
```

Expected cardinality: ~30 entity types × 9 operations = ~270 base metric names. Well within InGraphs limits.

---

## Phase 1: Interface + No-Op in datahub-gma

### 1a. Create `BaseDaoBenchmarkMetrics` interface
**New file**: `dao-api/src/main/java/com/linkedin/metadata/dao/tracking/BaseDaoBenchmarkMetrics.java`

Three methods:
- `recordOperationLatency(String operationType, String entityType, long latencyMs)`
- `recordOperationError(String operationType, String entityType, String exceptionClass)`
- `isEnabled()` — for short-circuiting when disabled

Follows the pattern of `BaseTrackingManager` in the same package.

### 1b. Create `NoOpDaoBenchmarkMetrics`
**New file**: `dao-api/src/main/java/com/linkedin/metadata/dao/tracking/NoOpDaoBenchmarkMetrics.java`

All methods are no-ops. `isEnabled()` returns `false`. Follows `DummyTrackingManager` pattern.

### 1c. Create `InstrumentedEbeanLocalAccess` decorator
**New file**: `dao-impl/ebean-dao/src/main/java/com/linkedin/metadata/dao/InstrumentedEbeanLocalAccess.java`

Implements `IEbeanLocalAccess<URN>`, wraps each method with timing, delegates to the real impl. Uses `System.nanoTime()` for latency measurement. Records latency on success and on error; re-throws exceptions.

Entity type extracted from URN class simple name at construction time (once, not per-call): `urnClass.getSimpleName().replace("Urn", "").toLowerCase()` — e.g., `DatasetUrn` → `"dataset"`, `CorpUserUrn` → `"corpuser"`. Zero extra dependencies.

### 1d. Add `wrapLocalAccess()` to EbeanLocalDAO
**Modify**: `dao-impl/ebean-dao/src/main/java/com/linkedin/metadata/dao/EbeanLocalDAO.java`

```java
public void wrapLocalAccess(Function<IEbeanLocalAccess<URN>, IEbeanLocalAccess<URN>> wrapper) {
  if (_localAccess != null) {
    _localAccess = wrapper.apply(_localAccess);
  }
}
```

Guards against `_localAccess == null` (OLD_SCHEMA_ONLY mode). Minimal change — avoids touching the 16+ constructors.

### 1e. Unit tests
- `NoOpDaoBenchmarkMetricsTest` — verifies no-op behavior
- `InstrumentedEbeanLocalAccessTest` — mocks `IEbeanLocalAccess` and `BaseDaoBenchmarkMetrics`, verifies delegation, latency recording, error recording, and disabled bypass
- `testWrapLocalAccess` added to existing `EbeanLocalDAOTest`

---

## Phase 2: Real Implementation in metadata-graph-assets

### 2a. Create `DropwizardDaoBenchmarkMetrics`
**New file**: `metadata-graph-assets-dao-factories/src/main/java/com/linkedin/metadata/factory/common/DropwizardDaoBenchmarkMetrics.java`

Implements `BaseDaoBenchmarkMetrics`. Delegates to `MetricsCollector` (from `metrics-collector-util`):
- `recordOperationLatency` → `collector.updateHistogramMetrics("dao.benchmark.<entity>.<op>.latency", ms)` + `collector.incrementCounterMetrics("dao.benchmark.<entity>.<op>.count", 1)`
- `recordOperationError` → `collector.incrementCounterMetrics("dao.benchmark.<entity>.<op>.error.<exception>", 1)`
- `isEnabled()` backed by `AtomicBoolean`, togglable at runtime

`BasicMetricsCollector.updateHistogramMetrics` uses Dropwizard `MetricRegistry` internally → auto-computes p50/p75/p95/p99/max.

### 2b. Create `DaoBenchmarkMetricsFactory` (Offspring)
**New file**: `metadata-graph-assets-dao-factories/src/main/java/com/linkedin/metadata/factory/common/DaoBenchmarkMetricsFactory.java`

1. Creates `BasicMetricsCollector`
2. Wraps in `InGraphsMetricsClient`
3. Registers with `SensorRegistry`
4. Returns `DropwizardDaoBenchmarkMetrics(collector, cfg.enabled)`

Config toggle: `enabled = false` by default (Offspring `@Config`).

### 2c. Add dependency
**Modify**: `metadata-graph-assets-dao-factories/build.gradle`

Add `metrics-collector-util` dependency (`SensorRegistryFactory` / `healthcheck` already available).

### 2d. Unit test
- `DropwizardDaoBenchmarkMetricsTest` — mock `MetricsCollector`, verify correct metric names and values

---

## Phase 3: Wire Through All 6 BaseLocalDaoFactory Variants

Each factory gets the same 2-line change before `return dao;`:

```java
@Import(clazz = DaoBenchmarkMetricsFactory.class)  // add to existing @Import list
// ...in createInstance():
final BaseDaoBenchmarkMetrics benchmarkMetrics = getBean(DaoBenchmarkMetricsFactory.class);
dao.wrapLocalAccess(la -> new InstrumentedEbeanLocalAccess<>(la, benchmarkMetrics, _urnClass));
```

Files modified (all in `metadata-graph-assets-dao-factories/src/main/java/com/linkedin/metadata/factory/`):
1. `datahub/BaseLocalDaoFactory.java`
2. `dataset/BaseLocalDaoFactory.java`
3. `aim/BaseLocalDaoFactory.java`
4. `identity/BaseLocalDaoFactory.java`
5. `service/BaseLocalDaoFactory.java`
6. `datajob/BaseLocalDaoFactory.java` (has 2 factory methods — wire both)

All 6 share a single `DaoBenchmarkMetricsFactory` singleton bean → one sensor, dynamic metric names.

---

## Phase 4: Deploy + Verify

1. **Deploy with `enabled=false`** (default) — zero overhead, zero risk
2. **Enable in EI** (`daoBenchmarkMetricsFactory.enabled=true`) — verify metrics appear in Observe under `DaoBenchmarkMetrics` sensor
3. **Enable in prod** (canary → full) — start passive MySQL baseline collection
4. **Create Observe dashboard**: latency panels (p50/p95/p99) by operation × entity type, throughput panels, error rate panels

Expected metric cardinality: ~30 entity types × 9 operations = ~270 metric names. Well within InGraphs limits.

---

## Change Summary

| Repo | File | Action |
|------|------|--------|
| datahub-gma | `dao-api/.../tracking/BaseDaoBenchmarkMetrics.java` | NEW |
| datahub-gma | `dao-api/.../tracking/NoOpDaoBenchmarkMetrics.java` | NEW |
| datahub-gma | `dao-impl/ebean-dao/.../InstrumentedEbeanLocalAccess.java` | NEW |
| datahub-gma | `dao-impl/ebean-dao/.../EbeanLocalDAO.java` | MODIFY (+12 lines) |
| datahub-gma | tests (3 files) | NEW |
| metadata-graph-assets | `.../factory/common/DropwizardDaoBenchmarkMetrics.java` | NEW |
| metadata-graph-assets | `.../factory/common/DaoBenchmarkMetricsFactory.java` | NEW |
| metadata-graph-assets | `metadata-graph-assets-dao-factories/build.gradle` | MODIFY (+1 dep) |
| metadata-graph-assets | 6 × `BaseLocalDaoFactory.java` | MODIFY (+3 lines each) |
| metadata-graph-assets | test (1 file) | NEW |

**Total**: 8 new files, 8 modified files. EbeanLocalAccess (the actual SQL execution layer) is untouched.

---

## Sequencing

datahub-gma changes must ship first (Phase 1) since metadata-graph-assets depends on the new interface. Then Phase 2+3 ship together in metadata-graph-assets. Phase 4 is config-only.
