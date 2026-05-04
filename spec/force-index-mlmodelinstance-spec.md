# Spec: FORCE INDEX hint for `MlModelInstanceAssetService.filter`

**Status:** datahub-gma PR implemented ([#616](https://github.com/linkedin/datahub-gma/pull/616)), MGA wiring pending.
**Owner:** MG team (oncall: `naulashchick`). **Last decision:** All other options (histograms, drop-foot-gun, invisible
index, Hikari `prefer_ordering_index=off`, alternative composite indexes) are off the table. FORCE INDEX in code is the
only remaining structural fix.

---

## 1. Background

`MlModelInstanceAssetService.filter` (gRPC, served by `metadata-graph-assets`) intermittently fails for the AIM team
with `Code: Unknown` empty-message errors, concentrated at `paging.count = 100` (97.8% of observed failures over a 48h
window). The query gets killed by MySQL's 5s threshold.

### Failing query shape

App-generated; cannot be modified at the call site:

```sql
SELECT urn FROM metadata_entity_mlmodelinstance
WHERE i_urn$model_urn IN (<~15 model URNs>)
  AND a_model_instance_status IS NOT NULL
  AND JSON_EXTRACT(a_model_instance_status, '$.gma_deleted') IS NULL
  AND i_model_instance_status$status = 'ACTIVE'
  AND deleted_ts IS NULL
ORDER BY urn LIMIT 100 OFFSET 0;
```

### Root cause

MySQL's `prefer_ordering_index=on` heuristic. At small `LIMIT`, the optimizer prefers a no-filesort plan that walks an
index already ordered by `urn` (PRIMARY, or formerly `idx2_model_instance_status$status`), expecting to find `LIMIT`
matching rows quickly. In practice the IN-list matches are sparsely distributed across urn-space, so the walk traverses
millions of rows and times out.

### Existing schema state on `metadata_entity_mlmodelinstance` (~1.47M rows, ~117 GB)

- `PRIMARY KEY (urn)` -- clustered.
- `idx_urn$model_urn (urn, i_urn$model_urn)` -- dead (urn never constrained).
- `idx2_urn$model_urn (i_urn$model_urn)` -- useful for the IN-list seek.
- `idx_urn$model_urn$status (i_urn$model_urn, i_model_instance_status$status)` -- composite added in
  [PR #882](https://github.com/linkedin-multiproduct/metadata-graph-assets/pull/882) (Flyway V24). Useful for the
  IN-list seek with status as second column.
- `idx_model_instance_status$status (urn, i_model_instance_status$status)` -- dead.
- `idx2_model_instance_status$status (i_model_instance_status$status)` -- formerly mis-picked at small `LIMIT`; dropped
  via [PR #898](https://github.com/linkedin-multiproduct/metadata-graph-assets/pull/898) (Flyway V26). After drop, the
  optimizer just shifted to PRIMARY scan with the same pathology.
- `idx_model_instance_info$trained_model_urn (i_model_instance_info$trained_model_urn)` -- for `findByTrainedModelUrn`,
  unrelated.

### Why FORCE INDEX

Only structural fix that:

- Doesn't require running heavy DDL on a 117 GB prod table.
- Doesn't require server-side stats/optimizer changes (which the MySQL SRE team declined).
- Is bounded to one entity (not a global flag flip).
- Is reversible without touching prod schema.

`FORCE INDEX` (not `USE INDEX`): hard directive -- optimizer must use the named index. `USE INDEX` is only a suggestion.

---

## 2. Solution overview

Add `configureOptionalForceIndex(indexName, requiredCriteria)` to `datahub-gma`. The FORCE INDEX hint is only emitted
when the `IndexFilter` contains criteria matching ALL configured (aspect, path) pairs -- ensuring the hint only fires
for the exact composite query shape it was designed for. MGA registers the configuration on the mlmodelinstance DAO
only. No other entity, no other code path affected.

### End state -- generated SQL

For mlmodelinstance filter (when filter contains both `model_urn` and `status` criteria):

```sql
SELECT urn FROM metadata_entity_mlmodelinstance FORCE INDEX (`idx_urn$model_urn$status`)
WHERE i_urn$model_urn IN (...) AND ... ORDER BY urn LIMIT 100 OFFSET 0;
```

Count query (auto-derived via `replaceFirst` from the same base SQL):

```sql
SELECT COUNT(urn) AS _total_count FROM metadata_entity_mlmodelinstance FORCE INDEX (`idx_urn$model_urn$status`)
WHERE i_urn$model_urn IN (...) AND ...;
```

For every other entity, every other query shape, or when the filter lacks the required criteria: SQL output unchanged.

---

## 3. Code changes

Two repos. `datahub-gma`: 5 production files + tests. `metadata-graph-assets`: 1 file.

### 3.1. `datahub-gma` -- `SQLStatementUtils.createFilterSql`

**Path:** `dao-impl/ebean-dao/src/main/java/com/linkedin/metadata/dao/utils/SQLStatementUtils.java`

**Change:** New 5-arg overload with `@Nullable String forceIndexName`. Existing 4-arg delegates with `null`.

```java
public static String createFilterSql(String entityType, @Nullable IndexFilter indexFilter,
    boolean nonDollarVirtualColumnsEnabled, @Nonnull SchemaValidatorUtil schemaValidator) {
  return createFilterSql(entityType, indexFilter, nonDollarVirtualColumnsEnabled, schemaValidator, null);
}

public static String createFilterSql(String entityType, @Nullable IndexFilter indexFilter,
    boolean nonDollarVirtualColumnsEnabled, @Nonnull SchemaValidatorUtil schemaValidator,
    @Nullable String forceIndexName) {
  final String tableName = getTableName(entityType);
  final String whereClause = parseIndexFilter(entityType, indexFilter, nonDollarVirtualColumnsEnabled, schemaValidator);
  final String forceIndex = (forceIndexName == null) ? "" : " FORCE INDEX (`" + forceIndexName + "`)";
  return "SELECT urn FROM " + tableName + forceIndex + "\n" + whereClause;
}
```

The FORCE INDEX clause sits between the table name and `\n<WHERE>`, so the existing
`replaceFirst("SELECT urn", "SELECT COUNT(urn) AS _total_count")` count-query rewrite is unaffected.

### 3.2. `datahub-gma` -- `IEbeanLocalAccess` interface

**Path:** `dao-impl/ebean-dao/src/main/java/com/linkedin/metadata/dao/IEbeanLocalAccess.java`

**Change:** Add `configureOptionalForceIndex` to the interface.

```java
void configureOptionalForceIndex(@Nullable String indexName,
    @Nullable Map<Class<? extends RecordTemplate>, String> requiredCriteria);
```

### 3.3. `datahub-gma` -- `EbeanLocalAccess`

**Path:** `dao-impl/ebean-dao/src/main/java/com/linkedin/metadata/dao/EbeanLocalAccess.java`

**Fields:**

```java
private String _forceIndexName;
private Map<String, String> _forceIndexRequiredCriteria;  // aspect FQCN -> path
```

**`configureOptionalForceIndex`** -- converts `Class<?>` keys to FQCN strings at config time for compile-time safety:

```java
@Override
public void configureOptionalForceIndex(@Nullable String indexName,
    @Nullable Map<Class<? extends RecordTemplate>, String> requiredCriteria) {
  _forceIndexName = indexName;
  if (requiredCriteria != null) {
    _forceIndexRequiredCriteria = requiredCriteria.entrySet().stream()
        .collect(Collectors.toMap(e -> e.getKey().getCanonicalName(), Map.Entry::getValue));
  } else {
    _forceIndexRequiredCriteria = null;
  }
}
```

**`resolveForceIndex`** -- returns the index name only when ALL (aspect, path) pairs match the filter criteria:

```java
@Nullable
private String resolveForceIndex(@Nullable IndexFilter indexFilter) {
  if (_forceIndexName == null || _forceIndexRequiredCriteria == null || _forceIndexRequiredCriteria.isEmpty()) {
    return null;
  }
  if (indexFilter == null || !indexFilter.hasCriteria()) {
    return null;
  }
  boolean allMatch = _forceIndexRequiredCriteria.entrySet().stream().allMatch(required ->
      indexFilter.getCriteria().stream().anyMatch(c ->
          required.getKey().equals(c.getAspect())
              && c.hasPathParams()
              && required.getValue().equals(c.getPathParams().getPath())));
  return allMatch ? _forceIndexName : null;
}
```

**`validateForceIndex`** -- called during `ensureSchemaUpToDate()`. Uses `SchemaValidatorUtil.indexExists()` (cached).
If the index is missing, logs ERROR and auto-disables the hint instead of crashing:

```java
void validateForceIndex() {
  if (_forceIndexName == null) {
    return;
  }
  String tableName = getTableName(_entityType);
  if (!validator.indexExists(tableName, _forceIndexName)) {
    log.error("Configured forceIndexName '{}' does not exist on table '{}'. "
        + "Disabling FORCE INDEX hint to prevent query failures.", _forceIndexName, tableName);
    _forceIndexName = null;
    _forceIndexRequiredCriteria = null;
  }
}
```

**Offset-pagination `listUrns`** -- the only query path that receives the hint:

```java
@Override
public ListResult<URN> listUrns(@Nullable IndexFilter indexFilter,
    @Nullable IndexSortCriterion indexSortCriterion, int start, int pageSize) {
  // When configured, emit FORCE INDEX to override MySQL's prefer_ordering_index heuristic
  // which can pick a full-table PRIMARY scan instead of the composite index at small LIMITs.
  final String effectiveForceIndex = resolveForceIndex(indexFilter);
  final String baseSql = SQLStatementUtils.createFilterSql(_entityType, indexFilter,
      _nonDollarVirtualColumnsEnabled, validator, effectiveForceIndex);
  // Run COUNT in a separate query so neither query exceeds the 5s kill threshold.
  final String countSql = baseSql.replaceFirst("SELECT urn", "SELECT COUNT(urn) AS _total_count");
  // ... rest unchanged ...
}
```

The keyset-pagination `listUrns(IndexFilter, IndexSortCriterion, URN lastUrn, int pageSize)` is NOT modified.

### 3.4. `datahub-gma` -- `InstrumentedEbeanLocalAccess`

**Path:** `dao-impl/ebean-dao/src/main/java/com/linkedin/metadata/dao/InstrumentedEbeanLocalAccess.java`

Delegates `configureOptionalForceIndex` to the wrapped `_delegate`.

### 3.5. `datahub-gma` -- `EbeanLocalDAO`

**Path:** `dao-impl/ebean-dao/src/main/java/com/linkedin/metadata/dao/EbeanLocalDAO.java`

Public `configureOptionalForceIndex` that delegates to `_localAccess` when non-null (NEW_SCHEMA mode). No constructor
changes.

### 3.6. `metadata-graph-assets` -- wire the hint for mlmodelinstance only

**Path:**
`metadata-graph-assets-dao-factories/src/main/java/com/linkedin/metadata/factory/aim/MlModelInstanceLocalDaoFactory.java`

```diff
   @Override
   protected EbeanLocalDAO<InternalMlModelInstanceAspect, MlModelInstanceUrn> createInstance(@Nonnull ConfigView view) {
     EbeanLocalDAO<InternalMlModelInstanceAspect, MlModelInstanceUrn> dao = super.createInstance(view);
     dao.setUrnPathExtractor(new MlModelInstanceUrnPathExtractor());
     dao.setUseAspectColumnForRelationshipRemoval(true);
+    Map<Class<? extends RecordTemplate>, String> forceIndexCriteria = new LinkedHashMap<>();
+    forceIndexCriteria.put(MlModelInstanceUrn.class, "/model_urn");
+    forceIndexCriteria.put(MlModelInstanceStatus.class, "/status");
+    dao.configureOptionalForceIndex("idx_urn$model_urn$status", forceIndexCriteria);
     return dao;
   }
```

**Note:** The path values (`"/model_urn"`, `"/status"`) must match the `IndexCriterion.pathParams.path` format sent by
the gRPC client. The PDL spec for `IndexPathParams.path` documents the leading-`/` convention (e.g., `"/removed"`).
Verify the actual runtime values before the MGA PR.

---

## 4. Tests

### 4.1. `datahub-gma` unit tests

**File:** `dao-impl/ebean-dao/src/test/java/com/linkedin/metadata/dao/utils/SQLStatementUtilsTest.java`

- `testCreateFilterSqlWithForceIndex` -- null produces no hint (regression); non-null emits `FORCE INDEX`.
- `testForceIndexCountQueryRewriteCompatibility` -- `replaceFirst` count rewrite preserves the FORCE INDEX clause.

### 4.2. `datahub-gma` integration tests

**File:** `dao-impl/ebean-dao/src/test/java/com/linkedin/metadata/dao/EbeanLocalAccessTest.java`

Tests run against embedded MariaDB:

- `testListUrnsWithOffsetAndForceIndex` -- FORCE INDEX activates when filter matches required (aspect, path) criteria.
- `testForceIndexNotAppliedWhenFilterLacksRequiredAspect` -- wrong aspect in config, hint does not activate.
- `testForceIndexNotAppliedWhenFilterLacksRequiredPath` -- right aspect but wrong path, hint does not activate.
- `testListUrnsWithLastUrnIgnoresForceIndex` -- keyset-pagination path is unaffected.
- `testListUrnsWithOffsetAndNullForceIndex` -- null config produces default behavior.
- `testForceIndexNotAppliedWhenFilterIsNull` -- null IndexFilter, hint does not activate.
- `testValidateForceIndexDisablesHintWhenIndexMissing` -- non-existent index auto-disabled at validation.
- `testValidateForceIndexKeepsHintWhenIndexExists` -- PRIMARY index passes validation.
- `testValidateForceIndexNoOpWhenNotConfigured` -- null config, validation is a no-op.

**File:** `dao-impl/ebean-dao/src/test/java/com/linkedin/metadata/dao/InstrumentedEbeanLocalAccessTest.java`

- `testConfigureOptionalForceIndexDelegates` -- verifies delegation to wrapped implementation.

---

## 5. Release sequence

1. **datahub-gma PR** ([#616](https://github.com/linkedin/datahub-gma/pull/616)): all code + tests. Reviewed and merged.
2. **datahub-gma release:** wait for the next version cut.
3. **metadata-graph-assets PR:** bump datahub-gma dependency, add `configureOptionalForceIndex(...)` in
   `MlModelInstanceLocalDaoFactory`. Reviewed and merged.
4. **Deploy MGA to canary fabric** -- recommend `prod-lor1` since AIM-team failures concentrate there.
5. **Verify on canary** (see SS7).
6. **Roll to remaining prod fabrics** (ltx1, lva1).
7. **Coordinate with AIM team** to remove the `count == 100 -> 1000` clamp in mlops-midtier.

---

## 6. Pre-conditions

- V24 composite index `idx_urn$model_urn$status` already exists in all prod fabrics (deployed via
  [PR #882](https://github.com/linkedin-multiproduct/metadata-graph-assets/pull/882)). If missing, the startup
  validation auto-disables the hint and logs an ERROR (queries degrade to default plan, not crash).
- The `idx2_model_instance_status$status` drop
  ([PR #898](https://github.com/linkedin-multiproduct/metadata-graph-assets/pull/898)) is independent -- FORCE INDEX
  makes the optimizer's mis-pick irrelevant regardless.

---

## 7. Verification on canary fabric

After deploying the patched MGA to one prod fabric:

1. **Confirm the SQL the app emits.** Enable MGA's slow-query log or add a debug log in `EbeanLocalAccess.listUrns`.
   Confirm it contains `` FORCE INDEX (`idx_urn$model_urn$status`) `` for an mlmodelinstance filter call that includes
   both `model_urn` and `status` criteria.
2. **Run the failing query directly against MySQL** at `LIMIT 100` and verify:
   - `EXPLAIN` shows `key = idx_urn$model_urn$status`, `rows` in the low thousands.
   - Wall-clock completes in < 500 ms (preferably < 100 ms).
3. **Watch the gRPC error rate dashboard for the patched fabric.** Expect: error rate on the patched fabric drops to
   baseline within minutes.
4. **Confirm no regression on other entities** by sampling non-mlmodelinstance filter calls -- their SQL must not
   contain FORCE INDEX.

---

## 8. Risks & mitigations

| Risk                                                                             | Mitigation                                                                                                                    |
| -------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| Index renamed or dropped without updating MGA config                             | Startup validation auto-disables the hint and logs ERROR. Queries degrade to default plan instead of failing.                 |
| MariaDB-embedded tests parse FORCE INDEX differently than prod MySQL 8.0         | Verified in CI -- embedded MariaDB accepts the syntax.                                                                        |
| Future TiDB migration: TiDB accepts FORCE INDEX syntax but plan semantics differ | Per-entity opt-in means TiDB-backed entities can be left unconfigured.                                                        |
| Other gma-using MPs accidentally adopt the hint                                  | Javadoc on `configureOptionalForceIndex` notes it's per-asset opt-in for known pathological queries. Default null.            |
| Filter criteria path format mismatch (with/without leading `/`)                  | Verify actual runtime `IndexCriterion.pathParams.path` values before the MGA PR. Consider path normalization if formats vary. |

---

## 9. Rollback

Two levels:

1. **Per-fabric (fast):** revert the MGA wiring commit (`configureOptionalForceIndex` call in
   `MlModelInstanceLocalDaoFactory.java`) and redeploy. The datahub-gma library change stays in place, dormant. Single
   MGA deploy cycle (~1h).
2. **Full revert:** revert both the MGA PR and the datahub-gma PR. No DDL state to reconcile.

---

## 10. Acceptance criteria

- `EXPLAIN` of the failing query at `LIMIT 100` in prod shows `key = idx_urn$model_urn$status`, `rows` in the low
  thousands, completes in < 500 ms.
- `MlModelInstanceAssetService.filter` `Code: Unknown` error rate drops to baseline on each rolled-out fabric.
- AIM team's mlops-midtier `count == 100 -> 1000` clamp can be removed without re-introducing failures.
- No regression in any non-mlmodelinstance filter query (no FORCE INDEX in sampled SQL; error rates unchanged).
- Filter queries that lack the required (aspect, path) criteria do NOT receive the FORCE INDEX hint.

---

## 11. Reference materials

- Composite-index PR (V24): [PR #882](https://github.com/linkedin-multiproduct/metadata-graph-assets/pull/882)
- Foot-gun-drop PR (V26): [PR #898](https://github.com/linkedin-multiproduct/metadata-graph-assets/pull/898)
- datahub-gma implementation PR: [PR #616](https://github.com/linkedin/datahub-gma/pull/616)
- LinkedIn MySQL fork version: `8.0.28-li.1`
