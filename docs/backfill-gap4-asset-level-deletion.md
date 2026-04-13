# Gap 4: Asset-Level Deletion Invisible to Backfill Staleness Checks

## Problem

When an entire entity is deleted via `softDeleteAsset()`, the entity row's `deleted_ts` column is set to `NOW()` and all
aspect columns are marked with `{"gma_deleted": true}`. The row becomes invisible to read queries because
`batchGetUnion()` unconditionally filters `deleted_ts IS NULL` — even when `includeSoftDeleted=true`.

When a stale backfill event (e.g., DLQ replay) arrives for an aspect on this deleted entity:

1. `getLatest()` calls `batchGetUnion(..., includeSoftDeleted=true)` to fetch the current state
2. `batchGetUnion()` generates SQL via `createAspectReadSql()`
3. **Bug**: `createAspectReadSql()` unconditionally appends `AND deleted_ts IS NULL` regardless of the
   `includeSoftDeleted` parameter
4. The deleted row is filtered out, query returns empty
5. `getLatest()` returns `AspectEntry(null, null)` — null aspect, `isSoftDeleted=false`
6. `shouldBackfill()` sees `oldValue == null && !isSoftDeleted` → "aspect never existed" → allows backfill
7. The write resets `deleted_ts` to NULL, **resurrecting the deleted entity** with stale data

Note: Even though `deleteAll()` also sets aspect-level `{"gma_deleted": true}` markers (which PR #602's Gap 1 fix would
catch), those markers are never reached because the entire row is invisible at the SQL level.

## Root Cause

**File**: `SQLStatementUtils.java`, method `createAspectReadSql()`

```java
stringBuilder.append(urnList);
stringBuilder.append(RIGHT_PARENTHESIS);
stringBuilder.append(" AND ");
stringBuilder.append(DELETED_TS_IS_NULL_CHECK);  // Always appended — ignores includeSoftDeleted!
```

## Fix

**One change in `SQLStatementUtils.createAspectReadSql()`**: make `AND deleted_ts IS NULL` conditional on
`!includeSoftDeleted`:

```java
stringBuilder.append(urnList);
stringBuilder.append(RIGHT_PARENTHESIS);
if (!includeSoftDeleted) {
    stringBuilder.append(" AND ");
    stringBuilder.append(DELETED_TS_IS_NULL_CHECK);
}
```

This matches the pattern already used correctly in `createListAspectByUrnSql()`.

### Generated SQL (after fix)

```sql
-- includeSoftDeleted=false (normal reads — unchanged)
SELECT urn, a_aspectfoo, lastmodifiedon, lastmodifiedby
FROM metadata_entity_foo
WHERE JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL
AND urn IN ('urn:li:foo:1')
AND deleted_ts IS NULL

-- includeSoftDeleted=true (backfill path — now includes deleted rows)
SELECT urn, a_aspectfoo, lastmodifiedon, lastmodifiedby
FROM metadata_entity_foo
WHERE urn IN ('urn:li:foo:1')
```

## How This Solves the Issue

After the fix, when a stale backfill arrives for an asset-deleted entity:

1. `getLatest()` calls `batchGetUnion(..., includeSoftDeleted=true)`
2. `createAspectReadSql()` generates SQL **without** `deleted_ts IS NULL` filter
3. The deleted row is returned — if `deleteAll()` was used, aspects have `{"gma_deleted": true}` markers which the
   existing aspect-level soft-delete detection in `readSqlRow()` handles
4. If `softDeleteAsset()` was called directly (without aspect markers), `readSqlRow()` detects `deleted_ts` is non-null
   and marks the aspect as soft-deleted using `deleted_ts` as the deletion timestamp
5. `getLatest()` returns `AspectEntry(null, extraInfo, isSoftDeleted=true)`
6. `shouldBackfill()` (from PR #602) compares `emitTime > deletionTimestamp` → stale backfill rejected

Additionally, all UPDATE SQL templates now include `deleted_ts = NULL` so that any successful write (including via the
optimistic locking / changelog path in `saveLatest()`) properly revives an asset-deleted entity. Without this, the
`saveLatest()` changelog path would write the aspect but leave `deleted_ts` set, making the entity invisible to
subsequent reads.

## Files Changed

| File                                                                    | Change                                                                                           |
| ----------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------ |
| `dao-impl/ebean-dao/src/main/java/.../utils/SQLStatementUtils.java`     | Conditional `deleted_ts IS NULL` filter + `deleted_ts` in SELECT + `deleted_ts = NULL` in UPDATE |
| `dao-impl/ebean-dao/src/main/java/.../utils/EBeanDAOUtils.java`         | Handle asset-level deletion in `readSqlRow()` when `deleted_ts` is non-null                      |
| `dao-impl/ebean-dao/src/test/java/.../dao/EbeanLocalAccessTest.java`    | New test: asset-deleted entity visible with `includeSoftDeleted=true`                            |
| `dao-impl/ebean-dao/src/test/java/.../utils/SQLStatementUtilsTest.java` | Update expected SQL for `includeSoftDeleted=true` and optimistic lock UPDATE                     |

## Relationship to Other Gaps

| Gap       | What                                                   | Status           |
| --------- | ------------------------------------------------------ | ---------------- |
| Gap 1     | `shouldBackfill()` blind to soft-deletes               | Fixed in PR #602 |
| Gap 2     | Missing per-aspect deletion timestamp                  | Fixed in PR #602 |
| Gap 3     | Old schema loses emitTime on deletion                  | Fixed by Gap 2   |
| **Gap 4** | **Asset-level deletion invisible to staleness checks** | **This PR**      |

Gap 4 depends on PR #602: once the row is visible (Gap 4 fix), the aspect-level `{"gma_deleted": true}` markers are
detected by `readSqlRow()`, and PR #602's `shouldBackfill()` logic rejects stale backfills.

## EI Verification (April 13, 2026)

**Entity:** `urn:li:demo:rakagraw_gap4_test_004` **Environment:** ei-ltx1 (mg_db_1_ei)

Confirmed the bug on current EI (without PR #602 or #612):

1. Created entity with Status + DemoInfo
2. Asset-level delete (`delete` with empty aspectTypes) — `deleted_ts` set, both aspects marked `{"gma_deleted": true}`
3. GET returns NOT FOUND (correct)
4. Stale backfill (emitTime=1000, before deletion) — **entity resurrected**:
   - `deleted_ts` reset to NULL
   - `a_demo_info` overwritten with stale data
   - `a_status` still `{"gma_deleted": true}` (only backfilled aspect resurrected)

Full test results:
[Backfill Gap Fix Verification Results](https://docs.google.com/document/d/17XNuwhBBanPuT5w5Bo-40ozN5bhTSSqyHxSTYaiXybs/edit)
