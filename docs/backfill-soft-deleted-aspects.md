# Backfill Logic for Soft-Deleted Aspects

## Overview

GMA's backfill path now fully accounts for soft-deleted aspects. Previously, the backfill logic only distinguished
between "aspect exists" and "aspect does not exist." A soft-deleted aspect appeared identical to one that had never been
written, so backfill events would unconditionally overwrite the soft-delete marker and silently resurrect deleted data.
The updated logic treats soft-deleted aspects as a distinct state with its own staleness rules.

## How Backfill Decisions Work

When a backfill event arrives, `BaseLocalDAO.shouldBackfill()` evaluates the current state of the aspect through a
series of checks:

1. **Aspect has never existed** — If there is no value and the aspect is not soft-deleted, the backfill is accepted
   unconditionally. This is the only scenario where emitTime is not required, since there is nothing to compare against.

2. **No emitTime on the backfill event** — All remaining scenarios (soft-deleted or existing value) require the backfill
   event to carry an `emitTime` for staleness comparison. If emitTime is absent, the backfill is rejected to avoid
   blindly overwriting current state.

3. **Aspect is soft-deleted** — The backfill's emitTime is compared against the per-aspect deletion timestamp (stored in
   `SoftDeletedAspect.deleted_timestamp` and surfaced via `oldAuditStamp.getTime()`). The comparison uses strict
   greater-than (`>`): an event at the exact same millisecond as the deletion is treated as stale, since it was likely
   in-flight when the delete occurred. If the deletion timestamp is missing or invalid, the backfill is rejected with a
   warning log.

4. **Aspect exists with a current value** — Standard staleness check. The backfill's emitTime is compared against the
   existing event's emitTime (preferred) or, if unavailable, the aspect's last-modified audit timestamp as a fallback.

## Enriched Soft-Delete Metadata

To support per-aspect staleness comparisons, soft-delete markers now carry audit metadata beyond the original
`{"gma_deleted": true}` flag:

- `deleted_timestamp` (long, epoch millis) — when the aspect was soft-deleted
- `deleted_by` (string) — who deleted the aspect (corpuser URN or "backfill")

This is modeled via `SoftDeletedAspect.pdl` and serialized as JSON in the aspect column. The enriched format eliminates
the previous `long → String → Timestamp → long` conversion chain by storing the deletion timestamp directly as epoch
millis.

## Backward Compatibility

The system handles both legacy and enriched soft-delete formats transparently:

- **Legacy format** (`{"gma_deleted": true}`) — Still recognized as soft-deleted. When reading legacy rows, the system
  falls back to the entity-level `lastmodifiedon` column for the deletion timestamp and `lastmodifiedby` for the actor.
- **Enriched format**
  (`{"gma_deleted": true, "deleted_timestamp": 1741286519000, "deleted_by": "urn:li:corpuser:actor"}`) — Per-aspect
  deletion metadata is used directly.

No database migration is required. New soft-delete writes produce the enriched format, while reads handle both formats.

## Version History Preservation

When a backfill event passes the staleness check and overwrites a soft-deleted aspect, the soft-delete marker is saved
as a historical version via `saveLatest`. This preserves the full audit trail — the version history shows the deletion
event followed by the backfill restoration, rather than the deletion being silently lost.

## Key Files

- `BaseLocalDAO.java` — `shouldBackfill()` with the five-step decision logic described above
- `EBeanDAOUtils.java` — `buildDeletedValue()` for writing enriched markers, `isSoftDeletedMetadata()` for detection
- `EbeanLocalAccess.java` — Writes enriched soft-delete JSON when an aspect is deleted
- `SoftDeletedAspect.pdl` — PDL schema for the soft-delete marker with audit fields
- `AspectEntry.java` — Carries `isSoftDeleted` state through the DAO layer
