package com.linkedin.metadata.dao;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.tracking.BaseDaoUsageEmitter;
import com.linkedin.metadata.dao.tracking.DaoReadContext;
import com.linkedin.metadata.dao.tracking.DaoUsageTarget;
import com.linkedin.metadata.dao.urnpath.UrnPathExtractor;
import com.linkedin.metadata.events.IngestionTrackingContext;
import com.linkedin.metadata.query.IndexFilter;
import com.linkedin.metadata.query.IndexGroupByCriterion;
import com.linkedin.metadata.query.IndexSortCriterion;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;


/**
 * A decorator around {@link IEbeanLocalAccess} that emits one usage event per successful
 * read / write / delete via a {@link BaseDaoUsageEmitter}. Wrapping the model-agnostic access
 * layer lets a single instance capture usage for every {@link EbeanLocalDAO} consumer with no
 * per-service code.
 *
 * <p>Fire-and-forget: each method delegates first and returns/throws exactly what the delegate
 * does; emission happens only after success and is wrapped so it can never alter the result or
 * throw. Writes and deletes emit only when the delegate reports at least one affected row, so a
 * lost optimistic-locking race is not reported as a write. It short-circuits with zero overhead
 * when the emitter is disabled, and skips test-mode and backfill. Reads and writes/deletes are
 * captured; global scans, discovery and maintenance are delegated without emission. Writes carry
 * the caller from the {@link AuditStamp}; reads have none.
 *
 * <p>All captured reads honor {@link DaoReadContext}: a read taken inside a marked scope is not
 * attributable to a consumer (read-before-write, backfill) and is skipped, so the rule is the same
 * for every read method rather than special-cased per method.
 *
 * <p>The entity type is taken from {@link Urn#getEntityType()} on the URN being operated on, so it
 * matches the canonical entity type used elsewhere (e.g. {@code mlModel}) rather than a value
 * derived from the URN class name.
 *
 * @param <URN> the URN type for this entity
 */
@Slf4j
public class UsageTrackingEbeanLocalAccess<URN extends Urn> implements IEbeanLocalAccess<URN> {

  static final String OP_READ = "READ";
  static final String OP_WRITE = "WRITE";
  static final String OP_DELETE = "DELETE";
  static final String OP_DELETE_ALL = "DELETE_ALL";

  private final IEbeanLocalAccess<URN> _delegate;
  private final BaseDaoUsageEmitter _usageEmitter;

  /**
   * Creates a usage-tracking wrapper around the given local-access implementation.
   *
   * @param delegate     the real local-access implementation to wrap
   * @param usageEmitter the usage emitter (may be a no-op)
   */
  public UsageTrackingEbeanLocalAccess(@Nonnull IEbeanLocalAccess<URN> delegate,
      @Nonnull BaseDaoUsageEmitter usageEmitter) {
    _delegate = delegate;
    _usageEmitter = usageEmitter;
  }

  // ---------------------------------------------------------------------------------------
  // Pass-through (not usage-relevant): configuration / discovery / maintenance / admin
  // ---------------------------------------------------------------------------------------

  @Override
  public void setUrnPathExtractor(@Nonnull UrnPathExtractor<URN> urnPathExtractor) {
    _delegate.setUrnPathExtractor(urnPathExtractor);
  }

  @Override
  public void configureOptionalForceIndex(@Nullable String indexName,
      @Nullable Map<Class<?>, String> requiredCriteria) {
    _delegate.configureOptionalForceIndex(indexName, requiredCriteria);
  }

  @Override
  public Map<URN, EntityDeletionInfo> readDeletionInfoBatch(@Nonnull List<URN> urns,
      boolean isTestMode) {
    return _delegate.readDeletionInfoBatch(urns, isTestMode);
  }

  @Override
  public int batchSoftDeleteAssets(@Nonnull List<URN> urns, @Nonnull String cutoffTimestamp,
      boolean isTestMode) {
    return _delegate.batchSoftDeleteAssets(urns, cutoffTimestamp, isTestMode);
  }

  @Override
  public List<URN> listUrns(@Nullable IndexFilter indexFilter,
      @Nullable IndexSortCriterion indexSortCriterion, @Nullable URN lastUrn, int pageSize) {
    return _delegate.listUrns(indexFilter, indexSortCriterion, lastUrn, pageSize);
  }

  @Override
  public ListResult<URN> listUrns(@Nullable IndexFilter indexFilter,
      @Nullable IndexSortCriterion indexSortCriterion, int start, int pageSize) {
    return _delegate.listUrns(indexFilter, indexSortCriterion, start, pageSize);
  }

  @Override
  public boolean exists(@Nonnull URN urn) {
    return _delegate.exists(urn);
  }

  @Nonnull
  @Override
  public Map<String, Long> countAggregate(@Nullable IndexFilter indexFilter,
      @Nonnull IndexGroupByCriterion indexGroupByCriterion) {
    return _delegate.countAggregate(indexFilter, indexGroupByCriterion);
  }

  @Nonnull
  @Override
  public <ASPECT extends RecordTemplate> ListResult<URN> listUrns(@Nonnull Class<ASPECT> aspectClass,
      int start, int pageSize) {
    return _delegate.listUrns(aspectClass, start, pageSize);
  }

  @Nonnull
  @Override
  public <ASPECT extends RecordTemplate> ListResult<ASPECT> list(@Nonnull Class<ASPECT> aspectClass,
      int start, int pageSize) {
    // Global cross-URN scan -- no single-entity target, not captured as usage.
    return _delegate.list(aspectClass, start, pageSize);
  }

  @Override
  public void ensureSchemaUpToDate() {
    _delegate.ensureSchemaUpToDate();
  }

  // ---------------------------------------------------------------------------------------
  // Reads
  // ---------------------------------------------------------------------------------------

  @Nonnull
  @Override
  public <ASPECT extends RecordTemplate> List<EbeanMetadataAspect> batchGetUnion(
      @Nonnull List<AspectKey<URN, ? extends RecordTemplate>> keys, int keysCount, int position,
      boolean includeSoftDeleted, boolean isTestMode) {
    final List<EbeanMetadataAspect> result =
        _delegate.batchGetUnion(keys, keysCount, position, includeSoftDeleted, isTestMode);
    if (_usageEmitter.isEnabled() && !isTestMode && !DaoReadContext.isInternalRead()) {
      safeEmit(OP_READ, "batchGetUnion", null, () -> entityTypeFromKeys(keys, keysCount, position),
          () -> targetsFromKeys(keys, keysCount, position));
    }
    return result;
  }

  @Nonnull
  @Override
  public <ASPECT extends RecordTemplate> ListResult<ASPECT> list(@Nonnull Class<ASPECT> aspectClass,
      @Nonnull URN urn, int start, int pageSize) {
    final ListResult<ASPECT> result = _delegate.list(aspectClass, urn, start, pageSize);
    if (_usageEmitter.isEnabled() && !DaoReadContext.isInternalRead()) {
      safeEmit(OP_READ, "list", null, urn::getEntityType,
          () -> singleTarget(urn, aspectClass.getSimpleName()));
    }
    return result;
  }

  // ---------------------------------------------------------------------------------------
  // Writes / deletes
  // ---------------------------------------------------------------------------------------

  @Override
  public <ASPECT extends RecordTemplate> int add(@Nonnull URN urn, @Nullable ASPECT newValue,
      @Nonnull Class<ASPECT> aspectClass, @Nonnull AuditStamp auditStamp,
      @Nullable IngestionTrackingContext ingestionTrackingContext, boolean isTestMode) {
    final int result =
        _delegate.add(urn, newValue, aspectClass, auditStamp, ingestionTrackingContext, isTestMode);
    if (result > 0 && _usageEmitter.isEnabled() && !isTestMode && !isBackfill(ingestionTrackingContext)) {
      safeEmit(newValue != null ? OP_WRITE : OP_DELETE, "add", auditStamp, urn::getEntityType,
          () -> singleTarget(urn, aspectClass.getSimpleName()));
    }
    return result;
  }

  @Override
  public <ASPECT extends RecordTemplate> int addWithOptimisticLocking(@Nonnull URN urn,
      @Nullable ASPECT newValue, @Nonnull Class<ASPECT> aspectClass, @Nonnull AuditStamp auditStamp,
      @Nullable Timestamp oldTimestamp, @Nullable IngestionTrackingContext ingestionTrackingContext,
      boolean isTestMode, boolean softDeleteOverwrite) {
    final int result = _delegate.addWithOptimisticLocking(urn, newValue, aspectClass, auditStamp,
        oldTimestamp, ingestionTrackingContext, isTestMode, softDeleteOverwrite);
    if (result > 0 && _usageEmitter.isEnabled() && !isTestMode && !isBackfill(ingestionTrackingContext)) {
      safeEmit(newValue != null ? OP_WRITE : OP_DELETE, "addWithOptimisticLocking",
          auditStamp, urn::getEntityType, () -> singleTarget(urn, aspectClass.getSimpleName()));
    }
    return result;
  }

  @Override
  public <ASPECT_UNION extends RecordTemplate> int create(@Nonnull URN urn,
      @Nonnull List<? extends RecordTemplate> aspectValues,
      @Nonnull List<BaseLocalDAO.AspectCreateLambda<? extends RecordTemplate>> aspectCreateLambdas,
      @Nonnull AuditStamp auditStamp, @Nullable IngestionTrackingContext ingestionTrackingContext,
      boolean isTestMode) {
    final int result = _delegate.create(urn, aspectValues, aspectCreateLambdas, auditStamp,
        ingestionTrackingContext, isTestMode);
    if (result > 0 && _usageEmitter.isEnabled() && !isTestMode && !isBackfill(ingestionTrackingContext)) {
      safeEmit(OP_WRITE, "create", auditStamp, urn::getEntityType,
          () -> singleTarget(urn, aspectSimpleNames(aspectValues)));
    }
    return result;
  }

  @Override
  public <ASPECT_UNION extends RecordTemplate> int batchUpsert(@Nonnull URN urn,
      @Nonnull List<BaseLocalDAO.AspectUpdateContext<RecordTemplate>> updateContexts,
      @Nonnull AuditStamp auditStamp, @Nullable IngestionTrackingContext ingestionTrackingContext,
      boolean isTestMode) {
    final int result =
        _delegate.batchUpsert(urn, updateContexts, auditStamp, ingestionTrackingContext, isTestMode);
    if (result > 0 && _usageEmitter.isEnabled() && !isTestMode && !isBackfill(ingestionTrackingContext)) {
      safeEmit(OP_WRITE, "batchUpsert", auditStamp, urn::getEntityType,
          () -> singleTarget(urn, aspectNamesFromContexts(updateContexts)));
    }
    return result;
  }

  @Override
  public int softDeleteAsset(@Nonnull URN urn, boolean isTestMode) {
    final int result = _delegate.softDeleteAsset(urn, isTestMode);
    if (result > 0 && _usageEmitter.isEnabled() && !isTestMode) {
      // Whole-entity delete: no audit stamp on this path (actor null), empty aspect list.
      safeEmit(OP_DELETE_ALL, "softDeleteAsset", null, urn::getEntityType,
          () -> Collections.singletonList(
              new DaoUsageTarget(urn.toString(), Collections.emptyList())));
    }
    return result;
  }

  // ---------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------

  /**
   * Builds the targets and emits, swallowing any exception so usage capture can never affect
   * the DAO call. The caller is derived from the audit stamp inside the try block so a
   * malformed stamp cannot propagate into the DAO call path. Assumes
   * {@link BaseDaoUsageEmitter#isEnabled()} has already been checked.
   */
  private void safeEmit(@Nonnull String operationType, @Nonnull String sourceOperation,
      @Nullable AuditStamp auditStamp, @Nonnull Supplier<String> entityTypeSupplier,
      @Nonnull Supplier<List<DaoUsageTarget>> targetsSupplier) {
    try {
      final List<DaoUsageTarget> targets = targetsSupplier.get();
      if (targets.isEmpty()) {
        return;
      }
      final String entityType = entityTypeSupplier.get();
      if (entityType == null) {
        return;
      }
      final String actorUrn = auditStamp == null ? null : actorOf(auditStamp);
      final String impersonatorUrn = auditStamp == null ? null : impersonatorOf(auditStamp);
      _usageEmitter.emit(operationType, entityType, sourceOperation, actorUrn, impersonatorUrn,
          targets);
    } catch (Exception e) {
      // Fire-and-forget: never propagate an emission failure to the caller.
      log.warn("Failed to emit usage event for {} {}", operationType, sourceOperation, e);
    }
  }

  /**
   * Entity type for a read, taken from the first key in the window the delegate actually read.
   * Returns {@code null} when the window is empty, in which case there is nothing to report.
   */
  @Nullable
  private static <URN extends Urn> String entityTypeFromKeys(
      @Nonnull List<AspectKey<URN, ? extends RecordTemplate>> keys, int keysCount, int position) {
    final int start = Math.max(0, position);
    final int end = Math.min(keys.size(), start + Math.max(0, keysCount));
    return start < end ? keys.get(start).getUrn().getEntityType() : null;
  }

  private static boolean isBackfill(@Nullable IngestionTrackingContext ingestionTrackingContext) {
    return ingestionTrackingContext != null && ingestionTrackingContext.isBackfill();
  }

  @Nonnull
  private static String actorOf(@Nonnull AuditStamp auditStamp) {
    return auditStamp.getActor().toString();
  }

  @Nullable
  private static String impersonatorOf(@Nonnull AuditStamp auditStamp) {
    return auditStamp.hasImpersonator() ? auditStamp.getImpersonator().toString() : null;
  }

  @Nonnull
  private List<DaoUsageTarget> singleTarget(@Nonnull URN urn, @Nonnull String aspectSimpleName) {
    return Collections.singletonList(
        new DaoUsageTarget(urn.toString(), Collections.singletonList(aspectSimpleName)));
  }

  @Nonnull
  private List<DaoUsageTarget> singleTarget(@Nonnull URN urn, @Nonnull List<String> aspectNames) {
    return Collections.singletonList(new DaoUsageTarget(urn.toString(), aspectNames));
  }

  @Nonnull
  private static List<String> aspectSimpleNames(
      @Nonnull List<? extends RecordTemplate> aspectValues) {
    final List<String> names = new ArrayList<>(aspectValues.size());
    for (RecordTemplate aspect : aspectValues) {
      names.add(aspect.getClass().getSimpleName());
    }
    return names;
  }

  @Nonnull
  private static List<String> aspectNamesFromContexts(
      @Nonnull List<BaseLocalDAO.AspectUpdateContext<RecordTemplate>> updateContexts) {
    final List<String> names = new ArrayList<>(updateContexts.size());
    for (BaseLocalDAO.AspectUpdateContext<RecordTemplate> context : updateContexts) {
      names.add(context.getLambda().getAspectClass().getSimpleName());
    }
    return names;
  }

  /**
   * Groups the read keys by URN, collecting the aspect simple names per URN so a multi-URN
   * {@code batchGetUnion} produces one {@link DaoUsageTarget} per distinct URN.
   *
   * <p>Only the {@code [position, position + keysCount)} window is walked, mirroring the page
   * the delegate actually reads. Callers page over the same full key list, so emitting every
   * key on every page would report {@code keys.size()} targets per page instead of one target
   * set per key.
   */
  @Nonnull
  private static <URN extends Urn> List<DaoUsageTarget> targetsFromKeys(
      @Nonnull List<AspectKey<URN, ? extends RecordTemplate>> keys, int keysCount, int position) {
    final int start = Math.max(0, position);
    final int end = Math.min(keys.size(), start + Math.max(0, keysCount));
    final Map<String, List<String>> byUrn = new LinkedHashMap<>();
    for (int index = start; index < end; index++) {
      final AspectKey<URN, ? extends RecordTemplate> key = keys.get(index);
      byUrn.computeIfAbsent(key.getUrn().toString(), k -> new ArrayList<>())
          .add(key.getAspectClass().getSimpleName());
    }
    final List<DaoUsageTarget> targets = new ArrayList<>(byUrn.size());
    for (Map.Entry<String, List<String>> entry : byUrn.entrySet()) {
      targets.add(new DaoUsageTarget(entry.getKey(), entry.getValue()));
    }
    return targets;
  }
}
