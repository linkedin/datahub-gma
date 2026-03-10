package com.linkedin.metadata.dao;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.tracking.BaseDaoBenchmarkMetrics;
import com.linkedin.metadata.dao.urnpath.UrnPathExtractor;
import com.linkedin.metadata.events.IngestionTrackingContext;
import com.linkedin.metadata.query.IndexFilter;
import com.linkedin.metadata.query.IndexGroupByCriterion;
import com.linkedin.metadata.query.IndexSortCriterion;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * A decorator around {@link IEbeanLocalAccess} that records per-operation latency and error metrics
 * via {@link BaseDaoBenchmarkMetrics}.
 *
 * <p>Delegates every call to the wrapped implementation, measuring wall-clock time with
 * {@link System#nanoTime()}. On success, records latency; on error, records both latency and
 * the exception class, then re-throws.</p>
 *
 * <p>When {@link BaseDaoBenchmarkMetrics#isEnabled()} returns {@code false}, delegation is
 * direct with zero overhead (no timing).</p>
 *
 * @param <URN> the URN type for this entity
 */
public class InstrumentedEbeanLocalAccess<URN extends Urn> implements IEbeanLocalAccess<URN> {

  private final IEbeanLocalAccess<URN> _delegate;
  private final BaseDaoBenchmarkMetrics _metrics;
  private final String _entityType;

  /**
   * Creates an instrumented wrapper around the given local access implementation.
   *
   * @param delegate   the real local access implementation to wrap
   * @param metrics    the metrics recorder (may be a no-op)
   * @param urnClass   the URN class, used to derive the entity type name once at construction
   */
  public InstrumentedEbeanLocalAccess(@Nonnull IEbeanLocalAccess<URN> delegate,
      @Nonnull BaseDaoBenchmarkMetrics metrics, @Nonnull Class<URN> urnClass) {
    _delegate = delegate;
    _metrics = metrics;
    _entityType = urnClass.getSimpleName().replace("Urn", "").toLowerCase();
  }

  @Override
  public void setUrnPathExtractor(@Nonnull UrnPathExtractor<URN> urnPathExtractor) {
    _delegate.setUrnPathExtractor(urnPathExtractor);
  }

  @Override
  public <ASPECT extends RecordTemplate> int add(@Nonnull URN urn, @Nullable ASPECT newValue,
      @Nonnull Class<ASPECT> aspectClass, @Nonnull AuditStamp auditStamp,
      @Nullable IngestionTrackingContext ingestionTrackingContext, boolean isTestMode) {
    return instrument("add." + aspectClass.getSimpleName(), () -> _delegate.add(urn, newValue,
        aspectClass, auditStamp, ingestionTrackingContext, isTestMode));
  }

  @Override
  public <ASPECT extends RecordTemplate> int addWithOptimisticLocking(@Nonnull URN urn,
      @Nullable ASPECT newValue, @Nonnull Class<ASPECT> aspectClass, @Nonnull AuditStamp auditStamp,
      @Nullable Timestamp oldTimestamp, @Nullable IngestionTrackingContext ingestionTrackingContext,
      boolean isTestMode, boolean softDeleteOverwrite) {
    return instrument("addWithOptimisticLocking." + aspectClass.getSimpleName(),
        () -> _delegate.addWithOptimisticLocking(urn, newValue, aspectClass, auditStamp,
            oldTimestamp, ingestionTrackingContext, isTestMode, softDeleteOverwrite));
  }

  @Override
  public <ASPECT_UNION extends RecordTemplate> int create(@Nonnull URN urn,
      @Nonnull List<? extends RecordTemplate> aspectValues,
      @Nonnull List<BaseLocalDAO.AspectCreateLambda<? extends RecordTemplate>> aspectCreateLambdas,
      @Nonnull AuditStamp auditStamp, @Nullable IngestionTrackingContext ingestionTrackingContext,
      boolean isTestMode) {
    return instrument("create.aspects_" + aspectValues.size(),
        () -> _delegate.create(urn, aspectValues, aspectCreateLambdas,
            auditStamp, ingestionTrackingContext, isTestMode));
  }

  @Nonnull
  @Override
  public <ASPECT extends RecordTemplate> List<EbeanMetadataAspect> batchGetUnion(
      @Nonnull List<AspectKey<URN, ? extends RecordTemplate>> keys, int keysCount, int position,
      boolean includeSoftDeleted, boolean isTestMode) {
    return instrument("batchGetUnion.keys_" + keys.size(),
        () -> _delegate.batchGetUnion(keys, keysCount, position, includeSoftDeleted, isTestMode));
  }

  @Override
  public int softDeleteAsset(@Nonnull URN urn, boolean isTestMode) {
    return instrument("softDeleteAsset", () -> _delegate.softDeleteAsset(urn, isTestMode));
  }

  @Override
  public List<URN> listUrns(@Nullable IndexFilter indexFilter,
      @Nullable IndexSortCriterion indexSortCriterion, @Nullable URN lastUrn, int pageSize) {
    return instrument("listUrns.cursor",
        () -> _delegate.listUrns(indexFilter, indexSortCriterion, lastUrn, pageSize));
  }

  @Override
  public ListResult<URN> listUrns(@Nullable IndexFilter indexFilter,
      @Nullable IndexSortCriterion indexSortCriterion, int start, int pageSize) {
    return instrument("listUrns.offset",
        () -> _delegate.listUrns(indexFilter, indexSortCriterion, start, pageSize));
  }

  @Override
  public boolean exists(@Nonnull URN urn) {
    return instrument("exists", () -> _delegate.exists(urn));
  }

  @Nonnull
  @Override
  public Map<String, Long> countAggregate(@Nullable IndexFilter indexFilter,
      @Nonnull IndexGroupByCriterion indexGroupByCriterion) {
    return instrument("countAggregate",
        () -> _delegate.countAggregate(indexFilter, indexGroupByCriterion));
  }

  @Nonnull
  @Override
  public <ASPECT extends RecordTemplate> ListResult<URN> listUrns(@Nonnull Class<ASPECT> aspectClass,
      int start, int pageSize) {
    return instrument("listUrns", () -> _delegate.listUrns(aspectClass, start, pageSize));
  }

  @Nonnull
  @Override
  public <ASPECT extends RecordTemplate> ListResult<ASPECT> list(@Nonnull Class<ASPECT> aspectClass,
      @Nonnull URN urn, int start, int pageSize) {
    return instrument("list", () -> _delegate.list(aspectClass, urn, start, pageSize));
  }

  @Nonnull
  @Override
  public <ASPECT extends RecordTemplate> ListResult<ASPECT> list(@Nonnull Class<ASPECT> aspectClass,
      int start, int pageSize) {
    return instrument("list", () -> _delegate.list(aspectClass, start, pageSize));
  }

  @Nullable
  @Override
  public Timestamp getAssetDeletionTimestamp(@Nonnull URN urn, boolean isTestMode) {
    return instrument("getAssetDeletionTimestamp",
        () -> _delegate.getAssetDeletionTimestamp(urn, isTestMode));
  }

  @Override
  public void ensureSchemaUpToDate() {
    // Not instrumented — admin operation
    _delegate.ensureSchemaUpToDate();
  }

  /**
   * Core instrumentation wrapper. When metrics are disabled, delegates directly.
   * When enabled, times the operation and records latency on success, latency + error on failure.
   */
  private <T> T instrument(@Nonnull String operationType, @Nonnull Supplier<T> supplier) {
    if (!_metrics.isEnabled()) {
      return supplier.get();
    }
    final long startNanos = System.nanoTime();
    try {
      return supplier.get();
    } catch (RuntimeException ex) {
      _metrics.recordOperationError(operationType, _entityType, ex.getClass().getSimpleName());
      throw ex;
    } finally {
      _metrics.recordOperationLatency(operationType, _entityType,
          TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos));
    }
  }
}
