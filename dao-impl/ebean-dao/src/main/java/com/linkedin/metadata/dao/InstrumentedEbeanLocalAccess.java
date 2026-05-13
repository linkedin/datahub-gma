package com.linkedin.metadata.dao;

import com.google.common.annotations.VisibleForTesting;
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
 * A decorator around {@link IEbeanLocalAccess} that records per-operation latency and count
 * metrics via {@link BaseDaoBenchmarkMetrics}, passing dimensions (operation, aspect, count
 * bucket, status, error class) as separate attributes rather than baking them into a single
 * metric name.
 *
 * <p>Delegates every call to the wrapped implementation, measuring wall-clock time with
 * {@link System#nanoTime()}. On both success and failure, calls
 * {@link BaseDaoBenchmarkMetrics#recordOperation} once with the observed latency, status, and
 * (on failure) the thrown exception's simple class name; the exception is re-thrown unchanged.
 *
 * <p>When {@link BaseDaoBenchmarkMetrics#isEnabled()} returns {@code false}, delegation is
 * direct with zero overhead (no timing).
 *
 * @param <URN> the URN type for this entity
 */
public class InstrumentedEbeanLocalAccess<URN extends Urn> implements IEbeanLocalAccess<URN> {

  static final String STATUS_SUCCESS = "success";
  static final String STATUS_FAILURE = "failure";

  // 1..9 mapped to interned strings; 0 and 10+ handled separately. Avoids
  // Integer.toString() allocation on the hot path.
  private static final String[] COUNT_BUCKET_INTERNED =
      {"1", "2", "3", "4", "5", "6", "7", "8", "9"};
  private static final String BUCKET_ZERO = "0";
  private static final String BUCKET_OVERFLOW = "10+";

  private final IEbeanLocalAccess<URN> _delegate;
  private final BaseDaoBenchmarkMetrics _metrics;
  private final String _entityType;

  /**
   * Creates an instrumented wrapper around the given local access implementation.
   *
   * @param delegate the real local access implementation to wrap
   * @param metrics  the metrics recorder (may be a no-op)
   * @param urnClass the URN class, used to derive the entity type name once at construction
   */
  public InstrumentedEbeanLocalAccess(@Nonnull IEbeanLocalAccess<URN> delegate,
      @Nonnull BaseDaoBenchmarkMetrics metrics, @Nonnull Class<URN> urnClass) {
    _delegate = delegate;
    _metrics = metrics;
    _entityType = urnClass.getSimpleName().replace("Urn", "").toLowerCase();
  }

  /**
   * Bucket a count value into a small fixed set of labels to keep metric cardinality bounded.
   *
   * <p>Returns {@code "0"} for non-positive values (defensive; batch ops shouldn't hit this),
   * the interned digit string {@code "1".."9"} for 1-9, or {@code "10+"} for anything >= 10.
   */
  @VisibleForTesting
  static String bucketCount(int n) {
    if (n <= 0) {
      return BUCKET_ZERO;
    }
    if (n >= 10) {
      return BUCKET_OVERFLOW;
    }
    return COUNT_BUCKET_INTERNED[n - 1];
  }

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
  public <ASPECT extends RecordTemplate> int add(@Nonnull URN urn, @Nullable ASPECT newValue,
      @Nonnull Class<ASPECT> aspectClass, @Nonnull AuditStamp auditStamp,
      @Nullable IngestionTrackingContext ingestionTrackingContext, boolean isTestMode) {
    return instrument("add", aspectClass.getSimpleName(), null,
        () -> _delegate.add(urn, newValue, aspectClass, auditStamp,
            ingestionTrackingContext, isTestMode));
  }

  @Override
  public <ASPECT extends RecordTemplate> int addWithOptimisticLocking(@Nonnull URN urn,
      @Nullable ASPECT newValue, @Nonnull Class<ASPECT> aspectClass, @Nonnull AuditStamp auditStamp,
      @Nullable Timestamp oldTimestamp, @Nullable IngestionTrackingContext ingestionTrackingContext,
      boolean isTestMode, boolean softDeleteOverwrite) {
    return instrument("addWithOptimisticLocking", aspectClass.getSimpleName(), null,
        () -> _delegate.addWithOptimisticLocking(urn, newValue, aspectClass, auditStamp,
            oldTimestamp, ingestionTrackingContext, isTestMode, softDeleteOverwrite));
  }

  @Override
  public <ASPECT_UNION extends RecordTemplate> int create(@Nonnull URN urn,
      @Nonnull List<? extends RecordTemplate> aspectValues,
      @Nonnull List<BaseLocalDAO.AspectCreateLambda<? extends RecordTemplate>> aspectCreateLambdas,
      @Nonnull AuditStamp auditStamp, @Nullable IngestionTrackingContext ingestionTrackingContext,
      boolean isTestMode) {
    return instrument("create", null, bucketCount(aspectValues.size()),
        () -> _delegate.create(urn, aspectValues, aspectCreateLambdas,
            auditStamp, ingestionTrackingContext, isTestMode));
  }

  @Override
  public <ASPECT_UNION extends RecordTemplate> int batchUpsert(@Nonnull URN urn,
      @Nonnull List<BaseLocalDAO.AspectUpdateContext<RecordTemplate>> updateContexts,
      @Nonnull AuditStamp auditStamp, @Nullable IngestionTrackingContext ingestionTrackingContext,
      boolean isTestMode) {
    return instrument("batchUpsert", null, bucketCount(updateContexts.size()),
        () -> _delegate.batchUpsert(urn, updateContexts, auditStamp,
            ingestionTrackingContext, isTestMode));
  }

  @Nonnull
  @Override
  public <ASPECT extends RecordTemplate> List<EbeanMetadataAspect> batchGetUnion(
      @Nonnull List<AspectKey<URN, ? extends RecordTemplate>> keys, int keysCount, int position,
      boolean includeSoftDeleted, boolean isTestMode) {
    return instrument("batchGetUnion", null, bucketCount(keys.size()),
        () -> _delegate.batchGetUnion(keys, keysCount, position, includeSoftDeleted, isTestMode));
  }

  @Override
  public int softDeleteAsset(@Nonnull URN urn, boolean isTestMode) {
    return instrument("softDeleteAsset", null, null,
        () -> _delegate.softDeleteAsset(urn, isTestMode));
  }

  @Override
  public Map<URN, EntityDeletionInfo> readDeletionInfoBatch(@Nonnull List<URN> urns,
      boolean isTestMode) {
    return instrument("readDeletionInfoBatch", null, bucketCount(urns.size()),
        () -> _delegate.readDeletionInfoBatch(urns, isTestMode));
  }

  @Override
  public int batchSoftDeleteAssets(@Nonnull List<URN> urns, @Nonnull String cutoffTimestamp,
      boolean isTestMode) {
    return instrument("batchSoftDeleteAssets", null, bucketCount(urns.size()),
        () -> _delegate.batchSoftDeleteAssets(urns, cutoffTimestamp, isTestMode));
  }

  @Override
  public List<URN> listUrns(@Nullable IndexFilter indexFilter,
      @Nullable IndexSortCriterion indexSortCriterion, @Nullable URN lastUrn, int pageSize) {
    return instrument("listUrns.cursor", null, null,
        () -> _delegate.listUrns(indexFilter, indexSortCriterion, lastUrn, pageSize));
  }

  @Override
  public ListResult<URN> listUrns(@Nullable IndexFilter indexFilter,
      @Nullable IndexSortCriterion indexSortCriterion, int start, int pageSize) {
    return instrument("listUrns.offset", null, null,
        () -> _delegate.listUrns(indexFilter, indexSortCriterion, start, pageSize));
  }

  @Override
  public boolean exists(@Nonnull URN urn) {
    return instrument("exists", null, null, () -> _delegate.exists(urn));
  }

  @Nonnull
  @Override
  public Map<String, Long> countAggregate(@Nullable IndexFilter indexFilter,
      @Nonnull IndexGroupByCriterion indexGroupByCriterion) {
    return instrument("countAggregate", null, null,
        () -> _delegate.countAggregate(indexFilter, indexGroupByCriterion));
  }

  @Nonnull
  @Override
  public <ASPECT extends RecordTemplate> ListResult<URN> listUrns(@Nonnull Class<ASPECT> aspectClass,
      int start, int pageSize) {
    return instrument("listUrns", null, null,
        () -> _delegate.listUrns(aspectClass, start, pageSize));
  }

  @Nonnull
  @Override
  public <ASPECT extends RecordTemplate> ListResult<ASPECT> list(@Nonnull Class<ASPECT> aspectClass,
      @Nonnull URN urn, int start, int pageSize) {
    return instrument("list", null, null, () -> _delegate.list(aspectClass, urn, start, pageSize));
  }

  @Nonnull
  @Override
  public <ASPECT extends RecordTemplate> ListResult<ASPECT> list(@Nonnull Class<ASPECT> aspectClass,
      int start, int pageSize) {
    return instrument("list", null, null, () -> _delegate.list(aspectClass, start, pageSize));
  }

  @Override
  public void ensureSchemaUpToDate() {
    // Not instrumented — admin operation
    _delegate.ensureSchemaUpToDate();
  }

  /**
   * Core instrumentation wrapper. When metrics are disabled, delegates directly. When enabled,
   * times the operation and emits one {@code recordOperation} call in the {@code finally} block
   * with status and (on failure) error class populated.
   */
  private <T> T instrument(@Nonnull String operation, @Nullable String aspect,
      @Nullable String countBucket, @Nonnull Supplier<T> supplier) {
    if (!_metrics.isEnabled()) {
      return supplier.get();
    }
    final long startNanos = System.nanoTime();
    String status = STATUS_SUCCESS;
    String errorClass = null;
    try {
      return supplier.get();
    } catch (RuntimeException ex) {
      status = STATUS_FAILURE;
      errorClass = ex.getClass().getSimpleName();
      throw ex;
    } finally {
      _metrics.recordOperation(operation, _entityType, aspect, countBucket, status, errorClass,
          TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos));
    }
  }
}
