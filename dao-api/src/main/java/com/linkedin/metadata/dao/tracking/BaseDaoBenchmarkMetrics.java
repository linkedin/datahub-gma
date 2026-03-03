package com.linkedin.metadata.dao.tracking;

import javax.annotation.Nonnull;


/**
 * Interface for recording per-DAO-operation latency and error metrics.
 *
 * <p>Implementations collect histograms (latency) and counters (operation count, error count)
 * to benchmark DAO performance during the MySQL to TiDB migration evaluation.</p>
 *
 * <p>Follows the same pattern as {@link BaseTrackingManager} / {@link DummyTrackingManager}:
 * a no-op implementation ({@code NoOpDaoBenchmarkMetrics}) lives in the kernel (datahub-gma)
 * and the real Dropwizard-backed implementation lives in the service layer.</p>
 */
public interface BaseDaoBenchmarkMetrics {

  /**
   * Record a completed DAO operation. Implementations should record both the latency
   * (as a histogram) and increment the operation count.
   *
   * @param operationType the DAO operation name (e.g. "add", "batchGetUnion", "list")
   * @param entityType    the entity type derived from the URN class (e.g. "dataset", "corpuser")
   * @param latencyMs     wall-clock latency in milliseconds
   */
  void recordOperation(@Nonnull String operationType, @Nonnull String entityType, long latencyMs);

  /**
   * Record an error that occurred during a DAO operation.
   *
   * @param operationType  the DAO operation name (e.g. "add", "create")
   * @param entityType     the entity type derived from the URN class
   * @param exceptionClass the simple class name of the thrown exception (e.g. "SQLException")
   */
  void recordOperationError(@Nonnull String operationType, @Nonnull String entityType,
      @Nonnull String exceptionClass);

  /**
   * Whether metrics collection is enabled. Callers may short-circuit expensive
   * instrumentation when this returns {@code false}.
   *
   * @return true if metrics are being collected
   */
  boolean isEnabled();
}
