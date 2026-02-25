package com.linkedin.metadata.dao.tracking;

import javax.annotation.Nonnull;


/**
 * A no-op implementation of {@link BaseDaoBenchmarkMetrics} that discards all metrics.
 *
 * <p>Used as the default when no real metrics backend is configured.
 * Follows the same pattern as {@link DummyTrackingManager}.</p>
 */
public class NoOpDaoBenchmarkMetrics implements BaseDaoBenchmarkMetrics {

  @Override
  public void recordOperationLatency(@Nonnull String operationType, @Nonnull String entityType, long latencyMs) {
    // Do nothing
  }

  @Override
  public void recordOperationError(@Nonnull String operationType, @Nonnull String entityType,
      @Nonnull String exceptionClass) {
    // Do nothing
  }

  @Override
  public boolean isEnabled() {
    return false;
  }
}
