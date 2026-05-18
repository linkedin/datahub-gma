package com.linkedin.metadata.dao.tracking;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * A no-op implementation of {@link BaseDaoBenchmarkMetrics} that discards all observations.
 *
 * <p>Used as the default when no real metrics backend is configured.
 */
public class NoOpDaoBenchmarkMetrics implements BaseDaoBenchmarkMetrics {

  @Override
  public void recordOperation(@Nonnull String operation, @Nonnull String entityType,
      @Nullable String aspect, @Nullable String countBucket, @Nonnull String status,
      @Nullable String errorClass, long latencyMs) {
    // Do nothing
  }

  @Override
  public boolean isEnabled() {
    return false;
  }
}
