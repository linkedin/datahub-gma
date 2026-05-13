package com.linkedin.metadata.dao.tracking;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * Interface for recording per-DAO-operation latency and count metrics with dimensional attributes.
 *
 * <p>Each call records both a count (one increment) and a latency observation. Implementations
 * are expected to emit one metric per dimension instead of baking dimensions into metric names,
 * so consumers can aggregate or filter on individual attributes without enumerating every
 * concrete metric series.
 *
 * <p>A no-op implementation ({@link NoOpDaoBenchmarkMetrics}) lives in the kernel; concrete
 * backends (OTEL, Dropwizard, etc.) live in the service layer.
 */
public interface BaseDaoBenchmarkMetrics {

  /**
   * Record a completed DAO operation with its dimensional attributes.
   *
   * @param operation   pure operation name with no concatenation (e.g. {@code "add"},
   *                    {@code "batchGetUnion"})
   * @param entityType  entity type derived from the URN class (e.g. {@code "dataset"})
   * @param aspect      aspect class simple name for per-aspect operations, or {@code null} when
   *                    the operation is not per-aspect
   * @param countBucket pre-bucketed count label ({@code "1"} through {@code "9"}, {@code "10+"})
   *                    for batch operations, or {@code null} when there is no count dimension
   * @param status      outcome string, e.g. {@code "success"} or {@code "failure"}
   * @param errorClass  simple name of the thrown exception on failure, or {@code null} on success
   * @param latencyMs   wall-clock latency of the operation in milliseconds
   */
  void recordOperation(@Nonnull String operation, @Nonnull String entityType,
      @Nullable String aspect, @Nullable String countBucket, @Nonnull String status,
      @Nullable String errorClass, long latencyMs);

  /**
   * Whether metrics collection is enabled. Callers may short-circuit instrumentation when
   * this returns {@code false}.
   */
  boolean isEnabled();
}
