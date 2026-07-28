package com.linkedin.metadata.dao.tracking;

import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * Interface for emitting a Metadata Graph usage event once per successful DAO operation
 * (read / write / delete). Emission is an observability side-channel and MUST be
 * fire-and-forget: implementations are required to be asynchronous, non-blocking, and to
 * never throw back into the DAO call path.
 *
 * <p>The kernel passes only neutral primitives ({@link String} and {@link DaoUsageTarget})
 * so it does not depend on any Avro / event-schema module. A no-op implementation
 * ({@link NoOpDaoUsageEmitter}) lives in the kernel; the concrete producer-backed
 * implementation lives in the service layer.
 */
public interface BaseDaoUsageEmitter {

  /**
   * Record a completed DAO usage operation. Implementations MUST NOT block and MUST NOT
   * propagate exceptions to the caller.
   *
   * @param operationType    one of {@code "READ"}, {@code "WRITE"}, {@code "DELETE"},
   *                         {@code "DELETE_ALL"}
   * @param entityType       entity type derived from the URN class (e.g. {@code "dataset"})
   * @param sourceOperation  pure DAO method name (e.g. {@code "batchGetUnion"})
   * @param actorUrn         string form of the caller URN for writes; {@code null} for reads
   *                         (no audit stamp is available on the read path in v1)
   * @param impersonatorUrn  string form of the service-on-behalf-of URN, or {@code null}
   * @param targets          per-URN targets ({@code {urn, aspects[]}}); {@code aspects} is
   *                         empty for a whole-entity {@code DELETE_ALL}
   */
  void emit(@Nonnull String operationType, @Nonnull String entityType,
      @Nonnull String sourceOperation, @Nullable String actorUrn,
      @Nullable String impersonatorUrn, @Nonnull List<DaoUsageTarget> targets);

  /**
   * Whether usage emission is enabled. Callers short-circuit all instrumentation (building
   * targets, deriving the caller, etc.) when this returns {@code false}, giving zero
   * overhead in the default / disabled configuration.
   */
  boolean isEnabled();
}
