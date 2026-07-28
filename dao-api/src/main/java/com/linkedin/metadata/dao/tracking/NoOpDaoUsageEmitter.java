package com.linkedin.metadata.dao.tracking;

import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * A no-op implementation of {@link BaseDaoUsageEmitter} that discards all usage events.
 *
 * <p>Used as the default when no producer-backed emitter is configured. With this
 * implementation the {@code UsageTrackingEbeanLocalAccess} decorator (and any other
 * caller) short-circuits on {@link #isEnabled()} and does zero work.
 */
public class NoOpDaoUsageEmitter implements BaseDaoUsageEmitter {

  @Override
  public void emit(@Nonnull String operationType, @Nonnull String entityType,
      @Nonnull String sourceOperation, @Nullable String actorUrn,
      @Nullable String impersonatorUrn, @Nonnull List<DaoUsageTarget> targets) {
    // Do nothing
  }

  @Override
  public boolean isEnabled() {
    return false;
  }
}
