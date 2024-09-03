package com.linkedin.metadata.dao.ingestion;

import com.linkedin.data.template.RecordTemplate;
import javax.annotation.Nonnull;

/**
 * A registry maintains which aspect needs PreIngestionRouting.
 */
public interface RestliPreUpdateAspectRegistry {

  /**
   * Get PreUpdateRouting lambda for an aspect.
   */
  @Nonnull
  RestliCompliantPreUpdateRoutingClient getPreIngestionRouting(Class<? extends RecordTemplate> aspect);

  boolean isRegistered(Class<? extends RecordTemplate> aspectClass);

}