package com.linkedin.metadata.dao.ingestion;

import com.linkedin.data.template.RecordTemplate;
import javax.annotation.Nonnull;

/**
 * A registry maintains which aspect needs PreIngestionRouting.
 */
public interface RestliPreIngestionAspectRegistry {
  /**
   * Check if PreIngestionRouting is registered for an aspect.
   */
  <ASPECT extends RecordTemplate> boolean isRegistered(Class<ASPECT> aspectClass);

  /**
   * Get PreUpdateRouting lambda for an aspect.
   */
  @Nonnull
  <ASPECT extends RecordTemplate> RestliCompliantPreUpdateRoutingClient getPreIngestionRouting(ASPECT aspect);

}