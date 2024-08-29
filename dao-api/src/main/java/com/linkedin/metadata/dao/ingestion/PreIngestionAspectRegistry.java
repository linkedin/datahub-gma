package com.linkedin.metadata.dao.ingestion;

import com.linkedin.data.template.RecordTemplate;
import javax.annotation.Nonnull;

/**
 * A registry maintains which aspect needs PreIngestionRouting.
 */
public interface PreIngestionAspectRegistry {
  /**
   * Check if PreIngestionRouting is registered for an aspect.
   */
  <ASPECT extends RecordTemplate> boolean isRegistered(@Nonnull final Class<ASPECT> aspectClass);
}