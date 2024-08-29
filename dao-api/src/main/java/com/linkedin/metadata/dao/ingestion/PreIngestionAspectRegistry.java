package com.linkedin.metadata.dao.ingestion;

import com.linkedin.data.template.RecordTemplate;
import javax.annotation.Nonnull;


public interface PreIngestionAspectRegistry {
  <ASPECT extends RecordTemplate> boolean isRegistered(@Nonnull final Class<ASPECT> aspectClass);
}