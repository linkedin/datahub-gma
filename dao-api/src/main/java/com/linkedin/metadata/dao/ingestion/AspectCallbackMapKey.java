package com.linkedin.metadata.dao.ingestion;

import com.linkedin.data.template.RecordTemplate;
import lombok.Data;

@Data
public class AspectCallbackMapKey {
  private final Class<? extends RecordTemplate> aspectClass;
  private final String entityType;
}
