package com.linkedin.metadata.dao.ingestion.preupdate;

import com.linkedin.data.template.RecordTemplate;
import lombok.Data;

/**
 * Response of pre-update process that includes the updated aspect.
 */
@Data
public class PreUpdateResponse<ASPECT extends RecordTemplate> {
  private final ASPECT updatedAspect;
}
