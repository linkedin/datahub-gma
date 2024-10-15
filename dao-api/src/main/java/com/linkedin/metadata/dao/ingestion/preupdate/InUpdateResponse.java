package com.linkedin.metadata.dao.ingestion.preupdate;

import com.linkedin.data.template.RecordTemplate;
import lombok.Data;

/**
 * Response of pre-update process that includes the updated aspect.
 */
@Data
public class InUpdateResponse<ASPECT extends RecordTemplate> {
  private final ASPECT updatedAspect;

  // Constructor to initialize updatedAspect
  public InUpdateResponse(ASPECT updatedAspect) {
    this.updatedAspect = updatedAspect;
  }
}
