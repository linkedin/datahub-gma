package com.linkedin.metadata.dao.ingestion;

import com.linkedin.data.template.RecordTemplate;
import lombok.Data;

/**
 * Response of in-update process that includes the updated aspect. It can be extended to include additional information.
 */
@Data
public class InUpdateResponse<ASPECT extends RecordTemplate> {
  private final ASPECT updatedAspect;
}
