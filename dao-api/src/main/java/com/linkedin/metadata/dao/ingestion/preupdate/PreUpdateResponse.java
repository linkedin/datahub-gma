package com.linkedin.metadata.dao.ingestion.preupdate;

import com.linkedin.data.template.RecordTemplate;
import lombok.Data;


@Data
public class PreUpdateResponse<ASPECT extends RecordTemplate> {
  private final ASPECT updatedAspect;
}
