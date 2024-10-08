package com.linkedin.metadata.dao.ingestion.preupdate;

import com.google.protobuf.Message;
import lombok.Data;

@Data
public class PreUpdateResponse<ASPECT extends Message> {
  private final ASPECT updatedAspect;
}
