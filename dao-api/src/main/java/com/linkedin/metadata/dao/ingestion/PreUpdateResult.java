package com.linkedin.metadata.dao.ingestion;

import com.google.protobuf.Message;
import lombok.Builder;
import lombok.Data;


/**
 * A class that defines the result of the pre-update routing.
 */
@Data
@Builder
public class PreUpdateResult<ASPECT extends Message> {
  private ExitRoutingStatus routingStatus;
  private ASPECT aspect;
}
