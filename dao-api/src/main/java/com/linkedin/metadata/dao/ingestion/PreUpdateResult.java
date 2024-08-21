package com.linkedin.metadata.dao.ingestion;

import com.google.protobuf.Message;
import lombok.Data;


/**
 * A class that defines the result of the pre-update routing.
 */
@Data
public class PreUpdateResult<ASPECT extends Message> {
  private ExitRoutingStatus routingStatus;
  private ASPECT aspect;

  public PreUpdateResult(ExitRoutingStatus routingStatus, ASPECT aspect) {
    this.routingStatus = routingStatus;
    this.aspect = aspect;
  }
}
