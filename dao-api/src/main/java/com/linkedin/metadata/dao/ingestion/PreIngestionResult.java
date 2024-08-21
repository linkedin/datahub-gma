package com.linkedin.metadata.dao.ingestion;

import com.linkedin.data.template.RecordTemplate;
import lombok.Data;


/**
 * A class that defines the result of the pre-ingestion routing.
 */
@Data
public class PreIngestionResult {
  private ExitRoutingStatus routingStatus;
  private RecordTemplate aspect;
  public PreIngestionResult(ExitRoutingStatus routingStatus, RecordTemplate aspect) {
    this.routingStatus = routingStatus;
    this.aspect = aspect;
  }

}
