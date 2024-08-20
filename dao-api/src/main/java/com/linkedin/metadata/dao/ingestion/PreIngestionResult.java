package com.linkedin.metadata.dao.ingestion;

/**
 * A class that defines the result of the pre-ingestion routing.
 */

public class PreIngestionResult {
  private RoutingStatus routingStatus;
  public PreIngestionResult(RoutingStatus routingStatus) {
    this.routingStatus = routingStatus;
  }

  public RoutingStatus getRoutingStatus() {
    return routingStatus;
  }

  public void setIngestionStatus(RoutingStatus routingStatus) {
    this.routingStatus = routingStatus;
  }

}
