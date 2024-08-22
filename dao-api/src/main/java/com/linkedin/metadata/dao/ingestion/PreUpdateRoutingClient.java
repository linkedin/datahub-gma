package com.linkedin.metadata.dao.ingestion;

import com.google.protobuf.Message;


/**
 * An interface that defines methods to route update requests to the appropriate custom APIs.
 */

public interface PreUpdateRoutingClient<ASSET extends Message> {
  /**
   * A method that routes the update request to the appropriate custom API.
   * @param urn the urn of the asset
   * @param asset the aspect to update
   * @return updated asset value
   * @throws Exception if the routing fails
   */
  ASSET preUpdateLambda(Message urn, ASSET asset) throws Exception;
}