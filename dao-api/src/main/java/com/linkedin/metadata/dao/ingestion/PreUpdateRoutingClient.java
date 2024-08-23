package com.linkedin.metadata.dao.ingestion;

import com.google.protobuf.Message;


/**
 * An interface that defines methods to route update requests to the appropriate custom APIs.
 */

public interface PreUpdateRoutingClient<ASPECT extends Message> {
  /**
   * A method that routes the update request to the appropriate custom API.
   * @param urn the urn of the asset
   * @param aspect the aspect to be updated
   * @return a routing result
   * @throws Exception if the routing fails
   */
  ASPECT routingLambda(Message urn, ASPECT aspect) throws Exception;
}