package com.linkedin.metadata.dao.ingestion;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;


/**
 * An interface that defines methods to route update requests to the appropriate custom APIs.
 */

public interface PreUpdateRoutingClient {
     /**
      * A method that routes the update request to the appropriate custom API.
      * @param urn the urn of the asset
      * @param asset the asset to extract aspects from
      * @param <ASSET> must be a valid asset model defined in com.linkedin.metadata.asset
      * @return a routing result
      * @throws Exception if the routing fails
      */
     <ASSET extends RecordTemplate> PreIngestionResult routingLambda(Urn urn, ASSET asset) throws Exception;
}