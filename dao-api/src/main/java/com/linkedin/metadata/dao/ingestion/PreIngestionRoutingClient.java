package com.linkedin.metadata.dao.ingestion;

import com.google.protobuf.Any;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.ingestion.PreIngestionResult;


/**
 * An interface that defines methods to route ingestion requests to the appropriate custom APIs.
 */

public interface PreIngestionRoutingClient {
     PreIngestionResult routingLambda(Urn urn, Any asset) throws Exception;
}