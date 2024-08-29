package com.linkedin.metadata.dao.ingestion;

import com.google.protobuf.Message;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;


/**
 * An interface that defines methods to route update requests to the appropriate custom APIs.
 */

public interface PreUpdateRoutingClient<ASPECT extends Message> {
  /**
   * A method that routes the update request to the appropriate custom API.
   * @param urn the urn of the asset
   * @param aspect the aspect to be updated
   * @return the updated aspect
   * @throws Exception if the routing fails
   */
  ASPECT routingLambda(Message urn, ASPECT aspect) throws Exception;

  /**
   * Convert the Pegasus URN to a Protobuf Message URN.
   * @param pegasusUrn the Pegasus URN
   * @return the Protobuf Message URN
   */
  Message convertUrnToMessage(Urn pegasusUrn);

  /**
   * Convert the Pegasus aspect to a Protobuf Message aspect.
   * @param pegasusAspect the Pegasus aspect
   * @return the Protobuf Message aspect
   */
  ASPECT convertAspectToMessage(RecordTemplate pegasusAspect);

  /**
   * Convert the Protobuf Message aspect to a Pegasus aspect.
   * @param messageAspect the Protobuf Message aspect
   * @return the Pegasus aspect
   */
  RecordTemplate convertAspectFromMessage(ASPECT messageAspect);
}