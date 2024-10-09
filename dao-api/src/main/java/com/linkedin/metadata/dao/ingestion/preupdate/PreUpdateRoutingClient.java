package com.linkedin.metadata.dao.ingestion;

import com.google.protobuf.Message;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;


/**
 * An interface that defines methods to route update requests to the appropriate custom APIs for pre-ingestion process.
 */

public interface PreUpdateRoutingClient<ASPECT extends RecordTemplate> {
  /**
   * A method that routes the update request to the appropriate custom API.
   * @param urn the urn of the asset
   * @param aspect the aspect to be updated
   * @return the updated aspect
   */
  PreUpdateResponse<ASPECT> preUpdate(Message urn, ASPECT aspect);

  /**
   * Converts a RecordTemplate URN to a gRPC-compatible Message.
   *
   * @param urn The RecordTemplate URN.
   * @return The gRPC-compatible Message URN.
   */
  Message convertUrnToMessage(Urn urn);

  /**
   * Converts a RecordTemplate Aspect to a gRPC-compatible Message Aspect.
   *
   * @param aspect The RecordTemplate aspect.
   * @return The gRPC-compatible Message aspect.
   */
  Message convertAspectToMessage(RecordTemplate aspect);

  /**
   * Converts a gRPC-compatible Message Aspect back to a RecordTemplate Aspect.
   *
   * @param messageAspect The Message aspect.
   * @return The RecordTemplate aspect.
   */
  RecordTemplate convertAspectToRecordTemplate(Message messageAspect);

}