package com.linkedin.metadata.dao.ingestion.preupdate;

import com.google.protobuf.Message;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;


public interface PreUpdateClient<ASPECT extends RecordTemplate> {

  /**
   * Executes the gRPC pre-update logic, including building the request,
   * invoking the service, and handling the response.
   *
   * @param urn The URN of the entity to be updated.
   * @param aspect The aspect to be updated.
   * @return The updated aspect.
   */
  PreUpdateResponse<ASPECT> preUpdate(Urn urn, ASPECT aspect);

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

