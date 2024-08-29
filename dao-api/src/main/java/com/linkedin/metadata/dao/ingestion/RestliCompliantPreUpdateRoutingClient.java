package com.linkedin.metadata.dao.ingestion;

import com.google.protobuf.Message;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;


public interface RestliCompliantPreUpdateRoutingClient<ASPECT extends Message> extends PreUpdateRoutingClient {

  /**
   * Converts a URN to a message.
   */
  Message convertUrnToMessage(Urn pegasusUrn);
  /**
   * Converts a record template aspect to message aspect.
   */
  ASPECT convertAspectToMessage(RecordTemplate pegasusAspect);

  /**
   * Converts a message aspect to record template aspect.
   */
  RecordTemplate convertAspectFromMessage(ASPECT messageAspect);
}