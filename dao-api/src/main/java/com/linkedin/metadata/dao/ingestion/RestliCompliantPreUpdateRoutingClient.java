package com.linkedin.metadata.dao.ingestion;

import com.google.protobuf.Message;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;


public interface RestliCompliantPreUpdateRoutingClient<ASPECT extends Message> extends PreUpdateRoutingClient {
  Message convertUrnToMessage(Urn pegasusUrn);

  ASPECT convertAspectToMessage(RecordTemplate pegasusAspect);

  RecordTemplate convertAspectFromMessage(ASPECT messageAspect);
}