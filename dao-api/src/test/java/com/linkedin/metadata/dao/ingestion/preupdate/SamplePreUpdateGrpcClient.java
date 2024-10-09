package com.linkedin.metadata.dao.ingestion.preupdate;

import com.google.protobuf.Message;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;


public class SamplePreUpdateGrpcClient implements PreUpdateClient {
  @Override
  public PreUpdateResponse preUpdate(Urn urn, RecordTemplate recordTemplate) {
    return null;
  }
}
