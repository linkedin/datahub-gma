package com.linkedin.metadata.dao.ingestion;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.ingestion.preupdate.PreUpdateResponse;
import com.linkedin.metadata.dao.ingestion.preupdate.PreUpdateRoutingClient;
import com.linkedin.testing.AspectFoo;


public class SamplePreUpdateRoutingClient implements PreUpdateRoutingClient {

  @Override
  public PreUpdateResponse preUpdate(Urn urn, RecordTemplate recordTemplate) {
    AspectFoo aspectFoo = (AspectFoo) recordTemplate;
    aspectFoo.setValue("bar");
    return new PreUpdateResponse(aspectFoo);
  }
}
