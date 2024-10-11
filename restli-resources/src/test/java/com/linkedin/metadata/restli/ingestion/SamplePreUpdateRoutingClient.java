package com.linkedin.metadata.restli.ingestion;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.ingestion.preupdate.PreUpdateResponse;
import com.linkedin.metadata.dao.ingestion.preupdate.PreUpdateRoutingClient;
import com.linkedin.testing.AspectFoo;


public class SamplePreUpdateRoutingClient implements PreUpdateRoutingClient {

  @Override
  public PreUpdateResponse preUpdate(Urn urn, RecordTemplate recordTemplate) {

    // For testing, change the aspect value to "bar"
    RecordTemplate updatedAspect = new AspectFoo().setValue("foobar");
    // Return a new PreUpdateResponse with the updated aspect
    return new PreUpdateResponse<>(updatedAspect);
  }
}
