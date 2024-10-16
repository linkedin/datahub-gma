package com.linkedin.metadata.restli.ingestion;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.ingestion.preupdate.InUpdateResponse;
import com.linkedin.metadata.dao.ingestion.preupdate.InUpdateRoutingClient;
import com.linkedin.testing.AspectFoo;
import java.util.Optional;


public class SampleInUpdateRoutingClient implements InUpdateRoutingClient {

  @Override
  public InUpdateResponse inUpdate(Urn urn, RecordTemplate newAspectValue, Optional existingAspectValue) {

    // For testing, change the aspect value to "bar"
    RecordTemplate updatedAspect = new AspectFoo().setValue("foobar");
    // Return a new InUpdateResponse with the updated aspect
    return new InUpdateResponse<>(updatedAspect);
  }

}
