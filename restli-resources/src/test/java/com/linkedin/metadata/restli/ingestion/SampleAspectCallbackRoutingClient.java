package com.linkedin.metadata.restli.ingestion;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.ingestion.AspectCallbackResponse;
import com.linkedin.metadata.dao.ingestion.AspectCallbackRoutingClient;
import com.linkedin.testing.AspectFoo;
import java.util.Optional;


public class SampleAspectCallbackRoutingClient implements AspectCallbackRoutingClient {

  @Override
  public AspectCallbackResponse routeAspectCallback(Urn urn, RecordTemplate newAspectValue, Optional existingAspectValue) {

    // For testing, change the aspect value to "bar"
    RecordTemplate updatedAspect = new AspectFoo().setValue("foobar");
    // Return a new AspectCallbackResponse with the updated aspect
    return new AspectCallbackResponse<>(updatedAspect);
  }

}
