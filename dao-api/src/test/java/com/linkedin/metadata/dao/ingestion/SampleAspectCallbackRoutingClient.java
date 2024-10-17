package com.linkedin.metadata.dao.ingestion;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.testing.AspectFoo;
import java.util.Optional;


public class SampleAspectCallbackRoutingClient implements AspectCallbackRoutingClient {
  @Override
  public AspectCallbackResponse inUpdate(Urn urn, RecordTemplate newAspectValue, Optional existingAspectValue) {
    AspectFoo aspectFoo = (AspectFoo) newAspectValue;
    aspectFoo.setValue("bar");
    return new AspectCallbackResponse(aspectFoo);
  }
}
