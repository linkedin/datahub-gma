package com.linkedin.metadata.dao.ingestion;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.testing.AspectFoo;
import java.util.Optional;


public class SampleInUpdateRoutingClient implements InUpdateRoutingClient {
  @Override
  public InUpdateResponse inUpdate(Urn urn, RecordTemplate newAspectValue, Optional existingAspectValue) {
    AspectFoo aspectFoo = (AspectFoo) newAspectValue;
    aspectFoo.setValue("bar");
    return new InUpdateResponse(aspectFoo);
  }
}
