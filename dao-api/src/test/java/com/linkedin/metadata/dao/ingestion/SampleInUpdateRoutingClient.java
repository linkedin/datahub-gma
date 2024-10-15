package com.linkedin.metadata.dao.ingestion;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.ingestion.preupdate.InUpdateResponse;
import com.linkedin.metadata.dao.ingestion.preupdate.InUpdateRoutingClient;
import com.linkedin.testing.AspectFoo;
import java.util.Optional;


public class SampleInUpdateRoutingClient implements InUpdateRoutingClient {

  @Override
  public InUpdateResponse inUpdate(Urn urn, RecordTemplate newValue, Optional existingValue) {
    AspectFoo aspectFoo = (AspectFoo) newValue;
    aspectFoo.setValue("bar");
    return new InUpdateResponse(aspectFoo);
  }

  @Override
  public boolean isSkipProcessing() {
    return false;
  }
}
