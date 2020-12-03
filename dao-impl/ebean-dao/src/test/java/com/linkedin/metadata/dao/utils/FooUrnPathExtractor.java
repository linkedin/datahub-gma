package com.linkedin.metadata.dao.utils;

import com.linkedin.metadata.dao.scsi.UrnPathExtractor;
import com.linkedin.testing.urn.FooUrn;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;


public class FooUrnPathExtractor implements UrnPathExtractor<FooUrn> {
  @Override
  public Map<String, Object> extractPaths(@Nonnull FooUrn urn) {
    return Collections.unmodifiableMap(new HashMap<String, Object>() {
      {
        put("/fooId", urn.getFooIdEntity());
      }
    });
  }
}
