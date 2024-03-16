package com.linkedin.metadata.dao.utils;

import com.linkedin.metadata.dao.urnpath.UrnPathExtractor;
import com.linkedin.testing.urn.FooUrn;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;


public class FooUrnPathExtractor implements UrnPathExtractor<FooUrn> {
  private Map<String, Integer> urnPaths = new HashMap<String, Integer>() {
    {
      put("/dummyId", 10); // Hard-code the value to test ingestion multiple filters with @gma.aspect.ingestion
    }
  };

  @Override
  public Map<String, Object> extractPaths(@Nonnull FooUrn urn) {
    urnPaths.put("/fooId", urn.getFooIdEntity());
    return Collections.unmodifiableMap(urnPaths);
  }

  public void updateDummyEntry(Integer value) {
    urnPaths.put("/dummyId", value);
  }
}
