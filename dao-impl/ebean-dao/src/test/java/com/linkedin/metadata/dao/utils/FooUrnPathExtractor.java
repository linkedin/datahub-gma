package com.linkedin.metadata.dao.utils;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.dao.urnpath.UrnPathExtractor;
import com.linkedin.testing.urn.FooUrn;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;


public class FooUrnPathExtractor implements UrnPathExtractor {
  private Map<String, Integer> urnPaths = new HashMap<String, Integer>() {
    {
      put("/dummyId", 10); // Hard-code the value to test ingestion multiple filters with @gma.aspect.ingestion
    }
  };

  @Override
  public Map<String, Object> extractPaths(@Nonnull Urn urn) {
    try {
      FooUrn fooUrn = FooUrn.createFromString(urn.toString());
      urnPaths.put("/fooId", fooUrn.getFooIdEntity());
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }

    return Collections.unmodifiableMap(urnPaths);
  }

  public void updateDummyEntry(Integer value) {
    urnPaths.put("/dummyId", value);
  }
}
