package com.linkedin.metadata.dao.utils;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.dao.urnpath.UrnPathExtractor;
import com.linkedin.testing.urn.BarUrn;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;


public class BarUrnPathExtractor implements UrnPathExtractor {
  @Override
  public Map<String, Object> extractPaths(@Nonnull Urn urn) {

    try {
      BarUrn barUrn = BarUrn.createFromString(urn.toString());
      return Collections.unmodifiableMap(new HashMap<String, Integer>() {
        {
          put("/barId", barUrn.getBarIdEntity());
        }
      });
    } catch (URISyntaxException ignored) {
      return Collections.emptyMap();
    }
  }
}
