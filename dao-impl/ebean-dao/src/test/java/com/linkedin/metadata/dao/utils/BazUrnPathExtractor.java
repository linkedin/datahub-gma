package com.linkedin.metadata.dao.utils;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.dao.urnpath.UrnPathExtractor;
import com.linkedin.testing.urn.BazUrn;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;


public class BazUrnPathExtractor implements UrnPathExtractor {
  @Override
  public Map<String, Object> extractPaths(@Nonnull Urn urn) {

    try {
      BazUrn bazUrn = BazUrn.createFromString(urn.toString());

      return Collections.unmodifiableMap(new HashMap<String, Integer>() {
        {
          put("/bazId", bazUrn.getBazIdEntity());
        }
      });
    } catch (URISyntaxException e) {
      return Collections.emptyMap();
    }

  }
}
