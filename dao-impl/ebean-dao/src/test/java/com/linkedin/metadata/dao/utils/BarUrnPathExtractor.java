package com.linkedin.metadata.dao.utils;

import com.linkedin.metadata.dao.scsi.UrnPathExtractor;
import com.linkedin.testing.urn.BarUrn;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;


public class BarUrnPathExtractor implements UrnPathExtractor<BarUrn> {
  @Override
  public Map<String, Object> extractPaths(@Nonnull BarUrn urn) {
    return Collections.unmodifiableMap(new HashMap<String, Object>() {
      {
        put("/barId", urn.getBarIdEntity());
      }
    });
  }
}
