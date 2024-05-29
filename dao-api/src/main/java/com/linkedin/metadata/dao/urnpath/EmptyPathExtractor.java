package com.linkedin.metadata.dao.urnpath;

import com.linkedin.common.urn.Urn;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nonnull;


/**
 * A path extractor which does nothing.
 */
public final class EmptyPathExtractor implements UrnPathExtractor {
  @Nonnull
  @Override
  public Map<String, Object> extractPaths(@Nonnull Urn urn) {
    return Collections.emptyMap();
  }
}
