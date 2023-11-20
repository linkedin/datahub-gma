package com.linkedin.metadata.dao.generic;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;


/**
 * A generic DAO for reading metadata aspects.
 */
public abstract class ReadDAO {

  /**
   * Get metadata for a given entity and the corresponding keys.
   *
   * @param entity metadata entity name, e.g., "dataset"
   * @param keys a set of {@link Key}
   * @return a mapping of given keys to the corresponding result.
   */
  @Nonnull
  public abstract Map<Key, Optional<Record>> get(@Nonnull String entity, @Nonnull Set<Key> keys);
}