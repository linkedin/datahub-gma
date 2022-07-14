package com.linkedin.metadata.dao.builder;

import com.linkedin.data.template.RecordTemplate;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * A registry maintains mappings between ASPECT and its corresponding BaseLocalRelationshipBuilder.
 */
public interface LocalRelationshipBuilderRegistry {

  /**
   * Get corresponding local relationship builder for ASPECT. Returns null if not found.
   */
  @Nullable
  <ASPECT extends RecordTemplate> BaseLocalRelationshipBuilder getLocalRelationshipBuilder(@Nonnull final ASPECT aspect);
}
