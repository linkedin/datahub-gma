package com.linkedin.metadata.dao.scsi;

import javax.annotation.Nonnull;


/**
 * Provides the entity type that will be indexed in the aspect column, along with urn parts in the secondary index.
 *
 * <p>If this is anything other than the canonical name of urn class, then implement this interface.
 *
 */
public interface EntityTypeProvider {
  @Nonnull
  String entityType();
}