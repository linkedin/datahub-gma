package com.linkedin.metadata.dao;

import com.linkedin.common.AuditStamp;
import com.linkedin.metadata.query.ExtraInfo;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Value;


/**
 * A generic data access object without any type bound.
 */
public interface GenericLocalDAO {

  @Value
  class MetadataWithExtraInfo {
    String aspect;
    ExtraInfo extraInfo;
  }

  /**
   * Save the metadata into database.
   * @param urn The identifier of the entity which the metadata is associated with.
   * @param aspectClass The aspect class for the metadata.
   * @param metadata The metadata serialized as JSON string.
   * @param auditStamp audit stamp containing information on who and when the metadata is saved.
   */
  void save(@Nonnull String urn, @Nonnull Class aspectClass, @Nonnull String metadata, @Nonnull AuditStamp auditStamp);

  /**
   * Query the latest metadata from database.
   * @param urn The identifier of the entity which the metadata is associated with.
   * @param aspectClass The aspect class for the metadata.
   * @return The metadata with extra info regarding auditing.
   */
  Optional<MetadataWithExtraInfo> queryLatest(@Nonnull String urn, @Nonnull Class aspectClass);
}
