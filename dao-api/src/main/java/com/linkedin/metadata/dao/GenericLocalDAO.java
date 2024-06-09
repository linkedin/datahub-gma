package com.linkedin.metadata.dao;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.backfill.BackfillMode;
import com.linkedin.metadata.events.IngestionMode;
import com.linkedin.metadata.events.IngestionTrackingContext;
import com.linkedin.metadata.query.ExtraInfo;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
   * @param trackingContext Nullable tracking context contains information passed from metadata events.
   * @param ingestionMode Different options for ingestion.
   */
  void save(@Nonnull Urn urn, @Nonnull Class aspectClass, @Nonnull String metadata, @Nonnull AuditStamp auditStamp,
      @Nullable IngestionTrackingContext trackingContext, @Nullable IngestionMode ingestionMode);

  /**
   * Query the latest metadata from database.
   * @param urn The identifier of the entity which the metadata is associated with.
   * @param aspectClass The aspect class for the metadata.
   * @return The metadata with extra info regarding auditing.
   */
  Optional<MetadataWithExtraInfo> queryLatest(@Nonnull Urn urn, @Nonnull Class aspectClass);

  /**
   * Backfill secondary storages by triggering MAEs.
   * @param mode The backfill mode.
   * @param urnToAspect For each urn, the aspects to be backfilled.
   * @return The aspect class and its backfilled value.
   */
  Map<Urn, Map<Class<? extends RecordTemplate>, Optional<? extends RecordTemplate>>> backfill(@Nonnull BackfillMode mode,
      @Nonnull Map<Urn, Set<Class<? extends RecordTemplate>>> urnToAspect);
}
