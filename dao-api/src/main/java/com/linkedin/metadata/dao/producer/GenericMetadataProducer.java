package com.linkedin.metadata.dao.producer;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.events.ChangeType;
import com.linkedin.metadata.events.IngestionMode;
import com.linkedin.metadata.events.IngestionTrackingContext;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * Generic metadata producer without type-bound.
 */
public interface GenericMetadataProducer {

  /**
   * Produces an aspect specific Metadata Audit Event (MAE) after a metadata aspect is updated for an entity.
   *
   * @param urn {@link Urn} of the entity
   * @param oldValue The value prior to the update, or null if there's none.
   * @param newValue The value after the update
   * @param aspectClass the class of the aspect
   * @param auditStamp Containing version auditing information for the metadata change
   * @param trackingContext Nullable tracking context passed in to be appended to produced MAEv5s
   * @param ingestionMode Different options for ingestion.
   */
  void produceAspectSpecificMetadataAuditEvent(@Nonnull Urn urn, @Nullable RecordTemplate oldValue, @Nonnull RecordTemplate newValue,
      @Nonnull Class<? extends RecordTemplate> aspectClass,
      @Nullable AuditStamp auditStamp, @Nullable IngestionTrackingContext trackingContext, @Nullable IngestionMode ingestionMode);

  /**
   * Produces an aspect specific Metadata Audit Event (MAE) after a metadata aspect is updated for an entity with Change type.
   * @param urn {@link Urn} of the entity
   * @param oldValue the value prior to the update, or null if there's none.
   * @param newValue the value after the update
   * @param aspectClass the class of the aspect
   * @param auditStamp {@link AuditStamp} containing version auditing information for the metadata change
   * @param trackingContext nullable tracking context passed in to be appended to produced MAEv5s
   * @param ingestionMode {@link IngestionMode} of the change
   * @param changeType {@link ChangeType} of the change
   */
  void produceAspectSpecificMetadataAuditEvent(@Nonnull Urn urn, @Nullable RecordTemplate oldValue,
      @Nonnull RecordTemplate newValue, @Nonnull Class<? extends RecordTemplate> aspectClass,
      @Nullable AuditStamp auditStamp, @Nullable IngestionTrackingContext trackingContext,
      @Nullable IngestionMode ingestionMode, @Nonnull ChangeType changeType);


}
