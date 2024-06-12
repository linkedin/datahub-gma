package com.linkedin.metadata.dao.producer;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
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
   * @param auditStamp Containing version auditing information for the metadata change
   * @param trackingContext Nullable tracking context passed in to be appended to produced MAEv5s
   * @param ingestionMode Different options for ingestion.
   */
  void produceAspectSpecificMetadataAuditEvent(@Nonnull Urn urn, @Nullable RecordTemplate oldValue, @Nonnull RecordTemplate newValue,
      @Nullable AuditStamp auditStamp, @Nullable IngestionTrackingContext trackingContext, @Nullable IngestionMode ingestionMode);
}
