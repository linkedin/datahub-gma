package com.linkedin.metadata.dao.producer;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.events.IngestionMode;
import com.linkedin.metadata.events.IngestionTrackingContext;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


public interface GenericMetadataProducer {

  /**
   * Produces an aspect specific Metadata Audit Event (MAE) after a metadata aspect is updated for an entity.
   *
   * @param urn {@link Urn} of the entity
   * @param oldValue the value prior to the update, or null if there's none.
   * @param newValue the value after the update
   * @param trackingContext nullable tracking context passed in to be appended to produced MAEv5s
   * @param auditStamp {@link AuditStamp} containing version auditing information for the metadata change
   * @param ingestionMode {@link IngestionMode} of the change
   */
  void produceAspectSpecificMetadataAuditEvent(@Nonnull Urn urn, @Nullable RecordTemplate oldValue, @Nonnull RecordTemplate newValue,
      @Nullable AuditStamp auditStamp, @Nullable IngestionTrackingContext trackingContext, @Nullable IngestionMode ingestionMode);
}
