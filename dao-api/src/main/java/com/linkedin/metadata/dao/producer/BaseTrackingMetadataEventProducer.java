package com.linkedin.metadata.dao.producer;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.metadata.events.IngestionTrackingContext;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


public abstract class BaseTrackingMetadataEventProducer<SNAPSHOT extends RecordTemplate, ASPECT_UNION extends UnionTemplate, URN extends Urn>
    extends BaseMetadataEventProducer<SNAPSHOT, ASPECT_UNION, URN> {
  public BaseTrackingMetadataEventProducer(@Nonnull Class<SNAPSHOT> snapshotClass,
      @Nonnull Class<ASPECT_UNION> aspectUnionClass) {
    super(snapshotClass, aspectUnionClass);
  }

  /**
   * Same as inherited method {@link #produceAspectSpecificMetadataAuditEvent(Urn, RecordTemplate, RecordTemplate, AuditStamp)}
   * but with tracking context.
   * Produces an aspect specific Metadata Audit Event (MAE) after a metadata aspect is updated for an entity.
   *
   * @param urn {@link Urn} of the entity
   * @param oldValue the value prior to the update, or null if there's none.
   * @param newValue the value after the update
   * @param trackingContext nullable tracking context passed in to be appended to produced MAEv5s
   * @param auditStamp {@link AuditStamp} containing version auditing information for the metadata change
   */
  public abstract <ASPECT extends RecordTemplate> void produceAspectSpecificMetadataAuditEvent(@Nonnull URN urn,
      @Nullable ASPECT oldValue, @Nonnull ASPECT newValue, @Nullable AuditStamp auditStamp, @Nullable IngestionTrackingContext trackingContext);
}
