package com.linkedin.metadata.dao.producer;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dummy.DummyAspect;
import com.linkedin.metadata.dummy.DummySnapshot;
import com.linkedin.metadata.events.ChangeType;
import com.linkedin.metadata.events.IngestionMode;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * A dummy metadata event producer that doesn't actually produce any events.
 */
public class DummyMetadataEventProducer<URN extends Urn>
    extends BaseMetadataEventProducer<DummySnapshot, DummyAspect, URN> {

  public DummyMetadataEventProducer() {
    super(DummySnapshot.class, DummyAspect.class);
  }

  @Override
  public <ASPECT extends RecordTemplate> void produceSnapshotBasedMetadataChangeEvent(@Nonnull URN urn,
      @Nullable ASPECT newValue) {
    // Do nothing
  }

  @Override
  public <ASPECT extends RecordTemplate> void produceMetadataAuditEvent(@Nonnull URN urn, @Nullable ASPECT oldValue,
      @Nullable ASPECT newValue) {
    // Do nothing
  }

  @Override
  public <ASPECT extends RecordTemplate> void produceAspectSpecificMetadataAuditEvent(@Nonnull URN urn,
      @Nullable ASPECT oldValue, @Nullable ASPECT newValue, @Nonnull Class<ASPECT> aspectClass,
      @Nullable AuditStamp auditStamp, @Nullable IngestionMode ingestionMode) {
    // Do nothing
  }

  /**
   * Produces an aspect specific Metadata Audit Event (MAE) after a metadata aspect is updated for an entity with Change
   * type.
   *
   * @param urn           {@link Urn} of the entity
   * @param oldValue      the value prior to the update, or null if there's none.
   * @param newValue      the value after the update
   * @param aspectClass   the class of ASPECT
   * @param auditStamp    {@link AuditStamp} containing version auditing information for the metadata change
   * @param ingestionMode {@link IngestionMode} of the change
   * @param changeType    {@link ChangeType} of the change
   */
  @Override
  public <ASPECT extends RecordTemplate> void produceAspectSpecificMetadataAuditEvent(@Nonnull URN urn,
      @Nullable ASPECT oldValue, @Nullable ASPECT newValue, @Nonnull Class<ASPECT> aspectClass,
      @Nullable AuditStamp auditStamp, @Nullable IngestionMode ingestionMode, ChangeType changeType) {
    // Do nothing
  }

  @Override
  public void produceMetadataGraphSearchMetric(@Nonnull String input, @Nonnull String request, @Nonnull String index,
      @Nonnull List<String> topHits, @Nonnull String api) {
    // Do nothing
  }
}
