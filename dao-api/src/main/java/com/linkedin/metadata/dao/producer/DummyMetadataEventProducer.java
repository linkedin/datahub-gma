package com.linkedin.metadata.dao.producer;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dummy.DummyAspect;
import com.linkedin.metadata.dummy.DummySnapshot;
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
      @Nonnull ASPECT newValue) {
    // Do nothing
  }

  @Override
  public <ASPECT extends RecordTemplate> void produceMetadataAuditEvent(@Nonnull URN urn, @Nullable ASPECT oldValue,
      @Nonnull ASPECT newValue) {
    // Do nothing
  }

  @Override
  public <ASPECT extends RecordTemplate> void produceAspectSpecificMetadataAuditEvent(@Nonnull URN urn,
      @Nullable ASPECT oldValue, @Nonnull ASPECT newValue, @Nullable AuditStamp auditStamp, @Nullable IngestionMode ingestionMode) {
    // Do nothing
  }

  @Override
  public void produceMetadataGraphSearchMetric(@Nonnull String input, @Nonnull String request, @Nonnull String index,
      @Nonnull List<String> topHits, @Nonnull String api) {
    // Do nothing
  }
}
