package com.linkedin.metadata.dao.producer;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.metadata.dummy.DummyAspect;
import com.linkedin.metadata.dummy.DummySnapshot;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * A dummy metadata event producer that doesn't actually produce any events.
 */
public class DummyMetadataEventProducer<SNAPSHOT extends RecordTemplate, ASPECT_UNION extends UnionTemplate, URN extends Urn>
    extends BaseMetadataEventProducer<SNAPSHOT, ASPECT_UNION, URN> {

  @SuppressWarnings("unchecked")
  public DummyMetadataEventProducer() {
    super((Class<SNAPSHOT>) DummySnapshot.class, (Class<ASPECT_UNION>) DummyAspect.class);
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
      @Nullable ASPECT oldValue, @Nonnull ASPECT newValue) {
    // Do nothing
  }
}
