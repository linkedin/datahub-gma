package com.linkedin.metadata.dao.internal;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.events.IngestionTrackingContext;
import javax.annotation.Nonnull;


/**
 * A base class for all remote writer DAOs.
 *
 * <p>Remote writer DAO allows updating metadata aspects hosted on a remote service without knowing the exact
 * URN-to-service mapping.
 */
public abstract class BaseRemoteWriterDAO {

  /**
   * Creates a new metadata snapshot against a remote service.
   *
   * @param urn the {@link Urn} for the entity
   * @param snapshot the snapshot containing updated metadata aspects
   * @param <URN> must be the entity URN type in {@code SNAPSHOT}
   */
  abstract public <URN extends Urn> void create(@Nonnull URN urn, @Nonnull RecordTemplate snapshot);

  /**
   * Same as {@link #create(Urn, RecordTemplate)} but with tracking context attached.
   * @param urn the {@link Urn} for the entity
   * @param snapshot the snapshot containing updated metadata aspects
   * @param trackingContext {@link IngestionTrackingContext} to use for DAO tracking probes and to pass on to the MAE
   * @param <URN> must be the entity URN type in {@code SNAPSHOT}
   */
  abstract public <URN extends Urn> void createWithTracking(@Nonnull URN urn, @Nonnull RecordTemplate snapshot,
      @Nonnull IngestionTrackingContext trackingContext);
}
