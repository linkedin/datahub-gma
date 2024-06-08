package com.linkedin.metadata.restli;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.events.IngestionTrackingContext;
import com.linkedin.metadata.internal.IngestionParams;
import com.linkedin.restli.server.RestLiServiceException;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;


/**
 * A client interacts with standard GMS APIs.
 */
public abstract class BaseAspectRoutingGmsClient {

  /**
   * The entity type associated with the aspect / client.
   */
  @Getter
  private final String entityType;

  public BaseAspectRoutingGmsClient(String entityType) {
    this.entityType = entityType;
  }

  /**
   * Retrieves the latest version of the routing aspect for an entity.
   */
  public abstract <URN extends Urn> RecordTemplate get(URN urn) throws RestLiServiceException;

  /**
   * Backfill the routing aspect value for a given set of entity identified by the urns.
   */
  public abstract <URN extends Urn> BackfillResult backfill(Set<URN> urn) throws RestLiServiceException;

  /**
   * Ingests the latest version of the routing aspect for an entity.
   */
  public abstract <URN extends Urn> void ingest(URN urn, RecordTemplate aspect) throws RestLiServiceException;

  /**
   * Ingests the latest version of the routing aspect for an entity, with tracking context.
   */
  public abstract <URN extends Urn> void ingestWithTracking(URN urn, RecordTemplate aspect, @Nonnull
      IngestionTrackingContext trackingContext, @Nullable IngestionParams ingestionParams) throws RestLiServiceException;
}
