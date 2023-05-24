package com.linkedin.metadata.restli;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.restli.server.RestLiServiceException;
import java.util.Set;

/**
 * <p>
 * A client interacts with standard GMS APIs.
 * </p>
 */
public abstract class BaseGenericAspectRoutingGmsClient {

  /**
   * Retrieves the latest version of the routing aspect for an entity.
   */
  public abstract <URN extends Urn> RecordTemplate get(URN urn, Class routingAspectClass) throws RestLiServiceException;

  /**
   * Backfill the routing aspect value for a given set of entity identified by the urns.
   */
  public abstract <URN extends Urn> BackfillResult backfill(Set<URN> urn, Class routingAspectClass) throws RestLiServiceException;

  /**
   * Ingests the latest version of the routing aspect for an entity.
   */
  public abstract <URN extends Urn> void ingest(URN urn, RecordTemplate aspect) throws RestLiServiceException;
}
