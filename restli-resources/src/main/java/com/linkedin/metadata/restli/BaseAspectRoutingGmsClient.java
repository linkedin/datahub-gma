package com.linkedin.metadata.restli;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.restli.server.RestLiServiceException;
import java.util.Map;
import java.util.Set;


/**
 * A client interacts with standard GMS APIs.
 */
public abstract class BaseAspectRoutingGmsClient<ASPECT extends RecordTemplate> {

  /**
   * Retrieves the latest version of the routing aspect for an entity.
   */
  public abstract <URN extends Urn> ASPECT get(URN urn) throws RestLiServiceException;

  /**
   * Batch retrieve the latest version of the routing aspect for a set of entities.
   */
  public abstract <URN extends Urn> Map<URN, ASPECT> batchGet(Set<URN> urn) throws RestLiServiceException;

  /**
   * Ingests the latest version of the routing aspect for an entity.
   */
  public abstract <URN extends Urn> void ingest(URN urn, ASPECT aspect) throws RestLiServiceException;
}
