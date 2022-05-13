package com.linkedin.metadata.restli;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.restli.server.RestLiServiceException;


/**
 * A client interacts with standard GMS APIs.
 */
public abstract class BaseAspectRoutingGmsClient {

  /**
   * Retrieves the latest version of the routing aspect for an entity.
   */
  public abstract <KEY, ASPECT extends RecordTemplate> ASPECT get(KEY id) throws RestLiServiceException;

  /**
   * Ingests the latest version of the routing aspect for an entity.
   */
  public abstract <KEY, ASPECT extends RecordTemplate> void ingest(KEY id, ASPECT aspect) throws RestLiServiceException;
}
