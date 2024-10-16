package com.linkedin.metadata.dao.ingestion;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import java.util.Optional;


/**
 * An interface that defines the client for in-update callback.
*/
public interface InUpdateRoutingClient<ASPECT extends RecordTemplate> {
  /**
   * A method that routes the updates request to the appropriate custom API.
   * @param urn the urn of the asset
   * @param newAspectValue the aspect to be updated
   * @param existingAspectValue the existing aspect value
   * @return InUpdateResponse containing the updated aspect
   */
  InUpdateResponse<ASPECT> inUpdate(Urn urn, ASPECT newAspectValue, Optional<ASPECT> existingAspectValue);

  /**
   * A method that returns whether to skip processing further ingestion.
   * @return true if the ingestion should be skipped, false otherwise
   */
  default boolean isSkipProcessing() {
    return false;
  }
}