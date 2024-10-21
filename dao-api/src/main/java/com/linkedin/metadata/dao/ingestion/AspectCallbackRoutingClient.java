package com.linkedin.metadata.dao.ingestion;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.internal.IngestionParams;
import java.util.Optional;


/**
 * An interface that defines the client for aspect callback routing.
*/
public interface AspectCallbackRoutingClient<ASPECT extends RecordTemplate> {
  /**
   * A method that routes the updates request to the appropriate custom API.
   * @param urn the urn of the asset
   * @param newAspectValue the aspect to be updated
   * @param existingAspectValue the existing aspect value
   * @return AspectCallbackResponse containing the updated aspect
   */
  AspectCallbackResponse<ASPECT> routeAspectCallback(Urn urn, ASPECT newAspectValue, Optional<ASPECT> existingAspectValue);

  /**
   * A method that routes the updates request to the appropriate custom API.
   * @param urn the urn of the asset
   * @param newAspectValue the aspect to be updated
   * @param existingAspectValue the existing aspect value
   * @param ingestionParams the ingestionParams of current update
   * @return AspectCallbackResponse containing the updated aspect
   */
  default AspectCallbackResponse<ASPECT> routeAspectCallback(Urn urn, ASPECT newAspectValue,
      Optional<ASPECT> existingAspectValue, IngestionParams ingestionParams) {
    return routeAspectCallback(urn, newAspectValue, existingAspectValue);
  }
  /**
   * A method that returns whether to skip processing further ingestion.
   * @return true if the ingestion should be skipped, false otherwise
   */
  default boolean isSkipProcessing() {
    return false;
  }
}