package com.linkedin.metadata.dao.ingestion.preupdate;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import java.util.Optional;


/**
 * An interface that defines methods to route update requests to the appropriate custom APIs for pre-ingestion process.
 */

public interface InUpdateRoutingClient<ASPECT extends RecordTemplate> {
  /**
   * A method that routes the update request to the appropriate custom API.
   * @param urn the urn of the asset
   * @param newAspectValue the aspect to be updated
   * @param existingAspectValue the existing aspect value
   * @return the updated aspect,
   */
  InUpdateResponse<ASPECT> inUpdate(Urn urn, ASPECT newAspectValue, Optional<ASPECT> existingAspectValue);

  /**
   * A method that checks if normal ingestion should be skipped.
   * @return true if the normal ingestion should be skipped, false otherwise
   */
  boolean isSkipProcessing();
}