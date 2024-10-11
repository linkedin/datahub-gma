package com.linkedin.metadata.dao.ingestion.preupdate;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;


/**
 * An interface that defines methods to route update requests to the appropriate custom APIs for pre-ingestion process.
 */

public interface PreUpdateRoutingClient<ASPECT extends RecordTemplate> {
  /**
   * A method that routes the update request to the appropriate custom API.
   * @param urn the urn of the asset
   * @param aspect the aspect to be updated
   * @return the updated aspect
   */
  PreUpdateResponse<ASPECT> preUpdate(Urn urn, ASPECT aspect);
}