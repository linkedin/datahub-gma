package com.linkedin.metadata.dao.ingestion.preupdate;

import com.linkedin.data.template.RecordTemplate;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * A registry which maintains mapping of aspects and their getPreUpdateRoutingClient.
 */
public interface RestliPreUpdateAspectRegistry {

  /**
   * Get PreUpdateRoutingClient for an aspect.
   */
  @Nullable
  <ASPECT extends RecordTemplate> RestliCompliantPreUpdateRoutingClient getPreUpdateRoutingClient(@Nonnull final ASPECT aspect);

  /**
   * Check if PreUpdateRoutingClient is registered for an aspect.
   */
  <ASPECT extends RecordTemplate> boolean isRegistered(@Nonnull final Class<ASPECT> aspectClass);

}