package com.linkedin.metadata.dao.tracking;

import javax.annotation.Nonnull;
import com.linkedin.metadata.dao.tracking.TrackingUtils.ProcessType;


/**
 * A base class for all metadata tracking managers.
 */
public interface BaseTrackingManager {

  /**
   * Initialize the tracking request queue with the default properties.
   * @param processType the metadata process type of the request.
   */
  void register(@Nonnull ProcessType processType);

  /**
   * Capture the client request to the tracking manager.
   * @param id the unique tracking ID of the request.
   * @param processType the metadata process type of the request.
   */
  void trackRequest(@Nonnull byte[] id, @Nonnull ProcessType processType);
}
