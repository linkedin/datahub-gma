package com.linkedin.metadata.dao.tracking;

import javax.annotation.Nonnull;


/**
 * A dummy tracking manager that doesn't actually track any requests.
 */
public class DummyTrackingManager extends BaseTrackingManager {

  @Override
  public void register(@Nonnull TrackingUtils.ProcessType processType) {
    // Do nothing
  }

  @Override
  public void trackRequest(@Nonnull byte[] id, @Nonnull TrackingUtils.ProcessType process) {
    // Do nothing
  }
}
