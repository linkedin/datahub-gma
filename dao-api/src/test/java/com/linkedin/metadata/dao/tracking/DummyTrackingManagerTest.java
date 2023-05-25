package com.linkedin.metadata.dao.tracking;

import org.testng.annotations.Test;
import com.linkedin.metadata.dao.tracking.TrackingUtils.ProcessType;

import static com.linkedin.metadata.dao.tracking.TrackingUtils.*;


public class DummyTrackingManagerTest {

  // ensure the manager can be created by default
  @Test
  public void testCreateDummyTrackingManager() {
    DummyTrackingManager manager = new DummyTrackingManager();
    ProcessType processType = ProcessType.DAO_PROCESS_START;
    byte[] id = getRandomTrackingId();

    manager.register(processType);
    manager.trackRequest(id, processType);
  }
}
