package com.linkedin.metadata.dao.tracking;

import com.linkedin.avro2pegasus.events.UUID;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class TrackingUtilsTest {
  @Test
  public void testConvertUUID() {
    String str = "This is 16 chars";
    UUID uuid = new UUID(str);
    assertEquals(TrackingUtils.convertUUID(uuid), str.getBytes());
  }
}
