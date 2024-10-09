package com.linkedin.metadata.dao.ingestion.preupdate;

import com.google.protobuf.Message;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import static org.testng.AssertJUnit.*;
import static org.mockito.Mockito.*;


public class PreRoutingInfoTest {
  private PreRoutingInfo routingInfo;
  private PreUpdateClient<? extends Message> mockPreUpdateClient;

  @BeforeMethod
  public void setUp() {
    routingInfo = new PreRoutingInfo();
    mockPreUpdateClient = mock(PreUpdateClient.class);
  }

  @Test
  public void testPreUpdateClientSetterAndGetter() {
    routingInfo.setPreUpdateClient(mockPreUpdateClient);
    assertEquals(mockPreUpdateClient, routingInfo.getPreUpdateClient());
  }

  @Test
  public void testRoutingActionEnum() {
    assertEquals("PROCEED", PreRoutingInfo.RoutingAction.PROCEED.name());
    assertEquals("SKIP", PreRoutingInfo.RoutingAction.SKIP.name());
  }
}
