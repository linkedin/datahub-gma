package com.linkedin.metadata.dao.ingestion.preupdate;

import com.google.protobuf.Message;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.*;
import static org.mockito.Mockito.*;


public class InUpdateRoutingAccessorTest {
  private InUpdateRoutingAccessor routingInfo;
  private InUpdateRoutingClient<? extends Message> mockPreUpdateClient;

  @BeforeMethod
  public void setUp() {
    routingInfo = new InUpdateRoutingAccessor();
    mockPreUpdateClient = mock(InUpdateRoutingClient.class);
  }

  @Test
  public void testPreUpdateClientSetterAndGetter() {
    routingInfo.setPreUpdateClient(mockPreUpdateClient);
    assertEquals(mockPreUpdateClient, routingInfo.getPreUpdateClient());
  }

  @Test
  public void testRoutingActionEnum() {
    assertEquals("PROCEED", InUpdateRoutingAccessor.RoutingAction.PROCEED.name());
    assertEquals("SKIP", InUpdateRoutingAccessor.RoutingAction.SKIP.name());
  }
}
