package com.linkedin.metadata.dao.ingestion.preupdate;

import com.google.protobuf.Message;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.*;
import static org.mockito.Mockito.*;


public class PreUpdateRoutingAccessorTest {
  private PreUpdateRoutingAccessor routingInfo;
  private PreUpdateRoutingClient<? extends Message> mockPreUpdateClient;

  @BeforeMethod
  public void setUp() {
    routingInfo = new PreUpdateRoutingAccessor();
    mockPreUpdateClient = mock(PreUpdateRoutingClient.class);
  }

  @Test
  public void testPreUpdateClientSetterAndGetter() {
    routingInfo.setPreUpdateClient(mockPreUpdateClient);
    assertEquals(mockPreUpdateClient, routingInfo.getPreUpdateClient());
  }

  @Test
  public void testRoutingActionEnum() {
    assertEquals("PROCEED", PreUpdateRoutingAccessor.RoutingAction.PROCEED.name());
    assertEquals("SKIP", PreUpdateRoutingAccessor.RoutingAction.SKIP.name());
  }
}
