package com.linkedin.metadata.dao.ingestion.preupdate;

import com.google.protobuf.Message;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;


public class RoutingMapTest {
  private RoutingMap routingMap;
  private PreUpdateClient<? extends Message> mockPreUpdateClient;

  @BeforeMethod
  public void setUp() {
    routingMap = new RoutingMap();
    mockPreUpdateClient = mock(PreUpdateClient.class);
  }

  @Test
  public void testPreUpdateClientSetterAndGetter() {
    routingMap.setPreUpdateClient(mockPreUpdateClient);
    assertEquals(mockPreUpdateClient, routingMap.getPreUpdateClient());
  }

  @Test
  public void testRoutingActionEnum() {
    assertEquals("PROCEED", RoutingMap.RoutingAction.PROCEED.name());
    assertEquals("SKIP", RoutingMap.RoutingAction.SKIP.name());
  }
}
