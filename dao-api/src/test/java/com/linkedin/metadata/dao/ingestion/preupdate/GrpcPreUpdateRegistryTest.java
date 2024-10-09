package com.linkedin.metadata.dao.ingestion.preupdate;

import com.linkedin.data.template.RecordTemplate;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;


public class GrpcPreUpdateRegistryTest {

  private GrpcPreUpdateRegistry<RecordTemplate> registry;
  private RoutingMap mockRoutingMap;
  private RecordTemplate mockAspectInstance;

  @BeforeMethod
  public void setup() {
    registry = new GrpcPreUpdateRegistry<>();
    mockRoutingMap = mock(RoutingMap.class);
    mockAspectInstance = mock(RecordTemplate.class);
  }

  @Test
  public void testRegisterPreUpdateLambda() throws InstantiationException, IllegalAccessException {
    registry.registerPreUpdateLambda(mockAspectInstance.getClass(), mockRoutingMap);
    System.out.println(registry.getPreUpdateRoutingClient(mockAspectInstance));
    assertTrue(registry.isRegistered((Class<RecordTemplate>) mockAspectInstance.getClass()));
  }

  @Test
  public void testGetPreUpdateRoutingClient() {
    registry.registerPreUpdateLambda(mockAspectInstance.getClass(), mockRoutingMap);
    RoutingMap retrievedRoutingMap = registry.getPreUpdateRoutingClient(mockAspectInstance);
    assertEquals(mockRoutingMap, retrievedRoutingMap);
  }

  @Test
  public void testIsRegistered() {
    assertFalse(registry.isRegistered((Class<RecordTemplate>) mockAspectInstance.getClass()));
    registry.registerPreUpdateLambda(mockAspectInstance.getClass(), mockRoutingMap);
    assertTrue(registry.isRegistered((Class<RecordTemplate>) mockAspectInstance.getClass()));
  }
}
