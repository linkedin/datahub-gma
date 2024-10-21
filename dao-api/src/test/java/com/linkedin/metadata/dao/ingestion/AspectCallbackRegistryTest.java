package com.linkedin.metadata.dao.ingestion;

import java.util.HashMap;
import java.util.Map;
import com.linkedin.testing.AspectBar;
import com.linkedin.testing.AspectFoo;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class AspectCallbackRegistryTest {
  private AspectCallbackRegistry registry;
  private AspectCallbackRoutingClient client1;
  private AspectCallbackRoutingClient client2;

  @BeforeMethod
  public void setUp() {
    client1 = new SampleAspectCallbackRoutingClient();
    client2 = new SampleAspectCallbackRoutingClient();

    Map<AspectCallbackMapKey, AspectCallbackRoutingClient> aspectCallbackMap = new HashMap<>();
    aspectCallbackMap.put(new AspectCallbackMapKey(AspectFoo.class, "entityType1"), client1);
    aspectCallbackMap.put(new AspectCallbackMapKey(AspectBar.class, "entityType2"), client2);

    registry = new AspectCallbackRegistry(aspectCallbackMap);
  }

  @Test
  public void testConstructor() {
    assertNotNull(registry);
  }

  @Test
  public void testGetAspectCallbackRoutingClient() {
    assertEquals(registry.getAspectCallbackRoutingClient(AspectFoo.class, "entityType1"), client1);
    assertEquals(registry.getAspectCallbackRoutingClient(AspectBar.class, "entityType2"), client2);
    assertNull(registry.getAspectCallbackRoutingClient(AspectFoo.class, "entityType2"));
  }

  @Test
  public void testIsRegistered() {
    assertTrue(registry.isRegistered(AspectFoo.class, "entityType1"));
    assertTrue(registry.isRegistered(AspectBar.class, "entityType2"));
    assertFalse(registry.isRegistered(AspectFoo.class, "entityType2"));
  }
}
