package com.linkedin.metadata.dao.ingestion;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.testing.AspectBar;
import com.linkedin.testing.AspectFoo;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class AspectCallbackMapKeyTest {

  @Test
  public void testConstructorAndGetters() {
    // Use a real class instead of mocking
    Class<? extends RecordTemplate> mockAspectClass = RecordTemplate.class;
    String entityType = "testEntity";

    // Create an instance of AspectCallbackMapKey
    AspectCallbackMapKey key = new AspectCallbackMapKey(mockAspectClass, entityType);

    // Verify that the getters return the correct values
    assertEquals(mockAspectClass, key.getAspectClass());
    assertEquals(entityType, key.getEntityType());
  }

  @Test
  public void testEquals() {
    // Create mock instances of RecordTemplate class
    Class<? extends RecordTemplate> mockAspectClass1 = AspectFoo.class;
    Class<? extends RecordTemplate> mockAspectClass2 = AspectBar.class;
    String entityType1 = "testEntity1";
    String entityType2 = "testEntity2";

    // Create instances of AspectCallbackMapKey
    AspectCallbackMapKey key1 = new AspectCallbackMapKey(mockAspectClass1, entityType1);
    AspectCallbackMapKey key2 = new AspectCallbackMapKey(mockAspectClass1, entityType1);
    AspectCallbackMapKey key3 = new AspectCallbackMapKey(mockAspectClass2, entityType1);
    AspectCallbackMapKey key4 = new AspectCallbackMapKey(mockAspectClass1, entityType2);

    // Verify equality
    assertEquals(key1, key2); // Same class and entity type
    assertNotEquals(key1, key3); // Different class, same entity type
    assertNotEquals(key1, key4); // Same class, different entity type
    assertNotEquals(key1, null); // Not equal to null
    assertNotEquals(key1, new Object()); // Not equal to a different type
  }

  @Test
  public void testHashCode() {
    // Create mock instances of RecordTemplate class
    Class<? extends RecordTemplate> mockAspectClass1 = AspectFoo.class;
    Class<? extends RecordTemplate> mockAspectClass2 = AspectBar.class;
    String entityType1 = "testEntity1";
    String entityType2 = "testEntity2";

    // Create instances of AspectCallbackMapKey
    AspectCallbackMapKey key1 = new AspectCallbackMapKey(mockAspectClass1, entityType1);
    AspectCallbackMapKey key2 = new AspectCallbackMapKey(mockAspectClass1, entityType1);
    AspectCallbackMapKey key3 = new AspectCallbackMapKey(mockAspectClass2, entityType1);
    AspectCallbackMapKey key4 = new AspectCallbackMapKey(mockAspectClass1, entityType2);

    assertEquals(key1.hashCode(), key2.hashCode());
    assertNotEquals(key1.hashCode(), key3.hashCode());
    assertNotEquals(key1.hashCode(), key4.hashCode());
  }
}