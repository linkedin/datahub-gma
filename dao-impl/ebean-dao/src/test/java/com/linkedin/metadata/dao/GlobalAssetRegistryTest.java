package com.linkedin.metadata.dao;

import com.linkedin.testing.AspectBar;
import com.linkedin.testing.BarAsset;
import com.linkedin.testing.urn.BarUrn;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class GlobalAssetRegistryTest {
  @Test
  public void testGetInstance() {
    GlobalAssetRegistry.register(BarUrn.ENTITY_TYPE, BarAsset.class);
    try {
      GlobalAssetRegistry.register(BarUrn.ENTITY_TYPE, AspectBar.class);
      fail("should fail because of invalid aspect");
    } catch (Exception e) {
    }

    assertEquals(GlobalAssetRegistry.get(BarUrn.ENTITY_TYPE), BarAsset.class);
    assertNull(GlobalAssetRegistry.get("unknownType"));
  }
}