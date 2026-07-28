package com.linkedin.metadata.dao.tracking;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class DaoReadContextTest {

  @AfterMethod
  public void tearDown() {
    DaoReadContext.clear();
  }

  @Test
  public void testDefaultsToFalse() {
    assertFalse(DaoReadContext.isInternalRead());
  }

  @Test
  public void testMarkThenClear() {
    DaoReadContext.markInternalRead();
    assertTrue(DaoReadContext.isInternalRead());

    DaoReadContext.clear();
    assertFalse(DaoReadContext.isInternalRead());
  }

  @Test
  public void testClearIsIdempotent() {
    DaoReadContext.clear();
    DaoReadContext.clear();
    assertFalse(DaoReadContext.isInternalRead());
  }
}
