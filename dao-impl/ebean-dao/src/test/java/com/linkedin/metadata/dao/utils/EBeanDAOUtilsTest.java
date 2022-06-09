package com.linkedin.metadata.dao.utils;

import com.linkedin.testing.urn.FooUrn;
import java.net.URISyntaxException;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class EBeanDAOUtilsTest {

  @Test
  public void testCeilDiv() {
    assertEquals(EBeanDAOUtils.ceilDiv(1, 1), 1);
    assertEquals(EBeanDAOUtils.ceilDiv(2, 1), 2);
    assertEquals(EBeanDAOUtils.ceilDiv(3, 1), 3);
    assertEquals(EBeanDAOUtils.ceilDiv(3, 2), 2);
    assertEquals(EBeanDAOUtils.ceilDiv(1, 2), 1);
    assertEquals(EBeanDAOUtils.ceilDiv(-3, 2), -1);
    assertEquals(EBeanDAOUtils.ceilDiv(-3, -2), 2);
  }

  @Test
  public void testGetEntityType() {
    assertEquals(EBeanDAOUtils.getEntityType(FooUrn.class), "foo");
  }

  @Test
  public void testGetUrn() throws URISyntaxException {
    assertEquals(EBeanDAOUtils.getUrn("urn:li:foo:123", FooUrn.class), new FooUrn(123));
  }
}