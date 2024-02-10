package com.linkedin.metadata.dao.utils;

import com.linkedin.testing.AspectFoo;
import org.testng.annotations.Test;

import static com.linkedin.metadata.dao.utils.SQLSchemaUtils.*;
import static org.testng.Assert.*;


public class SQLSchemaUtilsTest {

  @Test
  public void testGetGeneratedColumnName() {
    String generatedColumnName = getGeneratedColumnName(AspectFoo.class.getCanonicalName(), "/value", false);
    assertEquals(generatedColumnName, "i_aspectfoo$value");

    generatedColumnName = getGeneratedColumnName(AspectFoo.class.getCanonicalName(), "/value", true);
    assertEquals(generatedColumnName, "i_aspectfoo0value");
  }

  @Test
  public void testGetAspectColumnName() {
    assertEquals("a_aspectbar", SQLSchemaUtils.getAspectColumnName("com.linkedin.testing.AspectBar"));
  }
}