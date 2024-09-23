package com.linkedin.metadata.dao.utils;

import com.linkedin.testing.AspectFoo;
import com.linkedin.testing.urn.BarUrn;
import com.linkedin.testing.urn.FooUrn;
import org.testng.annotations.Test;

import static com.linkedin.metadata.dao.utils.SQLSchemaUtils.*;
import static org.testng.Assert.*;


public class SQLSchemaUtilsTest {

  @Test
  public void testGetGeneratedColumnName() {
    String generatedColumnName =
        getGeneratedColumnName(FooUrn.ENTITY_TYPE, AspectFoo.class.getCanonicalName(), "/value", false);
    assertEquals(generatedColumnName, "i_aspectfoo$value");

    generatedColumnName =
        getGeneratedColumnName(FooUrn.ENTITY_TYPE, AspectFoo.class.getCanonicalName(), "/value", true);
    assertEquals(generatedColumnName, "i_aspectfoo0value");
  }

  @Test
  public void testGetAspectColumnName() {
    assertEquals("a_aspectbar",
        SQLSchemaUtils.getAspectColumnName(BarUrn.ENTITY_TYPE, "com.linkedin.testing.AspectBar"));
  }
}