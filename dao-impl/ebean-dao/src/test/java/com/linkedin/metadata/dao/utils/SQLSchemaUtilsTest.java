package com.linkedin.metadata.dao.utils;

import com.linkedin.testing.AspectFoo;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import static com.linkedin.metadata.dao.utils.SQLSchemaUtils.*;
import static org.testng.Assert.*;


public class SQLSchemaUtilsTest {

  @Factory(dataProvider = "inputList")
  public SQLSchemaUtilsTest(boolean nonDollarVirtualColumnsEnabled) {
    SQLSchemaUtils.setNonDollarVirtualColumnsEnabled(nonDollarVirtualColumnsEnabled);
  }

  @DataProvider(name = "inputList")
  public static Object[][] inputList() {
    return new Object[][] {
        { true },
        { false }
    };
  }

  @Test
  public void testGetGeneratedColumnName() {
    String generatedColumnName = getGeneratedColumnName(AspectFoo.class.getCanonicalName(), "/value");
    if (!SQLSchemaUtils.isNonDollarVirtualColumnsEnabled()) {
      assertEquals(generatedColumnName, "i_aspectfoo$value");
    } else {
      assertEquals(generatedColumnName, "i_aspectfoo0value");
    }
  }

  @Test
  public void testGetAspectColumnName() {
    assertEquals("a_aspectbar", SQLSchemaUtils.getAspectColumnName("com.linkedin.testing.AspectBar"));
  }
}