package com.linkedin.metadata.dao.utils;

import com.linkedin.testing.AspectFoo;
import org.testng.annotations.Test;

import static com.linkedin.metadata.dao.utils.SQLSchemaUtils.*;
import static org.testng.Assert.*;


public class SQLSchemaUtilsTest {

  @Test
  public void testFormatAspectName() {
    AspectFoo aspectFoo = new AspectFoo();
    String formattedAspectName = getColumnName(aspectFoo);
    assertEquals(formattedAspectName, "a_testing_aspectfoo");
  }

  @Test
  public void testChopColumnName() {
    // case 1: long name with LI domain, expect: succeed
    String longClassName1 = "com.linkedin.metadata.platform.utils.database.mysql.access.query.dao.ClassName";
    String choppedClassName = trimColumnName(longClassName1);
    assertEquals(choppedClassName, "platform.utils.database.mysql.access.query.dao.ClassName",
        "metadata should have been trimmed");

    // case 2: long name with MSFT domain, expect: succeed
    String longClassName2 = "com.microsoft.metadata.platform.utils.database.mysql.access.query.dao.ClassName";
    choppedClassName = trimColumnName(longClassName2);
    assertEquals(choppedClassName, "platform.utils.database.mysql.access.query.dao.ClassName",
        "com.microsoft.metadata should have been trimmed");

    // case 3: long name with 65 class class name, expect: fail
    String longClassName3 =
        "com.microsoft.metadata.platform.utils.database.mysql.access.query.dao.L65oooooooooooooooooooooooooooooooooooooooooooooooooooClassName";
    try {
      trimColumnName(longClassName3);
      fail("should failed to trim column name " + longClassName3);
    } catch (RuntimeException e) {
    }

    // case 3: long name with 64 class class name, expect: succeed
    String longClassName4 =
        "com.microsoft.metadata.platform.utils.database.mysql.access.query.dao.L64ooooooooooooooooooooooooooooooooooooooooooooooooooClassName";
    try {
      assertEquals(trimColumnName(longClassName4),
          "L64ooooooooooooooooooooooooooooooooooooooooooooooooooClassName");
    } catch (RuntimeException e) {
      fail("should succeed to trim column name " + longClassName4);
    }
  }

  @Test
  public void testGetColumnNameFromAnnotation() {
    assertEquals("a_bar_column", SQLSchemaUtils.getColumnNameFromAnnotation("com.linkedin.testing.AspectBar"));
  }
}