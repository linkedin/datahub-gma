package com.linkedin.metadata.dao.utils;

import com.linkedin.metadata.dao.GlobalAssetRegistry;
import com.linkedin.testing.AspectFoo;
import com.linkedin.testing.BarAsset;
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
    GlobalAssetRegistry.register(BarUrn.ENTITY_TYPE, BarAsset.class);
    assertEquals("a_aspect_bar",
        SQLSchemaUtils.getAspectColumnName(BarUrn.ENTITY_TYPE, "com.linkedin.testing.AspectBar"));
  }

  @Test
  public void testGetExpressionIndexName() {
    assertEquals(
        SQLSchemaUtils.getExpressionIndexName(FooUrn.ENTITY_TYPE, AspectFoo.class.getCanonicalName(), "/value"),
        "e_aspectfoo0value");
    assertEquals(
        SQLSchemaUtils.getExpressionIndexName(FooUrn.ENTITY_TYPE, AspectFoo.class.getCanonicalName(), "/value/fooBar"),
        "e_aspectfoo0value0fooBar");
  }

  @Test
  public void testGetExpressionIndexNameRelationship() {
    assertEquals(
        SQLSchemaUtils.getExpressionIndexNameRelationship("/value/blue"),
        "e_metadata0bvalue0blue");
  }

}