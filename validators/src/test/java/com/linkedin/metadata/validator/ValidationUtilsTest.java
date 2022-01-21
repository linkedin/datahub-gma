package com.linkedin.metadata.validator;

import com.linkedin.data.schema.UnionDataSchema;
import com.linkedin.testing.PizzaArrayAspect;
import com.linkedin.testing.PizzaEnumAspect;
import com.linkedin.testing.PizzaMapAspect;
import com.linkedin.testing.PizzaStringAspect;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class ValidationUtilsTest {

  @Test
  public void testUnionWithEnums() {
    UnionDataSchema dataSchema = ValidationUtils.getUnionSchema(PizzaEnumAspect.class);
    assertFalse(ValidationUtils.isUnionWithOnlyComplexMembers(dataSchema));
  }

  @Test
  public void testUnionWithMaps() {
    UnionDataSchema dataSchema = ValidationUtils.getUnionSchema(PizzaMapAspect.class);
    assertFalse(ValidationUtils.isUnionWithOnlyComplexMembers(dataSchema));
  }

  @Test
  public void testUnionWithArrays() {
    UnionDataSchema dataSchema = ValidationUtils.getUnionSchema(PizzaArrayAspect.class);
    assertFalse(ValidationUtils.isUnionWithOnlyComplexMembers(dataSchema));
  }

  @Test
  public void testUnionWithPrimitive() {
    UnionDataSchema dataSchema = ValidationUtils.getUnionSchema(PizzaStringAspect.class);
    assertFalse(ValidationUtils.isUnionWithOnlyComplexMembers(dataSchema));
  }
}
