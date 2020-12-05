package com.linkedin.metadata.generator;

import org.junit.jupiter.api.Test;

import static com.linkedin.metadata.generator.SchemaGeneratorUtil.*;
import static org.assertj.core.api.Assertions.*;


public class TestSchemaGeneratorUtil {

  private static final String TEST_NAME = "BarUrn";

  @Test
  public void testDeCapitalize() {
    assertThat(deCapitalize(TEST_NAME)).isEqualTo("barUrn");
  }

  @Test
  public void testGetEntityName() {
    assertThat(getEntityName(TEST_NAME)).isEqualTo("Bar");
  }

  @Test
  public void testStripNamespace() {
    assertThat(stripNamespace(TEST_NAME)).isEqualTo("BarUrn");
  }

  @Test
  public void testStripIllegalNamespace() {
    assertThatThrownBy(() -> stripNamespace(TEST_NAME + ".")).isInstanceOf(IllegalArgumentException.class);
  }
}