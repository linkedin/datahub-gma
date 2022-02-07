package com.linkedin.metadata.generator;

import com.linkedin.data.schema.Name;
import com.linkedin.data.schema.RecordDataSchema;
import org.junit.jupiter.api.Test;

import static com.linkedin.metadata.generator.SchemaGeneratorUtil.*;
import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.*;


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

  @Test
  public void shouldGetNamespace() {
    assertEquals(SchemaGeneratorUtil.getNamespace("com.a.b.A"), "com.a.b");
  }

  @Test
  public void testIsSnapshotSchema() {
    assertTrue(SchemaGeneratorUtil.isSnapshotSchema(new RecordDataSchema(new Name("XYZSnapshot"),
        RecordDataSchema.RecordType.RECORD)));

    assertFalse(SchemaGeneratorUtil.isSnapshotSchema(new RecordDataSchema(new Name("XYZ"),
        RecordDataSchema.RecordType.RECORD)));
  }
}