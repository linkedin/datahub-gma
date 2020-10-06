package com.linkedin.metadata.dao.equality;

import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.testing.AspectBaz;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.IOUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class DefaultEqualityTesterTest {

  @Test
  public void testSemanticEquality() throws IOException, CloneNotSupportedException {
    // "longField" is backed by Integer in DataMap when deserialized from JSON
    AspectBaz fromJson = RecordUtils.toRecordTemplate(AspectBaz.class,
        IOUtils.toString(ClassLoader.getSystemResourceAsStream("baz.json"), StandardCharsets.UTF_8));
    assertEquals(fromJson.data().get("longField").getClass(), Integer.class);

    // Clone and change the value in DataMap to Long
    AspectBaz cloned = fromJson.clone();
    cloned.setLongField(1234L);
    assertEquals(cloned.data().get("longField").getClass(), Long.class);

    // The two should still be semantically equal
    assertTrue(new DefaultEqualityTester().equals(fromJson, cloned));
  }
}
