package com.linkedin.metadata.generator;

import com.linkedin.metadata.annotations.testing.TestModels;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import javax.annotation.Nonnull;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rythmengine.Rythm;

import static com.linkedin.metadata.generator.SchemaGeneratorConstants.*;
import static com.linkedin.metadata.generator.TestEventSpec.*;
import static org.assertj.core.api.Assertions.*;


public class TestEventSchemaComposer {

  private static final String TEST_NAMESPACE = "bar" + File.separator + "annotatedAspectBar";

  @TempDir
  File tempDir;
  File inputDir;
  File outputDir;

  @BeforeEach
  public void copyTestData() throws IOException {
    inputDir = tempDir.toPath().resolve("testModels").toFile();
    outputDir = tempDir.toPath().resolve("events").toFile();
    FileUtils.copyInputStreamToFile(TestModels.getTestModelStream("com/linkedin/testing/AnnotatedAspectBar.pdl"),
        inputDir.toPath().resolve("com/linkedin/testing/AnnotatedAspectBar.pdl").toFile());
  }

  private void populateEvents() throws Exception {
    SchemaAnnotationRetriever schemaAnnotationRetriever = new SchemaAnnotationRetriever(inputDir.getPath());
    final String[] sources = {inputDir.getPath()};
    EventSchemaComposer eventSchemaComposer = new EventSchemaComposer();
    eventSchemaComposer.setupRythmEngine();
    eventSchemaComposer.render(schemaAnnotationRetriever.generate(sources), outputDir.toString());
    Rythm.shutdown();
  }

  private void assertSame(@Nonnull MetadataEventType eventType) throws URISyntaxException {
    assertThat(outputDir).exists();
    final File actualEvent =
        outputDir.toPath().resolve(TEST_NAMESPACE).resolve(eventType.getName() + PDL_SUFFIX).toFile();
    assertThat(actualEvent).hasSameContentAs(new File(this.getClass()
        .getClassLoader()
        .getResource("expectedSchemas/pegasus/com/linkedin/mxe" + File.separator + TEST_NAMESPACE + File.separator
            + eventType.getName() + PDL_SUFFIX)
        .toURI()));
  }

  @Test
  public void testMCESchemaRender() throws Exception {
    populateEvents();
    assertSame(MetadataEventType.CHANGE);
  }

  @Test
  public void testFMCESchemaRender() throws Exception {
    populateEvents();
    assertSame(MetadataEventType.FAILED_CHANGE);
  }

  @Test
  public void testMAESchemaRender() throws Exception {
    populateEvents();
    assertSame(MetadataEventType.AUDIT);
  }
}