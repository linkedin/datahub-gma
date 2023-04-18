package com.linkedin.metadata.generator;

import com.linkedin.avro2pegasus.events.KafkaAuditHeader;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.metadata.annotations.GmaEntitiesAnnotationAllowListImpl;
import com.linkedin.metadata.annotations.testing.TestModels;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.linkedin.metadata.events.ChangeType;
import com.linkedin.pegasus.generator.DataSchemaParser;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rythmengine.Rythm;

import static com.linkedin.metadata.generator.SchemaGeneratorConstants.*;
import static org.assertj.core.api.Assertions.*;


public class TestEventSchemaComposer {

  private static final String TEST_NAMESPACE = "com/linkedin/testing/mxe/bar" + File.separator + "annotatedAspectBar";

  @TempDir
  static File tempDir;
  static File inputDir;
  static File outputDir;

  @BeforeAll
  public static void prepareTests() throws Exception {
    inputDir = tempDir.toPath().resolve("testModels").toFile();
    outputDir = tempDir.toPath().resolve("events").toFile();
    FileUtils.copyInputStreamToFile(TestModels.getTestModelStream("com/linkedin/testing/AnnotatedAspectBar.pdl"),
            inputDir.toPath().resolve("com/linkedin/testing/AnnotatedAspectBar.pdl").toFile());
    FileUtils.copyInputStreamToFile(TestModels.getTestModelStream("com/linkedin/testing/AnotherAspectBar.pdl"),
            inputDir.toPath().resolve("com/linkedin/testing/AnotherAspectBar.pdl").toFile());
    FileUtils.copyInputStreamToFile(TestModels.getTestModelStream("com/linkedin/testing/BarUrn.pdl"),
            inputDir.toPath().resolve("com/linkedin/testing/BarUrn.pdl").toFile());
    FileUtils.copyInputStreamToFile(TestModels.getTestModelStream("com/linkedin/testing/BarAspect.pdl"),
            inputDir.toPath().resolve("com/linkedin/testing/BarAspect.pdl").toFile());

    populateEvents();
  }

  private static void populateEvents() throws Exception {
    SchemaAnnotationRetriever schemaAnnotationRetriever = new SchemaAnnotationRetriever(inputDir.getPath(),
            GmaEntitiesAnnotationAllowListImpl.DEFAULT, "com.linkedin.testing.mxe");
    final String[] sources = {inputDir.getPath()};
    EventSchemaComposer eventSchemaComposer = new EventSchemaComposer();
    eventSchemaComposer.setupRythmEngine();
    eventSchemaComposer.render(schemaAnnotationRetriever.generate(sources), outputDir.toString() + "/com/linkedin/testing/mxe");
    Rythm.shutdown();
  }

  @Test
  public void testOutputEventsAreParseable() throws Exception {
    List<String> outputResolutionSources = new ArrayList<>();
    outputResolutionSources.add(KafkaAuditHeader.class.getProtectionDomain().getCodeSource().getLocation().toString());
    outputResolutionSources.add(ChangeType.class.getProtectionDomain().getCodeSource().getLocation().toString());
    outputResolutionSources.add(inputDir.getPath());
    outputResolutionSources.add(outputDir.getPath());
    DataSchemaParser outputParser = new DataSchemaParser(String.join(File.pathSeparator, outputResolutionSources));
    DataSchemaParser.ParseResult result = outputParser.parseSources(new String[]{outputDir.getPath()});

    Set<String> outputs = result.getSchemaAndLocations().keySet().stream().filter(schema -> schema.getType() == DataSchema.Type.RECORD)
            .map(s -> (RecordDataSchema)s).map(RecordDataSchema::getFullName).collect(Collectors.toSet());
    assertThat(outputs).contains(
            "com.linkedin.testing.mxe.bar.annotatedAspectBar.MetadataChangeEvent",
            "com.linkedin.testing.mxe.bar.annotatedAspectBar.FailedMetadataChangeEvent",
            "com.linkedin.testing.mxe.bar.annotatedAspectBar.MetadataAuditEvent",
            "com.linkedin.testing.mxe.bar.MCE_BarAspect",
            "com.linkedin.testing.mxe.bar.FailedMCE_BarAspect"
    );
  }

  private void assertSame(File baseOutputDir, File relativeFilePath) throws URISyntaxException, IOException {
    File outputPath = baseOutputDir.toPath().resolve(relativeFilePath.toPath()).toFile();
    assertThat(outputPath).exists();

    try(InputStream expected = this.getClass().getClassLoader().getResourceAsStream(relativeFilePath.toString());
        InputStream actual = FileUtils.openInputStream(outputPath)) {
      assertThat(expected).hasSameContentAs(actual);
    }
  }

  @Test
  public void testMCESchemaRender() throws Exception {
    assertSame(outputDir,
            new File(TEST_NAMESPACE).toPath().resolve(MetadataEventType.CHANGE.getName() + PDL_SUFFIX).toFile());
  }

  @Test
  public void testFMCESchemaRender() throws Exception {
    assertSame(outputDir,
            new File(TEST_NAMESPACE).toPath().resolve(MetadataEventType.FAILED_CHANGE.getName() + PDL_SUFFIX).toFile());
  }

  @Test
  public void testMAESchemaRender() throws Exception {
    assertSame(outputDir,
            new File(TEST_NAMESPACE).toPath().resolve(MetadataEventType.AUDIT.getName() + PDL_SUFFIX).toFile());
  }

  @Test
  public void testUnionSchemaRender() throws Exception {
    assertSame(outputDir, new File("com/linkedin/testing/mxe/bar/MCE_BarAspect.pdl"));
    assertSame(outputDir, new File("com/linkedin/testing/mxe/bar/FailedMCE_BarAspect.pdl"));
  }
}