package com.linkedin.metadata.generator;

import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.linkedin.metadata.annotations.AlwaysAllowList;
import com.linkedin.metadata.annotations.GmaAnnotationParser;
import com.linkedin.metadata.annotations.testing.TestModels;
import java.io.File;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Java6Assertions.*;


public class TestEventSpec {

  @Test
  public void testEventSpecParse(@Nonnull @TempDir File tempDir) throws Exception {
    final File inputDir = tempDir.toPath().resolve("testModels").toFile();
    FileUtils.copyInputStreamToFile(TestModels.getTestModelStream("com/linkedin/testing/AnnotatedAspectBar.pdl"),
        inputDir.toPath().resolve("com/linkedin/testing/AnnotatedAspectBar.pdl").toFile());
    FileUtils.copyInputStreamToFile(TestModels.getTestModelStream("com/linkedin/testing/AnnotatedAspectFoo.pdl"),
        inputDir.toPath().resolve("com/linkedin/testing/AnnotatedAspectFoo.pdl").toFile());
    FileUtils.copyInputStreamToFile(TestModels.getTestModelStream("com/linkedin/testing/AnnotatedAspectOtherFoo.pdl"),
        inputDir.toPath().resolve("com/linkedin/testing/AnnotatedAspectOtherFoo.pdl").toFile());

    final SchemaAnnotationRetriever schemaAnnotationRetriever = new SchemaAnnotationRetriever(inputDir.toString());
    final String[] sources = {inputDir.toString()};
    final List<EventSpec> eventSpecs = schemaAnnotationRetriever.generate(sources);

    // Check if annotations are correctly generated.
    final Multimap<String, EventSpec> eventSpecMap = mapAspectToUrn(eventSpecs);
    assertThat(eventSpecMap.asMap()).containsOnlyKeys("com.linkedin.testing.FooUrn", "com.linkedin.testing.BarUrn");
    assertThat(eventSpecMap.get("com.linkedin.testing.FooUrn")
        .stream()
        .map(EventSpec::getValueType)
        .collect(Collectors.toList())).contains("AnnotatedAspectFoo", "AnnotatedAspectOtherFoo");
    assertThat(eventSpecMap.get("com.linkedin.testing.BarUrn")
        .stream()
        .map(EventSpec::getValueType)
        .collect(Collectors.toList())).contains("AnnotatedAspectBar");
  }

  @Test
  public void testEventSpecParseEntitiesAnnotation(@Nonnull @TempDir File tempDir) throws Exception {
    final File inputDir = tempDir.toPath().resolve("testModels").toFile();
    FileUtils.copyInputStreamToFile(TestModels.getTestModelStream("com/linkedin/testing/CommonAspect.pdl"),
        inputDir.toPath().resolve("com/linkedin/testing/CommonAspect.pdl").toFile());

    final SchemaAnnotationRetriever schemaAnnotationRetriever =
        new SchemaAnnotationRetriever(inputDir.toString(), new GmaAnnotationParser(new AlwaysAllowList()));
    final String[] sources = {inputDir.toString()};
    final List<EventSpec> eventSpecs = schemaAnnotationRetriever.generate(sources);

    // Check if annotations are correctly generated.
    final Multimap<String, EventSpec> eventSpecMap = mapAspectToUrn(eventSpecs);
    assertThat(eventSpecMap.asMap()).containsOnlyKeys("com.linkedin.testing.FooUrn", "com.linkedin.testing.BarUrn");
    assertThat(eventSpecMap.get("com.linkedin.testing.FooUrn")
        .stream()
        .map(EventSpec::getValueType)
        .collect(Collectors.toList())).contains("CommonAspect");
    assertThat(eventSpecMap.get("com.linkedin.testing.BarUrn")
        .stream()
        .map(EventSpec::getValueType)
        .collect(Collectors.toList())).contains("CommonAspect");
  }

  @Nonnull
  private Multimap<String, EventSpec> mapAspectToUrn(@Nonnull List<EventSpec> eventSpecs) {
    return Multimaps.index(eventSpecs, EventSpec::getUrn);
  }
}