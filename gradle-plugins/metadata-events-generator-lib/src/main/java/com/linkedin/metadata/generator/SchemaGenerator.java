package com.linkedin.metadata.generator;

import com.linkedin.metadata.annotations.GmaEntitiesAnnotationAllowList;
import java.io.IOException;
import java.util.Collection;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.rythmengine.Rythm;


public class SchemaGenerator {
  private void generate(@Nonnull Collection<String> sourcePaths, @Nonnull String generatedFileOutput,
      @Nonnull SchemaAnnotationRetriever schemaAnnotationRetriever) throws IOException {
    schemaAnnotationRetriever.generate(sourcePaths.toArray(new String[sourcePaths.size()]));

    final EventSchemaComposer eventSchemaComposer = new EventSchemaComposer();
    eventSchemaComposer.setupRythmEngine();
    eventSchemaComposer.render(schemaAnnotationRetriever.generate(sourcePaths.toArray(new String[0])),
        generatedFileOutput);

    Rythm.shutdown();
  }

  /**
   * Generates event schemas for the models in the given path.
   *
   * @param resolverPaths paths to use to resolve models
   * @param sourcePaths the paths to find models to generate events for
   * @param generatedFileOutput the path to write events to
   * @param allowList the list of aspects which are allowed to use the plural annotation
   * @throws IOException exception on input error
   */
  public void generate(@Nonnull Collection<String> resolverPaths, @Nonnull Collection<String> sourcePaths,
      @Nonnull String generatedFileOutput, @Nonnull GmaEntitiesAnnotationAllowList allowList) throws IOException {
    generate(sourcePaths, generatedFileOutput,
        new SchemaAnnotationRetriever(resolverPaths.stream().collect(Collectors.joining(":")), allowList, null));
  }

  /**
   * Generates event schemas for the models in the given path.
   *
   * @param resolverPaths paths to use to resolve models
   * @param sourcePaths the paths to find models to generate events for
   * @param generatedFileOutput the path to write events to
   * @throws IOException exception on input error
   */
  public void generate(@Nonnull Collection<String> resolverPaths, @Nonnull Collection<String> sourcePaths,
      @Nonnull String generatedFileOutput) throws IOException {
    generate(sourcePaths, generatedFileOutput,
        new SchemaAnnotationRetriever(resolverPaths.stream().collect(Collectors.joining(":"))));
  }
}