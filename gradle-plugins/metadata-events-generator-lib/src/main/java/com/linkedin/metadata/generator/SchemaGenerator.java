package com.linkedin.metadata.generator;

import java.io.IOException;
import java.util.Collection;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.rythmengine.Rythm;


public class SchemaGenerator {

  /**
   * Generates event schemas for the models in the given path.
   *
   * @param resolverPaths paths to use to resolve models
   * @param sourcePaths the paths to find models to generate events for
   * @param generatedFileOutput the path to write events to
   * @throws IOException
   */
  public void generate(@Nonnull Collection<String> resolverPaths, @Nonnull Collection<String> sourcePaths,
      @Nonnull String generatedFileOutput) throws IOException {
    final SchemaAnnotationRetriever schemaAnnotationRetriever =
        new SchemaAnnotationRetriever(resolverPaths.stream().collect(Collectors.joining(":")));
    schemaAnnotationRetriever.generate(sourcePaths.toArray(new String[sourcePaths.size()]));

    final EventSchemaComposer eventSchemaComposer = new EventSchemaComposer();
    eventSchemaComposer.setupRythmEngine();
    eventSchemaComposer.render(schemaAnnotationRetriever.generate(sourcePaths.toArray(new String[0])),
        generatedFileOutput);

    Rythm.shutdown();
  }
}