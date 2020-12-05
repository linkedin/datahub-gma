package com.linkedin.metadata.gradle;

import javax.annotation.Nonnull;
import org.gradle.api.Project;
import org.gradle.api.file.ConfigurableFileCollection;


/**
 * Extension for the metadata events plugin.
 *
 * <p>Generally this extension shouldn't be needed, but is available if you need to customize the plugin.
 */
public class MetadataEventsGeneratorPluginExtension {
  private ConfigurableFileCollection _outputDir;
  private ConfigurableFileCollection _resolverPaths;
  private ConfigurableFileCollection _inputModels;

  /**
   * Helper method to create an extension; as it has file collection properties that need initialization.
   */
  public static MetadataEventsGeneratorPluginExtension create(@Nonnull Project project, @Nonnull String name) {
    final MetadataEventsGeneratorPluginExtension extension =
        project.getExtensions().create(name, MetadataEventsGeneratorPluginExtension.class);
    extension._resolverPaths = project.files();
    extension._inputModels = project.files();
    extension._outputDir = project.files();
    return extension;
  }

  /**
   * Single directory to generate events to.
   *
   * <p>This is a {@link ConfigurableFileCollection} rather than a {@link org.gradle.api.file.DirectoryProperty} to take
   * advantage of {@link ConfigurableFileCollection#builtBy(Object...)}, which will be set to the event generation task.
   * This makes it easy to wire the tasks from this plugin to other tasks via this extension (just set the input of some
   * other task / extension as the this output dir, and you get the task dependencies for free!).
   */
  public ConfigurableFileCollection getOutputDir() {
    return _outputDir;
  }

  /**
   * Paths to use to resolve model references.
   */
  public ConfigurableFileCollection getResolverPaths() {
    return _resolverPaths;
  }

  /**
   * Paths to scan for models to generate events for.
   */
  public ConfigurableFileCollection getInputModels() {
    return _inputModels;
  }
}
