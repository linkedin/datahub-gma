package com.linkedin.metadata.gradle.tasks;

import com.linkedin.metadata.annotations.GmaEntitiesAnnotationAllowList;
import com.linkedin.metadata.generator.SchemaGenerator;
import java.io.File;
import java.io.IOException;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.commons.io.FileUtils;
import org.gradle.api.DefaultTask;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.OutputDirectories;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;


/**
 * A task which generates metadata events (change, audit, failed audit, etc).
 */
@CacheableTask
public class GenerateMetadataEventsTask extends DefaultTask {
  private final ConfigurableFileCollection _inputModelPaths = getProject().files();
  private final ConfigurableFileCollection _resolverPaths = getProject().files();
  private final ConfigurableFileCollection _outputDirectory = getProject().files();
  private final Property<GmaEntitiesAnnotationAllowList> _allowList =
      getProject().getObjects().property(GmaEntitiesAnnotationAllowList.class);

  /**
   * The paths to resolve referenced models from.
   */
  @InputFiles
  @PathSensitive(PathSensitivity.ABSOLUTE)
  @Nonnull
  public ConfigurableFileCollection getResolverPath() {
    return _resolverPaths;
  }

  @InputFiles
  @PathSensitive(PathSensitivity.RELATIVE)
  @Nonnull
  public ConfigurableFileCollection getInputModelPaths() {
    return _inputModelPaths;
  }

  @OutputDirectories
  @Nonnull
  public ConfigurableFileCollection getOutputDirectory() {
    return _outputDirectory;
  }

  @Nonnull
  public Property<GmaEntitiesAnnotationAllowList> getEntitiesAnnotationAllowList() {
    return _allowList;
  }

  @TaskAction
  public void generateEvents() throws IOException {
    final SchemaGenerator schemaGenerator = new SchemaGenerator();

    // For caching to work across git branches, this entire directory needs to be deleted. The entire directory is
    // cached; so if anything is left in here from other branch builds it will be incorrectly cached. Delete it to
    // ensure that it is in 100% valid state after this task (and when cached).
    FileUtils.deleteDirectory(getOutputDirectory().getSingleFile());

    final String outputDir =
        getOutputDirectory().getSingleFile().toPath().resolve("com").resolve("linkedin").resolve("mxe").toString();
    schemaGenerator.generate(_resolverPaths.getFiles().stream().map(File::toString).collect(Collectors.toList()),
        _inputModelPaths.getFiles().stream().map(File::toString).collect(Collectors.toList()), outputDir,
        _allowList.get());
  }
}
