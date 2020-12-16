package com.linkedin.metadata.gradle;

import com.linkedin.metadata.gradle.tasks.GenerateMetadataEventsTask;
import com.linkedin.pegasus.gradle.PegasusPlugin;
import com.linkedin.pegasus.gradle.tasks.GenerateDataTemplateTask;
import java.io.File;
import java.nio.file.Path;
import java.util.Locale;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.plugins.JavaPluginConvention;
import org.gradle.api.tasks.SourceSet;
import org.gradle.jvm.tasks.Jar;


/**
 * Gradle plugin which auto generates the schema for metadata events (change, audit, failed change, etc) based on
 * annotations of PDL metadata models.
 *
 * <p><em>Extensions</em></p>
 *
 * <p>This plugin makes one extension per source set. The main sourceset extension is "metadataEvents". Other sourcesets
 * follow a pattern of adding the sourceset as a prefix, e.g. "testMetadataEvents". See {@link
 * MetadataEventsGeneratorPluginExtension} for properties.</p>
 *
 * <p><em>Inputs</em></p>
 * <p>This plugin will, by default, find all PDL files in your src/$SOURCESET/pegasus directories. It will scan them for
 * {@code @gma} annotations, and generate metadata event (MCE, MAE, FMCE) PDL schemas based on those annotations.</p>
 *
 * <p>This will piggy back off the pegasus plugin and use the {@code dataModel} configurations for dependencies.</p>
 *
 * <p><em>Outputs</em></p>
 * <p>For every source set, this will generate metadata events for each aspect-entity relationship defined in
 * {@code @gma.aspect.entity} annotations.</p>
 */
public final class MetadataEventsGeneratorPlugin implements Plugin<Project> {
  private static final Pattern TEST_DIR_REGEX = Pattern.compile("^(integ)?[Tt]est");

  @Override
  public void apply(Project project) {
    // Apply the open source plugin if the internal plugin is not applied (they are not compatible); ask LI to
    // separately still apply internal plugin if applying this plugin standalone.
    if (!project.getPlugins().hasPlugin("li-pegasus2")) {
      project.getPlugins().apply(PegasusPlugin.class);
    }

    // Mimic the pegasus plugin behavior. It crawls through source sets.
    final JavaPluginConvention javaPluginConvention = project.getConvention().getPlugin(JavaPluginConvention.class);

    javaPluginConvention.getSourceSets().all(sourceSet -> {
      setupTasks(project, sourceSet);
    });
  }

  @Nonnull
  public static String getExtensionName(@Nonnull SourceSet sourceSet) {
    final String baseName = sourceSet.getName().toLowerCase(Locale.ENGLISH);
    return baseName.equals("main") ? "metadataEvents" : baseName + "MetadataEvents";
  }

  // start: Copied from pegasus plugin
  private static boolean isTestSourceSet(SourceSet sourceSet) {
    return TEST_DIR_REGEX.matcher(sourceSet.getName()).find();
  }

  @Nonnull
  private static Configuration getDataModelConfig(@Nonnull Project project, @Nonnull SourceSet sourceSet) {
    return isTestSourceSet(sourceSet) ? project.getConfigurations().getByName("testDataModel")
        : project.getConfigurations().getByName("dataModel");
  }
  // end: copied from pegasus plugin

  private void setupTasks(@Nonnull Project project, @Nonnull SourceSet sourceSet) {
    final MetadataEventsGeneratorPluginExtension extension =
        MetadataEventsGeneratorPluginExtension.create(project, getExtensionName(sourceSet));

    // By default, look for models in src/<sourceset>/pegasus (same as pegasus plugin).
    String inputPath = "src" + File.separatorChar + sourceSet.getName() + File.separatorChar + "pegasus";
    extension.getInputModels().setFrom(inputPath);

    // Use the same configuration as pegasus. Need to also include input models on the resolver path.
    extension.getResolverPaths().setFrom(getDataModelConfig(project, sourceSet), inputPath);

    final GenerateMetadataEventsTask generateMetadataEventsTask = project.getTasks()
        .create(sourceSet.getTaskName("generate", "metadataEventsSchema"), GenerateMetadataEventsTask.class, task -> {
          task.getInputModelPaths().setFrom(extension.getInputModels());
          task.getResolverPath().setFrom(extension.getResolverPaths());
          task.getOutputDirectory().setFrom(extension.getOutputDir());
          task.getEntitiesAnnotationAllowList().set(extension.getEntitiesAnnotationAllowList());
        });

    final Path baseOutputPath = project.getRootProject()
        .getBuildDir()
        .toPath()
        .resolve(project.getName())
        .resolve(generateMetadataEventsTask.getName());
    extension.getOutputDir().setFrom(baseOutputPath.toFile());
    extension.getOutputDir().builtBy(generateMetadataEventsTask);

    // null if data template generation is not enabled.
    final GenerateDataTemplateTask dataTemplateTask =
        (GenerateDataTemplateTask) project.getTasks().findByName(sourceSet.getTaskName("generate", "dataTemplate"));

    // Make a separate GenerateDataTemplateTask because the pegasus plugin does not easily allow for customization
    // (adding a second input directory - our generated events). This new task will actually override some of the common
    // models involve with the original task. We have to output to the same directory so the jar task finds them.
    // Do not generate avro for these events. Currently, we need to generate 1.4 and 1.7 internally, so we'll do that
    // in a different module / library than from where the models are defined.
    if (dataTemplateTask != null) {
      final Task eventsDataTemplate = project.getTasks()
          .create(sourceSet.getTaskName("generate", "metadataEventDataTemplate"), GenerateDataTemplateTask.class,
              task -> {
                task.dependsOn(generateMetadataEventsTask);
                task.setInputDir(baseOutputPath.toFile());
                task.setDestinationDir(dataTemplateTask.getDestinationDir());
                task.setCodegenClasspath(dataTemplateTask.getCodegenClasspath());
                task.setResolverPath(
                    project.files(baseOutputPath, extension.getInputModels(), extension.getResolverPaths()));
                dataTemplateTask.finalizedBy(task);
              });

      // make sure that java source files have been generated before compiling them
      final JavaPluginConvention javaPluginConvention = project.getConvention().getPlugin(JavaPluginConvention.class);
      final SourceSet targetSourceSet =
          javaPluginConvention.getSourceSets().getByName(sourceSet.getName() + "GeneratedDataTemplate");
      final Task compileTask = project.getTasks().getByName(targetSourceSet.getCompileJavaTaskName());
      compileTask.dependsOn(eventsDataTemplate);

      // Include PDL files in the data template jar, as other PDL files are also included.
      final Jar dataTemplateJarTask = (Jar) project.getTasks().findByPath(sourceSet.getName() + "DataTemplateJar");
      dataTemplateJarTask.dependsOn(eventsDataTemplate);
      dataTemplateJarTask.from(generateMetadataEventsTask.getOutputDirectory(), copySpec -> {
        copySpec.eachFile(fileCopyDetails -> {
          fileCopyDetails.setPath("pegasus" + File.separator + fileCopyDetails.getPath());
        });
      });
    }
  }
}
