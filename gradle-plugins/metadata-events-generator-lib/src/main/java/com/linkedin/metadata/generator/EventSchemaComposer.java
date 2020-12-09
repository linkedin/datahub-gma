package com.linkedin.metadata.generator;

import com.linkedin.metadata.rythm.RythmGenerator;
import java.io.File;
import java.io.IOException;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.rythmengine.Rythm;

import static com.linkedin.metadata.generator.SchemaGeneratorConstants.*;
import static com.linkedin.metadata.generator.SchemaGeneratorUtil.*;


/**
 * Render the property annotations to the MXE pdl schema.
 */
@Slf4j
public class EventSchemaComposer extends RythmGenerator {

  public void render(@Nonnull List<EventSpec> eventSpecs, @Nonnull String mainOutput) {
    eventSpecs.forEach(eventSpec -> {
      switch (eventSpec.getEventType()) {
        case CHANGE:
        case AUDIT:
        case FAILED_CHANGE:
          processRecord(eventSpec, mainOutput);
          break;
        default:
          throw new IllegalArgumentException(
              String.format("Unrecognized event type %s to be rendered.", eventSpec.getEventType()));
      }
    });
  }

  private void processRecord(@Nonnull EventSpec eventSpec, @Nonnull String mainOutput) {
    try {
      generateResultFile(eventSpec.getEventType(), eventSpec.getUrn(), eventSpec, mainOutput);
    } catch (IOException ex) {
      log.error(String.format("Generate result file failed due to %s.", ex.getCause()));
    }
  }

  private void generateResultFile(@Nonnull MetadataEventType eventType, @Nonnull String entityUrn,
      @Nonnull EventSpec eventSpec, @Nonnull String mainOutput) throws IOException {
    final String entityName = deCapitalize(getEntityName(entityUrn));
    final String aspectName = deCapitalize(eventSpec.getValueType());
    final File directory = createOutputFolder(mainOutput + File.separator + entityName + File.separator + aspectName);
    final String namespace = String.format(".%s.%s", entityName, aspectName);

    writeToFile(new File(directory, eventType.getName() + PDL_SUFFIX),
        renderToString(eventSpec, entityUrn, namespace, EVENT_TEMPLATES.get(eventType)));
  }

  @Nonnull
  private String renderToString(@Nullable EventSpec eventSpec, @Nonnull String entityUrn, @Nonnull String namespace,
      @Nonnull String template) throws IOException {
    final String result = Rythm.renderIfTemplateExists(template, entityUrn, namespace, eventSpec);
    if (result.isEmpty()) {
      throw new IOException(String.format("Template does not exist: %s.", template));
    }
    return result;
  }
}