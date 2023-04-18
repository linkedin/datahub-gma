package com.linkedin.metadata.generator;

import com.linkedin.metadata.rythm.RythmGenerator;
import java.io.File;
import java.io.IOException;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.rythmengine.Rythm;


/**
 * Render the property annotations to the MXE pdl schema.
 */
@Slf4j
public class EventSchemaComposer extends RythmGenerator {

  public void render(@Nonnull List<EventSpec> eventSpecs, @Nonnull String mainOutput) {
    eventSpecs.forEach(eventSpec -> processRecord(eventSpec, mainOutput));
  }

  private void processRecord(@Nonnull EventSpec eventSpec, @Nonnull String mainOutput) {
    try {
      eventSpec.renderEventSchemas(new File(mainOutput));
    } catch (IOException ex) {
      log.error(String.format("Generate result file failed due to %s.", ex.getCause()));
    }
  }

  @Nonnull
  static String renderToString(@Nullable EventSpec eventSpec, @Nonnull String template) throws IOException {
    final String result = Rythm.renderIfTemplateExists(template, eventSpec);
    if (result.isEmpty()) {
      throw new IOException(String.format("Template does not exist: %s.", template));
    }
    return result;
  }
}