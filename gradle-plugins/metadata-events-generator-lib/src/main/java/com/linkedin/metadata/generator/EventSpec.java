package com.linkedin.metadata.generator;

import lombok.Getter;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Collection;

/**
 * Contains event metadata for rendering events.
 */
@Getter
public abstract class EventSpec {

  /** Entity that this aspect refers to, such as: com.linkedin.common.CorpuserUrn. */
  private final String urnType;

  /**
   * Short name without namespace for the {@link #urnType}.
   */
  private final String shortUrn;

  /**
   * {@link #shortUrn} without Urn ending.
   */
  private final String entityName;

  /** Base namespace where rendered events will be created. */
  private final String baseNamespace;

  public EventSpec(String urnType, @Nullable String baseNamespace) {
    this.urnType = urnType;
    this.shortUrn = SchemaGeneratorUtil.stripNamespace(urnType);
    this.entityName = SchemaGeneratorUtil.getEntityName(urnType);
    this.baseNamespace = baseNamespace == null ? "com.linkedin.mxe" : baseNamespace;
  }

  /**
   * Render all events related to this spec.
   */
  abstract Collection<File> renderEventSchemas(File baseDirectory) throws IOException;

  /** namespace of the model, such as: com.linkedin.identity. */
  public String getNamespace() {
    return String.format("%s.%s", this.baseNamespace, SchemaGeneratorUtil.deCapitalize(this.getEntityName()));
  }

  /** Render a single event file with the given template. */
  File renderFile(File file, String templateName) throws IOException {
    SchemaGeneratorUtil.createOutputFolder(file.getParentFile());
    SchemaGeneratorUtil.writeToFile(file,
            EventSchemaComposer.renderToString(this, templateName));
    return file;
  }

}