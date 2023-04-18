package com.linkedin.metadata.generator;

import javax.annotation.Nonnull;


/**
 * Constants used in PDL/Rythm to describe event schemas.
 */
public class SchemaGeneratorConstants {
  private SchemaGeneratorConstants() {
  }

  // used in EventSchemaComposer
  static final String PDL_SUFFIX = ".pdl";

  public enum MetadataEventType {
    CHANGE("MetadataChangeEvent", "MetadataChangeEvent.rythm"),
    AUDIT("MetadataAuditEvent", "MetadataAuditEvent.rythm"),
    FAILED_CHANGE("FailedMetadataChangeEvent", "FailedMetadataChangeEvent.rythm");

    private final String _name;
    private final String _templateName;

    MetadataEventType(@Nonnull String name, @Nonnull String templateName) {
      _name = name;
      _templateName = templateName;
    }

    @Nonnull
    public String getName() {
      return _name;
    }

    public String getTemplateName() {
      return _templateName;
    }

    public String getDefaultFileName() {
      return _name + PDL_SUFFIX;
    }
  }

}