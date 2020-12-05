package com.linkedin.metadata.generator;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import javax.annotation.Nonnull;


/**
 * Constants used in PDL/Rythm to describe event schemas.
 */
public class SchemaGeneratorConstants {
  private SchemaGeneratorConstants() {
  }

  public enum MetadataEventType {
    CHANGE("MetadataChangeEvent"), AUDIT("MetadataAuditEvent"), FAILED_CHANGE("FailedMetadataChangeEvent");

    private final String _name;

    MetadataEventType(@Nonnull String name) {
      _name = name;
    }

    @Nonnull
    public String getName() {
      return _name;
    }
  }

  // used in EventSchemaComposer
  static final String PDL_SUFFIX = ".pdl";
  static final Map<MetadataEventType, String> EVENT_TEMPLATES =
      ImmutableMap.<MetadataEventType, String>builder().put(MetadataEventType.FAILED_CHANGE,
          "FailedMetadataChangeEvent.rythm")
          .put(MetadataEventType.AUDIT, "MetadataAuditEvent.rythm")
          .put(MetadataEventType.CHANGE, "MetadataChangeEvent.rythm")
          .build();
}