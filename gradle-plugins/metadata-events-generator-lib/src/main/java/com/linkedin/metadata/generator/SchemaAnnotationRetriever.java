package com.linkedin.metadata.generator;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.metadata.annotations.AspectEntityAnnotation;
import com.linkedin.metadata.annotations.GmaAnnotationParser;
import com.linkedin.metadata.annotations.GmaEntitiesAnnotationAllowList;
import com.linkedin.pegasus.generator.DataSchemaParser;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;


/**
 * Parse the property annotations from the pdl schema.
 */
@Slf4j
public class SchemaAnnotationRetriever {

  private final DataSchemaParser _dataSchemaParser;
  private final GmaAnnotationParser _gmaAnnotationParser;

  private SchemaAnnotationRetriever(@Nonnull String resolverPath, @Nonnull GmaAnnotationParser gmaAnnotationParser) {
    _dataSchemaParser = new DataSchemaParser(resolverPath);
    _gmaAnnotationParser = gmaAnnotationParser;
  }

  public SchemaAnnotationRetriever(@Nonnull String resolverPath, @Nonnull GmaEntitiesAnnotationAllowList allowList) {
    this(resolverPath, new GmaAnnotationParser(allowList));
  }

  public SchemaAnnotationRetriever(@Nonnull String resolverPath) {
    this(resolverPath, new GmaAnnotationParser());
  }

  public List<EventSpec> generate(@Nonnull String[] sources) throws IOException {

    final DataSchemaParser.ParseResult parseResult = _dataSchemaParser.parseSources(sources);
    final List<EventSpec> eventSpecs = new ArrayList<>();
    for (DataSchema dataSchema : parseResult.getSchemaAndLocations().keySet()) {
      if (dataSchema.getType() == DataSchema.Type.RECORD) {
        eventSpecs.addAll(generate((RecordDataSchema) dataSchema));
      }
    }
    return eventSpecs;
  }

  @Nonnull
  private List<EventSpec> generate(@Nonnull RecordDataSchema schema) {
    return _gmaAnnotationParser.parse(schema).map(gma -> {
      final List<EventSpec> eventSpecs = new ArrayList<>();

      final List<AspectEntityAnnotation> entityAnnotations = new ArrayList<>();

      if (gma.hasAspect()) {
        if (gma.getAspect().hasEntity()) {
          entityAnnotations.add(gma.getAspect().getEntity());
        }

        if (gma.getAspect().hasEntities()) {
          entityAnnotations.addAll(gma.getAspect().getEntities());
        }
      }

      for (AspectEntityAnnotation entityAnnotation : entityAnnotations) {
        final EventSpec.EventSpecBuilder builder = EventSpec.builder();
        builder.namespace(schema.getNamespace());
        builder.fullValueType(schema.getFullName());
        builder.valueType(SchemaGeneratorUtil.stripNamespace(schema.getFullName()));
        builder.urn(entityAnnotation.getUrn());

        builder.eventType(SchemaGeneratorConstants.MetadataEventType.CHANGE);
        eventSpecs.add(builder.build());

        builder.eventType(SchemaGeneratorConstants.MetadataEventType.AUDIT);
        eventSpecs.add(builder.build());

        builder.eventType(SchemaGeneratorConstants.MetadataEventType.FAILED_CHANGE);
        eventSpecs.add(builder.build());
      }

      return eventSpecs;
    }).orElse(Collections.emptyList());
  }
}