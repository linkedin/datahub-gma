package com.linkedin.metadata.generator;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.UnionDataSchema;
import com.linkedin.metadata.annotations.AspectEntityAnnotation;
import com.linkedin.metadata.annotations.DataSchemaUtil;
import com.linkedin.metadata.annotations.GmaAnnotation;
import com.linkedin.metadata.annotations.GmaAnnotationParser;
import com.linkedin.metadata.annotations.GmaEntitiesAnnotationAllowList;
import com.linkedin.pegasus.generator.DataSchemaParser;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;


/**
 * Parse the property annotations from the pdl schema.
 */
@Slf4j
public class SchemaAnnotationRetriever {

  private final DataSchemaParser _dataSchemaParser;
  private final GmaAnnotationParser _gmaAnnotationParser;
  private final String _baseNamespace;

  private SchemaAnnotationRetriever(@Nonnull String resolverPath, @Nonnull GmaAnnotationParser gmaAnnotationParser,
                                    String baseNamespace) {
    _dataSchemaParser = new DataSchemaParser(resolverPath);
    _gmaAnnotationParser = gmaAnnotationParser;
    _baseNamespace = baseNamespace;
  }

  public SchemaAnnotationRetriever(@Nonnull String resolverPath, @Nonnull GmaEntitiesAnnotationAllowList allowList,
                                   String baseNamespace) {
    this(resolverPath, new GmaAnnotationParser(allowList), baseNamespace);
  }

  public SchemaAnnotationRetriever(@Nonnull String resolverPath) {
    this(resolverPath, new GmaAnnotationParser(), null);
  }

  public List<EventSpec> generate(@Nonnull String[] sources) throws IOException {

    final DataSchemaParser.ParseResult parseResult = _dataSchemaParser.parseSources(sources);
    final List<EventSpec> eventSpecs = new ArrayList<>();
    for (DataSchema dataSchema : parseResult.getSchemaAndLocations().keySet()) {
      if (dataSchema.getType() == DataSchema.Type.RECORD
          || dataSchema.getType() == DataSchema.Type.TYPEREF && dataSchema.getDereferencedType() == DataSchema.Type.RECORD) {
        eventSpecs.addAll(generateSingleAspectSpec(dataSchema));
      } else if (dataSchema.getType() == DataSchema.Type.TYPEREF && dataSchema.getDereferencedType() == DataSchema.Type.UNION) {
        Optional<GmaAnnotation> gmaAnnotation = _gmaAnnotationParser.parse(dataSchema);
        if (gmaAnnotation.isPresent()) {
          UnionDataSchema unionSchema = ((UnionDataSchema) dataSchema.getDereferencedDataSchema());
          eventSpecs.addAll(generateAspectUnionSpec(DataSchemaUtil.getFullName(dataSchema),
                  unionSchema, gmaAnnotation.get()));
        }
      }
    }
    return eventSpecs;
  }

  private Collection<AspectUnionEventSpec> generateAspectUnionSpec(String typerefName, UnionDataSchema schema,
                                                                   GmaAnnotation gma) {
    if (gma.hasAspect() && gma.getAspect().hasEntity()) {
      AspectEntityAnnotation entityAnnotation = gma.getAspect().getEntity();
      Collection<String> valueTypes = schema.getMembers().stream().map(UnionDataSchema.Member::getType)
          .filter(member -> member.getType() == DataSchema.Type.RECORD)
          .map(DataSchemaUtil::getFullName).collect(Collectors.toList());
      return Collections.singleton(new AspectUnionEventSpec(entityAnnotation.getUrn(), typerefName, valueTypes,
              _baseNamespace));
    } else {
      return Collections.emptyList();
    }
  }

  @Nonnull
  private List<EventSpec> generateSingleAspectSpec(@Nonnull DataSchema schema) {
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
        eventSpecs.add(new SingleAspectEventSpec(DataSchemaUtil.getFullName(schema), entityAnnotation.getUrn(),
                _baseNamespace));
      }

      return eventSpecs;
    }).orElse(Collections.emptyList());
  }
}