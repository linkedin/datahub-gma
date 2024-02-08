package com.linkedin.metadata.annotations;

import com.google.common.base.Joiner;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.validation.RequiredMode;
import com.linkedin.data.schema.validation.UnrecognizedFieldMode;
import com.linkedin.data.schema.validation.ValidateDataAgainstSchema;
import com.linkedin.data.schema.validation.ValidationOptions;
import com.linkedin.data.schema.validation.ValidationResult;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.DataTemplateUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


public class GmaAnnotationParser {
  private static final String GMA = "gma";
  private static final String SEARCH = "search";

  private final GmaEntitiesAnnotationAllowList _gmaEntitiesAnnotationAllowList;

  public GmaAnnotationParser(@Nullable GmaEntitiesAnnotationAllowList gmaEntitiesAnnotationAllowList) {
    _gmaEntitiesAnnotationAllowList = gmaEntitiesAnnotationAllowList;
  }

  public GmaAnnotationParser() {
    this(GmaEntitiesAnnotationAllowListImpl.DEFAULT);
  }

  public static final class GmaAnnotationParseException extends RuntimeException {
    public GmaAnnotationParseException(String message) {
      super(message);
    }
  }

  /**
   * Obtains the {@code @gma} annotations from the given schema.
   *
   * @throws GmaAnnotationParseException upon failure.
   */
  public Optional<GmaAnnotation> parse(@Nonnull DataSchema schema) {
    Optional<GmaAnnotation> gmaAnnotation = parseTopLevelAnnotations(schema);
    final IndexAnnotationArrayMap searchIndexFields = parseSearchIndexFields(schema);

    // No top-level annotations
    if (!gmaAnnotation.isPresent()) {
      if (searchIndexFields.isEmpty()) {
        // if there are no field-level search annotations, there are in totality no annotations: return empty
        return Optional.empty();
      } else {
        // this means there are field-level search annotations, just return those
        return Optional.of(new GmaAnnotation().setSearch(new SearchAnnotation().setIndex(searchIndexFields)));
      }
    }

    // Normal case:
    // if parsing top-level annotations resulted in a non-null GmaAnnotation, then fill out the indexMappings and return
    gmaAnnotation.get().getSearch().setIndex(searchIndexFields);
    return gmaAnnotation;
  }

  /**
   *
   * Obtains the top-level {@code @gma} annotations from the given schema, if there are any.
   * Note: this is the old parse() method before field-level parsing was introduced
   *
   * @throws GmaAnnotationParseException if the provided {@code @gma} annotations do not match the schema.
   */
  private Optional<GmaAnnotation> parseTopLevelAnnotations(@Nonnull DataSchema schema) {
    final Map<String, Object> properties = schema.getProperties();
    final Object gmaObj = properties.get(GMA);

    if (gmaObj == null) {
      return Optional.empty();
    }

    final ValidationOptions options = new ValidationOptions();
    options.setRequiredMode(RequiredMode.CAN_BE_ABSENT_IF_HAS_DEFAULT);
    options.setUnrecognizedFieldMode(UnrecognizedFieldMode.DISALLOW);

    final ValidationResult result =
            ValidateDataAgainstSchema.validate(gmaObj, DataTemplateUtil.getSchema(GmaAnnotation.class), options);

    final GmaAnnotation gmaAnnotation = DataTemplateUtil.wrap(gmaObj, GmaAnnotation.class);

    if (!result.isValid()) {
      throw new GmaAnnotationParseException(String.format("Error parsing @gma on %s: \n%s", DataSchemaUtil.getFullName(schema),
              Joiner.on('\n').join(result.getMessages())));
    }

    if (_gmaEntitiesAnnotationAllowList != null && gmaAnnotation.hasAspect() && gmaAnnotation.getAspect().hasEntities()) {
      _gmaEntitiesAnnotationAllowList.check(schema, gmaAnnotation.getAspect());
    }

    return Optional.of(gmaAnnotation);
  }

  /**
   * Obtains the {@code @gma} search annotations from the schema.
   *
   * @throws GmaAnnotationParseException if the provided {@code @gma} annotation does not match the schema.
   */
  private IndexAnnotationArrayMap parseSearchIndexFields(@Nonnull DataSchema schema) {
//    return null;
    Map<String, IndexAnnotationArray> javaMap = new HashMap<>();
    for(RecordDataSchema.Field f : ((RecordDataSchema) schema).getFields()) {
      final Object gmaObj = f.getProperties().get(GMA);
      if (gmaObj == null) {
        continue;
      }

      final Object searchObj = ((DataMap) gmaObj).get(SEARCH);
      if (searchObj == null) {
        continue;
      }

      final Object indexObj = ((DataMap) searchObj).get("index");
      if (indexObj == null) {
        continue;
      }

      // TODO: at this point, run any desired validations just like parseTopLevelAnnotations()'s flow of logic

      final IndexAnnotationArray fieldIndexAnnotations = DataTemplateUtil.wrap(indexObj, IndexAnnotationArray.class);
      javaMap.put(f.getName(), fieldIndexAnnotations);
    }

    return new IndexAnnotationArrayMap(javaMap);
  }
}
