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
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


public class GmaAnnotationParser {
  private static final String GMA = "gma";
  private static final String SEARCH = "search";
  private static final String INDEX = "index";

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

    // if annotation ecosystem grows, can abstract the following into a subroutine
    final IndexAnnotationArrayMap searchIndexFields = parseSearchIndexFields(schema);
    if (!searchIndexFields.isEmpty()) {
      // we need to augment GmaAnnotation with SearchIndex metadata
      if (!gmaAnnotation.isPresent()) {
        // no top-level annotations, so we need to create one and fill out SearchIndex metadata
        return Optional.of(new GmaAnnotation().setSearch(new SearchAnnotation().setIndex(searchIndexFields)));
      } else {
        // yes top-level annotations, so we need to just fill out SearchIndex metadata
        gmaAnnotation.get().setSearch(new SearchAnnotation().setIndex(searchIndexFields));
      }
    }

    return gmaAnnotation;
  }

  /**
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
   * Currently does not perform any validation checks.
   */
  private @Nonnull IndexAnnotationArrayMap parseSearchIndexFields(@Nonnull DataSchema schema) {
    Map<String, IndexAnnotationArray> fieldNameToEntityUrnClassNames = new HashMap<>();

    // only Record types will have fields with SearchIndex annotations
    if (schema.getType().equals(DataSchema.Type.RECORD)) {
      for (RecordDataSchema.Field f : ((RecordDataSchema) schema).getFields()) {
        final Object gmaObj = f.getProperties().get(GMA);
        if (gmaObj == null) {
          continue;
        }

        final Object searchObj = ((DataMap) gmaObj).get(SEARCH);
        if (searchObj == null) {
          continue;
        }

        final Object indexObj = ((DataMap) searchObj).get(INDEX);
        if (indexObj == null) {
          continue;
        }

        // TODO: at this point, run any desired validations just like parseTopLevelAnnotations()'s flow of logic

        final IndexAnnotationArray fieldIndexAnnotations = DataTemplateUtil.wrap(indexObj, IndexAnnotationArray.class);
        fieldNameToEntityUrnClassNames.put(f.getName(), fieldIndexAnnotations);
      }
    }

    return new IndexAnnotationArrayMap(fieldNameToEntityUrnClassNames);
  }
}
