package com.linkedin.metadata.annotations;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.validation.RequiredMode;
import com.linkedin.data.schema.validation.UnrecognizedFieldMode;
import com.linkedin.data.schema.validation.ValidateDataAgainstSchema;
import com.linkedin.data.schema.validation.ValidationOptions;
import com.linkedin.data.schema.validation.ValidationResult;
import com.linkedin.data.template.DataTemplateUtil;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;


public class GmaAnnotationParser {
  private static final String GMA = "gma";

  private final GmaEntitiesAnnotationAllowList _gmaEntitiesAnnotationAllowList;

  @VisibleForTesting
  public GmaAnnotationParser(@Nonnull GmaEntitiesAnnotationAllowList gmaEntitiesAnnotationAllowList) {
    _gmaEntitiesAnnotationAllowList = gmaEntitiesAnnotationAllowList;
  }

  public GmaAnnotationParser() {
    this(DefaultGmaEntitiesAnnotationAllowList.DEFAULT);
  }

  public static final class GmaAnnotationParseException extends RuntimeException {
    public GmaAnnotationParseException(String message) {
      super(message);
    }
  }

  /**
   * Obtains the {@code @gma} annotation from the given schema, if there is one.
   *
   * @throws GmaAnnotationParseException if the provided {@code @gma} annotation does not match the schema.
   */
  public Optional<GmaAnnotation> parse(@Nonnull RecordDataSchema schema) {
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
      throw new GmaAnnotationParseException(String.format("Error parsing @gma on %s: \n%s", schema.getFullName(),
          Joiner.on('\n').join(result.getMessages())));
    }

    if (gmaAnnotation.hasAspect() && gmaAnnotation.getAspect().hasEntities()) {
      _gmaEntitiesAnnotationAllowList.check(schema, gmaAnnotation.getAspect());
    }

    return Optional.of(gmaAnnotation);
  }
}
