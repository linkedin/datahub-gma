package com.linkedin.metadata.validator;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.schema.ArrayDataSchema;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.UnionDataSchema;
import com.linkedin.data.schema.validation.CoercionMode;
import com.linkedin.data.schema.validation.RequiredMode;
import com.linkedin.data.schema.validation.UnrecognizedFieldMode;
import com.linkedin.data.schema.validation.ValidateDataAgainstSchema;
import com.linkedin.data.schema.validation.ValidationOptions;
import com.linkedin.data.schema.validation.ValidationResult;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.metadata.aspect.SoftDeletedAspect;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;


/**
 * Utility class for schema validation classes.
 */
public final class ValidationUtils {

  public static final Set<DataSchema.Type> PRIMITIVE_TYPES =
      Collections.unmodifiableSet(new HashSet<DataSchema.Type>() {
        {
          add(DataSchema.Type.BOOLEAN);
          add(DataSchema.Type.INT);
          add(DataSchema.Type.LONG);
          add(DataSchema.Type.FLOAT);
          add(DataSchema.Type.DOUBLE);
          add(DataSchema.Type.STRING);
          add(DataSchema.Type.ENUM);
        }
      });

  private static final Map<String, String> SOFT_DELETED_FIELD_METADATA = fieldMetadata(new SoftDeletedAspect().schema());

  public static final ValidationOptions VALIDATION_OPTIONS =
      new ValidationOptions(RequiredMode.CAN_BE_ABSENT_IF_HAS_DEFAULT, CoercionMode.NORMAL, UnrecognizedFieldMode.DISALLOW);

  private ValidationUtils() {
    // Util class
  }

  public static void invalidSchema(@Nonnull String format, Object... args) {
    throw new InvalidSchemaException(String.format(format, args));
  }

  /**
   * Gets the {@link RecordDataSchema} of a {@link RecordTemplate} via reflection.
   */
  @Nonnull
  public static RecordDataSchema getRecordSchema(@Nonnull Class<? extends RecordTemplate> clazz) {
    try {
      Field field = clazz.getDeclaredField("SCHEMA");
      field.setAccessible(true);
      return (RecordDataSchema) field.get(null);
    } catch (NoSuchFieldException e) {
      throw new RuntimeException("Unable to find SCHEMA field in " + clazz.getCanonicalName());
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Gets the {@link UnionDataSchema} of a {@link UnionTemplate} via reflection.
   */
  @Nonnull
  public static UnionDataSchema getUnionSchema(@Nonnull Class<? extends UnionTemplate> clazz) {
    try {
      Field field = clazz.getDeclaredField("SCHEMA");
      field.setAccessible(true);
      return (UnionDataSchema) field.get(null);
    } catch (NoSuchFieldException e) {
      throw new RuntimeException("Unable to find SCHEMA field in " + clazz.getCanonicalName());
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Returns true if the supply schema has exactly one field matching the predicate.
   */
  public static boolean schemaHasExactlyOneSuchField(@Nonnull RecordDataSchema schema,
      @Nonnull Predicate<RecordDataSchema.Field> predicate) {
    return schema.getFields().stream().filter(predicate).count() == 1;
  }

  /**
   * Returns true if the passed-in field matches the field name and is of URN type.
   */
  public static boolean isValidUrnField(@Nonnull RecordDataSchema.Field field, @Nonnull String fieldName) {
    return field.getName().equals(fieldName)
        && field.getType().getType() == DataSchema.Type.TYPEREF && Urn.class.isAssignableFrom(getUrnClass(field));
  }

  /**
   * Similar to {@link #isValidUrnField(RecordDataSchema.Field, String)} but with a fixed field "urn".
   */
  public static boolean isValidUrnField(@Nonnull RecordDataSchema.Field field) {
    return isValidUrnField(field, "urn");
  }

  /**
   * Returns true if the passed-in field matches the field name and is of UNION type.
   */
  public static boolean isValidUnionField(@Nonnull RecordDataSchema.Field field, @Nonnull String fieldName) {
    return field.getName().equals(fieldName)
        && field.getType().getType() == DataSchema.Type.UNION;
  }

  /**
   * Returns the Java class for an URN typeref field.
   */
  public static Class<Urn> getUrnClass(@Nonnull RecordDataSchema.Field field) {
    try {
      @SuppressWarnings("unchecked")
      final Class<Urn> clazz =
          (Class<Urn>) Class.forName(((DataMap) field.getType().getProperties().get("java")).getString("class"));
      return clazz;
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Returns all the non-whitelisted, non-optional fields in a {@link RecordDataSchema}.
   */
  @Nonnull
  public static List<RecordDataSchema.Field> nonOptionalFields(@Nonnull RecordDataSchema schema,
      @Nonnull Set<String> whitelistedFields) {
    return schema.getFields().stream().filter(field -> {
      if (!whitelistedFields.contains(field.getName())) {
        if (!field.getOptional()) {
          return true;
        }
      }
      return false;
    }).collect(Collectors.toList());
  }

  /**
   * Returns all the non-whitelisted, optional fields in a {@link RecordDataSchema}.
   */
  @Nonnull
  public static List<RecordDataSchema.Field> optionalFields(@Nonnull RecordDataSchema schema,
      @Nonnull Set<String> whitelistedFields) {
    return schema.getFields().stream().filter(field -> {
      if (!whitelistedFields.contains(field.getName())) {
        if (field.getOptional()) {
          return true;
        }
      }
      return false;
    }).collect(Collectors.toList());
  }

  /**
   * Return all the fields in a {@link RecordDataSchema} that are not using one of the allowed types.
   */
  @Nonnull
  public static List<RecordDataSchema.Field> fieldsUsingInvalidType(@Nonnull RecordDataSchema schema,
      @Nonnull Set<DataSchema.Type> allowedTypes) {
    return schema.getFields()
        .stream()
        .filter(field -> !allowedTypes.contains(getFieldOrArrayItemType(field)) && !field.getName().equals("hostedSearchEventMetadata"))
        .collect(Collectors.toList());
  }

  public static boolean isUnionWithOnlyComplexMembers(UnionDataSchema unionDataSchema) {
    return unionDataSchema.getMembers().stream().allMatch(member -> member.getType().isComplex());
  }

  public static boolean isUnionWithSoftDeletedAspect(UnionDataSchema unionDataSchema) {
    return unionDataSchema.getMembers().stream().anyMatch(ValidationUtils::fieldsOfSoftDeletedAspect);
  }

  private static boolean fieldsOfSoftDeletedAspect(@Nonnull UnionDataSchema.Member member) {
    if (member.getType().getDereferencedDataSchema().getType().equals(DataSchema.Type.RECORD)) {
      final RecordDataSchema dataSchema = (RecordDataSchema) member.getType().getDereferencedDataSchema();
      return fieldMetadata(dataSchema).equals(SOFT_DELETED_FIELD_METADATA);
    }
    return false;
  }

  /**
   * Returns field name and the type of all the fields in a {@link RecordDataSchema} schema.
   *
   * @param schema input schema of type {@link RecordDataSchema}
   * @return map of key value pairs where key is the field name and value is the field type, both converted to string
   */
  private static Map<String, String> fieldMetadata(RecordDataSchema schema) {
    return schema.getFields().stream().collect(Collectors.toMap(
        RecordDataSchema.Field::getName, f -> f.getType().toString()));
  }

  @Nonnull
  private static DataSchema.Type getFieldOrArrayItemType(@Nonnull RecordDataSchema.Field field) {
    DataSchema type = field.getType().getType() == DataSchema.Type.ARRAY
        ? ((ArrayDataSchema) field.getType()).getItems() : field.getType();
    if (type.getType() == DataSchema.Type.TYPEREF) {
      return type.getDereferencedType();
    }
    return type.getType();
  }

  public static void throwNullFieldException(String field) {
    throw new NullFieldException(String.format("The '%s' field cannot be null.", field));
  }

  /**
   * Validates a model against its schema, useful for checking to make sure that a (curli) ingestion request's
   * fields are all valid fields of the model.
   *
   * <p>This is copied from BaseLocalDAO, which uses this for Aspect-level ingestion validation. This is meant for
   * Asset-level validation.</p>
   *
   * <p>Reference ticket: META-21242</p>
   */
  public static void validateAgainstSchema(@Nonnull RecordTemplate model) {
    ValidationResult result = ValidateDataAgainstSchema.validate(model, VALIDATION_OPTIONS);
    if (!result.isValid()) {
      invalidSchema(String.valueOf(result.getMessages()));
    }
  }

}
