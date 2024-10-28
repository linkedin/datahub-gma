package com.linkedin.metadata.validator;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataList;
import com.linkedin.data.DataMap;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.UnionDataSchema;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.UnionTemplate;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.Value;


public class RelationshipValidator {

  // A cache of validated classes
  private static final Set<Class<? extends RecordTemplate>> VALIDATED = ConcurrentHashMap.newKeySet();

  // A cache of validated classes
  private static final Set<Class<? extends UnionTemplate>> UNION_VALIDATED = ConcurrentHashMap.newKeySet();

  private static final String DESTINATION_FIELD = "destination";
  private static final String SOURCE_FIELD = "source";

  @Value
  private static class Pair {
    String source;
    String destination;
  }

  private RelationshipValidator() {
    // Util class
  }

  /**
   * Validates a specific relationship model defined in com.linkedin.metadata.relationship.
   *
   * @param schema schema for the model
   */
  public static void validateRelationshipSchema(@Nonnull RecordDataSchema schema) {

    final String className = schema.getBindingName();

    if (!ValidationUtils.schemaHasExactlyOneSuchField(schema,
        field -> (ValidationUtils.isValidUrnField(field, SOURCE_FIELD) || ValidationUtils.isValidUnionField(field, SOURCE_FIELD)))) {
      ValidationUtils.invalidSchema("Relationship '%s' must contain a '%s' field of URN type or UNION type || %s",
          className, SOURCE_FIELD, schema.getFields().stream().map(f -> f.getName()).collect(Collectors.joining(",")));
    }

    if (!ValidationUtils.schemaHasExactlyOneSuchField(schema,
        field -> (ValidationUtils.isValidUrnField(field, DESTINATION_FIELD) || ValidationUtils.isValidUnionField(field, DESTINATION_FIELD)))) {
      ValidationUtils.invalidSchema("Relationship '%s' must contain a '%s' field of URN type or UNION type",
          className, DESTINATION_FIELD);
    }

    // Relationship V2 uses AuditStamp class, instead of primitive types
//    ValidationUtils.fieldsUsingInvalidType(schema, ValidationUtils.PRIMITIVE_TYPES).forEach(field -> {
//      ValidationUtils.invalidSchema("Relationship '%s' contains a field '%s' that makes use of a disallowed type '%s'.",
//          className, field.getName(), field.getType().getType());
//    });
    // Relationship V2 no longer uses the 'pairings' property
//    validatePairings(schema);
  }


  /**
   * Similar to {@link #validateRelationshipSchema(RecordDataSchema)} but take a {@link Class} instead and caches results.
   */
  public static void validateRelationshipSchema(@Nonnull Class<? extends RecordTemplate> clazz) {
    if (VALIDATED.contains(clazz)) {
      return;
    }

    validateRelationshipSchema(ValidationUtils.getRecordSchema(clazz));
    VALIDATED.add(clazz);
  }

  /**
   * Similar to {@link #validateRelationshipUnionSchema(UnionDataSchema, String)} but take a {@link Class} instead and caches results.
   */
  public static void validateRelationshipUnionSchema(@Nonnull Class<? extends UnionTemplate> clazz) {
    if (UNION_VALIDATED.contains(clazz)) {
      return;
    }

    validateRelationshipUnionSchema(ValidationUtils.getUnionSchema(clazz), clazz.getCanonicalName());
    UNION_VALIDATED.add(clazz);
  }

  /**
   * Validates the union of relationship model defined in com.linkedin.metadata.relationship.
   *
   * @param schema schema for the model
   */
  public static void validateRelationshipUnionSchema(@Nonnull UnionDataSchema schema, @Nonnull String relationshipClassName) {

    if (!ValidationUtils.isUnionWithOnlyComplexMembers(schema)) {
      ValidationUtils.invalidSchema("Relationship '%s' must be a union containing only record type members", relationshipClassName);
    }
  }

  private static void validatePairings(@Nonnull RecordDataSchema schema) {

    final String className = schema.getBindingName();

    Map<String, Object> properties = schema.getProperties();
    if (!properties.containsKey("pairings")) {
      ValidationUtils.invalidSchema("Relationship '%s' must contain a 'pairings' property", className);
    }

    DataList pairings = (DataList) properties.get("pairings");
    Set<Pair> registeredPairs = new HashSet<>();
    pairings.stream().forEach(obj -> {
      DataMap map = (DataMap) obj;
      if (!map.containsKey(SOURCE_FIELD) || !map.containsKey(DESTINATION_FIELD)) {
        ValidationUtils.invalidSchema("Relationship '%s' contains an invalid 'pairings' item. "
            + "Each item must contain a 'source' and 'destination' properties.", className);
      }

      String sourceUrn = map.getString(SOURCE_FIELD);
      if (!isValidUrnClass(sourceUrn)) {
        ValidationUtils.invalidSchema(
            "Relationship '%s' contains an invalid item in 'pairings'. %s is not a valid URN class name.", className,
            sourceUrn);
      }

      String destinationUrn = map.getString(DESTINATION_FIELD);
      if (!isValidUrnClass(destinationUrn)) {
        ValidationUtils.invalidSchema(
            "Relationship '%s' contains an invalid item in 'pairings'. %s is not a valid URN class name.", className,
            destinationUrn);
      }

      Pair pair = new Pair(sourceUrn, destinationUrn);
      if (registeredPairs.contains(pair)) {
        ValidationUtils.invalidSchema("Relationship '%s' contains a repeated 'pairings' item (%s, %s)", className,
            sourceUrn, destinationUrn);
      }
      registeredPairs.add(pair);
    });
  }

  private static boolean isValidUrnClass(String className) {
    try {
      return Urn.class.isAssignableFrom(Class.forName(className));
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
}