package com.linkedin.metadata.validator;

import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.RecordTemplate;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;


public class AssetValidator {

  // A cache of validated classes
  private static final Set<Class<? extends RecordTemplate>> VALIDATED = ConcurrentHashMap.newKeySet();

  private AssetValidator() {
    // Util class
  }

  /**
   * Validates an asset model defined in com.linkedin.metadata.asset.
   *
   * @param schema schema for the model
   */
  public static void validateAssetSchema(@Nonnull RecordDataSchema schema) {

    final String className = schema.getBindingName();

    if (!ValidationUtils.schemaHasExactlyOneSuchField(schema, ValidationUtils::isValidUrnField)) {
      ValidationUtils.invalidSchema("Asset '%s' must contain an 'urn' field of URN type", className);
    }
  }

  /**
   * Similar to {@link #validateAssetSchema(RecordDataSchema)} but take a {@link Class} instead and caches results.
   */
  public static void validateAssetSchema(@Nonnull Class<? extends RecordTemplate> clazz) {
    if (VALIDATED.contains(clazz)) {
      return;
    }

    validateAssetSchema(ValidationUtils.getRecordSchema(clazz));
    VALIDATED.add(clazz);
  }
}
