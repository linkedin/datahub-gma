package com.linkedin.metadata.annotations;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.TyperefDataSchema;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;


/**
 * Utility operations on DataSchema.
 */
public class DataSchemaUtil {

  private DataSchemaUtil() {
  }

  @CheckForNull
  public static String getFullName(@Nonnull DataSchema dataSchema) {
    if (dataSchema instanceof RecordDataSchema) {
      return ((RecordDataSchema) dataSchema).getFullName();
    } else if (dataSchema instanceof TyperefDataSchema) {
      return ((TyperefDataSchema) dataSchema).getFullName();
    }

    return null;
  }

  @CheckForNull
  public static String getNamespace(@Nonnull DataSchema dataSchema) {
    if (dataSchema instanceof RecordDataSchema) {
      return ((RecordDataSchema) dataSchema).getNamespace();
    } else if (dataSchema instanceof TyperefDataSchema) {
      return ((TyperefDataSchema) dataSchema).getNamespace();
    }

    return null;
  }
}
