package com.linkedin.metadata.dao.generic;

import lombok.NonNull;
import lombok.Value;


/**
 * A value class that holds the result of metadata retrieval.
 */
@Value
public class Record {

  /**
   * Fully qualified class name of the aspect class. Can be used to deserialize the metadata to pegasus model.
   */
  @NonNull
  String aspect;

  /**
   * String format metadata serialized from pegasus record template.
   */
  @NonNull
  String metadata;
}
