package com.linkedin.metadata.dao.generic;

import lombok.NonNull;
import lombok.Value;


/**
 * A value class that holds the components of a key for metadata retrieval.
 */
@Value
public class Key {

  /**
   * Fully qualified class name of the aspect class.
   */
  @NonNull
  String aspect;

  /**
   * String representation of a URN (Uniform Resource Name) for a Linkedin entity.
   */
  @NonNull
  String urn;

  /**
   * Version of the aspect.
   */
  long version;
}