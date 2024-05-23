package com.linkedin.metadata.validator;

/**
 * Thrown when a field which is not supposed to be null is null.
 */
public class NullFieldException extends RuntimeException {

  public NullFieldException(String message) {
    super(message);
  }
}
