package com.linkedin.metadata.dao.exception;

/**
 * Exception thrown when the requested aspect is not defined in the asset model / there is invalid aspect present
 * in the database that is not defined in the asset model.
 */
public class ModelValidationException extends RuntimeException {

  public ModelValidationException(String message) {
    super(message);
  }
}
