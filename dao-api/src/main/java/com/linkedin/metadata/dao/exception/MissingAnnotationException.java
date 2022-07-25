package com.linkedin.metadata.dao.exception;

public class MissingAnnotationException extends RuntimeException {

  public MissingAnnotationException(String message) {
    super(message);
  }

  public MissingAnnotationException(String message, Throwable throwable) {
    super(message, throwable);
  }
}
