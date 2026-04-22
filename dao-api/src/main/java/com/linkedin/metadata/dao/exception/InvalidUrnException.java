package com.linkedin.metadata.dao.exception;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;


/**
 * Thrown when a URN fails content validation on a write path.
 *
 * <p>Extends {@link IllegalArgumentException} so existing {@code catch (IllegalArgumentException)}
 * blocks in gRPC/Rest.li handlers automatically map it to an INVALID_ARGUMENT / 400 status.
 */
@Getter
public class InvalidUrnException extends IllegalArgumentException {

  public enum Reason {
    BLANK_FIELD,
    CONTAINS_WHITESPACE,
    CONTROL_CHARACTER
  }

  private final String entityType;
  private final Reason reason;
  private final String fieldPath;
  private final String rawValue;

  public InvalidUrnException(@Nonnull String entityType, @Nonnull Reason reason,
      @Nonnull String fieldPath, @Nullable String rawValue, @Nonnull String message) {
    super(message);
    this.entityType = entityType;
    this.reason = reason;
    this.fieldPath = fieldPath;
    this.rawValue = rawValue;
  }
}
