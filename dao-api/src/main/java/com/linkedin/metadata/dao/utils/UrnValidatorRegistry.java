package com.linkedin.metadata.dao.utils;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.dao.exception.InvalidUrnException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;


/**
 * Registry that maps entity types to {@link UrnValidator} instances.
 *
 * <p>Each DAO holds its own registry so that different services can register different
 * validation rules per entity type. Entity types without an explicit registration
 * fall back to the configured default validator ({@link DefaultUrnValidator} unless
 * overridden via {@link Builder#defaultValidator}).
 */
@Slf4j
public final class UrnValidatorRegistry {

  private final Map<String, UrnValidator> _validators;
  private final UrnValidator _defaultValidator;

  private UrnValidatorRegistry(@Nonnull Map<String, UrnValidator> validators,
      @Nonnull UrnValidator defaultValidator) {
    _validators = Collections.unmodifiableMap(new HashMap<>(validators));
    _defaultValidator = defaultValidator;
  }

  @Nonnull
  public UrnValidator getValidator(@Nonnull Urn urn) {
    return _validators.getOrDefault(urn.getEntityType(), _defaultValidator);
  }

  /**
   * Looks up the validator for the URN's entity type, validates, and logs on rejection.
   *
   * @param operation  label for the write operation (e.g. "add", "save", "delete")
   * @param auditStamp caller's audit stamp — used for forensic tracing in the rejection log
   * @param urn        the URN to validate
   * @throws InvalidUrnException if the URN fails validation
   */
  public void validateUrnForWrite(@Nonnull String operation, @Nullable AuditStamp auditStamp,
      @Nonnull Urn urn) {
    UrnValidator validator = getValidator(urn);
    try {
      validator.validate(urn);
    } catch (InvalidUrnException e) {
      logRejection(e, operation, auditStamp, urn.toString());
      throw e;
    }
  }

  private static void logRejection(@Nonnull InvalidUrnException e, @Nonnull String operation,
      @Nullable AuditStamp auditStamp, @Nonnull String fullUrn) {
    log.error(
        "URN validation rejected operation={} entityType={} reason={} field={} value={} actor={} urn={}",
        DefaultUrnValidator.quote(operation), DefaultUrnValidator.quote(e.getEntityType()),
        e.getReason(), DefaultUrnValidator.quote(e.getFieldPath()),
        e.getRawValue() == null ? "null" : DefaultUrnValidator.quote(e.getRawValue()),
        DefaultUrnValidator.quote(actorOf(auditStamp)), DefaultUrnValidator.quote(fullUrn));
  }

  @Nonnull
  private static String actorOf(@Nullable AuditStamp stamp) {
    return (stamp == null || !stamp.hasActor()) ? "unknown" : stamp.getActor().toString();
  }

  @Nonnull
  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private final Map<String, UrnValidator> _validators = new HashMap<>();
    private UrnValidator _defaultValidator = new DefaultUrnValidator();

    private Builder() {
    }

    @Nonnull
    public Builder register(@Nonnull String entityType, @Nonnull UrnValidator validator) {
      _validators.put(entityType, validator);
      return this;
    }

    @Nonnull
    public Builder defaultValidator(@Nonnull UrnValidator validator) {
      _defaultValidator = validator;
      return this;
    }

    @Nonnull
    public UrnValidatorRegistry build() {
      return new UrnValidatorRegistry(_validators, _defaultValidator);
    }
  }
}
