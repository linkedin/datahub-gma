package com.linkedin.metadata.dao.utils;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.dao.exception.InvalidUrnException;
import javax.annotation.Nonnull;


/**
 * Validates a Pegasus URN before database writes.
 *
 * <p>Implementations define entity-type-specific validation rules. Register custom validators
 * with {@link UrnValidatorRegistry} to override the {@link DefaultUrnValidator} for specific
 * entity types.
 */
@FunctionalInterface
public interface UrnValidator {

  void validate(@Nonnull Urn urn) throws InvalidUrnException;
}
