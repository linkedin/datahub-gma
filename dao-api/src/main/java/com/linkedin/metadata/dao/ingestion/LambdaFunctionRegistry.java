package com.linkedin.metadata.dao.ingestion;

import com.linkedin.data.template.RecordTemplate;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * A registry maintains mappings between ASPECT and its corresponding BaseLambdaFunction.
 */
public interface LambdaFunctionRegistry {

  /**
   * Get corresponding lambda functions for an aspect. Returns null if not found.
   */
  @Nullable
  <ASPECT extends RecordTemplate> BaseLambdaFunction getLambdaFunctions(@Nonnull final ASPECT aspect);

  /**
   * Check if lambda functions are registered for an aspect.
   */
  <ASPECT extends RecordTemplate> boolean isRegistered(@Nonnull final Class<ASPECT> aspectClass);
}
