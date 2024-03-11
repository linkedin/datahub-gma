package com.linkedin.metadata.dao.ingestion;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import javax.annotation.Nonnull;


/**
 * Apply lambda functions based on an ASPECT.
 */
public abstract class BaseLambdaFunction<ASPECT extends RecordTemplate> {

  private final Class<ASPECT> _aspectClass;

  public BaseLambdaFunction(@Nonnull Class<ASPECT> aspectClass) {
    _aspectClass = aspectClass;
  }

  /**
   * Returns a corresponding lambda function update for the given metadata aspect.
   */
  @Nonnull
  public abstract <URN extends Urn> ASPECT applyLambdaFunctions(@Nonnull URN urn, @Nonnull ASPECT aspect);
}
