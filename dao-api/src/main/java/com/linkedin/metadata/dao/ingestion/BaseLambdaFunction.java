package com.linkedin.metadata.dao.ingestion;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


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
  public abstract <URN extends Urn> ASPECT apply(@Nonnull URN urn, @Nullable Optional<ASPECT> oldAspect,
      @Nonnull ASPECT newAspect);
}
