package com.linkedin.metadata.annotations;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.data.schema.DataSchema;
import javax.annotation.Nonnull;


@VisibleForTesting
public class AlwaysAllowList implements GmaEntitiesAnnotationAllowList {
  @Override
  public void check(@Nonnull DataSchema schema, @Nonnull AspectAnnotation aspectAnnotation) {
    // nothing
  }
}
