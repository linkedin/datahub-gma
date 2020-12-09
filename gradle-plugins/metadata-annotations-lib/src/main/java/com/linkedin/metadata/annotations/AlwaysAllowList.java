package com.linkedin.metadata.annotations;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.data.schema.RecordDataSchema;
import javax.annotation.Nonnull;


@VisibleForTesting
public class AlwaysAllowList implements GmaEntitiesAnnotationAllowList {
  @Override
  public void check(@Nonnull RecordDataSchema schema, @Nonnull AspectAnnotation aspectAnnotation) {
    // nothing
  }
}
