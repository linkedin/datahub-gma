package com.linkedin.metadata.annotations;

import com.linkedin.data.schema.DataSchema;
import javax.annotation.Nonnull;


public class AlwaysAllowList implements GmaEntitiesAnnotationAllowList {
  @Override
  public void check(@Nonnull DataSchema schema, @Nonnull AspectAnnotation aspectAnnotation) {
    // nothing
  }
}
