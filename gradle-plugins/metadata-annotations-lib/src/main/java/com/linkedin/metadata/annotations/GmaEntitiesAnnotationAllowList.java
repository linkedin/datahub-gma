package com.linkedin.metadata.annotations;

import com.linkedin.data.schema.RecordDataSchema;
import javax.annotation.Nonnull;


/**
 * Simple allow list for the {@code @gma.aspect.entities} annotation.
 *
 * <p>This annotation is only used for helping us migrate old "common" aspects to v5 and should not be used for anything
 * new in v5.
 */
public interface GmaEntitiesAnnotationAllowList {
  final class AnnotationNotAllowedException extends RuntimeException {
    public AnnotationNotAllowedException(String message) {
      super(message);
    }
  }

  void check(@Nonnull RecordDataSchema schema, @Nonnull AspectAnnotation aspectAnnotation);
}
