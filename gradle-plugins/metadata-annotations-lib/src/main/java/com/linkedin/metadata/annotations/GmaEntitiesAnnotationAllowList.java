package com.linkedin.metadata.annotations;

import com.linkedin.data.schema.RecordDataSchema;
import javax.annotation.Nonnull;


/**
 * Simple allow list for the {@code @gma.aspect.entities} annotation.
 *
 * <p>This annotation is only used for helping us migrate old "common" aspects to v5 and should not be used for anything
 * new in v5.
 *
 * <p>You should be able to override the default by accessing the generator plugin extension. It is recommended that you
 * make your own wrapper plugin to change this default, and distribute that plugin within your organization for
 * consistency.
 */
public interface GmaEntitiesAnnotationAllowList {
  final class AnnotationNotAllowedException extends RuntimeException {
    public AnnotationNotAllowedException(String message) {
      super(message);
    }
  }

  void check(@Nonnull RecordDataSchema schema, @Nonnull AspectAnnotation aspectAnnotation);
}
