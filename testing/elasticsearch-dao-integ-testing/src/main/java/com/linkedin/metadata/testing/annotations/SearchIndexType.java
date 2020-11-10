package com.linkedin.metadata.testing.annotations;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.testing.SearchIndex;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import javax.annotation.Nonnull;


/**
 * Annotates the given {@link SearchIndex} field with the document type.
 *
 * <p>Required annotation for {@link SearchIndex} instances in tests.</p>
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface SearchIndexType {
  /**
   * The search document class for this index.
   *
   * <p>Used to create an instance of the {@link SearchIndex} during testing.
   */
  @Nonnull
  Class<? extends RecordTemplate> value();
}
