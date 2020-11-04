package com.linkedin.metadata.testing.annotations;

import com.linkedin.data.template.RecordTemplate;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import javax.annotation.Nonnull;


/**
 * Annotates the given {@link com.linkedin.metadata.testing.SearchIndex} field with the document type.
 *
 * <p>Required annotation for {@link com.linkedin.metadata.testing.SearchIndex} instances in tests.</p>
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface SearchIndexType {
  /**
   * The search document class for this index.
   *
   * <p>Used to create an instance of the {@link com.linkedin.metadata.testing.SearchIndex} during testing.
   */
  @Nonnull
  Class<? extends RecordTemplate> value();
}
