package com.linkedin.metadata.testing.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 * Indicates the settings this index should be created with.
 *
 * <p>Optional parameter for {@link com.linkedin.metadata.testing.SearchIndex}es in tests. Can be set directly on the
 * index after creation with {@link com.linkedin.metadata.testing.SearchIndex#setSettings(String)} (String)}.
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface SearchIndexSettings {
  /**
   * The JSON resource file to load Elasticsearch settings from.
   */
  String value();
}
