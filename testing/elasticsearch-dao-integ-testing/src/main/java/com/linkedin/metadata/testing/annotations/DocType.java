package com.linkedin.metadata.testing.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 * Indicates the document type used for the documents in this index.
 *
 * <p>Optional parameter for {@link com.linkedin.metadata.testing.SearchIndex}es in tests.
 *
 * <p>Primarily a work around for LinkedIn internally, where Datasets are old and did not set the doc type to "doc".
 *
 * <p>Also note that doctypes as a whole are deprecated in ES7, so this annotation is not carried forward to the ES7
 * framework.
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface DocType {
  /**
   * The document type.
   */
  String value();
}
