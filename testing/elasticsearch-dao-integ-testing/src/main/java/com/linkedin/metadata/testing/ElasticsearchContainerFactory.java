package com.linkedin.metadata.testing;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import javax.annotation.Nonnull;
import org.junit.jupiter.api.extension.ExtensionContext;


/**
 * Factory which can start and stop an Elasticsearch instance.
 */
public interface ElasticsearchContainerFactory extends ExtensionContext.Store.CloseableResource {
  @Target(ElementType.TYPE)
  @Retention(RetentionPolicy.RUNTIME)
  @interface Implementation {
  }

  /**
   * Starts an Elasticsearch instance for testing and returns clients connected to it.
   */
  @Nonnull
  ElasticsearchConnection start() throws Exception;
}
