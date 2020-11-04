package com.linkedin.metadata.testing;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;


/**
 * JUnit 5 extension to start an Elasticsearch instance and create indexes for testing with GMA.
 *
 * <p>See {@link ElasticsearchIntegrationTest}.
 */
final class ElasticsearchIntegrationTestExtension
    implements BeforeAllCallback, AfterAllCallback, BeforeEachCallback, AfterEachCallback {

  @Override
  public void afterAll(ExtensionContext context) throws Exception {
    // TODO
  }

  @Override
  public void afterEach(ExtensionContext context) throws Exception {
    // TODO
  }

  @Override
  public void beforeAll(ExtensionContext context) throws Exception {
    // TODO
  }

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    // TODO
  }
}
