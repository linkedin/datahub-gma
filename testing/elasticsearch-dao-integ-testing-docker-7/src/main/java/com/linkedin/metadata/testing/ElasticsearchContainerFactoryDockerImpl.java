package com.linkedin.metadata.testing;

import javax.annotation.Nonnull;
import org.apache.http.HttpHost;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.testcontainers.containers.GenericContainer;


/**
 * Uses the TestContainers framework to launch an Elasticsearch instance using docker.
 */
@ElasticsearchContainerFactory.Implementation
public final class ElasticsearchContainerFactoryDockerImpl implements ElasticsearchContainerFactory {
  private static final String IMAGE_NAME = "docker.elastic.co/elasticsearch/elasticsearch:5.6.8";
  private static final int HTTP_PORT = 9200;

  /**
   * Simple implementation that has no extra behavior and is just used to help with the generic typing.
   */
  private static final class GenericContainerImpl extends GenericContainer<GenericContainerImpl> {
    public GenericContainerImpl(@Nonnull String dockerImageName) {
      super(dockerImageName);
    }
  }

  private GenericContainerImpl _container;

  @Nonnull
  private static RestHighLevelClient buildRestClient(@Nonnull GenericContainerImpl gc) {
    final RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", gc.getMappedPort(HTTP_PORT), "http"))
        .setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultIOReactorConfig(
            IOReactorConfig.custom().setIoThreadCount(1).build()));

    builder.setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder.
        setConnectionRequestTimeout(3000));

    return new RestHighLevelClient(builder);
  }

  @Nonnull
  @Override
  public ElasticsearchConnection start() throws Exception {
    if (_container == null) {
      _container = new GenericContainerImpl(IMAGE_NAME).withExposedPorts(HTTP_PORT)
          .withEnv("xpack.security.enabled", "false");
      _container.start();
    }

    return new ElasticsearchConnection(buildRestClient(_container));
  }

  @Override
  public void close() throws Throwable {
    if (_container == null) {
      return;
    }

    try {
      _container.close();
    } finally {
      _container = null;
    }
  }
}
