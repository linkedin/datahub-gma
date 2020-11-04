package com.linkedin.metadata.testing;

import javax.annotation.Nonnull;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.transport.TransportClient;


/**
 * POJO to hold Elasticsearch client objects.
 */
public final class ElasticsearchConnection {
  private final RestHighLevelClient _restHighLevelClient;
  private final TransportClient _transportClient;

  public ElasticsearchConnection(@Nonnull RestHighLevelClient restHighLevelClient,
      @Nonnull TransportClient transportClient) {
    _restHighLevelClient = restHighLevelClient;
    _transportClient = transportClient;
  }

  @Nonnull
  public RestHighLevelClient getRestHighLevelClient() {
    return _restHighLevelClient;
  }

  @Nonnull
  public TransportClient getTransportClient() {
    return _transportClient;
  }
}
