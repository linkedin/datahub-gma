package com.linkedin.metadata.testing;

import javax.annotation.Nonnull;
import org.elasticsearch.client.RestHighLevelClient;


/**
 * POJO to hold Elasticsearch client objects.
 */
public final class ElasticsearchConnection {
  private final RestHighLevelClient _restHighLevelClient;

  public ElasticsearchConnection(@Nonnull RestHighLevelClient restHighLevelClient) {
    _restHighLevelClient = restHighLevelClient;
  }

  @Nonnull
  public RestHighLevelClient getRestHighLevelClient() {
    return _restHighLevelClient;
  }
}
