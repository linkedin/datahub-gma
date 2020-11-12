package com.linkedin.metadata.testing;

import com.linkedin.data.template.RecordTemplate;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;


/**
 * Factory to create {@link SearchIndex} instances for testing.
 */
final class SearchIndexFactory {
  private final ElasticsearchConnection _connection;

  SearchIndexFactory(@Nonnull ElasticsearchConnection connection) {
    _connection = connection;
  }

  /**
   * Creates a search index to read / write the given document type for testing.
   *
   * <p>This will create an index on the Elasticsearch instance with a unique name.
   *
   * @param documentClass the document type
   * @param name the name to use for the index
   */
  public <DOCUMENT extends RecordTemplate> SearchIndex<DOCUMENT> createIndex(@Nonnull Class<DOCUMENT> documentClass,
      @Nonnull String name, @Nullable String settingsJson, @Nullable String mappingsJson) {
    final CreateIndexRequest createIndexRequest = new CreateIndexRequest(name);

    if (settingsJson != null) {
      createIndexRequest.settings(settingsJson, XContentType.JSON);
    }

    if (mappingsJson != null) {
      // TODO
      createIndexRequest.mapping("doc", mappingsJson, XContentType.JSON);
    }

    try {
      _connection.getTransportClient().admin().indices().create(createIndexRequest).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }

    return new SearchIndex<>(documentClass, _connection, name);
  }
}
