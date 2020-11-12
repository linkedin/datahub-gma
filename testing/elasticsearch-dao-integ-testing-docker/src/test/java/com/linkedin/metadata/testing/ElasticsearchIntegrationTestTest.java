package com.linkedin.metadata.testing;

import com.linkedin.metadata.dao.SearchResult;
import com.linkedin.metadata.testing.annotations.SearchIndexMappings;
import com.linkedin.metadata.testing.annotations.SearchIndexSettings;
import com.linkedin.metadata.testing.annotations.SearchIndexType;
import com.linkedin.testing.BarSearchDocument;
import com.linkedin.testing.urn.BarUrn;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.settings.Settings;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

import static com.linkedin.metadata.testing.asserts.SearchIndexAssert.assertThat;
import static com.linkedin.metadata.testing.asserts.SearchResultAssert.assertThat;
import static org.assertj.core.api.Assertions.assertThat;


@ElasticsearchIntegrationTest
public class ElasticsearchIntegrationTestTest {
  @SearchIndexType(BarSearchDocument.class)
  public static SearchIndex<BarSearchDocument> classIndex;

  @SearchIndexType(BarSearchDocument.class)
  @SearchIndexSettings("/settings.json")
  @SearchIndexMappings("/mappings.json")
  public SearchIndex<BarSearchDocument> searchIndex;

  @SearchIndexType(BarSearchDocument.class)
  public SearchIndex<BarSearchDocument> secondSearchIndex;

  private static String _classIndexName;
  private static String _methodIndexName;

  @Test
  public void staticIndexInjected() {
    assertThat(classIndex).isNotNull();
  }

  @Test
  public void instanceIndexesInjected() {
    assertThat(searchIndex).isNotNull();
    assertThat(secondSearchIndex).isNotNull();
  }

  @Test
  public void uniqueIndexesAreMadeForEachVariable() {
    assertThat(classIndex.getName()).isNotEqualTo(searchIndex.getName());
    assertThat(searchIndex.getName()).isNotEqualTo(secondSearchIndex.getName());
  }

  @Test
  @Order(1)
  public void saveIndexNames() {
    // not a real test, values used to test the life cycle later
    _classIndexName = classIndex.getName();
    _methodIndexName = searchIndex.getName();
  }

  @Test
  @Order(2)
  public void staticIndexIsSame() {
    assertThat(_classIndexName).isEqualTo(classIndex.getName());
  }

  @Test
  @Order(2)
  public void instanceIndexIsDifferent() {
    assertThat(_methodIndexName).isNotEqualTo(searchIndex.getName());
  }

  @Test
  @Order(2)
  public void instanceIsCleanedUpBetweenMethods() {
    // given
    final IndicesAdminClient indicesAdminClient = searchIndex.getConnection().getTransportClient().admin().indices();

    // when
    final boolean exists = indicesAdminClient.prepareExists(_methodIndexName).get().isExists();

    // then
    assertThat(exists).isFalse();
  }

  @Test
  public void canWriteToIndex() throws Exception {
    // given
    final BarSearchDocument searchDocument = new BarSearchDocument().setUrn(new BarUrn(42));

    // when
    searchIndex.getWriteDao().upsertDocument(searchDocument, "mydoc");
    searchIndex.getRequestContainer().flushAndSettle();

    // then
    assertThat(searchIndex).bulkRequests().documentIds().containsExactly("mydoc");
  }

  @Test
  public void canReadAllFromIndex() throws Exception {
    // given
    final BarUrn urn = new BarUrn(42);
    final BarSearchDocument searchDocument = new BarSearchDocument().setUrn(urn);
    searchIndex.getWriteDao().upsertDocument(searchDocument, "mydoc");
    searchIndex.getRequestContainer().flushAndSettle();

    // when
    final SearchResult<BarSearchDocument> result = searchIndex.createReadAllDocumentsDao().search("", null, null, 0, 1);

    // then
    assertThat(result).hasNoMoreResults();
    assertThat(result).hasTotalCount(1);
    assertThat(result).documents().containsExactly(searchDocument);
    assertThat(result).urns().containsExactly(urn);
  }

  @Test
  public void settingsAndMappingsAnnotation() throws Exception {
    // when
    final GetSettingsResponse response = searchIndex.getConnection()
        .getTransportClient()
        .admin()
        .indices()
        .prepareGetSettings(searchIndex.getName())
        .get();
    final Settings settings = response.getIndexToSettings().get(searchIndex.getName());
    final String actual = settings.get("index.analysis.filter.autocomplete_filter.type");

    // then
    assertThat(actual).isEqualTo("edge_ngram");
  }
}
