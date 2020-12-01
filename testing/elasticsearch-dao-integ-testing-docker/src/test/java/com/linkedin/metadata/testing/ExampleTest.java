package com.linkedin.metadata.testing;

import com.linkedin.metadata.dao.SearchResult;
import com.linkedin.metadata.testing.annotations.SearchIndexMappings;
import com.linkedin.metadata.testing.annotations.SearchIndexSettings;
import com.linkedin.metadata.testing.annotations.SearchIndexType;
import com.linkedin.metadata.testing.asserts.SearchResultAssert;
import com.linkedin.testing.BarSearchDocument;
import com.linkedin.testing.urn.BarUrn;
import org.junit.jupiter.api.Test;

import static com.linkedin.metadata.testing.asserts.SearchIndexAssert.assertThat;
import static org.assertj.core.api.Assertions.assertThat;


@ElasticsearchIntegrationTest
public class ExampleTest {
  @SearchIndexType(BarSearchDocument.class)
  @SearchIndexSettings("/settings.json")
  @SearchIndexMappings("/mappings.json")
  public SearchIndex<BarSearchDocument> _searchIndex;

  @Test
  public void canWriteToIndex() throws Exception {
    // given
    final BarSearchDocument searchDocument = new BarSearchDocument().setUrn(new BarUrn(42));

    // when
    _searchIndex.getWriteDao().upsertDocument(searchDocument, "mydoc");
    _searchIndex.getRequestContainer().flushAndSettle();

    // then
    assertThat(_searchIndex).bulkRequests().allRequestsSettled();
    assertThat(_searchIndex).bulkRequests().hadNoErrors();
    assertThat(_searchIndex).bulkRequests().documentIds().containsExactly("mydoc");
  }

  @Test
  public void canReadAllFromIndex() throws Exception {
    // given
    final BarUrn urn = new BarUrn(42);
    final BarSearchDocument searchDocument = new BarSearchDocument().setUrn(urn);
    _searchIndex.getWriteDao().upsertDocument(searchDocument, "mydoc");
    _searchIndex.getRequestContainer().flushAndSettle();

    // when
    final SearchResult<BarSearchDocument> result =
        _searchIndex.createReadAllDocumentsDao().search("", null, null, 0, 1);

    // then
    SearchResultAssert.assertThat(result).hasNoMoreResults();
    SearchResultAssert.assertThat(result).hasTotalCount(1);
    SearchResultAssert.assertThat(result).documents().containsExactly(searchDocument);
    SearchResultAssert.assertThat(result).urns().containsExactly(urn);
  }
}
