package com.linkedin.metadata.testing;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.SearchResult;
import com.linkedin.metadata.dao.search.BaseSearchConfig;
import javax.annotation.Nonnull;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import static com.linkedin.metadata.testing.asserts.SearchIndexAssert.assertThat;
import static com.linkedin.metadata.testing.asserts.SearchResultAssert.assertThat;


/**
 * Base class for simple, sanity tests that verify data can be written to / read from an elastic search index (i.e.
 * settings and mappings are valid).
 */
@ElasticsearchIntegrationTest
public abstract class BaseSearchSanityTests<T extends RecordTemplate> {
  private final Urn _urn;
  private final T _data;
  private final BaseSearchConfig<T> _searchConfig;

  protected BaseSearchSanityTests(@Nonnull Urn urn, @Nonnull T data, @Nonnull BaseSearchConfig<T> searchConfig) {
    _urn = urn;
    _data = data;
    _searchConfig = searchConfig;
  }

  /**
   * Returns the test index to test.
   *
   * <p>This is a getter as {@link SearchIndex} must be constructed / declared in the subclass using annotations.
   */
  @Nonnull
  public abstract SearchIndex<T> getIndex();

  @Test
  public void canWriteDocuments() throws Exception {
    // given static test data

    // when
    getIndex().getWriteDao().upsertDocument(_data, _urn.toString());
    getIndex().getRequestContainer().flushAndSettle();

    // then
    assertThat(getIndex()).bulkRequests().allRequestsSettled();
    assertThat(getIndex()).bulkRequests().hadNoErrors();
    assertThat(getIndex()).bulkRequests().documentIds().containsExactly(_urn.toString());
  }

  @Test
  public void canReadDocuments() throws Exception {
    // given
    getIndex().getWriteDao().upsertDocument(_data, _urn.toString());
    getIndex().getRequestContainer().flushAndSettle();

    // when
    final SearchResult<T> result = getIndex().createReadAllDocumentsDao().search("", null, null, 0, 1);

    // then
    assertThat(result).hasNoMoreResults();
    assertThat(result).hasTotalCount(1);
    assertThat(result).documents().containsExactly(_data);
    assertThat(result).urns().containsExactly(_urn);
  }

  @Test
  public void canCreateDaoWithConfig() {
    Assertions.assertThatCode(() -> getIndex().createReadDao(_searchConfig)).doesNotThrowAnyException();
  }

  @Test
  public void canUseSearchQueryTemplate() throws Exception {
    // given
    getIndex().getWriteDao().upsertDocument(_data, _urn.toString());
    getIndex().getRequestContainer().flushAndSettle();

    Assertions.assertThatCode(() -> {
      // when
      getIndex().createReadDao(_searchConfig).search("*", null, null, 0, 1);
      // then
    }).doesNotThrowAnyException();
  }

  @Test
  public void canUseAutocompleteTemplate() throws Exception {
    // given
    getIndex().getWriteDao().upsertDocument(_data, _urn.toString());
    getIndex().getRequestContainer().flushAndSettle();

    Assertions.assertThatCode(() -> {
      // when
      getIndex().createReadDao(_searchConfig).autoComplete("*", null, null, 1);
      // then
    }).doesNotThrowAnyException();
  }
}
