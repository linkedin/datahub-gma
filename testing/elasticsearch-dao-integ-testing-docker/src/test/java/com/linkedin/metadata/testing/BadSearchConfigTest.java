package com.linkedin.metadata.testing;

import com.linkedin.metadata.dao.exception.ESQueryException;
import com.linkedin.metadata.dao.search.BaseSearchConfig;
import com.linkedin.metadata.dao.search.ESSearchDAO;
import com.linkedin.metadata.testing.annotations.SearchIndexMappings;
import com.linkedin.metadata.testing.annotations.SearchIndexSettings;
import com.linkedin.metadata.testing.annotations.SearchIndexType;
import com.linkedin.testing.BarSearchDocument;
import java.util.Collections;
import java.util.Set;
import javax.annotation.Nonnull;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;


@ElasticsearchIntegrationTest
public class BadSearchConfigTest {
  @SearchIndexType(BarSearchDocument.class)
  @SearchIndexSettings("/settings.json")
  @SearchIndexMappings("/mappings.json")
  public SearchIndex<BarSearchDocument> _searchIndex;

  private static final class SearchConfig extends BaseSearchConfig<BarSearchDocument> {

    @Nonnull
    @Override
    public Set<String> getFacetFields() {
      return Collections.emptySet();
    }

    @Nonnull
    @Override
    public Class<BarSearchDocument> getSearchDocument() {
      return BarSearchDocument.class;
    }

    @Nonnull
    @Override
    public String getDefaultAutocompleteField() {
      return "";
    }

    @Nonnull
    @Override
    public String getSearchQueryTemplate() {
      return "invalid";
    }

    @Nonnull
    @Override
    public String getAutocompleteQueryTemplate() {
      return "invalid";
    }
  }

  /**
   * Tests that invalid query templates cause an exception when searching via the DAO, ensuring that {@link
   * BaseSearchSanityTests#canUseSearchQueryTemplate()} ()} is valid test.
   */
  @Test
  public void invalidSearchQueryTemplateDoesThrowOnSearch() {
    // given
    final ESSearchDAO<BarSearchDocument> dao = _searchIndex.createReadDao(new SearchConfig());

    // then
    final Throwable thrown = catchThrowable(() -> dao.search("", null, null, 0, 1));
    assertThat(thrown).isInstanceOf(ESQueryException.class).hasCauseInstanceOf(ElasticsearchStatusException.class);
    assertThat(((ElasticsearchStatusException) thrown.getCause()).status()).isEqualTo(RestStatus.BAD_REQUEST);
  }

  /**
   * Tests that invalid query templates cause an exception when searching via the DAO, ensuring that {@link
   * BaseSearchSanityTests#canUseAutocompleteTemplate()} is valid test.
   */
  @Test
  public void invalidAutocompleteQueryTemplateDoesThrowOnSearch() {
    // given
    final ESSearchDAO<BarSearchDocument> dao = _searchIndex.createReadDao(new SearchConfig());

    // then
    final Throwable thrown = catchThrowable(() -> dao.autoComplete("", null, null, 1));
    assertThat(thrown).isInstanceOf(ESQueryException.class).hasCauseInstanceOf(ElasticsearchStatusException.class);
    assertThat(((ElasticsearchStatusException) thrown.getCause()).status()).isEqualTo(RestStatus.BAD_REQUEST);
  }
}
