package com.linkedin.metadata.testing.asserts;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.SearchResult;
import java.util.ArrayList;
import javax.annotation.Nonnull;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ListAssert;


/**
 * Assertions for {@link SearchResult}.
 */
public class SearchResultAssert<DOCUMENT extends RecordTemplate>
    extends AbstractAssert<SearchResultAssert<DOCUMENT>, SearchResult<DOCUMENT>> {
  public SearchResultAssert(@Nonnull SearchResult<DOCUMENT> actual) {
    super(actual, SearchResultAssert.class);
  }

  public static <DOCUMENT extends RecordTemplate> SearchResultAssert<DOCUMENT> assertThat(
      SearchResult<DOCUMENT> actual) {
    return new SearchResultAssert<>(actual);
  }

  /**
   * Asserts that the total number of search hits is equal to the expected value.
   */
  public SearchResultAssert<DOCUMENT> hasTotalCount(int expected) {
    if (expected != actual.getTotalCount()) {
      failWithMessage("Expected %s total results, got %s.", expected, actual.getTotalCount());
    }
    return this;
  }

  /**
   * Asserts that there are more results from this query.
   */
  public SearchResultAssert<DOCUMENT> hasMoreResults() {
    if (!actual.isHavingMore()) {
      failWithMessage("Expected more results but had no more.");
    }
    return this;
  }

  /**
   * Asserts that there are no more results from this query.
   */
  public SearchResultAssert<DOCUMENT> hasNoMoreResults() {
    if (actual.isHavingMore()) {
      failWithMessage("Expected no more results but had more.");
    }
    return this;
  }

  /**
   * Returns an assertion on the result's Urns.
   */
  public ListAssert<Urn> urns() {
    return Assertions.assertThat(new ArrayList<>(actual.getSearchResultMetadata().getUrns())).as("Search result urns");
  }

  /**
   * Returns an assertion on the result's documents.
   */
  public ListAssert<DOCUMENT> documents() {
    return Assertions.assertThat(actual.getDocumentList()).as("Search result documents");
  }
}