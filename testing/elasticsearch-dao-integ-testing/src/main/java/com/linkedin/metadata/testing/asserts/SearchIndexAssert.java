package com.linkedin.metadata.testing.asserts;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.testing.SearchIndex;
import javax.annotation.Nonnull;
import org.assertj.core.api.AbstractAssert;


/**
 * Assertions for {@link SearchIndex} with assertj.
 */
public class SearchIndexAssert<DOCUMENT extends RecordTemplate>
    extends AbstractAssert<SearchIndexAssert<DOCUMENT>, SearchIndex<DOCUMENT>> {
  public SearchIndexAssert(@Nonnull SearchIndex<DOCUMENT> actual) {
    super(actual, SearchIndexAssert.class);
  }

  public static <DOCUMENT extends RecordTemplate> SearchIndexAssert<DOCUMENT> assertThat(
      @Nonnull SearchIndex<DOCUMENT> actual) {
    return new SearchIndexAssert<>(actual);
  }

  /**
   * Allows easy access to assert the state of the bulk requests.
   */
  public BulkRequestsContainerAssert bulkRequests() {
    return BulkRequestsContainerAssert.assertThat(actual.getRequestContainer()).as("Index `%s`", actual.getName());
  }
}
