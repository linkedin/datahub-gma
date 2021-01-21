package com.linkedin.metadata.testing;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.search.BaseSearchConfig;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * Search config for use in testing, which can wrap another config but use a different index name.
 *
 * <p>Useful for tests which want to name the index after the test, not the document.
 */
final class TestSearchConfig<DOCUMENT extends RecordTemplate> extends BaseSearchConfig<DOCUMENT> {
  private final BaseSearchConfig<DOCUMENT> _wrapped;
  private final String _indexName;

  /**
   * Constructor.
   *
   * @param wrapped the config to wrap
   * @param indexName the ES index to use
   */
  TestSearchConfig(@Nonnull BaseSearchConfig<DOCUMENT> wrapped, @Nonnull String indexName) {
    _wrapped = wrapped;
    _indexName = indexName;
  }

  @Nonnull
  @Override
  public Set<String> getFacetFields() {
    return _wrapped.getFacetFields();
  }

  @Nullable
  @Override
  public Set<String> getLowCardinalityFields() {
    return _wrapped.getLowCardinalityFields();
  }

  @Nonnull
  @Override
  public String getIndexName() {
    return _indexName;
  }

  @Nonnull
  @Override
  public Class<DOCUMENT> getSearchDocument() {
    return _wrapped.getSearchDocument();
  }

  @Nonnull
  @Override
  public String getDefaultAutocompleteField() {
    return _wrapped.getDefaultAutocompleteField();
  }

  @Nonnull
  @Override
  public String getSearchQueryTemplate() {
    return _wrapped.getSearchQueryTemplate();
  }

  @Nonnull
  @Override
  public String getAutocompleteQueryTemplate() {
    return _wrapped.getAutocompleteQueryTemplate();
  }
}
