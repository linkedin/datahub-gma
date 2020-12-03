package com.linkedin.metadata.testing;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.search.BaseSearchConfig;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


final class TestSearchConfig<DOCUMENT extends RecordTemplate> extends BaseSearchConfig<DOCUMENT> {
  private final BaseSearchConfig<DOCUMENT> _wrapper;
  private final String _indexName;

  TestSearchConfig(BaseSearchConfig<DOCUMENT> wrapper, String indexName) {
    _wrapper = wrapper;
    _indexName = indexName;
  }

  @Nonnull
  @Override
  public Set<String> getFacetFields() {
    return _wrapper.getFacetFields();
  }

  @Nullable
  @Override
  public Set<String> getLowCardinalityFields() {
    return _wrapper.getLowCardinalityFields();
  }

  @Nonnull
  @Override
  public String getIndexName() {
    return _indexName;
  }

  @Nonnull
  @Override
  public Class<DOCUMENT> getSearchDocument() {
    return _wrapper.getSearchDocument();
  }

  @Nonnull
  @Override
  public String getDefaultAutocompleteField() {
    return _wrapper.getDefaultAutocompleteField();
  }

  @Nonnull
  @Override
  public String getSearchQueryTemplate() {
    return _wrapper.getSearchQueryTemplate();
  }

  @Nonnull
  @Override
  public String getAutocompleteQueryTemplate() {
    return _wrapper.getAutocompleteQueryTemplate();
  }
}
