package com.linkedin.metadata.testing;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.browse.BaseBrowseConfig;
import com.linkedin.metadata.dao.search.BaseSearchConfig;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


final class TestBrowseConfig<DOCUMENT extends RecordTemplate> extends BaseBrowseConfig<DOCUMENT> {
  private final BaseBrowseConfig<DOCUMENT> _wrapper;
  private final String _indexName;

  TestBrowseConfig(BaseBrowseConfig<DOCUMENT> wrapper, String indexName) {
    _wrapper = wrapper;
    _indexName = indexName;
  }

  @Override
  public Class<DOCUMENT> getSearchDocument() {
    return _wrapper.getSearchDocument();
  }

  @Nonnull
  @Override
  public String getBrowseDepthFieldName() {
    return _wrapper.getBrowseDepthFieldName();
  }

  @Nonnull
  @Override
  public String getBrowsePathFieldName() {
    return _wrapper.getBrowsePathFieldName();
  }

  @Nonnull
  @Override
  public String getUrnFieldName() {
    return _wrapper.getUrnFieldName();
  }

  @Nonnull
  @Override
  public String getSortingField() {
    return _wrapper.getSortingField();
  }

  @Nonnull
  @Override
  public String getRemovedField() {
    return _wrapper.getRemovedField();
  }

  @Override
  public boolean hasFieldInSchema(@Nonnull String fieldName) {
    return _wrapper.hasFieldInSchema(fieldName);
  }

  @Nonnull
  @Override
  public String getIndexName() {
    return _indexName;
  }
}
