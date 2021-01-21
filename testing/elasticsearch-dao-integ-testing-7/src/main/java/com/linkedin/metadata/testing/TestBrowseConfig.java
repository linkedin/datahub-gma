package com.linkedin.metadata.testing;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.browse.BaseBrowseConfig;
import javax.annotation.Nonnull;


/**
 * Browse config for use in testing, which can wrap another config but use a different index name.
 *
 * <p>Useful for tests which want to name the index after the test, not the document.
 */
final class TestBrowseConfig<DOCUMENT extends RecordTemplate> extends BaseBrowseConfig<DOCUMENT> {
  private final BaseBrowseConfig<DOCUMENT> _wrapped;
  private final String _indexName;

  /**
   * Constructor.
   *
   * @param wrapped the config to wrap
   * @param indexName the ES index to use
   */
  TestBrowseConfig(@Nonnull BaseBrowseConfig<DOCUMENT> wrapped, @Nonnull String indexName) {
    _wrapped = wrapped;
    _indexName = indexName;
  }

  @Override
  public Class<DOCUMENT> getSearchDocument() {
    return _wrapped.getSearchDocument();
  }

  @Nonnull
  @Override
  public String getBrowseDepthFieldName() {
    return _wrapped.getBrowseDepthFieldName();
  }

  @Nonnull
  @Override
  public String getBrowsePathFieldName() {
    return _wrapped.getBrowsePathFieldName();
  }

  @Nonnull
  @Override
  public String getUrnFieldName() {
    return _wrapped.getUrnFieldName();
  }

  @Nonnull
  @Override
  public String getSortingField() {
    return _wrapped.getSortingField();
  }

  @Nonnull
  @Override
  public String getRemovedField() {
    return _wrapped.getRemovedField();
  }

  @Override
  public boolean hasFieldInSchema(@Nonnull String fieldName) {
    return _wrapped.hasFieldInSchema(fieldName);
  }

  @Nonnull
  @Override
  public String getIndexName() {
    return _indexName;
  }
}
