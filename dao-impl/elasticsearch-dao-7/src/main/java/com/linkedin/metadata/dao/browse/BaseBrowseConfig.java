package com.linkedin.metadata.dao.browse;

import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.RecordTemplate;
import java.util.Collections;
import java.util.Set;
import javax.annotation.Nonnull;


public abstract class BaseBrowseConfig<DOCUMENT extends RecordTemplate> {
  private static RecordDataSchema searchDocumentSchema;

  @Nonnull
  public String getBrowseDepthFieldName() {
    return "browsePaths.length";
  }

  @Nonnull
  public String getBrowsePathFieldName() {
    return "browsePaths";
  }

  @Nonnull
  public String getUrnFieldName() {
    return "urn";
  }

  @Nonnull
  public String getSortingField() {
    return "urn";
  }

  @Nonnull
  public String getRemovedField() {
    return "removed";
  }

  public boolean hasFieldInSchema(@Nonnull String fieldName) {
    return getSearchDocumentSchema().contains(fieldName);
  }

  private RecordDataSchema getSearchDocumentSchema() {
    if (searchDocumentSchema == null) {
      try {
        searchDocumentSchema = getSearchDocument().newInstance().schema();
      } catch (InstantiationException | IllegalAccessException e) {
        throw new RuntimeException("Couldn't create an instance of the search document");
      }
    }
    return searchDocumentSchema;
  }

  @Nonnull
  public String getIndexName() {
    return getSearchDocument().getSimpleName().toLowerCase();
  }

  public boolean shouldUseMapExecutionHint() {
    return false;
  }

  /**
   * By default cache is disabled.
   * @return whether caching is enabled.
   */
  public boolean enableCache() {
    return false;
  }

  /**
   * To avoid cold cache on application start up, eagerly load some search result into cache when
   * DAO instance is instantiated.
   * @return a set of browse paths for which search result will be loaded into cache on DAO instantiation.
   */
  @Nonnull
  public Set<String> eagerLoadCachedBrowsePaths() {
    // By default, only cache the top level categories.
    return Collections.singleton("");
  }

  public abstract Class<DOCUMENT> getSearchDocument();
}
