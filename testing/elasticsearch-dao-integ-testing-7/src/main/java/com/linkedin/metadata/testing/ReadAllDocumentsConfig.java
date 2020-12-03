package com.linkedin.metadata.testing;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.search.BaseSearchConfig;
import java.util.Collections;
import java.util.Set;
import javax.annotation.Nonnull;


/**
 * Simple config for testing that will always use a query all query for searches and autocomplete.
 */
class ReadAllDocumentsConfig<DOCUMENT extends RecordTemplate> extends BaseSearchConfig<DOCUMENT> {
  private final Class<DOCUMENT> _documentClass;

  ReadAllDocumentsConfig(@Nonnull Class<DOCUMENT> documentClass) {
    _documentClass = documentClass;
  }

  @Nonnull
  @Override
  public Set<String> getFacetFields() {
    return Collections.emptySet();
  }

  @Nonnull
  @Override
  public Class<DOCUMENT> getSearchDocument() {
    return _documentClass;
  }

  @Nonnull
  @Override
  public String getDefaultAutocompleteField() {
    return "";
  }

  @Nonnull
  @Override
  public String getSearchQueryTemplate() {
    return "{\"match_all\": {} }";
  }

  @Nonnull
  @Override
  public String getAutocompleteQueryTemplate() {
    return "{\"match_all\": {} }";
  }
}
