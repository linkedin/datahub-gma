package com.linkedin.metadata.dao.search;

import com.google.common.collect.ImmutableList;
import com.linkedin.testing.EntityDocument;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;

public class TestSearchConfig extends BaseSearchConfig<EntityDocument> {
  @Override
  @Nonnull
  public Set<String> getFacetFields() {
    return Collections.singleton("value");
  }

  @Override
  @Nonnull
  public Class<EntityDocument> getSearchDocument() {
    return EntityDocument.class;
  }

  @Override
  @Nonnull
  public String getDefaultAutocompleteField() {
    return "urn";
  }

  @Override
  @Nonnull
  public String getSearchQueryTemplate() {
    return "{}";
  }

  @Override
  @Nonnull
  public String getAutocompleteQueryTemplate() {
    return "";
  }

  @Override
  @Nonnull
  public List<String> getFieldsToHighlightMatch() {
    return ImmutableList.of("field1", "field2");
  }
}
