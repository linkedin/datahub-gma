package com.linkedin.metadata.testing;

import com.linkedin.metadata.dao.search.BaseSearchConfig;
import com.linkedin.metadata.testing.annotations.SearchIndexMappings;
import com.linkedin.metadata.testing.annotations.SearchIndexSettings;
import com.linkedin.metadata.testing.annotations.SearchIndexType;
import com.linkedin.testing.BarSearchDocument;
import com.linkedin.testing.urn.BarUrn;
import java.util.Collections;
import java.util.Set;
import javax.annotation.Nonnull;


public class ExampleSanityTest extends BaseSearchSanityTests<BarSearchDocument> {
  @SearchIndexType(BarSearchDocument.class)
  @SearchIndexSettings("/settings.json")
  @SearchIndexMappings("/mappings.json")
  public SearchIndex<BarSearchDocument> _searchIndex;

  private static final BarUrn URN = new BarUrn(42);
  private static final BarSearchDocument DOCUMENT = new BarSearchDocument().setUrn(URN).setValue("value");

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
      return "{ \"query_string\": { \"query\": \"$INPUT\" }}";
    }

    @Nonnull
    @Override
    public String getAutocompleteQueryTemplate() {
      return "{ \"query_string\": { \"query\": \"~$INPUT\" }}";
    }
  }

  protected ExampleSanityTest() {
    super(URN, DOCUMENT, new SearchConfig());
  }

  @Nonnull
  @Override
  public SearchIndex<BarSearchDocument> getIndex() {
    return _searchIndex;
  }
}
