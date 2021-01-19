package com.linkedin.metadata.testing;

import com.linkedin.metadata.testing.annotations.SearchIndexMappings;
import com.linkedin.metadata.testing.annotations.SearchIndexSettings;
import com.linkedin.metadata.testing.annotations.SearchIndexType;
import com.linkedin.testing.BarSearchDocument;
import com.linkedin.testing.urn.BarUrn;
import javax.annotation.Nonnull;


public class ExampleSanityTest extends BaseSearchSanityTests<BarSearchDocument> {
  @SearchIndexType(BarSearchDocument.class)
  @SearchIndexSettings("/settings.json")
  @SearchIndexMappings("/mappings.json")
  public SearchIndex<BarSearchDocument> _searchIndex;

  private static final BarUrn URN = new BarUrn(42);
  private static final BarSearchDocument DOCUMENT = new BarSearchDocument().setUrn(URN).setValue("value");

  protected ExampleSanityTest() {
    // Use your real search config here.
    super(URN, DOCUMENT, new ReadAllDocumentsConfig<>(BarSearchDocument.class));
  }

  @Nonnull
  @Override
  public SearchIndex<BarSearchDocument> getIndex() {
    return _searchIndex;
  }
}
