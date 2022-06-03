package com.linkedin.metadata.restli;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.LongMap;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.BaseSearchDAO;
import com.linkedin.metadata.dao.SearchResult;
import com.linkedin.metadata.query.AggregationMetadata;
import com.linkedin.metadata.query.AggregationMetadataArray;
import com.linkedin.metadata.query.CriterionArray;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.SearchResultMetadata;
import com.linkedin.parseq.BaseEngineTest;
import com.linkedin.restli.server.CollectionResult;
import com.linkedin.restli.server.PagingContext;
import com.linkedin.restli.server.PathKeys;
import com.linkedin.restli.server.ResourceContext;
import com.linkedin.testing.AspectFoo;
import com.linkedin.testing.EntityAspectUnion;
import com.linkedin.testing.EntityDocument;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.testing.TestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class BaseSearchableAspectResourceTest extends BaseEngineTest {

    private BaseLocalDAO<EntityAspectUnion, Urn> _mockLocalDAO;
    private BaseSearchDAO<EntityDocument> _mockSearchDAO;

    private com.linkedin.metadata.restli.BaseSearchableAspectResourceTest.TestResource
        _resource = new com.linkedin.metadata.restli.BaseSearchableAspectResourceTest.TestResource();

    private static final Urn ENTITY_URN = makeUrn(1234);

    class TestResource extends BaseSearchableAspectResource<Urn, EntityAspectUnion, AspectFoo, EntityDocument> {

      public TestResource() {
        super(EntityAspectUnion.class, AspectFoo.class);
      }

      @Override
      protected BaseLocalDAO<EntityAspectUnion, Urn> getLocalDAO() {
        return _mockLocalDAO;
      }

      @Override
      protected Urn getUrn(@Nonnull PathKeys entityPathKeys) {
        return ENTITY_URN;
      }

      @Override
      public ResourceContext getContext() {
        return mock(ResourceContext.class);
      }

      @Nonnull
      @Override
      protected BaseSearchDAO<EntityDocument> getSearchDAO() {
        return _mockSearchDAO;
      }
    }

    @BeforeMethod
    public void setup() {
      _mockLocalDAO = mock(BaseLocalDAO.class);
      _mockSearchDAO = mock(BaseSearchDAO.class);
    }

    @Test
    public void testSearch() {
      Urn urn1 = makeUrn(1);
      Urn urn2 = makeUrn(2);
      AspectFoo foo1 = new AspectFoo().setValue("foo1");
      AspectFoo foo2 = new AspectFoo().setValue("foo2");

      Filter filter = new Filter().setCriteria(new CriterionArray());
      SearchResultMetadata searchResultMetadata = makeSearchResultMetadata(new AggregationMetadata().setName("agg")
          .setAggregations(new LongMap(ImmutableMap.of("bucket1", 1L, "bucket2", 2L))));

      when(_mockSearchDAO.search("bar", filter, null, 1, 2)).thenReturn(
          makeSearchResult(ImmutableList.of(makeDocument(urn1), makeDocument(urn2)), 10, searchResultMetadata));

      when(_mockLocalDAO.get(AspectFoo.class, ImmutableSet.of(urn1, urn2))).thenReturn(
          ImmutableMap.of(urn1, Optional.of(foo1), urn2, Optional.of(foo2)));

      CollectionResult<AspectFoo, SearchResultMetadata> searchResult =
          runAndWait(_resource.search("bar", filter, null, new PagingContext(1, 2)));

      // Verify
      assertEquals(searchResult.getElements().size(), 2);
      assertEquals(searchResult.getTotal().intValue(), 10);
      assertEquals(searchResult.getMetadata(), searchResultMetadata);
      assertEquals(searchResult.getMetadata().getUrns().size(), 2);
    }

  private SearchResult<EntityDocument> makeSearchResult(List<EntityDocument> documents, int totalCount,
      SearchResultMetadata searchResultMetadata) {
    return SearchResult.<EntityDocument>builder().documentList(documents)
        .searchResultMetadata(searchResultMetadata)
        .totalCount(totalCount)
        .build();
  }

  private SearchResultMetadata makeSearchResultMetadata(AggregationMetadata... aggregationMetadata) {
    return new SearchResultMetadata().setSearchResultMetadatas(
        new AggregationMetadataArray(Arrays.asList(aggregationMetadata)));
  }
}
