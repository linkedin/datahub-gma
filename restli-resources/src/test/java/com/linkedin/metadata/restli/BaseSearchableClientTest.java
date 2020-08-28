package com.linkedin.metadata.restli;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.query.SortCriterion;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.Client;
import com.linkedin.restli.common.CollectionMetadata;
import com.linkedin.restli.common.CollectionResponse;
import com.linkedin.testing.EntityValue;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

public class BaseSearchableClientTest {

  private Client _mockRestClient;

  public static class TestSearchableClient extends BaseSearchableClient<EntityValue, Urn> {

    public TestSearchableClient(@Nonnull Client restliClient) {
      super(restliClient);
    }

    @Override
    @Nonnull
    public CollectionResponse<EntityValue> search(@Nonnull String input, @Nonnull StringArray aspectNames, @Nullable Map<String, String> requestFilters,
        @Nullable SortCriterion sortCriterion, int start, int count) throws RemoteInvocationException {
      CollectionResponse<EntityValue> collectionResponse = new CollectionResponse<>(EntityValue.class);
      collectionResponse.setPaging(new CollectionMetadata().setTotal(100));
      return collectionResponse;
    }

    @Override
    @Nonnull
    public BackfillResult backfill(@Nonnull Set<Urn> urnSet, @Nullable List<String> aspects) throws RemoteInvocationException {
      return new BackfillResult();
    }
  }

  @BeforeMethod
  public void setup() {
    _mockRestClient = mock(Client.class);
  }

  @Test
  public void testClient() throws RemoteInvocationException {
    TestSearchableClient testSearchableClient = new TestSearchableClient(_mockRestClient);
    assertEquals(testSearchableClient.search("test", new StringArray(), new HashMap<>(), null, 0,
        10).getPaging().getTotal().intValue(), 100);
  }
}