package com.linkedin.metadata.restli;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.Client;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import static com.linkedin.testing.TestUtils.*;


public class BaseBackfillableClientTest {

  private Client _mockRestClient;

  public static class TestBackfillableClient extends BaseBackfillableClient<Urn> {

    public TestBackfillableClient(@Nonnull Client restliClient) {
      super(restliClient);
    }

    @Override
    @Nonnull
    public BackfillResult backfill(@Nonnull Set<Urn> urnSet, @Nullable List<String> aspects) throws RemoteInvocationException {
      final BackfillResultEntity backfillResultEntity = new BackfillResultEntity()
          .setUrn(makeUrn(1))
          .setAspects(new StringArray(Collections.singleton("dummyAspect")));
      return new BackfillResult().setEntities(new BackfillResultEntityArray(Collections.singleton(backfillResultEntity)));
    }
  }

  @BeforeMethod
  public void setup() {
    _mockRestClient = mock(Client.class);
  }

  @Test
  public void testClient() throws RemoteInvocationException {
    TestBackfillableClient testBackfillableClient = new TestBackfillableClient(_mockRestClient);
    BackfillResult result = testBackfillableClient.backfill(Collections.emptySet(), new StringArray());

    assertEquals(result.getEntities().size(), 1);
    assertEquals(result.getEntities().get(0).getUrn(), makeUrn(1));
    assertEquals(result.getEntities().get(0).getAspects(), new StringArray(Collections.singleton("dummyAspect")));
  }
}
