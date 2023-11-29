package com.linkedin.metadata.restli;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.metadata.backfill.BackfillMode;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.events.IngestionMode;
import com.linkedin.parseq.BaseEngineTest;
import com.linkedin.restli.client.RestLiResponseException;
import com.linkedin.testing.EntityAspectUnion;
import com.linkedin.testing.urn.FooUrn;
import java.util.Collections;
import java.util.Set;
import javax.annotation.Nonnull;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.testing.TestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class BaseEntityAgnosticResourceTest extends BaseEngineTest {

  private BaseLocalDAO<EntityAspectUnion, FooUrn> _mockLocalDAO;

  private LocalDaoRegistry _registry;

  class TestResource extends BaseEntityAgnosticResource {

    @Nonnull
    @Override
    protected LocalDaoRegistry getLocalDaoRegistry() {
      return _registry;
    }
  }

  @BeforeMethod
  public void setup() {
    _mockLocalDAO = mock(BaseLocalDAO.class);
    when(_mockLocalDAO.getUrnClass()).thenReturn(FooUrn.class);
    _registry = new LocalDaoRegistry(ImmutableMap.of("foo", _mockLocalDAO));
  }

  @Test
  public void testBackfillMAESuccess() {
    TestResource testResource = new TestResource();
    Set<String> urnSet = ImmutableSet.of(makeFooUrn(1).toString());

    try {
      testResource.backfillMAE("foo", urnSet, null, IngestionMode.BACKFILL);
    } catch (RestLiResponseException e) {
      fail();
    }
    verify(_mockLocalDAO, times(1)).backfillMAE(
        BackfillMode.BACKFILL_INCLUDING_LIVE_INDEX, null, urnSet);
  }

  @Test
  public void testBackfillMAENoSuchEntity() {
    TestResource testResource = new TestResource();
    Set<String> urnSet = ImmutableSet.of(makeFooUrn(1).toString());

    assertThrows(RestLiResponseException.class, () -> {
      testResource.backfillMAE("foo1", urnSet, null, IngestionMode.BACKFILL);
    });
    verify(_mockLocalDAO, times(0)).backfillMAE(any(), any(), any());
  }

  @Test
  public void testBackfillMAENoopMode() throws RestLiResponseException {
    TestResource testResource = new TestResource();
    Set<String> urnSet = ImmutableSet.of(makeFooUrn(1).toString());

    assertEquals(
        testResource.backfillMAE("foo", urnSet, null, IngestionMode.LIVE),
        Collections.emptyMap()
    );
    verify(_mockLocalDAO, times(0)).backfillMAE(any(), any(), any());
  }
}
