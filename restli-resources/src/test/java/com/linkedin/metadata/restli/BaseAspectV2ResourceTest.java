package com.linkedin.metadata.restli;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.Urns;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.AspectKey;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.BaseSearchDAO;
import com.linkedin.metadata.dao.ListResult;
import com.linkedin.metadata.events.IngestionTrackingContext;
import com.linkedin.metadata.query.ExtraInfo;
import com.linkedin.metadata.query.ExtraInfoArray;
import com.linkedin.metadata.query.ListResultMetadata;
import com.linkedin.parseq.BaseEngineTest;
import com.linkedin.restli.server.CollectionResult;
import com.linkedin.restli.server.CreateKVResponse;
import com.linkedin.restli.server.PagingContext;
import com.linkedin.restli.server.ResourceContext;
import com.linkedin.testing.AspectFoo;
import com.linkedin.testing.EntityAspectUnion;
import com.linkedin.testing.EntityDocument;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.metadata.dao.BaseReadDAO.*;
import static com.linkedin.testing.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class BaseAspectV2ResourceTest extends BaseEngineTest {
  private BaseLocalDAO<EntityAspectUnion, Urn> _mockLocalDAO;
  private BaseSearchDAO<EntityDocument> _mockSearchDAO;

  private BaseAspectV2ResourceTest.TestResource _resource =
      new com.linkedin.metadata.restli.BaseAspectV2ResourceTest.TestResource();

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
  public void testGet() {
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectKey<AspectFoo> aspectKey = new AspectKey<>(AspectFoo.class, ENTITY_URN, LATEST_VERSION);
    when(_mockLocalDAO.get(aspectKey)).thenReturn(Optional.of(foo));
    AspectFoo result = runAndWait(_resource.get(ENTITY_URN));
    assertEquals(result, foo);
  }

  @Test
  public void testBackfill() {
    AspectFoo aspectFoo = new AspectFoo().setValue("value1");
    Set<Urn> urns = ImmutableSet.of(makeUrn(111), makeUrn(222));

    Map<Urn, Map<Class<? extends RecordTemplate>, Optional<? extends RecordTemplate>>> backfillResult = new HashMap<>();
    backfillResult.put(makeUrn(111), ImmutableMap.of(AspectFoo.class, Optional.of(aspectFoo)));
    backfillResult.put(makeUrn(222), ImmutableMap.of(AspectFoo.class, Optional.empty()));
    when(_mockLocalDAO.backfill(ImmutableSet.of(AspectFoo.class), urns)).thenReturn(backfillResult);
    BackfillResult result = runAndWait(_resource.backfillWithUrns(urns));

    assertEquals(result.getEntities().size(), 2);
  }

  @Test
  public void testGetAllWithMetadata() {
    List<AspectFoo> foos = ImmutableList.of(new AspectFoo().setValue("v1"), new AspectFoo().setValue("v2"));
    ExtraInfo extraInfo1 = makeExtraInfo(ENTITY_URN, 1L,
        new AuditStamp().setActor(Urns.createFromTypeSpecificString("testUser", "bar1")).setTime(0L));
    ExtraInfo extraInfo2 = makeExtraInfo(ENTITY_URN, 2L,
        new AuditStamp().setActor(Urns.createFromTypeSpecificString("testUser", "bar2")).setTime(0L));
    ListResultMetadata listResultMetadata =
        new ListResultMetadata().setExtraInfos(new ExtraInfoArray(ImmutableList.of(extraInfo1, extraInfo2)));
    ListResult listResult = ListResult.<AspectFoo>builder().values(foos).metadata(listResultMetadata).build();
    when(_mockLocalDAO.list(AspectFoo.class, ENTITY_URN, 1, 2)).thenReturn(listResult);

    CollectionResult<AspectFoo, ListResultMetadata> collectionResult =
        runAndWait(_resource.getAllWithMetadata(ENTITY_URN, new PagingContext(1, 2)));

    assertEquals(collectionResult.getElements(), foos);
    assertEquals(collectionResult.getMetadata(), listResultMetadata);
  }

  private ExtraInfo makeExtraInfo(Urn urn, Long version, AuditStamp audit) {
    return new ExtraInfo().setUrn(urn).setVersion(version).setAudit(audit);
  }

  @Test
  public void testCreate() {
    AspectFoo foo = new AspectFoo().setValue("foo");

    runAndWait(_resource.create(ENTITY_URN, foo));

    verify(_mockLocalDAO, times(1)).add(eq(ENTITY_URN), eq(foo), any(AuditStamp.class));
    verifyNoMoreInteractions(_mockLocalDAO);
  }

  @Test
  public void testCreateWithTracking() {
    AspectFoo foo = new AspectFoo().setValue("foo");
    IngestionTrackingContext trackingContext = new IngestionTrackingContext();

    runAndWait(_resource.createWithTracking(ENTITY_URN, foo, trackingContext, null));

    verify(_mockLocalDAO, times(1)).add(eq(ENTITY_URN), eq(foo), any(AuditStamp.class), eq(trackingContext), eq(null));
    verifyNoMoreInteractions(_mockLocalDAO);
  }

  @Test
  public void testSoftDelete() {

    runAndWait(_resource.delete(ENTITY_URN));

    // this should test that delete method of DAO is being called once
    verify(_mockLocalDAO, times(1)).delete(eq(ENTITY_URN), eq(AspectFoo.class), any(AuditStamp.class));
    verifyNoMoreInteractions(_mockLocalDAO);
  }

  @Test
  public void testCreateResponseViaLambda() {
    AspectFoo foo = new AspectFoo().setValue("foo");
    Function<Optional<AspectFoo>, AspectFoo> createLambda = (prev) -> foo;
    when(_mockLocalDAO.add(eq(ENTITY_URN), eq(AspectFoo.class), eq(createLambda), any())).thenReturn(foo);

    CreateKVResponse<Urn, AspectFoo>
        response = runAndWait(_resource.createAndGet(ENTITY_URN, createLambda));

    assertEquals(response.getStatus().getCode(), 201);
    assertEquals(response.getEntity(), foo);
    assertEquals(response.getId(), ENTITY_URN);
  }
}
