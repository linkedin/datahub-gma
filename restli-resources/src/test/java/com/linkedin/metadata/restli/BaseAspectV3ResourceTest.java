package com.linkedin.metadata.restli;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.AspectKey;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.events.IngestionTrackingContext;
import com.linkedin.metadata.restli.dao.DefaultLocalDaoRegistryImpl;
import com.linkedin.metadata.restli.dao.LocalDaoRegistry;
import com.linkedin.parseq.BaseEngineTest;
import com.linkedin.testing.AspectFoo;
import com.linkedin.testing.EntityAspectUnion;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.metadata.dao.BaseReadDAO.*;
import static com.linkedin.testing.TestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class BaseAspectV3ResourceTest extends BaseEngineTest {
  private BaseLocalDAO<EntityAspectUnion, Urn> _mockLocalDAO;

  private final BaseAspectV3ResourceTest.TestResource _resource =
      new com.linkedin.metadata.restli.BaseAspectV3ResourceTest.TestResource();

  private static final Urn ENTITY_URN = makeUrn(1234);

  class TestResource extends BaseAspectV3Resource<AspectFoo> {

    public TestResource() {
      super(AspectFoo.class);
    }

    @Nonnull
    @Override
    protected LocalDaoRegistry getLocalDaoRegistry() {
      return DefaultLocalDaoRegistryImpl.init(Collections.singletonMap("testing", _mockLocalDAO));
    }
  }

  @BeforeMethod
  public void setup() {
    _mockLocalDAO = mock(BaseLocalDAO.class);
  }

  @Test
  public void testGet() {
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectKey<AspectFoo> aspectKey = new AspectKey<>(AspectFoo.class, ENTITY_URN, LATEST_VERSION);
    when(_mockLocalDAO.get(aspectKey)).thenReturn(Optional.of(foo));
    AspectFoo result = runAndWait(_resource.get(ENTITY_URN.toString()));
    assertEquals(result, foo);
  }

  @Test
  public void testCreate() {
    AspectFoo foo = new AspectFoo().setValue("foo");
    runAndWait(_resource.create(ENTITY_URN.toString(), foo));
    verify(_mockLocalDAO, times(1)).add(eq(ENTITY_URN), eq(foo), any(AuditStamp.class));
    verifyNoMoreInteractions(_mockLocalDAO);
  }

  @Test
  public void testCreateWithTracking() {
    AspectFoo foo = new AspectFoo().setValue("foo");
    IngestionTrackingContext trackingContext = new IngestionTrackingContext();
    runAndWait(_resource.createWithTracking(ENTITY_URN.toString(), foo, trackingContext, null));
    verify(_mockLocalDAO, times(1)).add(eq(ENTITY_URN), eq(foo), any(AuditStamp.class), eq(trackingContext), eq(null));
    verifyNoMoreInteractions(_mockLocalDAO);
  }

  @Test
  public void testBackfill() {
    AspectFoo aspectFoo = new AspectFoo().setValue("value1");
    Set<String> urnStrs = ImmutableSet.of(makeUrn(111).toString(), makeUrn(222).toString());
    Set<Urn> urns = ImmutableSet.of(makeUrn(111), makeUrn(222));
    Map<Urn, Map<Class<? extends RecordTemplate>, Optional<? extends RecordTemplate>>> backfillResult1 = new HashMap<>();
    backfillResult1.put(makeUrn(111), ImmutableMap.of(AspectFoo.class, Optional.of(aspectFoo)));

    Map<Urn, Map<Class<? extends RecordTemplate>, Optional<? extends RecordTemplate>>> backfillResult2 = new HashMap<>();
    backfillResult2.put(makeUrn(222), ImmutableMap.of(AspectFoo.class, Optional.empty()));

    when(_mockLocalDAO.backfill(ImmutableSet.of(AspectFoo.class), Collections.singleton(makeUrn(111)))).thenReturn(backfillResult1);
    when(_mockLocalDAO.backfill(ImmutableSet.of(AspectFoo.class), Collections.singleton(makeUrn(222)))).thenReturn(backfillResult2);

    BackfillResult result = runAndWait(_resource.backfillWithUrns(urnStrs));

    assertEquals(result.getEntities().size(), 2);
  }
}
