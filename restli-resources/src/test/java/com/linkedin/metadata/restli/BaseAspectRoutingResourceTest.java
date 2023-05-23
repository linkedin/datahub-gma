package com.linkedin.metadata.restli;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.dao.AspectKey;
import com.linkedin.metadata.dao.BaseBrowseDAO;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.BaseSearchDAO;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.parseq.BaseEngineTest;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.server.ResourceContext;
import com.linkedin.testing.AspectAttributes;
import com.linkedin.testing.AspectBar;
import com.linkedin.testing.AspectFoo;
import com.linkedin.testing.EntityAspectUnion;
import com.linkedin.testing.EntityAspectUnionArray;
import com.linkedin.testing.EntityDocument;
import com.linkedin.testing.EntityKey;
import com.linkedin.testing.EntitySnapshot;
import com.linkedin.testing.EntityValue;
import com.linkedin.testing.urn.FooUrn;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.metadata.dao.BaseReadDAO.*;
import static com.linkedin.testing.TestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class BaseAspectRoutingResourceTest extends BaseEngineTest {
  private BaseBrowseDAO _mockBrowseDAO;
  private BaseLocalDAO _mockLocalDAO;
  private BaseAspectRoutingGmsClient _mockGmsClient;
  private BaseGenericAspectRoutingGmsClient _mockGenericGmsClient;
  private BaseAspectRoutingResourceTest.TestResource _resource = new BaseAspectRoutingResourceTest.TestResource();


  class TestResource extends BaseAspectRoutingResource<
      // format
      ComplexResourceKey<EntityKey, EmptyRecord>, EntityValue, Urn, EntitySnapshot, EntityAspectUnion, EntityDocument, AspectFoo> {

    public TestResource() {
      // super(EntitySnapshot.class, EntityAspectUnion.class, AspectFoo.class, EntityValue.class);
      super(EntitySnapshot.class, EntityAspectUnion.class,
          ImmutableMap.of(AspectFoo.class, "setFoo", AspectAttributes.class, "setAttributes"), EntityValue.class);
    }

    @Nonnull
    @Override
    protected BaseLocalDAO<EntityAspectUnion, Urn> getLocalDAO() {
      return _mockLocalDAO;
    }

    @Nonnull
    @Override
    protected BaseSearchDAO getSearchDAO() {
      throw new UnsupportedOperationException("Not implemented");
    }

    @Nonnull
    @Override
    protected BaseBrowseDAO getBrowseDAO() {
      return _mockBrowseDAO;
    }

    @Nonnull
    @Override
    protected Urn createUrnFromString(@Nonnull String urnString) {
      try {
        return Urn.createFromString(urnString);
      } catch (URISyntaxException e) {
        throw RestliUtils.badRequestException("Invalid URN: " + urnString);
      }
    }

    @Nonnull
    @Override
    protected FooUrn toUrn(@Nonnull ComplexResourceKey<EntityKey, EmptyRecord> key) {
      return makeFooUrn(key.getKey().getId().intValue());
    }

    @Nonnull
    @Override
    protected ComplexResourceKey<EntityKey, EmptyRecord> toKey(@Nonnull Urn urn) {
      return new ComplexResourceKey<>(new EntityKey().setId(urn.getIdAsLong()), new EmptyRecord());
    }

    @Nonnull
    @Override
    protected EntityValue toValue(@Nonnull EntitySnapshot snapshot) {
      EntityValue value = new EntityValue();
      ModelUtils.getAspectsFromSnapshot(snapshot).forEach(a -> {
        if (a instanceof AspectFoo) {
          value.setFoo(AspectFoo.class.cast(a));
        } else if (a instanceof AspectBar) {
          value.setBar(AspectBar.class.cast(a));
        } else if (a instanceof AspectAttributes) {
          value.setAttributes(AspectAttributes.class.cast(a));
        }
      });
      return value;
    }

    @Nonnull
    @Override
    protected EntitySnapshot toSnapshot(@Nonnull EntityValue value, @Nonnull Urn urn) {
      EntitySnapshot snapshot = new EntitySnapshot().setUrn(urn);
      EntityAspectUnionArray aspects = new EntityAspectUnionArray();
      if (value.hasFoo()) {
        aspects.add(ModelUtils.newAspectUnion(EntityAspectUnion.class, value.getFoo()));
      }
      if (value.hasBar()) {
        aspects.add(ModelUtils.newAspectUnion(EntityAspectUnion.class, value.getBar()));
      }

      snapshot.setAspects(aspects);
      return snapshot;
    }

    @Nonnull
    @Override
    public String getRoutingAspectFieldName() {
      return "Foo";
    }

    @Nonnull
    @Override
    public BaseAspectRoutingGmsClient getGmsClient() {
      return _mockGmsClient;
    }

    @Override
    public BaseGenericAspectRoutingGmsClient getGenericGmsClient() {
      return _mockGenericGmsClient;
    }

    @Override
    public ResourceContext getContext() {
      return mock(ResourceContext.class);
    }
  }

  @BeforeMethod
  public void setup() {
    _mockGmsClient = mock(BaseAspectRoutingGmsClient.class);
    _mockGenericGmsClient = mock(BaseGenericAspectRoutingGmsClient.class);
    _mockLocalDAO = mock(BaseLocalDAO.class);
  }

  @Test
  public void testGetWithRoutingAspect() {
    FooUrn urn = makeFooUrn(1234);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectBar bar = new AspectBar().setValue("bar");
    AspectAttributes attributes = new AspectAttributes();

    AspectKey<FooUrn, AspectBar> aspectBarKey = new AspectKey<>(AspectBar.class, urn, LATEST_VERSION);

    when(_mockLocalDAO.exists(urn)).thenReturn(true);
    when(_mockLocalDAO.get(new HashSet<>(Arrays.asList(aspectBarKey)))).thenReturn(
        Collections.singletonMap(aspectBarKey, Optional.of(bar)));
    when(_mockGmsClient.get(urn)).thenReturn(foo);
    when(_mockGenericGmsClient.get(urn, AspectFoo.class)).thenReturn(foo);
    when(_mockGenericGmsClient.get(urn, AspectAttributes.class)).thenReturn(attributes);

    EntityValue value = runAndWait(_resource.get(makeResourceKey(urn),
        new String[]{AspectFoo.class.getCanonicalName(), AspectBar.class.getCanonicalName(), AspectAttributes.class.getCanonicalName()}));

    assertTrue(value.hasFoo());
    assertEquals(value.getFoo(), foo);

    assertTrue(value.hasBar());
    assertEquals(value.getBar(), bar);

    assertTrue(value.hasAttributes());
    assertEquals(value.getAttributes(), attributes);
  }

  @Test
  public void testGetWithoutRoutingAspect() {
    FooUrn urn = makeFooUrn(1234);
    AspectBar bar = new AspectBar().setValue("bar");

    AspectKey<FooUrn, AspectBar> aspectBarKey = new AspectKey<>(AspectBar.class, urn, LATEST_VERSION);

    when(_mockLocalDAO.exists(urn)).thenReturn(true);
    when(_mockLocalDAO.get(new HashSet<>(Arrays.asList(aspectBarKey)))).thenReturn(
        Collections.singletonMap(aspectBarKey, Optional.of(bar)));

    EntityValue value = runAndWait(_resource.get(makeResourceKey(urn), new String[]{AspectBar.class.getCanonicalName()}));

    assertFalse(value.hasFoo());
    verifyZeroInteractions(_mockGmsClient);

    assertTrue(value.hasBar());
    assertEquals(value.getBar(), bar);
  }

  @Test
  public void testGetWithOnlyRoutingAspect() {
    FooUrn urn = makeFooUrn(1234);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectAttributes attributes = new AspectAttributes();

    when(_mockLocalDAO.exists(urn)).thenReturn(true);
    when(_mockGmsClient.get(urn)).thenReturn(foo);
    when(_mockGenericGmsClient.get(urn, AspectFoo.class)).thenReturn(foo);
    when(_mockGenericGmsClient.get(urn, AspectAttributes.class)).thenReturn(attributes);

    EntityValue value = runAndWait(_resource.get(makeResourceKey(urn), new String[]{AspectFoo.class.getCanonicalName()}));

    assertTrue(value.hasFoo());
    assertEquals(value.getFoo(), foo);

    assertFalse(value.hasBar());
    verify(_mockLocalDAO, times(0)).get(anySet());

    value = runAndWait(_resource.get(makeResourceKey(urn), new String[]{AspectAttributes.class.getCanonicalName()}));

    assertTrue(value.hasAttributes());
    assertEquals(value.getAttributes(), attributes);

    assertFalse(value.hasBar());
    verify(_mockLocalDAO, times(0)).get(anySet());
  }

  @Test
  public void testGetWithEmptyValueFromLocalDao() {
    FooUrn urn = makeFooUrn(1234);
    AspectFoo foo = new AspectFoo().setValue("foo");

    AspectKey<FooUrn, AspectBar> aspectBarKey = new AspectKey<>(AspectBar.class, urn, LATEST_VERSION);

    when(_mockLocalDAO.exists(urn)).thenReturn(true);
    when(_mockLocalDAO.get(new HashSet<>(Arrays.asList(aspectBarKey)))).thenReturn(
        Collections.singletonMap(aspectBarKey, Optional.empty()));
    when(_mockGmsClient.get(urn)).thenReturn(foo);
    when(_mockGenericGmsClient.get(urn, AspectFoo.class)).thenReturn(foo);

    EntityValue value = runAndWait(_resource.get(makeResourceKey(urn), new String[]{AspectFoo.class.getCanonicalName(), AspectBar.class.getCanonicalName()}));

    assertTrue(value.hasFoo());
    assertEquals(value.getFoo(), foo);
    assertFalse(value.hasBar());
  }

  @Test
  public void testGetWithNullValueFromGms() {
    FooUrn urn = makeFooUrn(1234);
    AspectBar bar = new AspectBar().setValue("bar");

    AspectKey<FooUrn, AspectBar> aspectBarKey = new AspectKey<>(AspectBar.class, urn, LATEST_VERSION);

    when(_mockLocalDAO.exists(urn)).thenReturn(true);
    when(_mockLocalDAO.get(new HashSet<>(Arrays.asList(aspectBarKey)))).thenReturn(
        Collections.singletonMap(aspectBarKey, Optional.of(bar)));
    when(_mockGmsClient.get(urn)).thenReturn(null);

    EntityValue value = runAndWait(_resource.get(makeResourceKey(urn), new String[]{AspectFoo.class.getCanonicalName(), AspectBar.class.getCanonicalName()}));

    assertTrue(value.hasBar());
    assertEquals(value.getBar(), bar);
    assertFalse(value.hasFoo());
  }

  @Test
  public void testIngestWithRoutingAspect() {
    FooUrn urn = makeFooUrn(1);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectBar bar = new AspectBar().setValue("bar");
    AspectAttributes attributes = new AspectAttributes();
    List<EntityAspectUnion> aspects = Arrays.asList(ModelUtils.newAspectUnion(EntityAspectUnion.class, foo),
        ModelUtils.newAspectUnion(EntityAspectUnion.class, bar), ModelUtils.newAspectUnion(EntityAspectUnion.class, attributes));
    EntitySnapshot snapshot = ModelUtils.newSnapshot(EntitySnapshot.class, urn, aspects);

    runAndWait(_resource.ingest(snapshot));

    verify(_mockLocalDAO, times(1)).add(eq(urn), eq(bar), any());
    // verify(_mockGmsClient, times(1)).ingest(eq(urn), eq(foo));
    verify(_mockGenericGmsClient, times(1)).ingest(eq(urn), eq(foo));
    verify(_mockGenericGmsClient, times(1)).ingest(eq(urn), eq(attributes));
    verifyNoMoreInteractions(_mockLocalDAO);
  }

  @Test
  public void testIngestWithoutRoutingAspect() {
    FooUrn urn = makeFooUrn(1);
    AspectBar bar = new AspectBar().setValue("bar");
    List<EntityAspectUnion> aspects = Arrays.asList(ModelUtils.newAspectUnion(EntityAspectUnion.class, bar));
    EntitySnapshot snapshot = ModelUtils.newSnapshot(EntitySnapshot.class, urn, aspects);

    runAndWait(_resource.ingest(snapshot));

    verify(_mockLocalDAO, times(1)).add(eq(urn), eq(bar), any());
    verifyZeroInteractions(_mockGmsClient);
    verifyNoMoreInteractions(_mockLocalDAO);
  }

  @Test
  public void testIngestWithOnlyRoutingAspect() {
    FooUrn urn = makeFooUrn(1);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectAttributes attributes = new AspectAttributes();
    List<EntityAspectUnion> aspects = Arrays.asList(ModelUtils.newAspectUnion(EntityAspectUnion.class, foo),
        ModelUtils.newAspectUnion(EntityAspectUnion.class, attributes));
    EntitySnapshot snapshot = ModelUtils.newSnapshot(EntitySnapshot.class, urn, aspects);

    runAndWait(_resource.ingest(snapshot));

    verifyZeroInteractions(_mockLocalDAO);
    // verify(_mockGmsClient, times(1)).ingest(eq(urn), eq(foo));
    verify(_mockGenericGmsClient, times(1)).ingest(eq(urn), eq(foo));
    verify(_mockGenericGmsClient, times(1)).ingest(eq(urn), eq(attributes));
    verifyNoMoreInteractions(_mockGmsClient);
  }

  @Test
  public void testGetSnapshotWithoutRoutingAspect() {
    FooUrn urn = makeFooUrn(1);
    AspectFoo bar = new AspectFoo().setValue("bar");
    AspectKey<FooUrn, ? extends RecordTemplate> barKey = new AspectKey<>(AspectBar.class, urn, LATEST_VERSION);
    when(_mockLocalDAO.get(ImmutableSet.of(barKey))).thenReturn(ImmutableMap.of(barKey, Optional.of(bar)));

    EntitySnapshot snapshot = runAndWait(_resource.getSnapshot(urn.toString(), new String[]{AspectBar.class.getCanonicalName()}));

    assertEquals(snapshot.getUrn(), urn);
    assertEquals(snapshot.getAspects().size(), 1);
    Set<RecordTemplate> aspects =
        snapshot.getAspects().stream().map(RecordUtils::getSelectedRecordTemplateFromUnion).collect(Collectors.toSet());
    assertEquals(aspects, ImmutableSet.of(bar));
  }

  @Test
  public void testGetSnapshotWithRoutingAspect() {
    FooUrn urn = makeFooUrn(1);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectFoo bar = new AspectFoo().setValue("bar");
    AspectAttributes attributes = new AspectAttributes();
    AspectKey<FooUrn, ? extends RecordTemplate> barKey = new AspectKey<>(AspectBar.class, urn, LATEST_VERSION);
    Set<AspectKey<FooUrn, ? extends RecordTemplate>> aspectKeys = ImmutableSet.of(barKey);
    when(_mockLocalDAO.get(aspectKeys)).thenReturn(ImmutableMap.of(barKey, Optional.of(bar)));
    when(_mockGmsClient.get(urn)).thenReturn(foo);
    when(_mockGenericGmsClient.get(urn, AspectFoo.class)).thenReturn(foo);
    when(_mockGenericGmsClient.get(urn, AspectAttributes.class)).thenReturn(attributes);
    EntitySnapshot snapshot = runAndWait(_resource.getSnapshot(urn.toString(),
        new String[]{AspectFoo.class.getCanonicalName(), AspectBar.class.getCanonicalName(), AspectAttributes.class.getCanonicalName()}));

    assertEquals(snapshot.getUrn(), urn);
    Set<RecordTemplate> aspects =
        snapshot.getAspects().stream().map(RecordUtils::getSelectedRecordTemplateFromUnion).collect(Collectors.toSet());
    assertEquals(aspects, ImmutableSet.of(foo, bar, attributes));
  }

  @Test
  public void testGetSnapshotWithOnlyRoutingAspect() {
    FooUrn urn = makeFooUrn(1);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectAttributes attributes = new AspectAttributes();
    when(_mockGmsClient.get(urn)).thenReturn(foo);
    when(_mockGenericGmsClient.get(urn, AspectFoo.class)).thenReturn(foo);
    when(_mockGenericGmsClient.get(urn, AspectAttributes.class)).thenReturn(attributes);

    EntitySnapshot snapshot = runAndWait(_resource.getSnapshot(urn.toString(),
        new String[]{AspectFoo.class.getCanonicalName(), AspectAttributes.class.getCanonicalName()}));
    assertEquals(snapshot.getUrn(), urn);

    Set<RecordTemplate> aspects =
        snapshot.getAspects().stream().map(RecordUtils::getSelectedRecordTemplateFromUnion).collect(Collectors.toSet());

    assertEquals(snapshot.getUrn(), urn);
    assertEquals(aspects, ImmutableSet.of(foo, attributes));
    verifyZeroInteractions(_mockLocalDAO);
  }

  @Test
  public void testBackfillWithRoutingAspect() {
    FooUrn fooUrn1 = makeFooUrn(1);
    FooUrn fooUrn2 = makeFooUrn(2);
    AspectBar bar1 = new AspectBar().setValue("bar1");
    AspectBar bar2 = new AspectBar().setValue("bar2");
    AspectAttributes attrs1 = new AspectAttributes();
    AspectAttributes attrs2 = new AspectAttributes();

    Map<FooUrn, Map<Class<? extends RecordTemplate>, Optional<? extends RecordTemplate>>> daoResult =
        ImmutableMap.of(fooUrn1,
            ImmutableMap.of(AspectBar.class, Optional.of(bar1), AspectAttributes.class, Optional.of(attrs1)), fooUrn2,
            ImmutableMap.of(AspectBar.class, Optional.of(bar2), AspectAttributes.class, Optional.of(attrs2)));

    BackfillResult gmsResult = new BackfillResult();
    BackfillResultEntity resultEntity1 = new BackfillResultEntity().setUrn(fooUrn1)
        .setAspects(new StringArray(AspectFoo.class.getCanonicalName(), AspectAttributes.class.getCanonicalName()));
    BackfillResultEntity resultEntity2 = new BackfillResultEntity().setUrn(fooUrn2)
        .setAspects(new StringArray(AspectFoo.class.getCanonicalName(), AspectAttributes.class.getCanonicalName()));
    gmsResult.setEntities(new BackfillResultEntityArray(resultEntity1, resultEntity2));

    when(_mockLocalDAO.backfill(new HashSet<>(Arrays.asList(new Class[]{AspectBar.class})),
        ImmutableSet.of(fooUrn1, fooUrn2))).thenReturn(daoResult);
    when(_mockGmsClient.backfill(ImmutableSet.of(fooUrn1, fooUrn2))).thenReturn(gmsResult);

    BackfillResult backfillResult = runAndWait(_resource.backfill(new String[]{fooUrn1.toString(), fooUrn2.toString()},
        new String[]{AspectFoo.class.getCanonicalName(), AspectBar.class.getCanonicalName(),
            AspectAttributes.class.getCanonicalName()}));

    assertEquals(backfillResult.getEntities().size(), 2);
    assertTrue(backfillResult.getEntities().get(0).getAspects().contains(AspectBar.class.getCanonicalName()));
    assertTrue(backfillResult.getEntities().get(0).getAspects().contains(AspectFoo.class.getCanonicalName()));
    assertTrue(backfillResult.getEntities().get(0).getAspects().contains(AspectAttributes.class.getCanonicalName()));
    assertTrue(backfillResult.getEntities().get(1).getAspects().contains(AspectBar.class.getCanonicalName()));
    assertTrue(backfillResult.getEntities().get(1).getAspects().contains(AspectFoo.class.getCanonicalName()));
    assertTrue(backfillResult.getEntities().get(1).getAspects().contains(AspectAttributes.class.getCanonicalName()));
  }

  @Test
  public void testBackfillWithoutRoutingAspect() {
    FooUrn fooUrn1 = makeFooUrn(1);
    FooUrn fooUrn2 = makeFooUrn(2);
    AspectBar bar1 = new AspectBar().setValue("bar1");
    AspectBar bar2 = new AspectBar().setValue("bar2");

    Map<FooUrn, Map<Class<? extends RecordTemplate>, Optional<? extends RecordTemplate>>> daoResult =
        ImmutableMap.of(fooUrn1, Collections.singletonMap(AspectBar.class, Optional.of(bar1)),
            fooUrn2, Collections.singletonMap(AspectBar.class, Optional.of(bar2)));

    when(_mockLocalDAO.backfill(Collections.singleton(AspectBar.class), ImmutableSet.of(fooUrn1, fooUrn2))).thenReturn(daoResult);
    BackfillResult backfillResult = runAndWait(_resource.backfill(new String[]{fooUrn1.toString(), fooUrn2.toString()},
        new String[]{AspectBar.class.getCanonicalName()}));

    assertEquals(backfillResult.getEntities().size(), 2);
    verifyZeroInteractions(_mockGmsClient);
  }

  @Test
  public void testBackfillWithOnlyRoutingAspect() {
    FooUrn fooUrn1 = makeFooUrn(1);
    FooUrn fooUrn2 = makeFooUrn(2);
    AspectFoo foo1 = new AspectFoo().setValue("foo1");
    AspectFoo foo2 = new AspectFoo().setValue("foo2");
    AspectAttributes attrs1 = new AspectAttributes();
    AspectAttributes attrs2 = new AspectAttributes();

    BackfillResult gmsResult = new BackfillResult();
    BackfillResultEntity resultEntity1 =
        new BackfillResultEntity().setUrn(fooUrn1).setAspects(new StringArray(foo1.getClass().getCanonicalName(), attrs1.getClass().getCanonicalName()));
    BackfillResultEntity resultEntity2 =
        new BackfillResultEntity().setUrn(fooUrn2).setAspects(new StringArray(foo2.getClass().getCanonicalName(), attrs2.getClass().getCanonicalName()));
    gmsResult.setEntities(new BackfillResultEntityArray(resultEntity1, resultEntity2));
    when(_mockGmsClient.backfill(ImmutableSet.of(fooUrn1, fooUrn2))).thenReturn(gmsResult);

    BackfillResult backfillResult = runAndWait(_resource.backfill(new String[]{fooUrn1.toString(), fooUrn2.toString()},
        new String[]{AspectFoo.class.getCanonicalName(), AspectAttributes.class.getCanonicalName()}));

    assertEquals(backfillResult.getEntities().size(), 2);
    assertFalse(backfillResult.getEntities().get(0).getAspects().contains(AspectBar.class.getCanonicalName()));
    assertTrue(backfillResult.getEntities().get(0).getAspects().contains(AspectFoo.class.getCanonicalName()));
    assertTrue(backfillResult.getEntities().get(0).getAspects().contains(AspectAttributes.class.getCanonicalName()));
    assertFalse(backfillResult.getEntities().get(1).getAspects().contains(AspectBar.class.getCanonicalName()));
    assertTrue(backfillResult.getEntities().get(1).getAspects().contains(AspectFoo.class.getCanonicalName()));
    assertTrue(backfillResult.getEntities().get(1).getAspects().contains(AspectAttributes.class.getCanonicalName()));
  }
}
