package com.linkedin.metadata.restli;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.dao.AspectKey;
import com.linkedin.metadata.dao.BaseBrowseDAO;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.BaseSearchDAO;
import com.linkedin.metadata.dao.ingestion.preupdate.PreRoutingInfo;
import com.linkedin.metadata.dao.ingestion.preupdate.PreUpdateAspectRegistry;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.events.IngestionTrackingContext;
import com.linkedin.metadata.restli.ingestion.SamplePreUpdateRoutingClient;
import com.linkedin.parseq.BaseEngineTest;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.server.ResourceContext;
import com.linkedin.testing.AspectAttributes;
import com.linkedin.testing.AspectBar;
import com.linkedin.testing.AspectBaz;
import com.linkedin.testing.AspectFoo;
import com.linkedin.testing.EntityAspectUnion;
import com.linkedin.testing.EntityAspectUnionArray;
import com.linkedin.testing.EntityAsset;
import com.linkedin.testing.EntityDocument;
import com.linkedin.testing.EntityKey;
import com.linkedin.testing.EntitySnapshot;
import com.linkedin.testing.EntityValue;
import com.linkedin.testing.InternalEntityAspectUnion;
import com.linkedin.testing.InternalEntitySnapshot;
import com.linkedin.testing.urn.BazUrn;
import com.linkedin.testing.urn.FooUrn;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
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
  private BaseAspectRoutingGmsClient _mockAspectFooGmsClient;
  private BaseAspectRoutingGmsClient _mockAspectBarGmsClient;
  private BaseAspectRoutingGmsClient _mockAspectBazGmsClient;
  private BaseAspectRoutingGmsClient _mockAspectAttributeGmsClient;

  private BaseAspectRoutingResourceTest.TestResource _resource = new BaseAspectRoutingResourceTest.TestResource();

  private final AspectRoutingGmsClientManager _aspectRoutingGmsClientManager = new AspectRoutingGmsClientManager();

  class TestResource extends BaseAspectRoutingResource<
      // format
      ComplexResourceKey<EntityKey, EmptyRecord>, EntityValue, FooUrn, EntitySnapshot, EntityAspectUnion, EntityDocument,
      InternalEntitySnapshot, InternalEntityAspectUnion, EntityAsset> {

    public TestResource() {
      super(EntitySnapshot.class, EntityAspectUnion.class, FooUrn.class, EntityValue.class, InternalEntitySnapshot.class,
          InternalEntityAspectUnion.class, EntityAsset.class);
    }

    @Nonnull
    @Override
    protected BaseLocalDAO<InternalEntityAspectUnion, FooUrn> getLocalDAO() {
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
    protected FooUrn createUrnFromString(@Nonnull String urnString) {
      try {
        return FooUrn.createFromString(urnString);
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
    protected ComplexResourceKey<EntityKey, EmptyRecord> toKey(@Nonnull FooUrn urn) {
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
      value.setId(snapshot.getUrn().getIdAsLong());
      return value;
    }

    @Nonnull
    @Override
    protected EntitySnapshot toSnapshot(@Nonnull EntityValue value, @Nonnull FooUrn urn) {
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

    @Override
    public AspectRoutingGmsClientManager getAspectRoutingGmsClientManager() {
      return _aspectRoutingGmsClientManager;
    }

    @Override
    public ResourceContext getContext() {
      return mock(ResourceContext.class);
    }
  }

  @BeforeMethod
  public void setup() {
    _mockAspectFooGmsClient = mock(BaseAspectRoutingGmsClient.class);
    _mockAspectBarGmsClient = mock(BaseAspectRoutingGmsClient.class);
    _mockAspectAttributeGmsClient = mock(BaseAspectRoutingGmsClient.class);
    _mockAspectBazGmsClient = mock(BaseAspectRoutingGmsClient.class);
    when(_mockAspectFooGmsClient.getEntityType()).thenReturn(FooUrn.ENTITY_TYPE);
    when(_mockAspectAttributeGmsClient.getEntityType()).thenReturn(FooUrn.ENTITY_TYPE);
    when(_mockAspectBazGmsClient.getEntityType()).thenReturn(BazUrn.ENTITY_TYPE);
    _mockLocalDAO = mock(BaseLocalDAO.class);
    _aspectRoutingGmsClientManager.registerRoutingGmsClient(AspectFoo.class, "setFoo", _mockAspectFooGmsClient);
    _aspectRoutingGmsClientManager.registerRoutingGmsClient(AspectAttributes.class, "setAttributes", _mockAspectAttributeGmsClient);
    _aspectRoutingGmsClientManager.registerRoutingGmsClient(AspectBaz.class, "setBaz", _mockAspectBazGmsClient);
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
    when(_mockAspectFooGmsClient.get(urn)).thenReturn(foo);
    when(_mockAspectAttributeGmsClient.get(urn)).thenReturn(attributes);

    EntityValue value = runAndWait(_resource.get(makeResourceKey(urn),
        new String[]{AspectFoo.class.getCanonicalName(), AspectBar.class.getCanonicalName(), AspectAttributes.class.getCanonicalName()}));

    assertTrue(value.hasFoo());
    assertEquals(value.getFoo(), foo);

    assertTrue(value.hasBar());
    assertEquals(value.getBar(), bar);

    assertTrue(value.hasAttributes());
    assertEquals(value.getAttributes(), attributes);
    assertEquals(value.getId(), urn.getIdAsLong());
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
    verifyNoInteractions(_mockAspectFooGmsClient);

    assertTrue(value.hasBar());
    assertEquals(value.getBar(), bar);
    assertEquals(value.getId(), urn.getIdAsLong());
  }

  @Test
  public void testGetWithOnlyRoutingAspect() {
    FooUrn urn = makeFooUrn(1234);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectAttributes attributes = new AspectAttributes();

    when(_mockLocalDAO.exists(urn)).thenReturn(true);
    when(_mockAspectFooGmsClient.get(urn)).thenReturn(foo);
    when(_mockAspectAttributeGmsClient.get(urn)).thenReturn(attributes);

    EntityValue value = runAndWait(_resource.get(makeResourceKey(urn), new String[]{AspectFoo.class.getCanonicalName()}));

    assertTrue(value.hasFoo());
    assertEquals(value.getFoo(), foo);

    assertFalse(value.hasBar());
    verify(_mockLocalDAO, times(0)).get(anySet());

    value = runAndWait(_resource.get(makeResourceKey(urn), new String[]{AspectAttributes.class.getCanonicalName()}));

    assertTrue(value.hasAttributes());
    assertEquals(value.getAttributes(), attributes);
    assertEquals(value.getId(), urn.getIdAsLong());
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
    when(_mockAspectFooGmsClient.get(urn)).thenReturn(foo);

    EntityValue value = runAndWait(_resource.get(makeResourceKey(urn), new String[]{AspectFoo.class.getCanonicalName(), AspectBar.class.getCanonicalName()}));

    assertTrue(value.hasFoo());
    assertEquals(value.getFoo(), foo);
    assertEquals(value.getId(), urn.getIdAsLong());
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
    when(_mockAspectFooGmsClient.get(urn)).thenReturn(null);

    EntityValue value = runAndWait(_resource.get(makeResourceKey(urn), new String[]{AspectFoo.class.getCanonicalName(), AspectBar.class.getCanonicalName()}));

    assertTrue(value.hasBar());
    assertEquals(value.getBar(), bar);
    assertEquals(value.getId(), urn.getIdAsLong());
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

    verify(_mockLocalDAO, times(1)).add(eq(urn), eq(bar), any(), eq(null), eq(null));
    verify(_mockAspectFooGmsClient, times(1)).ingest(eq(urn), eq(foo));
    verify(_mockAspectAttributeGmsClient, times(1)).ingest(eq(urn), eq(attributes));
    verify(_mockLocalDAO, times(2)).getPreUpdateAspectRegistry();
    verify(_mockLocalDAO, times(1)).rawAdd(eq(urn), eq(foo), any(), any(), any());
  }

  @Test
  public void testIngestWithTrackingWithRoutingAspect() {
    FooUrn urn = makeFooUrn(1);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectBar bar = new AspectBar().setValue("bar");
    AspectAttributes attributes = new AspectAttributes();
    List<EntityAspectUnion> aspects = Arrays.asList(ModelUtils.newAspectUnion(EntityAspectUnion.class, foo),
        ModelUtils.newAspectUnion(EntityAspectUnion.class, bar), ModelUtils.newAspectUnion(EntityAspectUnion.class, attributes));
    EntitySnapshot snapshot = ModelUtils.newSnapshot(EntitySnapshot.class, urn, aspects);
    IngestionTrackingContext trackingContext = new IngestionTrackingContext();

    runAndWait(_resource.ingestWithTracking(snapshot, trackingContext, null));

    verify(_mockLocalDAO, times(1)).add(eq(urn), eq(bar), any(), eq(trackingContext), eq(null));
    verify(_mockAspectFooGmsClient, times(1)).ingestWithTracking(eq(urn), eq(foo), eq(trackingContext), eq(null));
    verify(_mockAspectAttributeGmsClient, times(1)).ingestWithTracking(eq(urn), eq(attributes), eq(trackingContext), eq(null));
    verify(_mockLocalDAO, times(2)).getPreUpdateAspectRegistry();
    verify(_mockLocalDAO, times(1)).rawAdd(eq(urn), eq(foo), any(), any(), any());
  }

  @Test
  public void testIngestWithoutRoutingAspect() {
    FooUrn urn = makeFooUrn(1);
    AspectBar bar = new AspectBar().setValue("bar");
    List<EntityAspectUnion> aspects = Arrays.asList(ModelUtils.newAspectUnion(EntityAspectUnion.class, bar));
    EntitySnapshot snapshot = ModelUtils.newSnapshot(EntitySnapshot.class, urn, aspects);

    runAndWait(_resource.ingest(snapshot));
    verify(_mockLocalDAO, times(1)).add(eq(urn), eq(bar), any(), eq(null), eq(null));
    verifyNoInteractions(_mockAspectFooGmsClient);
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

    verify(_mockLocalDAO, times(2)).getPreUpdateAspectRegistry();
    verify(_mockLocalDAO, times(1)).rawAdd(eq(urn), eq(foo), any(), any(), any());
    // verify(_mockGmsClient, times(1)).ingest(eq(urn), eq(foo));
    verify(_mockAspectFooGmsClient, times(1)).ingest(eq(urn), eq(foo));
    verify(_mockAspectAttributeGmsClient, times(1)).ingest(eq(urn), eq(attributes));
    verifyNoMoreInteractions(_mockAspectFooGmsClient);
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
    when(_mockAspectFooGmsClient.get(urn)).thenReturn(foo);
    when(_mockAspectAttributeGmsClient.get(urn)).thenReturn(attributes);

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
    when(_mockAspectFooGmsClient.get(urn)).thenReturn(foo);
    when(_mockAspectAttributeGmsClient.get(urn)).thenReturn(attributes);

    EntitySnapshot snapshot = runAndWait(_resource.getSnapshot(urn.toString(),
        new String[]{AspectFoo.class.getCanonicalName(), AspectAttributes.class.getCanonicalName()}));
    assertEquals(snapshot.getUrn(), urn);

    Set<RecordTemplate> aspects =
        snapshot.getAspects().stream().map(RecordUtils::getSelectedRecordTemplateFromUnion).collect(Collectors.toSet());

    assertEquals(snapshot.getUrn(), urn);
    assertEquals(aspects, ImmutableSet.of(foo, attributes));
    verifyNoInteractions(_mockLocalDAO);
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

    BackfillResult gmsResult1 = new BackfillResult();
    BackfillResultEntity resultEntity1 = new BackfillResultEntity().setUrn(fooUrn1)
        .setAspects(new StringArray(AspectFoo.class.getCanonicalName()));
    BackfillResultEntity resultEntity2 = new BackfillResultEntity().setUrn(fooUrn2)
        .setAspects(new StringArray(AspectFoo.class.getCanonicalName()));
    gmsResult1.setEntities(new BackfillResultEntityArray(resultEntity1, resultEntity2));


    BackfillResult gmsResult2 = new BackfillResult();
    BackfillResultEntity resultEntity3 = new BackfillResultEntity().setUrn(fooUrn1)
        .setAspects(new StringArray(AspectAttributes.class.getCanonicalName()));
    BackfillResultEntity resultEntity4 = new BackfillResultEntity().setUrn(fooUrn2)
        .setAspects(new StringArray(AspectAttributes.class.getCanonicalName()));
    gmsResult2.setEntities(new BackfillResultEntityArray(resultEntity3, resultEntity4));

    when(_mockLocalDAO.backfill(new HashSet<>(Arrays.asList(new Class[]{AspectBar.class})),
        ImmutableSet.of(fooUrn1, fooUrn2))).thenReturn(daoResult);
    when(_mockAspectFooGmsClient.backfill(ImmutableSet.of(fooUrn1, fooUrn2))).thenReturn(gmsResult1);
    when(_mockAspectAttributeGmsClient.backfill(ImmutableSet.of(fooUrn1, fooUrn2))).thenReturn(gmsResult2);

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

    verify(_mockAspectBazGmsClient, times(1)).getEntityType();
    verify(_mockAspectBazGmsClient, never()).backfill(any());
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
    verifyNoInteractions(_mockAspectFooGmsClient);

    verifyNoInteractions(_mockAspectBazGmsClient);
  }

  @Test
  public void testBackfillWithOnlyRoutingAspect() {
    FooUrn fooUrn1 = makeFooUrn(1);
    FooUrn fooUrn2 = makeFooUrn(2);
    AspectFoo foo1 = new AspectFoo().setValue("foo1");
    AspectFoo foo2 = new AspectFoo().setValue("foo2");
    AspectAttributes attrs1 = new AspectAttributes();
    AspectAttributes attrs2 = new AspectAttributes();

    BackfillResult gmsResult1 = new BackfillResult();
    BackfillResult gmsResult2 = new BackfillResult();
    BackfillResultEntity resultEntity1 =
        new BackfillResultEntity().setUrn(fooUrn1).setAspects(new StringArray(foo1.getClass().getCanonicalName()));
    BackfillResultEntity resultEntity2 =
        new BackfillResultEntity().setUrn(fooUrn2).setAspects(new StringArray(foo2.getClass().getCanonicalName()));
    gmsResult1.setEntities(new BackfillResultEntityArray(resultEntity1, resultEntity2));


    BackfillResultEntity resultEntity3 =
        new BackfillResultEntity().setUrn(fooUrn1).setAspects(new StringArray(attrs1.getClass().getCanonicalName()));
    BackfillResultEntity resultEntity4 =
        new BackfillResultEntity().setUrn(fooUrn2).setAspects(new StringArray(attrs2.getClass().getCanonicalName()));
    gmsResult2.setEntities(new BackfillResultEntityArray(resultEntity3, resultEntity4));


    when(_mockAspectFooGmsClient.backfill(ImmutableSet.of(fooUrn1, fooUrn2))).thenReturn(gmsResult1);
    when(_mockAspectAttributeGmsClient.backfill(ImmutableSet.of(fooUrn1, fooUrn2))).thenReturn(gmsResult2);

    BackfillResult backfillResult = runAndWait(_resource.backfill(new String[]{fooUrn1.toString(), fooUrn2.toString()},
        new String[]{AspectFoo.class.getCanonicalName(), AspectAttributes.class.getCanonicalName()}));

    assertEquals(backfillResult.getEntities().size(), 2);
    assertFalse(backfillResult.getEntities().get(0).getAspects().contains(AspectBar.class.getCanonicalName()));
    assertTrue(backfillResult.getEntities().get(0).getAspects().contains(AspectFoo.class.getCanonicalName()));
    assertTrue(backfillResult.getEntities().get(0).getAspects().contains(AspectAttributes.class.getCanonicalName()));
    assertFalse(backfillResult.getEntities().get(1).getAspects().contains(AspectBar.class.getCanonicalName()));
    assertTrue(backfillResult.getEntities().get(1).getAspects().contains(AspectFoo.class.getCanonicalName()));
    assertTrue(backfillResult.getEntities().get(1).getAspects().contains(AspectAttributes.class.getCanonicalName()));

    verify(_mockAspectBazGmsClient, times(1)).getEntityType();
    verify(_mockAspectBazGmsClient, never()).backfill(any());
  }

  @Test
  public void testBackfillWithNewValue() {
    FooUrn fooUrn1 = makeFooUrn(1);
    FooUrn fooUrn2 = makeFooUrn(2);
    AspectBar bar1 = new AspectBar().setValue("bar1");
    AspectBar bar2 = new AspectBar().setValue("bar2");

    Map<FooUrn, Map<Class<? extends RecordTemplate>, Optional<? extends RecordTemplate>>> daoResult =
        ImmutableMap.of(fooUrn1, Collections.singletonMap(AspectBar.class, Optional.of(bar1)),
            fooUrn2, Collections.singletonMap(AspectBar.class, Optional.of(bar2)));

    when(_mockLocalDAO.backfillWithNewValue(Collections.singleton(AspectBar.class), ImmutableSet.of(fooUrn1, fooUrn2))).thenReturn(daoResult);
    BackfillResult backfillResult = runAndWait(_resource.backfillWithNewValue(new String[]{fooUrn1.toString(), fooUrn2.toString()},
        new String[]{AspectBar.class.getCanonicalName(), AspectFoo.class.getCanonicalName()}));

    assertEquals(backfillResult.getEntities().size(), 2);
    verifyNoInteractions(_mockAspectFooGmsClient);
  }

  @Test
  public void testPreUpdateRoutingWithRegisteredAspect() {
    FooUrn urn = makeFooUrn(1);
    AspectFoo foo = new AspectFoo().setValue("foo");

    List<EntityAspectUnion> aspects = Arrays.asList(ModelUtils.newAspectUnion(EntityAspectUnion.class, foo));
    EntitySnapshot snapshot = ModelUtils.newSnapshot(EntitySnapshot.class, urn, aspects);

    PreUpdateAspectRegistry registry = new PreUpdateAspectRegistry();
    PreRoutingInfo preRoutingInfo = new PreRoutingInfo();
    preRoutingInfo.setPreUpdateClient(new SamplePreUpdateRoutingClient());
    registry.registerPreUpdateLambda(AspectFoo.class, preRoutingInfo);
    when(_mockLocalDAO.getPreUpdateAspectRegistry()).thenReturn(registry);

    // given: ingest a snapshot containing a routed aspect which has a registered pre-update lambda.
    runAndWait(_resource.ingest(snapshot));

    verify(_mockLocalDAO, times(1)).getPreUpdateAspectRegistry();
    // expected: the pre-update lambda is executed first (aspect value is changed from foo to foobar) and then the aspect is dual-written.
    AspectFoo foobar = new AspectFoo().setValue("foobar");
    // dual write pt1: ensure the ingestion request is forwarded to the routed GMS.
    verify(_mockAspectFooGmsClient, times(1)).ingest(eq(urn), eq(foobar));
    // dual write pt2: ensure local write using rawAdd() and not add().
    verify(_mockLocalDAO, times(0)).add(any(), any(), any(), any(), any());
    verify(_mockLocalDAO, times(1)).rawAdd(eq(urn), eq(foobar), any(), any(), any());
  }

  @Test
  public void testPreUpdateRoutingWithNonRegisteredPreUpdateAspect() {
    FooUrn urn = makeFooUrn(1);
    AspectFoo foo = new AspectFoo().setValue("foo");

    List<EntityAspectUnion> aspects = Arrays.asList(ModelUtils.newAspectUnion(EntityAspectUnion.class, foo));
    EntitySnapshot snapshot = ModelUtils.newSnapshot(EntitySnapshot.class, urn, aspects);

    // given: ingest a snapshot containing a routed aspect which does not have a registered pre-update lambda.
    runAndWait(_resource.ingest(snapshot));

    // expected: the aspect value remains unchanged and the aspect is dual-written.
    // dual write pt1: ensure the ingestion request is forwarded to the routed GMS.
    verify(_mockAspectFooGmsClient, times(1)).ingest(eq(urn), eq(foo));
    // dual write pt2: ensure local write using rawAdd() and not add().
    verify(_mockLocalDAO, times(0)).add(any(), any(), any(), any(), any());
    verify(_mockLocalDAO, times(1)).rawAdd(eq(urn), eq(foo), any(), any(), any());
  }

  @Test
  public void testPreUpdateRoutingWithNonRoutedAspectAndRegisteredPreUpdate() {
    FooUrn urn = makeFooUrn(1);
    AspectBar bar = new AspectBar().setValue("bar");
    List<EntityAspectUnion> aspects = Arrays.asList(ModelUtils.newAspectUnion(EntityAspectUnion.class, bar));
    EntitySnapshot snapshot = ModelUtils.newSnapshot(EntitySnapshot.class, urn, aspects);

    PreUpdateAspectRegistry registry = new PreUpdateAspectRegistry();
    PreRoutingInfo preRoutingInfo = new PreRoutingInfo();
    preRoutingInfo.setPreUpdateClient(new SamplePreUpdateRoutingClient());
    registry.registerPreUpdateLambda(AspectFoo.class, preRoutingInfo);

    when(_mockLocalDAO.getPreUpdateAspectRegistry()).thenReturn(registry);

    // given: ingest a snapshot which contains a non-routed aspect which has a registered pre-update lambda.
    runAndWait(_resource.ingest(snapshot));

    // expected: the aspect is ingested locally only (not dual written). BaseLocalDAO::add will execute any pre-ingestion
    // lambdas which will change the aspect value from bar -> foobar. But no pre-ingestion lambdas are run from within
    // the BaseAspectRoutingResource class.
    verify(_mockAspectBarGmsClient, times(0)).ingest(any(), any());
    verify(_mockLocalDAO, times(1)).add(eq(urn), eq(bar), any(), eq(null), eq(null));
    verifyNoMoreInteractions(_mockLocalDAO);
  }

  @Test
  public void testPreUpdateRoutingWithNonRoutedAspectAndNonRegisteredPreUpdate() {
    FooUrn urn = makeFooUrn(1);
    AspectBar bar = new AspectBar().setValue("bar");
    List<EntityAspectUnion> aspects = Arrays.asList(ModelUtils.newAspectUnion(EntityAspectUnion.class, bar));
    EntitySnapshot snapshot = ModelUtils.newSnapshot(EntitySnapshot.class, urn, aspects);

    // given: ingest a snapshot which contains a non-routed aspect which does not have any registered pre-update lambdas.
    runAndWait(_resource.ingest(snapshot));

    // expected: the aspect value remains unchanged and the aspect is ingested locally only (not dual written).
    verify(_mockAspectBarGmsClient, times(0)).ingest(any(), any());
    verify(_mockLocalDAO, times(1)).add(eq(urn), eq(bar), any(), eq(null), eq(null));
    verifyNoMoreInteractions(_mockLocalDAO);
  }

  @Test
  public void testPreUpdateRoutingWithSkipIngestion() throws NoSuchFieldException, IllegalAccessException {
    // Access the SKIP_INGESTION_FOR_ASPECTS field
    Field skipIngestionField = BaseAspectRoutingResource.class.getDeclaredField("SKIP_INGESTION_FOR_ASPECTS");
    skipIngestionField.setAccessible(true);
    // Remove the final modifier from the field
    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    modifiersField.setInt(skipIngestionField, skipIngestionField.getModifiers() & ~Modifier.FINAL);
    // Modify the field to contain AspectFoo
    List<String> newSkipIngestionList = Arrays.asList("com.linkedin.testing.AspectFoo");
    skipIngestionField.set(null, newSkipIngestionList);

    FooUrn urn = makeFooUrn(1);
    AspectFoo foo = new AspectFoo().setValue("foo");
    List<EntityAspectUnion> aspects = Arrays.asList(ModelUtils.newAspectUnion(EntityAspectUnion.class, foo));
    EntitySnapshot snapshot = ModelUtils.newSnapshot(EntitySnapshot.class, urn, aspects);
    PreUpdateAspectRegistry registry = new PreUpdateAspectRegistry();
    PreRoutingInfo preRoutingInfo = new PreRoutingInfo();
    preRoutingInfo.setPreUpdateClient(new SamplePreUpdateRoutingClient());
    registry.registerPreUpdateLambda(AspectFoo.class, preRoutingInfo);
    when(_mockLocalDAO.getPreUpdateAspectRegistry()).thenReturn(registry);

    runAndWait(_resource.ingest(snapshot));
    verify(_mockAspectFooGmsClient, times(0)).ingest(any(), any());
    verify(_mockLocalDAO, times(1)).getPreUpdateAspectRegistry();
    // Should not add to local DAO
    verifyNoMoreInteractions(_mockLocalDAO);
  }

  //Testing the case when aspect has no pre lambda but skipIngestion contains the aspect, so it should not skip ingestion
  @Test
  public void testPreUpdateRoutingWithSkipIngestionNoPreLambda() throws NoSuchFieldException, IllegalAccessException {
    Field skipIngestionField = BaseAspectRoutingResource.class.getDeclaredField("SKIP_INGESTION_FOR_ASPECTS");
    skipIngestionField.setAccessible(true);
    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    modifiersField.setInt(skipIngestionField, skipIngestionField.getModifiers() & ~Modifier.FINAL);
    List<String> newSkipIngestionList = Arrays.asList("com.linkedin.testing.AspectFoo");
    skipIngestionField.set(null, newSkipIngestionList);

    FooUrn urn = makeFooUrn(1);
    AspectFoo foo = new AspectFoo().setValue("foo");
    List<EntityAspectUnion> aspects = Arrays.asList(ModelUtils.newAspectUnion(EntityAspectUnion.class, foo));
    EntitySnapshot snapshot = ModelUtils.newSnapshot(EntitySnapshot.class, urn, aspects);

    runAndWait(_resource.ingest(snapshot));
    // Should not skip ingestion
    verify(_mockAspectFooGmsClient, times(1)).ingest(eq(urn), eq(foo));
    // Should check for pre lambda
    verify(_mockLocalDAO, times(1)).getPreUpdateAspectRegistry();
    // Should continue to dual-write into local DAO
    verify(_mockLocalDAO, times(1)).rawAdd(eq(urn), eq(foo), any(), any(), any());
    verifyNoMoreInteractions(_mockLocalDAO);
  }
}
