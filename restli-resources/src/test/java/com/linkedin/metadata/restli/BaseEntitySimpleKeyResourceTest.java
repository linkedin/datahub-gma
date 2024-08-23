package com.linkedin.metadata.restli;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.dao.AspectKey;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.restli.lix.ResourceLix;
import com.linkedin.parseq.BaseEngineTest;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.ResourceContext;
import com.linkedin.restli.server.RestLiServiceException;
import com.linkedin.testing.AspectAttributes;
import com.linkedin.testing.AspectBar;
import com.linkedin.testing.AspectFoo;
import com.linkedin.testing.AspectFooEvolved;
import com.linkedin.testing.BarUrnArray;
import com.linkedin.testing.EntityAspectUnion;
import com.linkedin.testing.EntityAspectUnionArray;
import com.linkedin.testing.EntityAsset;
import com.linkedin.testing.EntitySnapshot;
import com.linkedin.testing.EntityValue;
import com.linkedin.testing.InternalEntityAspectUnion;
import com.linkedin.testing.InternalEntitySnapshot;
import com.linkedin.testing.localrelationship.AspectFooBar;
import com.linkedin.testing.urn.BarUrn;
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
import javax.annotation.Nullable;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.metadata.dao.BaseReadDAO.*;
import static com.linkedin.testing.TestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class BaseEntitySimpleKeyResourceTest extends BaseEngineTest {

  private BaseLocalDAO<InternalEntityAspectUnion, Urn> _mockLocalDAO;
  private TestResource _resource = new TestResource();
  private TestInternalResource _internalResource = new TestInternalResource();

  @SuppressWarnings("unchecked")
  @BeforeMethod
  public void setup() {
    _mockLocalDAO = mock(BaseLocalDAO.class);
  }

  @Test
  public void testGet() {
    long id = 1234;
    Urn urn = makeUrn(id);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectKey<Urn, AspectFoo> aspect1Key = new AspectKey<>(AspectFoo.class, urn, LATEST_VERSION);
    AspectKey<Urn, AspectBar> aspect2Key = new AspectKey<>(AspectBar.class, urn, LATEST_VERSION);
    AspectKey<Urn, AspectFooBar> aspect3Key = new AspectKey<>(AspectFooBar.class, urn, LATEST_VERSION);
    AspectKey<Urn, AspectAttributes> aspect4Key = new AspectKey<>(AspectAttributes.class, urn, LATEST_VERSION);

    when(_mockLocalDAO.exists(urn)).thenReturn(true);
    when(_mockLocalDAO.get(new HashSet<>(Arrays.asList(aspect1Key, aspect2Key, aspect3Key, aspect4Key))))
        .thenReturn(Collections.singletonMap(aspect1Key, Optional.of(foo)));

    EntityValue value = runAndWait(_resource.get(id, null));

    assertEquals(value.getFoo(), foo);
    assertFalse(value.hasBar());
  }

  @Test
  public void testInternalModelGet() {
    long id = 1234;
    Urn urn = makeUrn(id);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectKey<Urn, AspectFoo> aspect1Key = new AspectKey<>(AspectFoo.class, urn, LATEST_VERSION);
    AspectKey<Urn, AspectBar> aspect2Key = new AspectKey<>(AspectBar.class, urn, LATEST_VERSION);
    AspectKey<Urn, AspectFooBar> aspect3Key = new AspectKey<>(AspectFooBar.class, urn, LATEST_VERSION);
    AspectKey<Urn, AspectAttributes> aspect4Key = new AspectKey<>(AspectAttributes.class, urn, LATEST_VERSION);
    AspectKey<Urn, AspectFooEvolved> aspect5Key = new AspectKey<>(AspectFooEvolved.class, urn, LATEST_VERSION);

    when(_mockLocalDAO.exists(urn)).thenReturn(true);
    when(_mockLocalDAO.get(
        new HashSet<>(Arrays.asList(aspect1Key, aspect2Key, aspect3Key, aspect4Key, aspect5Key)))).thenReturn(
        Collections.singletonMap(aspect1Key, Optional.of(foo)));

    EntityValue value = runAndWait(_resource.get(id, null, true));

    assertEquals(value.getFoo(), foo);
    assertFalse(value.hasBar());
  }

  @Test
  public void testGetUrnNotFound() {
    long id = 1234;
    Urn urn = makeUrn(id);

    when(_mockLocalDAO.exists(urn)).thenReturn(false);

    try {
      runAndWait(_resource.get(id, new String[0]));
      fail("An exception should've been thrown!");
    } catch (RestLiServiceException e) {
      assertEquals(e.getStatus(), HttpStatus.S_404_NOT_FOUND);
    }
  }

  @Test
  public void testGetWithEmptyAspects() {
    long id = 1234;
    Urn urn = makeUrn(id);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectKey<Urn, AspectFoo> aspect1Key = new AspectKey<>(AspectFoo.class, urn, LATEST_VERSION);
    AspectKey<Urn, AspectBar> aspect2Key = new AspectKey<>(AspectBar.class, urn, LATEST_VERSION);

    when(_mockLocalDAO.exists(urn)).thenReturn(true);
    when(_mockLocalDAO.get(new HashSet<>(Arrays.asList(aspect1Key, aspect2Key))))
        .thenReturn(Collections.emptyMap());

    try {
      EntityValue value = runAndWait(_resource.get(id, new String[0]));
      assertFalse(value.hasFoo());
      assertFalse(value.hasBar());
    } catch (RestLiServiceException e) {
      fail("No exception should be thrown!");
    }
  }

  @Test
  public void testGetSpecificAspect() {
    long id = 1234;
    Urn urn = makeUrn(id);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectKey<Urn, AspectFoo> aspect1Key = new AspectKey<>(AspectFoo.class, urn, LATEST_VERSION);
    String[] aspectNames = {AspectFoo.class.getCanonicalName()};

    when(_mockLocalDAO.exists(urn)).thenReturn(true);
    when(_mockLocalDAO.get(new HashSet<>(Collections.singletonList(aspect1Key))))
        .thenReturn(Collections.singletonMap(aspect1Key, Optional.of(foo)));

    EntityValue value = runAndWait(_resource.get(id, aspectNames));
    assertEquals(value.getFoo(), foo);
    verify(_mockLocalDAO, times(1)).get(Collections.singleton(aspect1Key));
  }

  @Test
  public void testGetSpecificAspectNotFound() {
    long id = 1234;
    Urn urn = makeUrn(id);
    String[] aspectNames = {AspectFoo.class.getCanonicalName()};

    when(_mockLocalDAO.exists(urn)).thenReturn(true);

    try {
      EntityValue value = runAndWait(_resource.get(id, aspectNames));
      assertFalse(value.hasFoo());
      assertFalse(value.hasBar());
    } catch (RestLiServiceException e) {
      fail("No exception should be thrown!");
    }
  }

  @Test
  public void testBatchGet() {
    long id1 = 1;
    Urn urn1 = makeUrn(id1);
    long id2 = 2;
    Urn urn2 = makeUrn(id2);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectBar bar = new AspectBar().setValue("bar");

    AspectKey<Urn, AspectFoo> aspectFooKey1 = new AspectKey<>(AspectFoo.class, urn1, LATEST_VERSION);
    AspectKey<Urn, AspectBar> aspectBarKey1 = new AspectKey<>(AspectBar.class, urn1, LATEST_VERSION);
    AspectKey<Urn, AspectFoo> aspectFooKey2 = new AspectKey<>(AspectFoo.class, urn2, LATEST_VERSION);
    AspectKey<Urn, AspectBar> aspectBarKey2 = new AspectKey<>(AspectBar.class, urn2, LATEST_VERSION);
    AspectKey<Urn, AspectFooBar> aspectFooBarKey1 = new AspectKey<>(AspectFooBar.class, urn1, LATEST_VERSION);
    AspectKey<Urn, AspectFooBar> aspectFooBarKey2 = new AspectKey<>(AspectFooBar.class, urn2, LATEST_VERSION);
    AspectKey<Urn, AspectAttributes> aspectAttKey1 = new AspectKey<>(AspectAttributes.class, urn1, LATEST_VERSION);
    AspectKey<Urn, AspectAttributes> aspectAttKey2 = new AspectKey<>(AspectAttributes.class, urn2, LATEST_VERSION);

    when(_mockLocalDAO.get(ImmutableSet.of(aspectFooKey1, aspectBarKey1, aspectAttKey1, aspectFooKey2, aspectBarKey2,
        aspectAttKey2, aspectFooBarKey1, aspectFooBarKey2))).thenReturn(
        ImmutableMap.of(aspectFooKey1, Optional.of(foo), aspectFooKey2, Optional.of(bar)));

    Map<Long, EntityValue> keyValueMap = runAndWait(_resource.batchGet(ImmutableSet.of(id1, id2), null))
        .entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    assertEquals(keyValueMap.size(), 2);
    assertEquals(keyValueMap.get(id1).getFoo(), foo);
    assertFalse(keyValueMap.get(id1).hasBar());
    assertEquals(keyValueMap.get(id2).getBar(), bar);
    assertFalse(keyValueMap.get(id2).hasFoo());
  }

  @Test
  public void testInternalModelBatchGet() {
    long id1 = 1;
    Urn urn1 = makeUrn(id1);
    long id2 = 2;
    Urn urn2 = makeUrn(id2);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectBar bar = new AspectBar().setValue("bar");

    AspectKey<Urn, AspectFoo> aspectFooKey1 = new AspectKey<>(AspectFoo.class, urn1, LATEST_VERSION);
    AspectKey<Urn, AspectFooEvolved> aspectFooEvolvedKey1 = new AspectKey<>(AspectFooEvolved.class, urn1, LATEST_VERSION);
    AspectKey<Urn, AspectBar> aspectBarKey1 = new AspectKey<>(AspectBar.class, urn1, LATEST_VERSION);
    AspectKey<Urn, AspectFoo> aspectFooKey2 = new AspectKey<>(AspectFoo.class, urn2, LATEST_VERSION);
    AspectKey<Urn, AspectFooEvolved> aspectFooEvolvedKey2 = new AspectKey<>(AspectFooEvolved.class, urn2, LATEST_VERSION);
    AspectKey<Urn, AspectBar> aspectBarKey2 = new AspectKey<>(AspectBar.class, urn2, LATEST_VERSION);
    AspectKey<Urn, AspectFooBar> aspectFooBarKey1 = new AspectKey<>(AspectFooBar.class, urn1, LATEST_VERSION);
    AspectKey<Urn, AspectFooBar> aspectFooBarKey2 = new AspectKey<>(AspectFooBar.class, urn2, LATEST_VERSION);
    AspectKey<Urn, AspectAttributes> aspectAttKey1 = new AspectKey<>(AspectAttributes.class, urn1, LATEST_VERSION);
    AspectKey<Urn, AspectAttributes> aspectAttKey2 = new AspectKey<>(AspectAttributes.class, urn2, LATEST_VERSION);

    when(_mockLocalDAO.get(
        ImmutableSet.of(aspectFooKey1, aspectFooEvolvedKey1, aspectBarKey1, aspectAttKey1, aspectFooKey2,
            aspectFooEvolvedKey2, aspectBarKey2, aspectAttKey2, aspectFooBarKey1, aspectFooBarKey2))).thenReturn(
        ImmutableMap.of(aspectFooKey1, Optional.of(foo), aspectFooKey2, Optional.of(bar)));

    Map<Long, EntityValue> keyValueMap = runAndWait(_internalResource.batchGet(ImmutableSet.of(id1, id2), null))
        .entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    assertEquals(keyValueMap.size(), 2);
    assertEquals(keyValueMap.get(id1).getFoo(), foo);
    assertFalse(keyValueMap.get(id1).hasBar());
    assertEquals(keyValueMap.get(id2).getBar(), bar);
    assertFalse(keyValueMap.get(id2).hasFoo());
  }

  @Test
  public void testBatchGetSpecificAspect() {
    long id1 = 1;
    Urn urn1 = makeUrn(id1);
    long id2 = 2;
    Urn urn2 = makeUrn(id2);
    AspectKey<Urn, AspectFoo> fooKey1 = new AspectKey<>(AspectFoo.class, urn1, LATEST_VERSION);
    AspectKey<Urn, AspectFoo> fooKey2 = new AspectKey<>(AspectFoo.class, urn2, LATEST_VERSION);
    String[] aspectNames = {ModelUtils.getAspectName(AspectFoo.class)};

    runAndWait(_resource.batchGet(ImmutableSet.of(id1, id2), aspectNames));

    verify(_mockLocalDAO, times(1)).get(ImmutableSet.of(fooKey1, fooKey2));
    verifyNoMoreInteractions(_mockLocalDAO);
  }

  @Test
  public void testIngest() {
    Urn urn = makeUrn(1);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectBar bar = new AspectBar().setValue("bar");
    List<EntityAspectUnion> aspects = Arrays.asList(ModelUtils.newAspectUnion(EntityAspectUnion.class, foo),
        ModelUtils.newAspectUnion(EntityAspectUnion.class, bar));
    EntitySnapshot snapshot = ModelUtils.newSnapshot(EntitySnapshot.class, urn, aspects);

    runAndWait(_resource.ingest(snapshot));

    verify(_mockLocalDAO, times(1)).add(eq(urn), eq(foo), any(), eq(null), eq(null));
    verify(_mockLocalDAO, times(1)).add(eq(urn), eq(bar), any(), eq(null), eq(null));
    verifyNoMoreInteractions(_mockLocalDAO);
  }

  @Test
  public void testGetSnapshotWithOneAspect() {
    Urn urn = makeUrn(1);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectKey<Urn, ? extends RecordTemplate> fooKey = new AspectKey<>(AspectFoo.class, urn, LATEST_VERSION);
    Set<AspectKey<Urn, ? extends RecordTemplate>> aspectKeys = ImmutableSet.of(fooKey);
    when(_mockLocalDAO.get(aspectKeys)).thenReturn(ImmutableMap.of(fooKey, Optional.of(foo)));
    String[] aspectNames = new String[]{ModelUtils.getAspectName(AspectFoo.class)};

    EntitySnapshot snapshot = runAndWait(_resource.getSnapshot(urn.toString(), aspectNames));

    assertEquals(snapshot.getUrn(), urn);
    assertEquals(snapshot.getAspects().size(), 1);
    assertEquals(snapshot.getAspects().get(0).getAspectFoo(), foo);
  }

  @Test
  public void testGetSnapshotWithAllAspects() {
    Urn urn = makeUrn(1);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectBar bar = new AspectBar().setValue("bar");
    AspectFooBar fooBar = new AspectFooBar().setBars(new BarUrnArray(new BarUrn(1)));
    AspectAttributes att = new AspectAttributes().setAttributes(new StringArray("a"));
    AspectKey<Urn, ? extends RecordTemplate> fooKey = new AspectKey<>(AspectFoo.class, urn, LATEST_VERSION);
    AspectKey<Urn, ? extends RecordTemplate> barKey = new AspectKey<>(AspectBar.class, urn, LATEST_VERSION);
    AspectKey<Urn, ? extends RecordTemplate> fooBarKey = new AspectKey<>(AspectFooBar.class, urn, LATEST_VERSION);
    AspectKey<Urn, ? extends RecordTemplate> attKey = new AspectKey<>(AspectAttributes.class, urn, LATEST_VERSION);
    Set<AspectKey<Urn, ? extends RecordTemplate>> aspectKeys = ImmutableSet.of(fooKey, barKey, fooBarKey, attKey);
    when(_mockLocalDAO.get(aspectKeys)).thenReturn(ImmutableMap.of(fooKey, Optional.of(foo), barKey, Optional.of(bar),
        fooBarKey, Optional.of(fooBar), attKey, Optional.of(att)));

    EntitySnapshot snapshot = runAndWait(_resource.getSnapshot(urn.toString(), null));

    assertEquals(snapshot.getUrn(), urn);

    Set<RecordTemplate> aspects = snapshot.getAspects().stream().map(RecordUtils::getSelectedRecordTemplateFromUnion).collect(Collectors.toSet());
    assertEquals(aspects, ImmutableSet.of(foo, bar, fooBar, att));
  }

  @Test
  public void testInternalModelGetSnapshotWithAllAspects() {
    Urn urn = makeUrn(1);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectFooEvolved fooEvolved = new AspectFooEvolved().setValue("fooEvolved");
    AspectBar bar = new AspectBar().setValue("bar");
    AspectFooBar fooBar = new AspectFooBar().setBars(new BarUrnArray(new BarUrn(1)));
    AspectAttributes att = new AspectAttributes().setAttributes(new StringArray("a"));
    AspectKey<Urn, ? extends RecordTemplate> fooKey = new AspectKey<>(AspectFoo.class, urn, LATEST_VERSION);
    AspectKey<Urn, ? extends RecordTemplate> fooEvolvedKey = new AspectKey<>(AspectFooEvolved.class, urn, LATEST_VERSION);
    AspectKey<Urn, ? extends RecordTemplate> barKey = new AspectKey<>(AspectBar.class, urn, LATEST_VERSION);
    AspectKey<Urn, ? extends RecordTemplate> fooBarKey = new AspectKey<>(AspectFooBar.class, urn, LATEST_VERSION);
    AspectKey<Urn, ? extends RecordTemplate> attKey = new AspectKey<>(AspectAttributes.class, urn, LATEST_VERSION);
    Set<AspectKey<Urn, ? extends RecordTemplate>> aspectKeys =
        ImmutableSet.of(fooKey, fooEvolvedKey, barKey, fooBarKey, attKey);
    when(_mockLocalDAO.get(aspectKeys)).thenReturn(
        ImmutableMap.of(fooKey, Optional.of(foo), fooEvolvedKey, Optional.of(fooEvolved), barKey, Optional.of(bar),
            fooBarKey, Optional.of(fooBar), attKey, Optional.of(att)));

    EntitySnapshot snapshot = runAndWait(_resource.getSnapshot(urn.toString(), null, true));

    assertEquals(snapshot.getUrn(), urn);

    Set<RecordTemplate> aspects =
        snapshot.getAspects().stream().map(RecordUtils::getSelectedRecordTemplateFromUnion).collect(Collectors.toSet());
    assertEquals(aspects, ImmutableSet.of(foo, bar, fooBar, att));
  }

  @Test
  public void testGetSnapshotWithInvalidUrn() {
    try {
      runAndWait(_resource.getSnapshot("invalid urn", new String[]{ModelUtils.getAspectName(AspectFoo.class)}));
    } catch (RestLiServiceException e) {
      assertEquals(e.getStatus(), HttpStatus.S_400_BAD_REQUEST);
    }
  }

  @Test
  public void testBackfillOneAspect() {
    Urn urn = makeUrn(1);
    AspectFoo foo = new AspectFoo().setValue("foo");
    when(_mockLocalDAO.backfill(AspectFoo.class, urn)).thenReturn(Optional.of(foo));
    String[] aspectNames = new String[]{ModelUtils.getAspectName(AspectFoo.class)};

    BackfillResult backfillResult = runAndWait(_resource.backfill(urn.toString(), aspectNames));

    assertEquals(backfillResult.getEntities().size(), 1);

    BackfillResultEntity backfillResultEntity = backfillResult.getEntities().get(0);
    assertEquals(backfillResultEntity.getUrn(), urn);
    assertEquals(backfillResultEntity.getAspects().size(), 1);
    assertEquals(backfillResultEntity.getAspects().get(0), ModelUtils.getAspectName(AspectFoo.class));
  }

  @Test
  public void testBackfillAllAspects() {
    Urn urn = makeUrn(1);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectBar bar = new AspectBar().setValue("bar");
    when(_mockLocalDAO.backfill(AspectFoo.class, urn)).thenReturn(Optional.of(foo));
    when(_mockLocalDAO.backfill(AspectBar.class, urn)).thenReturn(Optional.of(bar));

    BackfillResult backfillResult = runAndWait(_resource.backfill(urn.toString(), null));

    assertEquals(backfillResult.getEntities().size(), 1);

    BackfillResultEntity backfillResultEntity = backfillResult.getEntities().get(0);
    assertEquals(backfillResultEntity.getUrn(), urn);
    assertEquals(backfillResultEntity.getAspects().size(), 2);
    assertEquals(ImmutableSet.copyOf(backfillResultEntity.getAspects()),
        ImmutableSet.of(ModelUtils.getAspectName(AspectFoo.class), ModelUtils.getAspectName(AspectBar.class)));
  }

  @Test
  public void testBackfillWithInvalidUrn() {
    try {
      runAndWait(_resource.backfill("invalid urn", new String[]{ModelUtils.getAspectName(AspectFoo.class)}));
    } catch (RestLiServiceException e) {
      assertEquals(e.getStatus(), HttpStatus.S_400_BAD_REQUEST);
    }
  }

  /**
   * Test class for {@link BaseEntityResource}.
   * */
  private class TestResource extends
                             BaseEntityResource<Long, EntityValue, Urn, EntitySnapshot, EntityAspectUnion,
                                 InternalEntitySnapshot, InternalEntityAspectUnion, EntityAsset> {

    TestResource() {
      super(EntitySnapshot.class, EntityAspectUnion.class, InternalEntitySnapshot.class,
          InternalEntityAspectUnion.class, EntityAsset.class);
    }



    @Override
    @Nonnull
    protected BaseLocalDAO<InternalEntityAspectUnion, Urn> getLocalDAO() {
      return _mockLocalDAO;
    }

    @Override
    @Nonnull
    protected Urn createUrnFromString(@Nonnull String urnString) {
      try {
        return Urn.createFromString(urnString);
      } catch (URISyntaxException e) {
        throw RestliUtils.badRequestException("Invalid URN: " + urnString);
      }
    }

    @Override
    @Nonnull
    protected Urn toUrn(@Nonnull Long key) {
      return makeUrn(key);
    }

    @Nonnull
    @Override
    protected Long toKey(@Nonnull Urn urn) {
      return urn.getIdAsLong();
    }

    @Override
    @Nonnull
    protected EntityValue toValue(@Nonnull EntitySnapshot snapshot) {
      EntityValue value = new EntityValue();
      ModelUtils.getAspectsFromSnapshot(snapshot).forEach(a -> {
        if (a instanceof AspectFoo) {
          value.setFoo((AspectFoo) a);
        } else if (a instanceof AspectBar) {
          value.setBar((AspectBar) a);
        }
      });
      return value;
    }

    @Override
    @Nonnull
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

    @Override
    public ResourceContext getContext() {
      return mock(ResourceContext.class);
    }
  }

  /**
   * Test class for {@link BaseEntityResource}.
   * */
  private class TestInternalResource extends
                             BaseEntityResource<Long, EntityValue, Urn, EntitySnapshot, EntityAspectUnion,
                                 InternalEntitySnapshot, InternalEntityAspectUnion, EntityAsset> {

    TestInternalResource() {
      super(EntitySnapshot.class, EntityAspectUnion.class, InternalEntitySnapshot.class,
          InternalEntityAspectUnion.class, EntityAsset.class, new ResourceLix() {
            @Override
            public boolean testGet(@Nonnull String urn, @Nonnull String entityType) {
              return false;
            }

            @Override
            public boolean testBatchGet(@Nullable String urn, @Nullable String entityType) {
              return true;
            }

            @Override
            public boolean testBatchGetWithErrors(@Nullable String urn, @Nullable String type) {
              return false;
            }

            @Override
            public boolean testIngest(@Nonnull String urn, @Nonnull String entityType, @Nullable String aspectName) {
              return false;
            }

            @Override
            public boolean testIngestWithTracking(@Nonnull String urn, @Nonnull String entityType,
                @Nullable String aspectName) {
              return false;
            }

            @Override
            public boolean testIngestAsset(@Nonnull String urn, @Nonnull String entityType,
                @Nullable String aspectName) {
              return false;
            }

            @Override
            public boolean testGetSnapshot(@Nullable String urn, @Nullable String entityType) {
              return false;
            }

            @Override
            public boolean testBackfillLegacy(@Nullable String urn, @Nullable String entityType) {
              return false;
            }

            @Override
            public boolean testBackfillWithUrns(@Nullable String urn, @Nullable String entityType) {
              return false;
            }

            @Override
            public boolean testEmitNoChangeMetadataAuditEvent(@Nullable String urn, @Nullable String entityType) {
              return false;
            }

            @Override
            public boolean testBackfillWithNewValue(@Nullable String urn, @Nullable String entityType) {
              return false;
            }

            @Override
            public boolean testBackfillEntityTables(@Nullable String urn, @Nullable String entityType) {
              return false;
            }

            @Override
            public boolean testBackfillRelationshipTables(@Nullable String urn, @Nullable String entityType) {
              return false;
            }

            @Override
            public boolean testBackfill(@Nonnull String assetType, @Nonnull String mode) {
              return false;
            }

            @Override
            public boolean testFilter(@Nonnull String assetType) {
              return false;
            }

            @Override
            public boolean testGetAll(@Nullable String urnType) {
              return false;
            }

            @Override
            public boolean testSearch(@Nullable String urnType) {
              return false;
            }

            @Override
            public boolean testSearchV2(@Nullable String urnType) {
              return false;
            }
          });
    }



    @Override
    @Nonnull
    protected BaseLocalDAO<InternalEntityAspectUnion, Urn> getLocalDAO() {
      return _mockLocalDAO;
    }

    @Override
    @Nonnull
    protected Urn createUrnFromString(@Nonnull String urnString) {
      try {
        return Urn.createFromString(urnString);
      } catch (URISyntaxException e) {
        throw RestliUtils.badRequestException("Invalid URN: " + urnString);
      }
    }

    @Override
    @Nonnull
    protected Urn toUrn(@Nonnull Long key) {
      return makeUrn(key);
    }

    @Nonnull
    @Override
    protected Long toKey(@Nonnull Urn urn) {
      return urn.getIdAsLong();
    }

    @Override
    @Nonnull
    protected EntityValue toValue(@Nonnull EntitySnapshot snapshot) {
      EntityValue value = new EntityValue();
      ModelUtils.getAspectsFromSnapshot(snapshot).forEach(a -> {
        if (a instanceof AspectFoo) {
          value.setFoo((AspectFoo) a);
        } else if (a instanceof AspectBar) {
          value.setBar((AspectBar) a);
        }
      });
      return value;
    }

    @Override
    @Nonnull
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

    @Override
    public ResourceContext getContext() {
      return mock(ResourceContext.class);
    }
  }
}
