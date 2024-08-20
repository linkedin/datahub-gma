package com.linkedin.metadata.restli;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.data.template.LongMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.backfill.BackfillMode;
import com.linkedin.metadata.dao.AspectKey;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.ListResult;
import com.linkedin.metadata.dao.UrnAspectEntry;
import com.linkedin.metadata.dao.builder.BaseLocalRelationshipBuilder.LocalRelationshipUpdates;
import com.linkedin.metadata.dao.internal.BaseGraphWriterDAO;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.events.IngestionMode;
import com.linkedin.metadata.events.IngestionTrackingContext;
import com.linkedin.metadata.internal.IngestionParams;
import com.linkedin.metadata.query.IndexCriterion;
import com.linkedin.metadata.query.IndexCriterionArray;
import com.linkedin.metadata.query.IndexFilter;
import com.linkedin.metadata.query.IndexGroupByCriterion;
import com.linkedin.metadata.query.IndexSortCriterion;
import com.linkedin.metadata.query.MapMetadata;
import com.linkedin.metadata.query.SortOrder;
import com.linkedin.metadata.restli.lix.ResourceLix;
import com.linkedin.parseq.BaseEngineTest;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.BatchResult;
import com.linkedin.restli.server.CollectionResult;
import com.linkedin.restli.server.PagingContext;
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
import com.linkedin.testing.EntityKey;
import com.linkedin.testing.EntitySnapshot;
import com.linkedin.testing.EntityValue;
import com.linkedin.testing.InternalEntityAspectUnion;
import com.linkedin.testing.InternalEntitySnapshot;
import com.linkedin.testing.localrelationship.AspectFooBar;
import com.linkedin.testing.localrelationship.BelongsTo;
import com.linkedin.testing.urn.BarUrn;
import com.linkedin.testing.urn.FooUrn;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
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


public class BaseEntityResourceTest extends BaseEngineTest {

  private BaseLocalDAO<InternalEntityAspectUnion, FooUrn> _mockLocalDAO;
  private TestResource _resource = new TestResource();
  private TestInternalResource _internalResource = new TestInternalResource();

  class TestResource extends
                     BaseEntityResource<ComplexResourceKey<EntityKey, EmptyRecord>, EntityValue, FooUrn, EntitySnapshot,
                         EntityAspectUnion, InternalEntitySnapshot, InternalEntityAspectUnion, EntityAsset> {

    public TestResource() {
      super(EntitySnapshot.class, EntityAspectUnion.class, FooUrn.class, InternalEntitySnapshot.class,
          InternalEntityAspectUnion.class, EntityAsset.class);
    }

    @Nonnull
    @Override
    protected BaseLocalDAO<InternalEntityAspectUnion, FooUrn> getLocalDAO() {
      return _mockLocalDAO;
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
      if (value.hasAttributes()) {
        aspects.add(ModelUtils.newAspectUnion(EntityAspectUnion.class, value.getAttributes()));
      }

      snapshot.setAspects(aspects);
      return snapshot;
    }

    @Override
    public ResourceContext getContext() {
      return mock(ResourceContext.class);
    }
  }

  class TestInternalResource extends
                     BaseEntityResource<ComplexResourceKey<EntityKey, EmptyRecord>, EntityValue, FooUrn, EntitySnapshot,
                         EntityAspectUnion, InternalEntitySnapshot, InternalEntityAspectUnion, EntityAsset> {

    public TestInternalResource() {
      super(EntitySnapshot.class, EntityAspectUnion.class, FooUrn.class, InternalEntitySnapshot.class,
          InternalEntityAspectUnion.class, EntityAsset.class, new ResourceLix() {

            @Override
            public boolean testGet(@Nonnull String urn, @Nonnull String entityType) {
              return true;
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
              return true;
            }

            @Override
            public boolean testIngestWithTracking(@Nonnull String urn, @Nonnull String entityType,
                @Nullable String aspectName) {
              return true;
            }

            @Override
            public boolean testGetSnapshot(@Nullable String urn, @Nullable String entityType) {
              return true;
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
              return true;
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

    @Nonnull
    @Override
    protected BaseLocalDAO<InternalEntityAspectUnion, FooUrn> getLocalDAO() {
      return _mockLocalDAO;
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
      if (value.hasAttributes()) {
        aspects.add(ModelUtils.newAspectUnion(EntityAspectUnion.class, value.getAttributes()));
      }

      snapshot.setAspects(aspects);
      return snapshot;
    }

    @Override
    public ResourceContext getContext() {
      return mock(ResourceContext.class);
    }
  }

  @BeforeMethod
  public void setup() {
    _mockLocalDAO = mock(BaseLocalDAO.class);
  }

  @Test
  public void testGet() {
    FooUrn urn = makeFooUrn(1234);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectKey<FooUrn, AspectFoo> aspect1Key = new AspectKey<>(AspectFoo.class, urn, LATEST_VERSION);
    AspectKey<FooUrn, AspectBar> aspect2Key = new AspectKey<>(AspectBar.class, urn, LATEST_VERSION);
    AspectKey<FooUrn, AspectFooBar> aspect3Key = new AspectKey<>(AspectFooBar.class, urn, LATEST_VERSION);
    AspectKey<FooUrn, AspectAttributes> aspect4Key = new AspectKey<>(AspectAttributes.class, urn, LATEST_VERSION);
    when(_mockLocalDAO.exists(urn)).thenReturn(true);
    when(_mockLocalDAO.get(new HashSet<>(Arrays.asList(aspect1Key, aspect2Key, aspect3Key, aspect4Key)))).thenReturn(
        Collections.singletonMap(aspect1Key, Optional.of(foo)));

    EntityValue value = runAndWait(_resource.get(makeResourceKey(urn), null));

    assertEquals(value.getFoo(), foo);
    assertFalse(value.hasBar());
  }

  @Test
  public void testInternalModelGet() {
    FooUrn urn = makeFooUrn(1234);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectKey<FooUrn, AspectFoo> aspect1Key = new AspectKey<>(AspectFoo.class, urn, LATEST_VERSION);
    AspectKey<FooUrn, AspectBar> aspect2Key = new AspectKey<>(AspectBar.class, urn, LATEST_VERSION);
    AspectKey<FooUrn, AspectFooBar> aspect3Key = new AspectKey<>(AspectFooBar.class, urn, LATEST_VERSION);
    AspectKey<FooUrn, AspectAttributes> aspect4Key = new AspectKey<>(AspectAttributes.class, urn, LATEST_VERSION);
    AspectKey<FooUrn, AspectFooEvolved> aspect5Key = new AspectKey<>(AspectFooEvolved.class, urn, LATEST_VERSION);
    when(_mockLocalDAO.exists(urn)).thenReturn(true);
    when(_mockLocalDAO.get(
        new HashSet<>(Arrays.asList(aspect1Key, aspect2Key, aspect3Key, aspect4Key, aspect5Key)))).thenReturn(
        Collections.singletonMap(aspect1Key, Optional.of(foo)));

    EntityValue value = runAndWait(_internalResource.get(makeResourceKey(urn), null, true));

    assertEquals(value.getFoo(), foo);
    assertFalse(value.hasBar());
  }

  @Test
  public void testGetUrnNotFound() {
    FooUrn urn = makeFooUrn(1234);

    AspectKey<FooUrn, AspectFoo> aspect1Key = new AspectKey<>(AspectFoo.class, urn, LATEST_VERSION);
    AspectKey<FooUrn, AspectBar> aspect2Key = new AspectKey<>(AspectBar.class, urn, LATEST_VERSION);

    when(_mockLocalDAO.exists(urn)).thenReturn(false);
    when(_mockLocalDAO.get(new HashSet<>(Arrays.asList(aspect1Key, aspect2Key)))).thenReturn(Collections.emptyMap());

    try {
      runAndWait(_resource.get(makeResourceKey(urn), new String[0]));
      fail("An exception should've been thrown!");
    } catch (RestLiServiceException e) {
      assertEquals(e.getStatus(), HttpStatus.S_404_NOT_FOUND);
    }
  }

  @Test
  public void testGetWithEmptyAspects() {
    FooUrn urn = makeFooUrn(1234);

    when(_mockLocalDAO.exists(urn)).thenReturn(true);

    try {
      EntityValue value = runAndWait(_resource.get(makeResourceKey(urn), new String[0]));
      assertFalse(value.hasFoo());
      assertFalse(value.hasBar());
    } catch (RestLiServiceException e) {
      fail("No exception should be thrown!");
    }
  }

  @Test
  public void testGetSpecificAspect() {
    FooUrn urn = makeFooUrn(1234);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectKey<FooUrn, AspectFoo> aspect1Key = new AspectKey<>(AspectFoo.class, urn, LATEST_VERSION);
    String[] aspectNames = {AspectFoo.class.getCanonicalName()};

    when(_mockLocalDAO.exists(urn)).thenReturn(true);
    when(_mockLocalDAO.get(new HashSet<>(Arrays.asList(aspect1Key)))).thenReturn(
        Collections.singletonMap(aspect1Key, Optional.of(foo)));

    EntityValue value = runAndWait(_resource.get(makeResourceKey(urn), aspectNames));
    assertEquals(value.getFoo(), foo);
    verify(_mockLocalDAO, times(1)).get(Collections.singleton(aspect1Key));
  }

  @Test
  public void testGetSpecificAspectNotFound() {
    FooUrn urn = makeFooUrn(1234);
    String[] aspectNames = {AspectFoo.class.getCanonicalName()};

    when(_mockLocalDAO.exists(urn)).thenReturn(true);

    try {
      EntityValue value = runAndWait(_resource.get(makeResourceKey(urn), aspectNames));
      assertFalse(value.hasFoo());
      assertFalse(value.hasBar());
    } catch (RestLiServiceException e) {
      fail("No exception should be thrown!");
    }
  }

  @Test
  public void testBatchGet() {
    FooUrn urn1 = makeFooUrn(1);
    FooUrn urn2 = makeFooUrn(2);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectBar bar = new AspectBar().setValue("bar");

    AspectKey<FooUrn, AspectFoo> aspectFooKey1 = new AspectKey<>(AspectFoo.class, urn1, LATEST_VERSION);
    AspectKey<FooUrn, AspectBar> aspectBarKey1 = new AspectKey<>(AspectBar.class, urn1, LATEST_VERSION);
    AspectKey<FooUrn, AspectFooBar> aspectFooBarKey1 = new AspectKey<>(AspectFooBar.class, urn1, LATEST_VERSION);
    AspectKey<FooUrn, AspectAttributes> aspectAttKey1 = new AspectKey<>(AspectAttributes.class, urn1, LATEST_VERSION);
    AspectKey<FooUrn, AspectFoo> aspectFooKey2 = new AspectKey<>(AspectFoo.class, urn2, LATEST_VERSION);
    AspectKey<FooUrn, AspectBar> aspectBarKey2 = new AspectKey<>(AspectBar.class, urn2, LATEST_VERSION);
    AspectKey<FooUrn, AspectFooBar> aspectFooBarKey2 = new AspectKey<>(AspectFooBar.class, urn2, LATEST_VERSION);
    AspectKey<FooUrn, AspectAttributes> aspectAttKey2 = new AspectKey<>(AspectAttributes.class, urn2, LATEST_VERSION);

    when(_mockLocalDAO.get(ImmutableSet.of(aspectFooBarKey1, aspectFooBarKey2, aspectFooKey1, aspectBarKey1, aspectFooKey2,
        aspectBarKey2, aspectAttKey1, aspectAttKey2)))
        .thenReturn(ImmutableMap.of(aspectFooKey1, Optional.of(foo), aspectFooKey2, Optional.of(bar)));

    Map<EntityKey, EntityValue> keyValueMap =
        runAndWait(_resource.batchGet(ImmutableSet.of(makeResourceKey(urn1), makeResourceKey(urn2)), null)).entrySet()
            .stream()
            .collect(Collectors.toMap(e -> e.getKey().getKey(), e -> e.getValue()));

    assertEquals(keyValueMap.size(), 2);
    assertEquals(keyValueMap.get(makeKey(1)).getFoo(), foo);
    assertFalse(keyValueMap.get(makeKey(1)).hasBar());
    assertEquals(keyValueMap.get(makeKey(2)).getBar(), bar);
    assertFalse(keyValueMap.get(makeKey(2)).hasFoo());
  }

  @Test
  public void testInternalModelBatchGet() {
    FooUrn urn1 = makeFooUrn(1);
    FooUrn urn2 = makeFooUrn(2);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectBar bar = new AspectBar().setValue("bar");

    AspectKey<FooUrn, AspectFoo> aspectFooKey1 = new AspectKey<>(AspectFoo.class, urn1, LATEST_VERSION);
    AspectKey<FooUrn, AspectFooEvolved> aspectFooEvolvedKey1 =
        new AspectKey<>(AspectFooEvolved.class, urn1, LATEST_VERSION);
    AspectKey<FooUrn, AspectBar> aspectBarKey1 = new AspectKey<>(AspectBar.class, urn1, LATEST_VERSION);
    AspectKey<FooUrn, AspectFooBar> aspectFooBarKey1 = new AspectKey<>(AspectFooBar.class, urn1, LATEST_VERSION);
    AspectKey<FooUrn, AspectAttributes> aspectAttKey1 = new AspectKey<>(AspectAttributes.class, urn1, LATEST_VERSION);
    AspectKey<FooUrn, AspectFoo> aspectFooKey2 = new AspectKey<>(AspectFoo.class, urn2, LATEST_VERSION);
    AspectKey<FooUrn, AspectFooEvolved> aspectFooEvolvedKey2 =
        new AspectKey<>(AspectFooEvolved.class, urn2, LATEST_VERSION);
    AspectKey<FooUrn, AspectBar> aspectBarKey2 = new AspectKey<>(AspectBar.class, urn2, LATEST_VERSION);
    AspectKey<FooUrn, AspectFooBar> aspectFooBarKey2 = new AspectKey<>(AspectFooBar.class, urn2, LATEST_VERSION);
    AspectKey<FooUrn, AspectAttributes> aspectAttKey2 = new AspectKey<>(AspectAttributes.class, urn2, LATEST_VERSION);

    when(_mockLocalDAO.get(
        ImmutableSet.of(aspectFooBarKey1, aspectFooBarKey2, aspectFooKey1, aspectBarKey1, aspectFooKey2, aspectBarKey2,
            aspectAttKey1, aspectAttKey2, aspectFooEvolvedKey1, aspectFooEvolvedKey2))).thenReturn(
        ImmutableMap.of(aspectFooKey1, Optional.of(foo), aspectFooKey2, Optional.of(bar)));

    Map<EntityKey, EntityValue> keyValueMap = runAndWait(
        _internalResource.batchGet(ImmutableSet.of(makeResourceKey(urn1), makeResourceKey(urn2)), null)).entrySet()
        .stream()
        .collect(Collectors.toMap(e -> e.getKey().getKey(), e -> e.getValue()));

    assertEquals(keyValueMap.size(), 2);
    assertEquals(keyValueMap.get(makeKey(1)).getFoo(), foo);
    assertFalse(keyValueMap.get(makeKey(1)).hasBar());
    assertEquals(keyValueMap.get(makeKey(2)).getBar(), bar);
    assertFalse(keyValueMap.get(makeKey(2)).hasFoo());
  }

  @Test
  public void testBatchGetSpecificAspect() {
    FooUrn urn1 = makeFooUrn(1);
    FooUrn urn2 = makeFooUrn(2);
    AspectKey<FooUrn, AspectFoo> fooKey1 = new AspectKey<>(AspectFoo.class, urn1, LATEST_VERSION);
    AspectKey<FooUrn, AspectFoo> fooKey2 = new AspectKey<>(AspectFoo.class, urn2, LATEST_VERSION);
    String[] aspectNames = {ModelUtils.getAspectName(AspectFoo.class)};

    runAndWait(_resource.batchGet(ImmutableSet.of(makeResourceKey(urn1), makeResourceKey(urn2)), aspectNames));

    verify(_mockLocalDAO, times(1)).get(ImmutableSet.of(fooKey1, fooKey2));
    verifyNoMoreInteractions(_mockLocalDAO);
  }

  @Test
  public void testBatchGetWithErrorsUrnsNotFound() {
    FooUrn urn1 = makeFooUrn(1);
    FooUrn urn2 = makeFooUrn(2);
    String[] aspectNames = {ModelUtils.getAspectName(AspectFoo.class)};

    AspectKey<FooUrn, AspectFoo> aspectFooKey1 = new AspectKey<>(AspectFoo.class, urn1, LATEST_VERSION);
    AspectKey<FooUrn, AspectFoo> aspectFooKey2 = new AspectKey<>(AspectFoo.class, urn1, LATEST_VERSION);

    when(_mockLocalDAO.get(ImmutableSet.of(aspectFooKey1, aspectFooKey2)))
        .thenReturn(Collections.emptyMap());

    BatchResult<ComplexResourceKey<EntityKey, EmptyRecord>, EntityValue> result =
        runAndWait(_resource.batchGetWithErrors(ImmutableSet.of(makeResourceKey(urn1), makeResourceKey(urn2)), aspectNames));

    // convert BatchResult<ComplexResourceKey<EntityKey, EmptyRecord>, EntityValue> to BatchResult<EntityKey, EntityValue>
    BatchResult<EntityKey, EntityValue> batchResultMap = convertBatchResult(result);

    // ensure there are 2 404s in the form of HttpStatus
    Map<EntityKey, HttpStatus> statuses = batchResultMap.getStatuses();
    assertEquals(statuses.size(), 2);
    assertEquals(statuses.get(makeKey(1)), HttpStatus.S_404_NOT_FOUND);
    assertEquals(statuses.get(makeKey(2)), HttpStatus.S_404_NOT_FOUND);

    // ensure there are 2 404s in the form of RestLiServiceException
    Map<EntityKey, RestLiServiceException> errors = batchResultMap.getErrors();
    assertEquals(errors.size(), 2);
    assertEquals(errors.get(makeKey(1)).getStatus(), HttpStatus.S_404_NOT_FOUND);
    assertEquals(errors.get(makeKey(2)).getStatus(), HttpStatus.S_404_NOT_FOUND);

    // ensure the urns that don't exist are not in the result data map
    assertEquals(batchResultMap.size(), 0);
  }

  @Test
  public void testBatchGetWithErrorsWithEmptyAspects() {
    FooUrn urn1 = makeFooUrn(1);
    FooUrn urn2 = makeFooUrn(2);

    AspectKey<FooUrn, AspectFoo> aspectFooKey1 = new AspectKey<>(AspectFoo.class, urn1, LATEST_VERSION);
    AspectKey<FooUrn, AspectBar> aspectBarKey1 = new AspectKey<>(AspectBar.class, urn1, LATEST_VERSION);
    AspectKey<FooUrn, AspectFooBar> aspectFooBarKey1 = new AspectKey<>(AspectFooBar.class, urn1, LATEST_VERSION);
    AspectKey<FooUrn, AspectAttributes> aspectAttKey1 = new AspectKey<>(AspectAttributes.class, urn1, LATEST_VERSION);
    AspectKey<FooUrn, AspectFoo> aspectFooKey2 = new AspectKey<>(AspectFoo.class, urn2, LATEST_VERSION);
    AspectKey<FooUrn, AspectBar> aspectBarKey2 = new AspectKey<>(AspectBar.class, urn2, LATEST_VERSION);
    AspectKey<FooUrn, AspectFooBar> aspectFooBarKey2 = new AspectKey<>(AspectFooBar.class, urn2, LATEST_VERSION);
    AspectKey<FooUrn, AspectAttributes> aspectAttKey2 = new AspectKey<>(AspectAttributes.class, urn2, LATEST_VERSION);

    when(_mockLocalDAO.get(ImmutableSet.of(aspectFooBarKey1, aspectFooBarKey2, aspectFooKey1, aspectBarKey1, aspectFooKey2,
        aspectBarKey2, aspectAttKey1, aspectAttKey2)))
        .thenReturn(Collections.emptyMap());

    BatchResult<ComplexResourceKey<EntityKey, EmptyRecord>, EntityValue> result =
        runAndWait(_resource.batchGetWithErrors(ImmutableSet.of(makeResourceKey(urn1), makeResourceKey(urn2)), new String[0]));

    // convert BatchResult<ComplexResourceKey<EntityKey, EmptyRecord>, EntityValue> to BatchResult<EntityKey, EntityValue>
    BatchResult<EntityKey, EntityValue> batchResultMap = convertBatchResult(result);

    // ensure there are 2 404s in the form of HttpStatus
    Map<EntityKey, HttpStatus> statuses = batchResultMap.getStatuses();
    assertEquals(statuses.size(), 2);
    assertEquals(statuses.get(makeKey(1)), HttpStatus.S_404_NOT_FOUND);
    assertEquals(statuses.get(makeKey(2)), HttpStatus.S_404_NOT_FOUND);

    // ensure there are 2 404s in the form of RestLiServiceException
    Map<EntityKey, RestLiServiceException> errors = batchResultMap.getErrors();
    assertEquals(errors.size(), 2);
    assertEquals(errors.get(makeKey(1)).getStatus(), HttpStatus.S_404_NOT_FOUND);
    assertEquals(errors.get(makeKey(2)).getStatus(), HttpStatus.S_404_NOT_FOUND);

    // ensure the urns that don't exist are not in the result data map
    assertEquals(batchResultMap.size(), 0);
  }

  @Test
  public void testBatchGetWithErrorsSpecificAspectsPartialSuccess() {
    FooUrn urn1 = makeFooUrn(1);
    FooUrn urn2 = makeFooUrn(2);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectBar bar = new AspectBar().setValue("bar");
    String[] aspectNames = {AspectFoo.class.getCanonicalName(), AspectBar.class.getCanonicalName()};

    AspectKey<FooUrn, AspectFoo> aspectFooKey1 = new AspectKey<>(AspectFoo.class, urn1, LATEST_VERSION);
    AspectKey<FooUrn, AspectBar> aspectBarKey1 = new AspectKey<>(AspectBar.class, urn1, LATEST_VERSION);
    AspectKey<FooUrn, AspectFoo> aspectFooKey2 = new AspectKey<>(AspectFoo.class, urn2, LATEST_VERSION);
    AspectKey<FooUrn, AspectBar> aspectBarKey2 = new AspectKey<>(AspectBar.class, urn2, LATEST_VERSION);

    when(_mockLocalDAO.get(ImmutableSet.of(aspectFooKey1, aspectBarKey1, aspectFooKey2, aspectBarKey2)))
        .thenReturn(ImmutableMap.of(aspectFooKey1, Optional.of(foo), aspectBarKey2, Optional.of(bar)));

    BatchResult<ComplexResourceKey<EntityKey, EmptyRecord>, EntityValue> result =
        runAndWait(_resource.batchGetWithErrors(ImmutableSet.of(makeResourceKey(urn1), makeResourceKey(urn2)), aspectNames));

    // convert BatchResult<ComplexResourceKey<EntityKey, EmptyRecord>, EntityValue> to BatchResult<EntityKey, EntityValue>
    BatchResult<EntityKey, EntityValue> batchResultMap = convertBatchResult(result);

    // ensure there are 2 200s and 0 404s in the form of HttpStatus
    Map<EntityKey, HttpStatus> statuses = batchResultMap.getStatuses();
    assertEquals(statuses.size(), 2);
    assertEquals(statuses.get(makeKey(1)), HttpStatus.S_200_OK);
    assertEquals(statuses.get(makeKey(2)), HttpStatus.S_200_OK);

    // ensure there are 0 404s in the form of RestLiServiceException
    Map<EntityKey, RestLiServiceException> errors = batchResultMap.getErrors();
    assertEquals(errors.size(), 0);

    // ensure there are 2 results in the result data map
    assertEquals(batchResultMap.size(), 2);
    assertEquals(batchResultMap.get(makeKey(1)).getFoo(), foo);
    assertFalse(batchResultMap.get(makeKey(1)).hasBar());
    assertEquals(batchResultMap.get(makeKey(2)).getBar(), bar);
    assertFalse(batchResultMap.get(makeKey(2)).hasFoo());
  }

  @Test
  public void testBatchGetWithErrorsUrnsPartialSuccess() {
    FooUrn urn1 = makeFooUrn(1);
    FooUrn urn2 = makeFooUrn(2);
    AspectFoo foo = new AspectFoo().setValue("foo");
    String[] aspectNames = {AspectFoo.class.getCanonicalName(), AspectBar.class.getCanonicalName()};

    AspectKey<FooUrn, AspectFoo> aspectFooKey1 = new AspectKey<>(AspectFoo.class, urn1, LATEST_VERSION);
    AspectKey<FooUrn, AspectBar> aspectBarKey1 = new AspectKey<>(AspectBar.class, urn1, LATEST_VERSION);
    AspectKey<FooUrn, AspectFoo> aspectFooKey2 = new AspectKey<>(AspectFoo.class, urn2, LATEST_VERSION);
    AspectKey<FooUrn, AspectBar> aspectBarKey2 = new AspectKey<>(AspectBar.class, urn2, LATEST_VERSION);

    when(_mockLocalDAO.get(ImmutableSet.of(aspectFooKey1, aspectBarKey1, aspectFooKey2, aspectBarKey2)))
        .thenReturn(ImmutableMap.of(aspectFooKey1, Optional.of(foo), aspectBarKey2, Optional.empty()));

    BatchResult<ComplexResourceKey<EntityKey, EmptyRecord>, EntityValue> result =
        runAndWait(_resource.batchGetWithErrors(ImmutableSet.of(makeResourceKey(urn1), makeResourceKey(urn2)), aspectNames));

    // convert BatchResult<ComplexResourceKey<EntityKey, EmptyRecord>, EntityValue> to BatchResult<EntityKey, EntityValue>
    BatchResult<EntityKey, EntityValue> batchResultMap = convertBatchResult(result);

    // ensure there is 1 200 (urn1) and 1 404 (urn2) in the form of HttpStatus
    Map<EntityKey, HttpStatus> statuses = batchResultMap.getStatuses();
    assertEquals(statuses.size(), 2);
    assertEquals(statuses.get(makeKey(1)), HttpStatus.S_200_OK);
    assertEquals(statuses.get(makeKey(2)), HttpStatus.S_404_NOT_FOUND);

    // ensure there is 1 404 in the form of RestLiServiceException (urn2)
    Map<EntityKey, RestLiServiceException> errors = batchResultMap.getErrors();
    assertEquals(errors.size(), 1);
    assertEquals(errors.get(makeKey(2)).getStatus(), HttpStatus.S_404_NOT_FOUND);

    // ensure there is 1 result in the result data map (urn1)
    assertEquals(batchResultMap.size(), 1);
    assertEquals(batchResultMap.get(makeKey(1)).getFoo(), foo);
    assertFalse(batchResultMap.get(makeKey(1)).hasBar());
  }

  // convert BatchResult<ComplexResourceKey<EntityKey, EmptyRecord>, EntityValue> to BatchResult<EntityKey, EntityValue>
  private BatchResult<EntityKey, EntityValue> convertBatchResult(BatchResult<ComplexResourceKey<EntityKey, EmptyRecord>, EntityValue> result) {
    Map<EntityKey, EntityValue> dataMap =
        result.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().getKey(), Map.Entry::getValue));
    Map<EntityKey, HttpStatus> statusMap =
        result.getStatuses().entrySet().stream().collect(Collectors.toMap(e -> e.getKey().getKey(), Map.Entry::getValue));
    Map<EntityKey, RestLiServiceException> errorMap =
        result.getErrors().entrySet().stream().collect(Collectors.toMap(e -> e.getKey().getKey(), Map.Entry::getValue));
    return new BatchResult<>(dataMap, statusMap, errorMap);
  }

  @Test
  public void testIngest() {
    FooUrn urn = makeFooUrn(1);
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
  public void testIngestWithTracking() {
    FooUrn urn = makeFooUrn(1);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectBar bar = new AspectBar().setValue("bar");
    List<EntityAspectUnion> aspects = Arrays.asList(ModelUtils.newAspectUnion(EntityAspectUnion.class, foo),
        ModelUtils.newAspectUnion(EntityAspectUnion.class, bar));
    EntitySnapshot snapshot = ModelUtils.newSnapshot(EntitySnapshot.class, urn, aspects);
    IngestionTrackingContext trackingContext = new IngestionTrackingContext();

    runAndWait(_resource.ingestWithTracking(snapshot, trackingContext, null));

    verify(_mockLocalDAO, times(1)).add(eq(urn), eq(foo), any(), eq(trackingContext), eq(null));
    verify(_mockLocalDAO, times(1)).add(eq(urn), eq(bar), any(), eq(trackingContext), eq(null));

    IngestionParams ingestionParams = new IngestionParams().setIngestionMode(IngestionMode.LIVE);
    runAndWait(_resource.ingestWithTracking(snapshot, trackingContext, ingestionParams));

    verify(_mockLocalDAO, times(1)).add(eq(urn), eq(foo), any(), eq(trackingContext), eq(ingestionParams));
    verify(_mockLocalDAO, times(1)).add(eq(urn), eq(bar), any(), eq(trackingContext), eq(ingestionParams));
    verifyNoMoreInteractions(_mockLocalDAO);
  }

  @Test
  public void testInternalModelIngest() {
    FooUrn urn = makeFooUrn(1);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectBar bar = new AspectBar().setValue("bar");
    List<EntityAspectUnion> aspects = Arrays.asList(ModelUtils.newAspectUnion(EntityAspectUnion.class, foo),
        ModelUtils.newAspectUnion(EntityAspectUnion.class, bar));
    EntitySnapshot snapshot = ModelUtils.newSnapshot(EntitySnapshot.class, urn, aspects);

    runAndWait(_internalResource.ingest(snapshot));

    verify(_mockLocalDAO, times(1)).add(eq(urn), eq(foo), any(), eq(null), eq(null));
    verify(_mockLocalDAO, times(1)).add(eq(urn), eq(bar), any(), eq(null), eq(null));
    verifyNoMoreInteractions(_mockLocalDAO);
  }

  @Test
  public void testInternalModelIngestWithTracking() {
    FooUrn urn = makeFooUrn(1);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectBar bar = new AspectBar().setValue("bar");
    List<EntityAspectUnion> aspects = Arrays.asList(ModelUtils.newAspectUnion(EntityAspectUnion.class, foo),
        ModelUtils.newAspectUnion(EntityAspectUnion.class, bar));
    EntitySnapshot snapshot = ModelUtils.newSnapshot(EntitySnapshot.class, urn, aspects);
    IngestionTrackingContext trackingContext = new IngestionTrackingContext();

    runAndWait(_internalResource.ingestWithTracking(snapshot, trackingContext, null));

    verify(_mockLocalDAO, times(1)).add(eq(urn), eq(foo), any(), eq(trackingContext), eq(null));
    verify(_mockLocalDAO, times(1)).add(eq(urn), eq(bar), any(), eq(trackingContext), eq(null));

    IngestionParams ingestionParams = new IngestionParams().setIngestionMode(IngestionMode.LIVE);
    runAndWait(_internalResource.ingestWithTracking(snapshot, trackingContext, ingestionParams));

    verify(_mockLocalDAO, times(1)).add(eq(urn), eq(foo), any(), eq(trackingContext), eq(ingestionParams));
    verify(_mockLocalDAO, times(1)).add(eq(urn), eq(bar), any(), eq(trackingContext), eq(ingestionParams));
    verifyNoMoreInteractions(_mockLocalDAO);
  }

  @Test
  public void testSkipIngestAspect() {
    FooUrn urn = makeFooUrn(1);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectBar bar = new AspectBar().setValue("bar");
    List<EntityAspectUnion> aspects = Arrays.asList(ModelUtils.newAspectUnion(EntityAspectUnion.class, foo),
        ModelUtils.newAspectUnion(EntityAspectUnion.class, bar));
    EntitySnapshot snapshot = ModelUtils.newSnapshot(EntitySnapshot.class, urn, aspects);

    runAndWait(_resource.ingestInternal(snapshot, Collections.singleton(AspectBar.class),
        null, null, false));

    verify(_mockLocalDAO, times(1)).add(eq(urn), eq(foo), any(), eq(null), eq(null));
    verifyNoMoreInteractions(_mockLocalDAO);
  }

  @Test
  public void testGetSnapshotWithOneAspect() {
    FooUrn urn = makeFooUrn(1);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectKey<FooUrn, ? extends RecordTemplate> fooKey = new AspectKey<>(AspectFoo.class, urn, LATEST_VERSION);
    Set<AspectKey<FooUrn, ? extends RecordTemplate>> aspectKeys = ImmutableSet.of(fooKey);
    when(_mockLocalDAO.get(aspectKeys)).thenReturn(ImmutableMap.of(fooKey, Optional.of(foo)));
    String[] aspectNames = new String[]{ModelUtils.getAspectName(AspectFoo.class)};

    EntitySnapshot snapshot = runAndWait(_resource.getSnapshot(urn.toString(), aspectNames));

    assertEquals(snapshot.getUrn(), urn);
    assertEquals(snapshot.getAspects().size(), 1);
    assertEquals(snapshot.getAspects().get(0).getAspectFoo(), foo);
  }

  @Test
  public void testGetSnapshotWithAllAspects() {
    FooUrn urn = makeFooUrn(1);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectFoo bar = new AspectFoo().setValue("bar");
    AspectFooBar fooBar = new AspectFooBar().setBars(new BarUrnArray(new BarUrn(1)));
    AspectAttributes attributes = new AspectAttributes().setAttributes(new StringArray("a"));

    AspectKey<FooUrn, ? extends RecordTemplate> fooKey = new AspectKey<>(AspectFoo.class, urn, LATEST_VERSION);
    AspectKey<FooUrn, ? extends RecordTemplate> barKey = new AspectKey<>(AspectBar.class, urn, LATEST_VERSION);
    AspectKey<FooUrn, ? extends RecordTemplate> fooBarKey = new AspectKey<>(AspectFooBar.class, urn, LATEST_VERSION);
    AspectKey<FooUrn, ? extends RecordTemplate> attKey = new AspectKey<>(AspectAttributes.class, urn, LATEST_VERSION);

    Set<AspectKey<FooUrn, ? extends RecordTemplate>> aspectKeys = ImmutableSet.of(fooKey, barKey, fooBarKey, attKey);
    when(_mockLocalDAO.get(aspectKeys)).thenReturn(ImmutableMap.of(fooKey, Optional.of(foo), barKey, Optional.of(bar),
        fooBarKey, Optional.of(fooBar), attKey, Optional.of(attributes)));

    EntitySnapshot snapshot = runAndWait(_resource.getSnapshot(urn.toString(), null));

    assertEquals(snapshot.getUrn(), urn);

    Set<RecordTemplate> aspects =
        snapshot.getAspects().stream().map(RecordUtils::getSelectedRecordTemplateFromUnion).collect(Collectors.toSet());
    assertEquals(aspects, ImmutableSet.of(foo, bar, fooBar, attributes));
  }

  @Test
  public void testInternalModelGetSnapshotWithAllAspects() {
    FooUrn urn = makeFooUrn(1);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectFooEvolved fooEvolved = new AspectFooEvolved().setValue("fooEvolved");
    AspectBar bar = new AspectBar().setValue("bar");
    AspectFooBar fooBar = new AspectFooBar().setBars(new BarUrnArray(new BarUrn(1)));
    AspectAttributes attributes = new AspectAttributes().setAttributes(new StringArray("a"));

    AspectKey<FooUrn, ? extends RecordTemplate> fooKey = new AspectKey<>(AspectFoo.class, urn, LATEST_VERSION);
    AspectKey<FooUrn, ? extends RecordTemplate> fooEvolvedKey = new AspectKey<>(AspectFooEvolved.class, urn, LATEST_VERSION);
    AspectKey<FooUrn, ? extends RecordTemplate> barKey = new AspectKey<>(AspectBar.class, urn, LATEST_VERSION);
    AspectKey<FooUrn, ? extends RecordTemplate> fooBarKey = new AspectKey<>(AspectFooBar.class, urn, LATEST_VERSION);
    AspectKey<FooUrn, ? extends RecordTemplate> attKey = new AspectKey<>(AspectAttributes.class, urn, LATEST_VERSION);

    Set<AspectKey<FooUrn, ? extends RecordTemplate>> aspectKeys =
        ImmutableSet.of(fooKey, fooEvolvedKey, barKey, fooBarKey, attKey);
    when(_mockLocalDAO.get(aspectKeys)).thenReturn(
        ImmutableMap.of(fooKey, Optional.of(foo), fooEvolvedKey, Optional.of(fooEvolved), barKey, Optional.of(bar),
            fooBarKey, Optional.of(fooBar), attKey, Optional.of(attributes)));

    EntitySnapshot snapshot = runAndWait(_internalResource.getSnapshot(urn.toString(), null, true));

    assertEquals(snapshot.getUrn(), urn);

    Set<RecordTemplate> aspects =
        snapshot.getAspects().stream().map(RecordUtils::getSelectedRecordTemplateFromUnion).collect(Collectors.toSet());
    assertEquals(aspects, ImmutableSet.of(foo, bar, fooBar, attributes));
  }

  @Test
  public void testGetSnapshotWithInvalidUrn() {
    try {
      runAndWait(_resource.getSnapshot("invalid urn", new String[]{ModelUtils.getAspectName(AspectFoo.class)}));
    } catch (RestLiServiceException e) {
      assertEquals(e.getStatus(), HttpStatus.S_400_BAD_REQUEST);
      return;
    }

    fail("No exception thrown");
  }

  @Test
  public void testBackfillOneAspect() {
    FooUrn urn = makeFooUrn(1);
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
    FooUrn urn = makeFooUrn(1);
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
      return;
    }

    fail("No exception thrown");
  }

  @Test
  public void testBatchBackfill() {
    FooUrn urn1 = makeFooUrn(1);
    FooUrn urn2 = makeFooUrn(2);
    AspectFoo foo1 = new AspectFoo().setValue("foo1");
    AspectBar bar1 = new AspectBar().setValue("bar1");
    AspectBar bar2 = new AspectBar().setValue("bar2");
    String[] aspects = new String[]{"com.linkedin.testing.AspectFoo", "com.linkedin.testing.AspectBar"};
    when(_mockLocalDAO.backfill(_resource.parseAspectsParam(aspects, false), ImmutableSet.of(urn1, urn2))).thenReturn(
        ImmutableMap.of(urn1, ImmutableMap.of(AspectFoo.class, Optional.of(foo1), AspectBar.class, Optional.of(bar1)),
            urn2, ImmutableMap.of(AspectBar.class, Optional.of(bar2))));

    BackfillResult backfillResult =
        runAndWait(_resource.backfill(new String[]{urn1.toString(), urn2.toString()}, aspects));
    assertEquals(backfillResult.getEntities().size(), 2);

    // Test first entity
    BackfillResultEntity backfillResultEntity = backfillResult.getEntities().get(0);
    assertEquals(backfillResultEntity.getUrn(), urn1);
    assertEquals(backfillResultEntity.getAspects().size(), 2);
    assertTrue(backfillResultEntity.getAspects().contains("com.linkedin.testing.AspectFoo"));
    assertTrue(backfillResultEntity.getAspects().contains("com.linkedin.testing.AspectBar"));

    // Test second entity
    backfillResultEntity = backfillResult.getEntities().get(1);
    assertEquals(backfillResultEntity.getUrn(), urn2);
    assertEquals(backfillResultEntity.getAspects().size(), 1);
    assertTrue(backfillResultEntity.getAspects().contains("com.linkedin.testing.AspectBar"));
  }

  @Test
  public void testBackfillUsingSCSI() {
    FooUrn urn1 = makeFooUrn(1);
    FooUrn urn2 = makeFooUrn(2);
    AspectFoo foo1 = new AspectFoo().setValue("foo1");
    AspectBar bar1 = new AspectBar().setValue("bar1");
    AspectBar bar2 = new AspectBar().setValue("bar2");
    String[] aspects = new String[]{"com.linkedin.testing.AspectFoo", "com.linkedin.testing.AspectBar"};
    when(
        _mockLocalDAO.backfill(BackfillMode.BACKFILL_ALL, _resource.parseAspectsParam(aspects, false), FooUrn.class, null, 10))
        .thenReturn(ImmutableMap.of(urn1,
            ImmutableMap.of(AspectFoo.class, Optional.of(foo1), AspectBar.class, Optional.of(bar1)), urn2,
            ImmutableMap.of(AspectBar.class, Optional.of(bar2))));

    BackfillResult backfillResult = runAndWait(_resource.backfill(BackfillMode.BACKFILL_ALL, aspects, null, 10));
    assertEquals(backfillResult.getEntities().size(), 2);

    // Test first entity
    BackfillResultEntity backfillResultEntity = backfillResult.getEntities().get(0);
    assertEquals(backfillResultEntity.getUrn(), urn1);
    assertEquals(backfillResultEntity.getAspects().size(), 2);
    assertTrue(backfillResultEntity.getAspects().contains("com.linkedin.testing.AspectFoo"));
    assertTrue(backfillResultEntity.getAspects().contains("com.linkedin.testing.AspectBar"));

    // Test second entity
    backfillResultEntity = backfillResult.getEntities().get(1);
    assertEquals(backfillResultEntity.getUrn(), urn2);
    assertEquals(backfillResultEntity.getAspects().size(), 1);
    assertTrue(backfillResultEntity.getAspects().contains("com.linkedin.testing.AspectBar"));
  }

  @Test
  public void testBackfillWithNewValue() {
    FooUrn urn1 = makeFooUrn(1);
    FooUrn urn2 = makeFooUrn(2);
    AspectFoo foo1 = new AspectFoo().setValue("foo1");
    AspectBar bar1 = new AspectBar().setValue("bar1");
    AspectBar bar2 = new AspectBar().setValue("bar2");
    String[] aspects = new String[]{"com.linkedin.testing.AspectFoo", "com.linkedin.testing.AspectBar"};
    when(_mockLocalDAO.backfillWithNewValue(_resource.parseAspectsParam(aspects, false), ImmutableSet.of(urn1, urn2)))
        .thenReturn(
            ImmutableMap.of(urn1, ImmutableMap.of(AspectFoo.class, Optional.of(foo1), AspectBar.class, Optional.of(bar1)),
            urn2, ImmutableMap.of(AspectBar.class, Optional.of(bar2)))
        );

    BackfillResult backfillResult =
        runAndWait(_resource.backfillWithNewValue(new String[]{urn1.toString(), urn2.toString()}, aspects));
    assertEquals(backfillResult.getEntities().size(), 2);

    // Test first entity
    BackfillResultEntity backfillResultEntity = backfillResult.getEntities().get(0);
    assertEquals(backfillResultEntity.getUrn(), urn1);
    assertEquals(backfillResultEntity.getAspects().size(), 2);
    assertTrue(backfillResultEntity.getAspects().contains("com.linkedin.testing.AspectFoo"));
    assertTrue(backfillResultEntity.getAspects().contains("com.linkedin.testing.AspectBar"));

    // Test second entity
    backfillResultEntity = backfillResult.getEntities().get(1);
    assertEquals(backfillResultEntity.getUrn(), urn2);
    assertEquals(backfillResultEntity.getAspects().size(), 1);
    assertTrue(backfillResultEntity.getAspects().contains("com.linkedin.testing.AspectBar"));
  }

  @Test
  public void testEmitNoChangeMetadataAuditEvent() {
    FooUrn urn1 = makeFooUrn(1);
    FooUrn urn2 = makeFooUrn(2);
    AspectFoo foo1 = new AspectFoo().setValue("foo1");
    AspectBar bar1 = new AspectBar().setValue("bar1");
    AspectBar bar2 = new AspectBar().setValue("bar2");
    String[] aspects = new String[]{"com.linkedin.testing.AspectFoo", "com.linkedin.testing.AspectBar"};
    when(_mockLocalDAO.backfill(BackfillMode.BACKFILL_INCLUDING_LIVE_INDEX, _resource.parseAspectsParam(aspects, false), ImmutableSet.of(urn1, urn2)))
        .thenReturn(
            ImmutableMap.of(urn1, ImmutableMap.of(AspectFoo.class, Optional.of(foo1), AspectBar.class, Optional.of(bar1)),
                urn2, ImmutableMap.of(AspectBar.class, Optional.of(bar2)))
        );

    BackfillResult backfillResult =
        runAndWait(_resource.emitNoChangeMetadataAuditEvent(new String[]{urn1.toString(), urn2.toString()}, aspects,
            IngestionMode.BACKFILL));
    assertEquals(backfillResult.getEntities().size(), 2);

    // Test first entity
    BackfillResultEntity backfillResultEntity = backfillResult.getEntities().get(0);
    assertEquals(backfillResultEntity.getUrn(), urn1);
    assertEquals(backfillResultEntity.getAspects().size(), 2);
    assertTrue(backfillResultEntity.getAspects().contains("com.linkedin.testing.AspectFoo"));
    assertTrue(backfillResultEntity.getAspects().contains("com.linkedin.testing.AspectBar"));

    // Test second entity
    backfillResultEntity = backfillResult.getEntities().get(1);
    assertEquals(backfillResultEntity.getUrn(), urn2);
    assertEquals(backfillResultEntity.getAspects().size(), 1);
    assertTrue(backfillResultEntity.getAspects().contains("com.linkedin.testing.AspectBar"));
  }

  @Test
  public void testEmitNoChangeMetadataAuditEventBootstrap() {
    FooUrn urn1 = makeFooUrn(1);
    FooUrn urn2 = makeFooUrn(2);
    AspectFoo foo1 = new AspectFoo().setValue("foo1");
    AspectBar bar1 = new AspectBar().setValue("bar1");
    AspectBar bar2 = new AspectBar().setValue("bar2");
    String[] aspects = new String[]{"com.linkedin.testing.AspectFoo", "com.linkedin.testing.AspectBar"};
    when(_mockLocalDAO.backfill(BackfillMode.BACKFILL_ALL, _resource.parseAspectsParam(aspects, false), ImmutableSet.of(urn1, urn2)))
        .thenReturn(
            ImmutableMap.of(urn1, ImmutableMap.of(AspectFoo.class, Optional.of(foo1), AspectBar.class, Optional.of(bar1)),
                urn2, ImmutableMap.of(AspectBar.class, Optional.of(bar2)))
        );

    BackfillResult backfillResult =
        runAndWait(_resource.emitNoChangeMetadataAuditEvent(new String[]{urn1.toString(), urn2.toString()}, aspects,
            IngestionMode.BOOTSTRAP));
    assertEquals(backfillResult.getEntities().size(), 2);

    // Test first entity
    BackfillResultEntity backfillResultEntity = backfillResult.getEntities().get(0);
    assertEquals(backfillResultEntity.getUrn(), urn1);
    assertEquals(backfillResultEntity.getAspects().size(), 2);
    assertTrue(backfillResultEntity.getAspects().contains("com.linkedin.testing.AspectFoo"));
    assertTrue(backfillResultEntity.getAspects().contains("com.linkedin.testing.AspectBar"));

    // Test second entity
    backfillResultEntity = backfillResult.getEntities().get(1);
    assertEquals(backfillResultEntity.getUrn(), urn2);
    assertEquals(backfillResultEntity.getAspects().size(), 1);
    assertTrue(backfillResultEntity.getAspects().contains("com.linkedin.testing.AspectBar"));
  }

  @Test
  public void testEmitNoChangeMetadataAuditEventNoResult() {
    FooUrn urn1 = makeFooUrn(1);
    FooUrn urn2 = makeFooUrn(2);
    AspectFoo foo1 = new AspectFoo().setValue("foo1");
    AspectBar bar1 = new AspectBar().setValue("bar1");
    AspectBar bar2 = new AspectBar().setValue("bar2");
    String[] aspects = new String[]{"com.linkedin.testing.AspectFoo", "com.linkedin.testing.AspectBar"};

    BackfillResult backfillResult =
        runAndWait(_resource.emitNoChangeMetadataAuditEvent(new String[]{urn1.toString(), urn2.toString()}, aspects,
            IngestionMode.LIVE));
    verify(_mockLocalDAO, times(0)).backfill(any(BackfillMode.class), any(Set.class), any(Set.class));
    assertFalse(backfillResult.hasEntities());
  }

  @Test
  public void testBackfillRelationshipTables() {
    FooUrn fooUrn = makeFooUrn(1);
    BarUrn barUrn = makeBarUrn(1);

    String[] aspects = new String[]{"com.linkedin.testing.AspectFoo"};
    BelongsTo belongsTo = new BelongsTo().setSource(fooUrn).setDestination(barUrn);
    List<BelongsTo> belongsTos = Collections.singletonList(belongsTo);

    LocalRelationshipUpdates updates = new LocalRelationshipUpdates(belongsTos, BelongsTo.class,
        BaseGraphWriterDAO.RemovalOption.REMOVE_ALL_EDGES_FROM_SOURCE);
    List<LocalRelationshipUpdates> relationships = Collections.singletonList(updates);

    when(_mockLocalDAO.backfillLocalRelationships(fooUrn, AspectFoo.class)).thenReturn(relationships);
    BackfillResult backfillResult = runAndWait(_resource.backfillRelationshipTables(new String[]{fooUrn.toString()}, aspects));

    assertTrue(backfillResult.hasRelationships());
    assertEquals(backfillResult.getRelationships().size(), 1);
    assertEquals(backfillResult.getRelationships().get(0).getDestination().toString(), "urn:li:bar:1");
    assertEquals(backfillResult.getRelationships().get(0).getSource().toString(), "urn:li:foo:1");
    assertEquals(backfillResult.getRelationships().get(0).getRelationship(), "BelongsTo");
    assertEquals(backfillResult.getRelationships().get(0).getRemovalOption(), "REMOVE_ALL_EDGES_FROM_SOURCE");
  }

  @Test
  public void testListUrnsFromIndex() {
    // case 1: indexFilter is non-null
    IndexCriterion indexCriterion1 = new IndexCriterion().setAspect("aspect1");
    IndexFilter indexFilter1 = new IndexFilter().setCriteria(new IndexCriterionArray(indexCriterion1));
    FooUrn urn1 = makeFooUrn(1);
    FooUrn urn2 = makeFooUrn(2);
    FooUrn urn3 = makeFooUrn(3);
    List<FooUrn> urns1 = Arrays.asList(urn2, urn3);

    when(_mockLocalDAO.listUrns(indexFilter1, urn1, 2)).thenReturn(urns1);
    String[] actual = runAndWait(_resource.listUrnsFromIndex(indexFilter1, urn1.toString(), 2));
    assertEquals(actual, new String[]{urn2.toString(), urn3.toString()});

    // case 2: indexFilter is null
    IndexCriterion indexCriterion2 = new IndexCriterion().setAspect(FooUrn.class.getCanonicalName());
    IndexFilter indexFilter2 = new IndexFilter().setCriteria(new IndexCriterionArray(indexCriterion2));
    when(_mockLocalDAO.listUrns(indexFilter2, urn1, 2)).thenReturn(urns1);
    actual = runAndWait(_resource.listUrnsFromIndex(indexFilter2, urn1.toString(), 2));
    assertEquals(actual, new String[]{urn2.toString(), urn3.toString()});

    // case 3: lastUrn is null
    List<FooUrn> urns3 = Arrays.asList(urn1, urn2);
    when(_mockLocalDAO.listUrns(indexFilter2, null, 2)).thenReturn(urns3);
    actual = runAndWait(_resource.listUrnsFromIndex(indexFilter2, null, 2));
    assertEquals(actual, new String[]{urn1.toString(), urn2.toString()});
  }

  @Test
  public void testFilterFromIndexEmptyAspects() {
    // case 1: indexFilter is non-null
    IndexCriterion indexCriterion1 = new IndexCriterion().setAspect("aspect1");
    IndexFilter indexFilter1 = new IndexFilter().setCriteria(new IndexCriterionArray(indexCriterion1));
    FooUrn urn1 = makeFooUrn(1);
    FooUrn urn2 = makeFooUrn(2);
    FooUrn urn3 = makeFooUrn(3);

    List<FooUrn> urns1 = Arrays.asList(urn2, urn3);

    when(_mockLocalDAO.listUrns(indexFilter1, null, urn1, 2)).thenReturn(urns1);
    List<EntityValue> actual =
        runAndWait(_resource.filter(indexFilter1, new String[0], urn1.toString(), new PagingContext(1, 2)));

    assertEquals(actual.size(), 2);
    assertEquals(actual.get(0), new EntityValue());
    assertEquals(actual.get(1), new EntityValue());

    // case 2: lastUrn is null
    List<FooUrn> urns2 = Arrays.asList(urn1, urn2);
    IndexCriterion indexCriterion2 = new IndexCriterion().setAspect(FooUrn.class.getCanonicalName());
    IndexFilter indexFilter2 = new IndexFilter().setCriteria(new IndexCriterionArray(indexCriterion2));
    when(_mockLocalDAO.listUrns(null, null, null, 2)).thenReturn(urns2);
    actual = runAndWait(_resource.filter(null, new String[0], null, new PagingContext(0, 2)));
    assertEquals(actual.size(), 2);
    assertEquals(actual.get(0), new EntityValue());
    assertEquals(actual.get(1), new EntityValue());

    // case 3: sortCriterion is not null
    List<FooUrn> urns3 = Arrays.asList(urn3, urn2);
    IndexSortCriterion indexSortCriterion = new IndexSortCriterion().setAspect("aspect1").setPath("/id")
        .setOrder(SortOrder.DESCENDING);
    when(_mockLocalDAO.listUrns(null, indexSortCriterion, null, 2)).thenReturn(urns3);
    actual = runAndWait(_resource.filter(null, indexSortCriterion, new String[0], null, 2));
    assertEquals(actual.size(), 2);
    assertEquals(actual.get(0), new EntityValue());
    assertEquals(actual.get(1), new EntityValue());

    // case 4: offset pagination
    ListResult<FooUrn> urnsListResult = ListResult.<FooUrn>builder()
        .values(urns3)
        .metadata(null)
        .nextStart(ListResult.INVALID_NEXT_START)
        .havingMore(false)
        .totalCount(2)
        .totalPageCount(1)
        .pageSize(2)
        .build();
    when(_mockLocalDAO.listUrns(null, indexSortCriterion, 0, 2)).thenReturn(urnsListResult);
    ListResult<EntityValue>
        listResultActual = runAndWait(_resource.filter(null, indexSortCriterion, new String[0], new PagingContext(0, 2)));
    List<EntityValue> actualValues = listResultActual.getValues();
    assertEquals(actualValues.size(), 2);
    assertEquals(actualValues.get(0), new EntityValue());
    assertEquals(actualValues.get(1), new EntityValue());
    assertEquals(listResultActual.getNextStart(), urnsListResult.getNextStart());
    assertEquals(listResultActual.isHavingMore(), urnsListResult.isHavingMore());
    assertEquals(listResultActual.getTotalCount(), urnsListResult.getTotalCount());
    assertEquals(listResultActual.getTotalPageCount(), urnsListResult.getTotalPageCount());
    assertEquals(listResultActual.getPageSize(), urnsListResult.getPageSize());
  }

  @Test
  public void testFilterFromIndexWithAspects() {
    FooUrn urn1 = makeFooUrn(1);
    FooUrn urn2 = makeFooUrn(2);
    AspectFoo foo1 = new AspectFoo().setValue("val1");
    AspectFoo foo2 = new AspectFoo().setValue("val2");
    AspectBar bar1 = new AspectBar().setValue("val1");
    AspectBar bar2 = new AspectBar().setValue("val2");

    UrnAspectEntry<FooUrn> entry1 = new UrnAspectEntry<>(urn1, Arrays.asList(foo1, bar1));
    UrnAspectEntry<FooUrn> entry2 = new UrnAspectEntry<>(urn2, Arrays.asList(foo2, bar2));

    IndexCriterion criterion = new IndexCriterion().setAspect(AspectFoo.class.getCanonicalName());
    IndexCriterionArray criterionArray = new IndexCriterionArray(criterion);
    IndexFilter indexFilter = new IndexFilter().setCriteria(criterionArray);
    IndexSortCriterion indexSortCriterion = new IndexSortCriterion().setAspect(AspectFoo.class.getCanonicalName())
        .setOrder(SortOrder.DESCENDING);
    String[] aspectNames = {ModelUtils.getAspectName(AspectFoo.class), ModelUtils.getAspectName(AspectBar.class)};

    // case 1: aspect list is provided, null last urn
    List<UrnAspectEntry<FooUrn>> listResult1 = Arrays.asList(entry1, entry2);

    when(_mockLocalDAO.getAspects(ImmutableSet.of(AspectFoo.class, AspectBar.class), indexFilter, null, null, 2))
        .thenReturn(listResult1);

    List<EntityValue> actual1 =
        runAndWait(_resource.filter(indexFilter, aspectNames, null, new PagingContext(0, 2)));

    assertEquals(actual1.size(), 2);
    assertEquals(actual1.get(0), new EntityValue().setFoo(foo1).setBar(bar1));
    assertEquals(actual1.get(1), new EntityValue().setFoo(foo2).setBar(bar2));

    // case 2: null aspects is provided i.e. all aspects in the aspect union will be returned, non-null last urn
    List<UrnAspectEntry<FooUrn>> listResult2 = Collections.singletonList(entry2);

    when(_mockLocalDAO.getAspects(ImmutableSet.of(AspectFoo.class, AspectBar.class, AspectFooBar.class, AspectAttributes.class), indexFilter, null, urn1, 2))
        .thenReturn(listResult2);

    List<EntityValue> actual2 =
        runAndWait(_resource.filter(indexFilter, null, urn1.toString(), new PagingContext(0, 2)));
    assertEquals(actual2.size(), 1);
    assertEquals(actual2.get(0), new EntityValue().setFoo(foo2).setBar(bar2));

    // case 3: non-null sort criterion is provided
    List<UrnAspectEntry<FooUrn>> listResult3 = Arrays.asList(entry2, entry1);

    when(_mockLocalDAO.getAspects(ImmutableSet.of(AspectFoo.class, AspectBar.class), indexFilter, indexSortCriterion, null, 2))
        .thenReturn(listResult3);

    List<EntityValue> actual3 =
        runAndWait(_resource.filter(indexFilter, indexSortCriterion, aspectNames, null, 2));

    assertEquals(actual3.size(), 2);
    assertEquals(actual3.get(0), new EntityValue().setFoo(foo2).setBar(bar2));
    assertEquals(actual3.get(1), new EntityValue().setFoo(foo1).setBar(bar1));

    // case 4: offset pagination
    ListResult<UrnAspectEntry<FooUrn>> urnsListResult = ListResult.<UrnAspectEntry<FooUrn>>builder()
        .values(Arrays.asList(entry2, entry1))
        .metadata(null)
        .nextStart(ListResult.INVALID_NEXT_START)
        .havingMore(false)
        .totalCount(2)
        .totalPageCount(1)
        .pageSize(2)
        .build();

    when(_mockLocalDAO.getAspects(ImmutableSet.of(AspectFoo.class, AspectBar.class), indexFilter, indexSortCriterion, 0, 2))
        .thenReturn(urnsListResult);

    ListResult<EntityValue> actual4 =
        runAndWait(_resource.filter(indexFilter, indexSortCriterion, aspectNames, new PagingContext(0, 2)));

    List<EntityValue> actualValues = actual4.getValues();
    assertEquals(actualValues.size(), 2);
    assertEquals(actualValues.get(0), new EntityValue().setFoo(foo2).setBar(bar2));
    assertEquals(actualValues.get(1), new EntityValue().setFoo(foo1).setBar(bar1));
    assertEquals(actual4.getNextStart(), urnsListResult.getNextStart());
    assertEquals(actual4.isHavingMore(), urnsListResult.isHavingMore());
    assertEquals(actual4.getTotalCount(), urnsListResult.getTotalCount());
    assertEquals(actual4.getTotalPageCount(), urnsListResult.getTotalPageCount());
    assertEquals(actual4.getPageSize(), urnsListResult.getPageSize());
  }

  @Test
  public void testInternalModelFilterFromIndexWithAspects() {
    FooUrn urn1 = makeFooUrn(1);
    FooUrn urn2 = makeFooUrn(2);
    AspectFoo foo1 = new AspectFoo().setValue("val1");
    AspectFoo foo2 = new AspectFoo().setValue("val2");
    AspectBar bar1 = new AspectBar().setValue("val1");
    AspectBar bar2 = new AspectBar().setValue("val2");

    UrnAspectEntry<FooUrn> entry1 = new UrnAspectEntry<>(urn1, Arrays.asList(foo1, bar1));
    UrnAspectEntry<FooUrn> entry2 = new UrnAspectEntry<>(urn2, Arrays.asList(foo2, bar2));

    IndexCriterion criterion = new IndexCriterion().setAspect(AspectFoo.class.getCanonicalName());
    IndexCriterionArray criterionArray = new IndexCriterionArray(criterion);
    IndexFilter indexFilter = new IndexFilter().setCriteria(criterionArray);
    IndexSortCriterion indexSortCriterion = new IndexSortCriterion().setAspect(AspectFoo.class.getCanonicalName())
        .setOrder(SortOrder.DESCENDING);
    String[] aspectNames = {ModelUtils.getAspectName(AspectFoo.class), ModelUtils.getAspectName(AspectBar.class)};

    // case 1: aspect list is provided, null last urn
    List<UrnAspectEntry<FooUrn>> listResult1 = Arrays.asList(entry1, entry2);

    when(_mockLocalDAO.getAspects(ImmutableSet.of(AspectFoo.class, AspectBar.class), indexFilter, null, null, 2))
        .thenReturn(listResult1);

    List<EntityValue> actual1 =
        runAndWait(_resource.filter(indexFilter, aspectNames, null, new PagingContext(0, 2)));

    assertEquals(actual1.size(), 2);
    assertEquals(actual1.get(0), new EntityValue().setFoo(foo1).setBar(bar1));
    assertEquals(actual1.get(1), new EntityValue().setFoo(foo2).setBar(bar2));

    // case 2: null aspects is provided i.e. all aspects in the aspect union will be returned, non-null last urn
    List<UrnAspectEntry<FooUrn>> listResult2 = Collections.singletonList(entry2);

    when(_mockLocalDAO.getAspects(
        ImmutableSet.of(AspectFoo.class, AspectBar.class, AspectFooEvolved.class, AspectFooBar.class,
            AspectAttributes.class), indexFilter, null, urn1, 2)).thenReturn(listResult2);

    List<EntityValue> actual2 = runAndWait(
        _internalResource.filter(indexFilter, null, null, urn1.toString(), new PagingContext(0, 2).getCount()));
    assertEquals(actual2.size(), 1);
    assertEquals(actual2.get(0), new EntityValue().setFoo(foo2).setBar(bar2));

    // case 3: non-null sort criterion is provided
    List<UrnAspectEntry<FooUrn>> listResult3 = Arrays.asList(entry2, entry1);

    when(_mockLocalDAO.getAspects(ImmutableSet.of(AspectFoo.class, AspectBar.class), indexFilter, indexSortCriterion, null, 2))
        .thenReturn(listResult3);

    List<EntityValue> actual3 =
        runAndWait(_resource.filter(indexFilter, indexSortCriterion, aspectNames, null, 2));

    assertEquals(actual3.size(), 2);
    assertEquals(actual3.get(0), new EntityValue().setFoo(foo2).setBar(bar2));
    assertEquals(actual3.get(1), new EntityValue().setFoo(foo1).setBar(bar1));

    // case 4: offset pagination
    ListResult<UrnAspectEntry<FooUrn>> urnsListResult = ListResult.<UrnAspectEntry<FooUrn>>builder()
        .values(Arrays.asList(entry2, entry1))
        .metadata(null)
        .nextStart(ListResult.INVALID_NEXT_START)
        .havingMore(false)
        .totalCount(2)
        .totalPageCount(1)
        .pageSize(2)
        .build();

    when(_mockLocalDAO.getAspects(ImmutableSet.of(AspectFoo.class, AspectBar.class), indexFilter, indexSortCriterion, 0, 2))
        .thenReturn(urnsListResult);

    ListResult<EntityValue> actual4 =
        runAndWait(_resource.filter(indexFilter, indexSortCriterion, aspectNames, new PagingContext(0, 2)));

    List<EntityValue> actualValues = actual4.getValues();
    assertEquals(actualValues.size(), 2);
    assertEquals(actualValues.get(0), new EntityValue().setFoo(foo2).setBar(bar2));
    assertEquals(actualValues.get(1), new EntityValue().setFoo(foo1).setBar(bar1));
    assertEquals(actual4.getNextStart(), urnsListResult.getNextStart());
    assertEquals(actual4.isHavingMore(), urnsListResult.isHavingMore());
    assertEquals(actual4.getTotalCount(), urnsListResult.getTotalCount());
    assertEquals(actual4.getTotalPageCount(), urnsListResult.getTotalPageCount());
    assertEquals(actual4.getPageSize(), urnsListResult.getPageSize());
  }

  @Test
  public void testParseAspectsParam() {
    // Only 1 aspect
    Set<Class<? extends RecordTemplate>> aspectClasses =
        _resource.parseAspectsParam(new String[]{AspectFoo.class.getCanonicalName()}, false);
    assertEquals(aspectClasses.size(), 1);
    assertTrue(aspectClasses.contains(AspectFoo.class));

    // No aspect
    aspectClasses = _resource.parseAspectsParam(new String[]{}, false);
    assertEquals(aspectClasses.size(), 0);

    // All aspects
    aspectClasses = _resource.parseAspectsParam(null, false);
    assertEquals(aspectClasses.size(), 4);
    assertTrue(aspectClasses.contains(AspectFoo.class));
    assertTrue(aspectClasses.contains(AspectBar.class));
    assertTrue(aspectClasses.contains(AspectFooBar.class));
    assertTrue(aspectClasses.contains(AspectAttributes.class));
  }

  @Test
  public void testInternalModelParseAspectsParam() {
    // Only 1 aspect
    Set<Class<? extends RecordTemplate>> aspectClasses =
        _resource.parseAspectsParam(new String[]{AspectFoo.class.getCanonicalName()}, true);
    assertEquals(aspectClasses.size(), 1);
    assertTrue(aspectClasses.contains(AspectFoo.class));

    // No aspect
    aspectClasses = _resource.parseAspectsParam(new String[]{}, true);
    assertEquals(aspectClasses.size(), 0);

    // All aspects
    aspectClasses = _resource.parseAspectsParam(null, true);
    assertEquals(aspectClasses.size(), 5);
    assertTrue(aspectClasses.contains(AspectFoo.class));
    assertTrue(aspectClasses.contains(AspectFooEvolved.class));
    assertTrue(aspectClasses.contains(AspectBar.class));
    assertTrue(aspectClasses.contains(AspectFooBar.class));
    assertTrue(aspectClasses.contains(AspectAttributes.class));
  }

  @Test
  public void testCountAggregate() {
    FooUrn urn1 = makeFooUrn(1);
    FooUrn urn2 = makeFooUrn(2);
    AspectFoo foo1 = new AspectFoo().setValue("val1");
    AspectFoo foo2 = new AspectFoo().setValue("val2");
    AspectBar bar1 = new AspectBar().setValue("val1");
    AspectBar bar2 = new AspectBar().setValue("val2");

    UrnAspectEntry<FooUrn> entry1 = new UrnAspectEntry<>(urn1, Arrays.asList(foo1, bar1));
    UrnAspectEntry<FooUrn> entry2 = new UrnAspectEntry<>(urn2, Arrays.asList(foo2, bar2));

    IndexCriterion criterion = new IndexCriterion().setAspect(AspectFoo.class.getCanonicalName());
    IndexCriterionArray criterionArray = new IndexCriterionArray(criterion);
    IndexFilter indexFilter = new IndexFilter().setCriteria(criterionArray);
    IndexGroupByCriterion indexGroupByCriterion = new IndexGroupByCriterion().setAspect(AspectFoo.class.getCanonicalName())
        .setPath("/value");
    Map<String, Long> mapResult = new HashMap<>();
    mapResult.put("val1", 1L);
    mapResult.put("val2", 1L);

    when(_mockLocalDAO.countAggregate(indexFilter, indexGroupByCriterion)).thenReturn(mapResult);
    Map<String, Long> actual =
        runAndWait(_resource.countAggregate(indexFilter, indexGroupByCriterion));

    assertEquals(actual, mapResult);
  }

  @Test
  public void testCountAggregateFilter() {
    FooUrn urn1 = makeFooUrn(1);
    FooUrn urn2 = makeFooUrn(2);
    AspectFoo foo1 = new AspectFoo().setValue("val1");
    AspectFoo foo2 = new AspectFoo().setValue("val2");
    AspectBar bar1 = new AspectBar().setValue("val1");
    AspectBar bar2 = new AspectBar().setValue("val2");

    UrnAspectEntry<FooUrn> entry1 = new UrnAspectEntry<>(urn1, Arrays.asList(foo1, bar1));
    UrnAspectEntry<FooUrn> entry2 = new UrnAspectEntry<>(urn2, Arrays.asList(foo2, bar2));

    IndexCriterion criterion = new IndexCriterion().setAspect(AspectFoo.class.getCanonicalName());
    IndexCriterionArray criterionArray = new IndexCriterionArray(criterion);
    IndexFilter indexFilter = new IndexFilter().setCriteria(criterionArray);
    IndexGroupByCriterion indexGroupByCriterion = new IndexGroupByCriterion().setAspect(AspectFoo.class.getCanonicalName())
        .setPath("/value");
    Map<String, Long> mapResult = new HashMap<>();
    mapResult.put("val1", 1L);
    mapResult.put("val2", 1L);

    when(_mockLocalDAO.countAggregate(indexFilter, indexGroupByCriterion)).thenReturn(mapResult);
    CollectionResult<EmptyRecord, MapMetadata> actual =
        runAndWait(_resource.countAggregateFilter(indexFilter, indexGroupByCriterion));

    assertEquals(actual.getMetadata().getLongMap(), new LongMap(mapResult));
  }

  @Test
  public void testIngestAsset() {
    FooUrn urn = makeFooUrn(1);
    EntityAsset asset = new EntityAsset();
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectBar bar = new AspectBar().setValue("bar");
    asset.setUrn(urn);
    asset.setAspectFoo(foo);
    asset.setAspectBar(bar);
    IngestionTrackingContext trackingContext = new IngestionTrackingContext();

    runAndWait(_resource.ingestAsset(asset, trackingContext, null));

    verify(_mockLocalDAO, times(1)).add(eq(urn), eq(foo), any(), eq(trackingContext), eq(null));
    verify(_mockLocalDAO, times(1)).add(eq(urn), eq(bar), any(), eq(trackingContext), eq(null));

    IngestionParams ingestionParams = new IngestionParams().setIngestionMode(IngestionMode.LIVE);
    runAndWait(_resource.ingestAsset(asset, trackingContext, ingestionParams));

    verify(_mockLocalDAO, times(1)).add(eq(urn), eq(foo), any(), eq(trackingContext), eq(ingestionParams));
    verify(_mockLocalDAO, times(1)).add(eq(urn), eq(bar), any(), eq(trackingContext), eq(ingestionParams));
    verifyNoMoreInteractions(_mockLocalDAO);
  }

  @Test
  public void testGetAsset() {
    FooUrn urn = makeFooUrn(1);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectBar bar = new AspectBar().setValue("bar");
    AspectFooEvolved fooEvolved = new AspectFooEvolved().setValue("fooEvolved");
    AspectFooBar fooBar = new AspectFooBar().setBars(new BarUrnArray(new BarUrn(1)));
    AspectAttributes attributes = new AspectAttributes().setAttributes(new StringArray("a"));

    AspectKey<FooUrn, ? extends RecordTemplate> fooKey = new AspectKey<>(AspectFoo.class, urn, LATEST_VERSION);
    AspectKey<FooUrn, ? extends RecordTemplate> fooEvolvedKey = new AspectKey<>(AspectFooEvolved.class, urn, LATEST_VERSION);
    AspectKey<FooUrn, ? extends RecordTemplate> barKey = new AspectKey<>(AspectBar.class, urn, LATEST_VERSION);
    AspectKey<FooUrn, ? extends RecordTemplate> fooBarKey = new AspectKey<>(AspectFooBar.class, urn, LATEST_VERSION);
    AspectKey<FooUrn, ? extends RecordTemplate> attKey = new AspectKey<>(AspectAttributes.class, urn, LATEST_VERSION);

    Set<AspectKey<FooUrn, ? extends RecordTemplate>> aspectKeys =
        ImmutableSet.of(fooKey, fooEvolvedKey, barKey, fooBarKey, attKey);
    when(_mockLocalDAO.get(aspectKeys)).thenReturn(
        ImmutableMap.of(fooKey, Optional.of(foo), fooEvolvedKey, Optional.of(fooEvolved), barKey, Optional.of(bar),
            fooBarKey, Optional.of(fooBar), attKey, Optional.of(attributes)));

    EntityAsset asset = runAndWait(_resource.getAsset(urn.toString(), null));

    assertEquals(asset.getUrn(), urn);

    assertEquals(asset.getAspectFoo(), foo);
    assertEquals(asset.getAspectFooEvolved(), fooEvolved);
    assertEquals(asset.getAspectBar(), bar);
    assertEquals(asset.getAspectFooBar(), fooBar);
    assertEquals(asset.getAspectAttributes(), attributes);
  }
}
