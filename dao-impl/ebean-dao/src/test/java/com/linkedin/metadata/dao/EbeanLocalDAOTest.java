package com.linkedin.metadata.dao;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.Urns;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.aspect.AuditedAspect;
import com.linkedin.metadata.backfill.BackfillMode;
import com.linkedin.metadata.dao.EbeanLocalDAO.FindMethodology;
import com.linkedin.metadata.dao.EbeanLocalDAO.SchemaConfig;
import com.linkedin.metadata.dao.EbeanMetadataAspect.PrimaryKey;
import com.linkedin.metadata.dao.builder.BaseLocalRelationshipBuilder;
import com.linkedin.metadata.dao.equality.AlwaysFalseEqualityTester;
import com.linkedin.metadata.dao.equality.DefaultEqualityTester;
import com.linkedin.metadata.dao.exception.InvalidMetadataType;
import com.linkedin.metadata.dao.exception.RetryLimitReached;
import com.linkedin.metadata.dao.localrelationship.SampleLocalRelationshipRegistryImpl;
import com.linkedin.metadata.dao.producer.BaseMetadataEventProducer;
import com.linkedin.metadata.dao.producer.BaseTrackingMetadataEventProducer;
import com.linkedin.metadata.dao.retention.TimeBasedRetention;
import com.linkedin.metadata.dao.retention.VersionBasedRetention;
import com.linkedin.metadata.dao.storage.LocalDAOStorageConfig;
import com.linkedin.metadata.dao.tracking.BaseTrackingManager;
import com.linkedin.metadata.dao.urnpath.UrnPathExtractor;
import com.linkedin.metadata.dao.utils.BarUrnPathExtractor;
import com.linkedin.metadata.dao.utils.EbeanServerUtils;
import com.linkedin.metadata.dao.utils.EmbeddedMariaInstance;
import com.linkedin.metadata.dao.utils.FooUrnPathExtractor;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.dao.utils.SQLSchemaUtils;
import com.linkedin.metadata.events.IngestionMode;
import com.linkedin.metadata.events.IngestionTrackingContext;
import com.linkedin.metadata.internal.IngestionParams;
import com.linkedin.metadata.query.Condition;
import com.linkedin.metadata.query.ExtraInfo;
import com.linkedin.metadata.query.IndexCriterion;
import com.linkedin.metadata.query.IndexCriterionArray;
import com.linkedin.metadata.query.IndexFilter;
import com.linkedin.metadata.query.IndexGroupByCriterion;
import com.linkedin.metadata.query.IndexPathParams;
import com.linkedin.metadata.query.IndexSortCriterion;
import com.linkedin.metadata.query.IndexValue;
import com.linkedin.metadata.query.ListResultMetadata;
import com.linkedin.metadata.query.LocalRelationshipCriterionArray;
import com.linkedin.metadata.query.LocalRelationshipFilter;
import com.linkedin.metadata.query.RelationshipDirection;
import com.linkedin.metadata.query.SortOrder;
import com.linkedin.testing.AspectAttributes;
import com.linkedin.testing.AspectBar;
import com.linkedin.testing.AspectBaz;
import com.linkedin.testing.AspectFoo;
import com.linkedin.testing.AspectInvalid;
import com.linkedin.testing.BarSnapshot;
import com.linkedin.testing.BarUrnArray;
import com.linkedin.testing.EntityAspectUnion;
import com.linkedin.testing.FooSnapshot;
import com.linkedin.testing.MixedRecord;
import com.linkedin.testing.localrelationship.AspectFooBar;
import com.linkedin.testing.localrelationship.AspectFooBaz;
import com.linkedin.testing.localrelationship.BelongsTo;
import com.linkedin.testing.localrelationship.BelongsToV2;
import com.linkedin.testing.localrelationship.BelongsToV2Array;
import com.linkedin.testing.localrelationship.ReportsTo;
import com.linkedin.testing.localrelationship.ReportsToArray;
import com.linkedin.testing.urn.BarUrn;
import com.linkedin.testing.urn.BurgerUrn;
import com.linkedin.testing.urn.FooUrn;
import io.ebean.Ebean;
import io.ebean.EbeanServer;
import io.ebean.ExpressionList;
import io.ebean.OrderBy;
import io.ebean.PagedList;
import io.ebean.Query;
import io.ebean.SqlQuery;
import io.ebean.SqlRow;
import io.ebean.Transaction;
import io.ebean.config.ServerConfig;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.persistence.OptimisticLockException;
import javax.persistence.RollbackException;
import org.mockito.ArgumentMatchers;
import org.mockito.InOrder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import static com.linkedin.common.AuditStamps.*;
import static com.linkedin.metadata.dao.internal.BaseGraphWriterDAO.RemovalOption.*;
import static com.linkedin.metadata.dao.utils.EBeanDAOUtils.*;
import static com.linkedin.metadata.dao.utils.ModelUtils.*;
import static com.linkedin.metadata.dao.utils.SQLSchemaUtils.*;
import static com.linkedin.testing.TestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class EbeanLocalDAOTest {
  private long _now;
  private EbeanServer _server;
  private BaseMetadataEventProducer _mockProducer;
  private BaseTrackingMetadataEventProducer _mockTrackingProducer;
  private BaseTrackingManager _mockTrackingManager;
  private AuditStamp _dummyAuditStamp;

  // run the tests 1 time for each of EbeanLocalDAO.SchemaConfig values (3 total)
  private final SchemaConfig _schemaConfig;

  // run the tests 1 time for each of EbeanLocalDAO.FindMethodology values (3 total)
  private final FindMethodology _findMethodology;

  private final boolean _enableChangeLog;

  private static final String NEW_SCHEMA_CREATE_ALL_SQL = "ebean-local-dao-create-all.sql";
  private static final String GMA_CREATE_ALL_SQL = "gma-create-all.sql";
  private static final String GMA_DROP_ALL_SQL = "gma-drop-all.sql";

  private static final String CREATE_ALL_WITH_NON_DOLLAR_VIRTUAL_COLUMN_SQL = "ebean-local-dao-create-all-with-non-dollar-virtual-column-names.sql";
  private final EBeanDAOConfig _eBeanDAOConfig = new EBeanDAOConfig();
  private static final LocalRelationshipFilter
      EMPTY_FILTER = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray());

  private static final LocalRelationshipFilter OUTGOING_FILTER = new LocalRelationshipFilter()
      .setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.OUTGOING);

  @Factory(dataProvider = "inputList")
  public EbeanLocalDAOTest(SchemaConfig schemaConfig, FindMethodology findMethodology, boolean enableChangeLog,
      boolean nonDollarVirtualColumnEnabled) {
    _eBeanDAOConfig.setNonDollarVirtualColumnsEnabled(nonDollarVirtualColumnEnabled);
    _schemaConfig = schemaConfig;
    _findMethodology = findMethodology;
    _enableChangeLog = enableChangeLog;
  }

  @Nonnull
  private String readSQLfromFile(@Nonnull String resourcePath) {
    try {
      return Resources.toString(Resources.getResource(resourcePath), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @DataProvider
  public static Object[][] inputList() {
    return new Object[][]{

        // tests with change history enabled (legacy mode)
        {SchemaConfig.OLD_SCHEMA_ONLY, FindMethodology.UNIQUE_ID, true, true},
        {SchemaConfig.NEW_SCHEMA_ONLY, FindMethodology.UNIQUE_ID, true, true},
        {SchemaConfig.DUAL_SCHEMA, FindMethodology.UNIQUE_ID, true, true},
        {SchemaConfig.OLD_SCHEMA_ONLY, FindMethodology.DIRECT_SQL, true, false},
        {SchemaConfig.NEW_SCHEMA_ONLY, FindMethodology.DIRECT_SQL, true, false},
        {SchemaConfig.DUAL_SCHEMA, FindMethodology.DIRECT_SQL, true, false},

        // tests with change history disabled (cold-archive mode)
        {SchemaConfig.OLD_SCHEMA_ONLY, FindMethodology.UNIQUE_ID, false, true},
        {SchemaConfig.NEW_SCHEMA_ONLY, FindMethodology.UNIQUE_ID, false, true},
        {SchemaConfig.DUAL_SCHEMA, FindMethodology.UNIQUE_ID, false, true},
        {SchemaConfig.OLD_SCHEMA_ONLY, FindMethodology.DIRECT_SQL, false, false},
        {SchemaConfig.NEW_SCHEMA_ONLY, FindMethodology.DIRECT_SQL, false, false},
        {SchemaConfig.DUAL_SCHEMA, FindMethodology.DIRECT_SQL, false, false},

    };
  }

  @BeforeMethod
  public void setupTest() {
    if (_schemaConfig == SchemaConfig.OLD_SCHEMA_ONLY) {
      _server.execute(Ebean.createSqlUpdate(readSQLfromFile(GMA_DROP_ALL_SQL)));
      _server.execute(Ebean.createSqlUpdate(readSQLfromFile(GMA_CREATE_ALL_SQL)));
    } else {
      if (_eBeanDAOConfig.isNonDollarVirtualColumnsEnabled()) {
        _server.execute(Ebean.createSqlUpdate(readSQLfromFile(CREATE_ALL_WITH_NON_DOLLAR_VIRTUAL_COLUMN_SQL)));
      } else {
        _server.execute(Ebean.createSqlUpdate(readSQLfromFile(NEW_SCHEMA_CREATE_ALL_SQL)));
      }
    }
    _mockProducer = mock(BaseMetadataEventProducer.class);
    _mockTrackingProducer = mock(BaseTrackingMetadataEventProducer.class);
    _mockTrackingManager = mock(BaseTrackingManager.class);
    _now = Instant.now().getEpochSecond() * 1000;
    _dummyAuditStamp = makeAuditStamp("foo", _now);
  }

  @BeforeClass
  public void setupServer() {
    _server = EmbeddedMariaInstance.getServer(EbeanLocalDAOTest.class.getSimpleName());
  }

  @Nonnull
  private <URN extends Urn> EbeanLocalDAO<EntityAspectUnion, URN> createDao(@Nonnull EbeanServer server,
      @Nonnull Class<URN> urnClass) {
    EbeanLocalDAO<EntityAspectUnion, URN> dao = new EbeanLocalDAO<>(EntityAspectUnion.class, _mockProducer, server,
        EmbeddedMariaInstance.SERVER_CONFIG_MAP.get(_server.getName()), urnClass, _schemaConfig, _findMethodology, _eBeanDAOConfig);
    // Since we added a_urn columns to both metadata_entity_foo and metadata_entity_bar tables in the SQL initialization scripts,
    // it is required that we set non-default UrnPathExtractors for the corresponding DAOs when initialized.
    if (urnClass == FooUrn.class) {
      dao.setUrnPathExtractor((UrnPathExtractor<URN>) new FooUrnPathExtractor());
    }
    if (urnClass == BarUrn.class) {
      dao.setUrnPathExtractor((UrnPathExtractor<URN>) new BarUrnPathExtractor());
    }
    dao.setEmitAuditEvent(true);
    dao.setChangeLogEnabled(_enableChangeLog);
    return dao;
  }

  @Nonnull
  private <URN extends Urn> EbeanLocalDAO<EntityAspectUnion, URN> createDao(@Nonnull Class<URN> urnClass) {
    return createDao(_server, urnClass);
  }

  @Test
  public void testPublicConstructorsWithTracking() {
    if (_schemaConfig == SchemaConfig.OLD_SCHEMA_ONLY) {
      ServerConfig serverConfig = mock(ServerConfig.class);
      try (MockedStatic<EbeanServerUtils> utils = Mockito.mockStatic(EbeanServerUtils.class)) {
        utils.when(() -> EbeanServerUtils.createServer(serverConfig)).thenReturn(_server);
        testPublicConstructorsWithTrackingHelper(serverConfig);
      }
    } else {
      ServerConfig serverConfig = EmbeddedMariaInstance.SERVER_CONFIG_MAP.get(_server.getName());
      testPublicConstructorsWithTrackingHelper(serverConfig);
    }
  }

  private void testPublicConstructorsWithTrackingHelper(ServerConfig serverConfig) {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao1 = new EbeanLocalDAO<>(EntityAspectUnion.class, _mockTrackingProducer,
        serverConfig, FooUrn.class, _mockTrackingManager);
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao2 = new EbeanLocalDAO<>(EntityAspectUnion.class, _mockTrackingProducer,
        serverConfig, FooUrn.class, _schemaConfig, _mockTrackingManager);
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao3 = new EbeanLocalDAO<>(_mockTrackingProducer,
        serverConfig, makeLocalDAOStorageConfig(AspectFoo.class,
        Collections.singletonList("/value")), FooUrn.class, new FooUrnPathExtractor(), _mockTrackingManager);
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao4 = new EbeanLocalDAO<>(_mockTrackingProducer,
        serverConfig, makeLocalDAOStorageConfig(AspectFoo.class,
        Collections.singletonList("/value")), FooUrn.class, new FooUrnPathExtractor(), _schemaConfig, _mockTrackingManager);
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao5 = new EbeanLocalDAO<>(_mockTrackingProducer,
        serverConfig, makeLocalDAOStorageConfig(AspectFoo.class,
        Collections.singletonList("/value")), FooUrn.class, _mockTrackingManager);
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao6 = new EbeanLocalDAO<>(_mockTrackingProducer,
        serverConfig, makeLocalDAOStorageConfig(AspectFoo.class,
        Collections.singletonList("/value")), FooUrn.class, _schemaConfig, _mockTrackingManager);

    assertNotNull(dao1._trackingManager);
    assertNull(dao1._producer);
    assertNotNull(dao1._trackingProducer);

    assertNotNull(dao2._trackingManager);
    assertNull(dao2._producer);
    assertNotNull(dao2._trackingProducer);

    assertNotNull(dao3._trackingManager);
    assertNull(dao3._producer);
    assertNotNull(dao3._trackingProducer);

    assertNotNull(dao4._trackingManager);
    assertNull(dao4._producer);
    assertNotNull(dao4._trackingProducer);

    assertNotNull(dao5._trackingManager);
    assertNull(dao5._producer);
    assertNotNull(dao5._trackingProducer);

    assertNotNull(dao6._trackingManager);
    assertNull(dao6._producer);
    assertNotNull(dao6._trackingProducer);
  }

  @Test(expectedExceptions = InvalidMetadataType.class)
  public void testMetadataAspectCheck() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);

    dao.add(makeFooUrn(1), new AspectInvalid().setValue("invalid"), _dummyAuditStamp);
  }

  @Test
  public void testAddOne() {
    Clock mockClock = mock(Clock.class);
    when(mockClock.millis()).thenReturn(_now);
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    dao.setClock(mockClock);
    FooUrn urn = makeFooUrn(1);
    String aspectName = ModelUtils.getAspectName(AspectFoo.class);
    AspectFoo expected = new AspectFoo().setValue("foo");
    Urn actor = Urns.createFromTypeSpecificString("test", "actor");
    Urn impersonator = Urns.createFromTypeSpecificString("test", "impersonator");

    dao.add(urn, expected, makeAuditStamp(actor, impersonator, _now));

    EbeanMetadataAspect aspect = getMetadata(urn, aspectName, 0);

    assertNotNull(aspect);
    assertEquals(aspect.getKey().getUrn(), urn.toString());
    assertEquals(aspect.getKey().getAspect(), aspectName);
    assertEquals(aspect.getKey().getVersion(), 0);
    assertEquals(aspect.getCreatedOn(), new Timestamp(_now));
    assertEquals(aspect.getCreatedBy(), "urn:li:test:actor");
    if (_schemaConfig != SchemaConfig.NEW_SCHEMA_ONLY) {
      // didn't even implement this in the new schema since the createdfor column is not being read by anyone. so skipping this check.
      assertEquals(aspect.getCreatedFor(), "urn:li:test:impersonator");
    }

    AspectFoo actual = RecordUtils.toRecordTemplate(AspectFoo.class, aspect.getMetadata());
    assertEquals(actual, expected);

    verify(_mockProducer, times(1)).produceMetadataAuditEvent(urn, null, expected);
    verifyNoMoreInteractions(_mockProducer);
  }

  @Test
  public void testAddOneInTestMode() {
    if (_schemaConfig == SchemaConfig.NEW_SCHEMA_ONLY && !_enableChangeLog) {
      EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
      FooUrn urn = makeFooUrn(1);
      AspectFoo foo = new AspectFoo().setValue("foo");
      IngestionParams ingestionParams = new IngestionParams().setTestMode(true);
      dao.setAlwaysEmitAuditEvent(false);
      dao.setAlwaysEmitAspectSpecificAuditEvent(false);

      dao.add(urn, foo, _dummyAuditStamp, null, ingestionParams);

      // no MAE should be emitted in test mode
      verifyNoMoreInteractions(_mockProducer);

      BaseLocalDAO.AspectEntry<AspectFoo> aspectEntry = dao.getLatest(urn, AspectFoo.class, true);
      assertEquals(aspectEntry.getAspect().getValue(), "foo");
    }
  }

  @Test
  public void testAddTwo() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    FooUrn urn = makeFooUrn(1);
    String aspectName = ModelUtils.getAspectName(AspectFoo.class);
    AspectFoo v1 = new AspectFoo().setValue("foo");
    AspectFoo v0 = new AspectFoo().setValue("bar");

    dao.add(urn, v1, _dummyAuditStamp);
    dao.add(urn, v0, _dummyAuditStamp);

    EbeanMetadataAspect aspect = getMetadata(urn, aspectName, 0);
    AspectFoo actual = RecordUtils.toRecordTemplate(AspectFoo.class, aspect.getMetadata());
    assertEquals(actual, v0);

    if (dao.isChangeLogEnabled()) {
      aspect = getMetadata(urn, aspectName, 1);
      actual = RecordUtils.toRecordTemplate(AspectFoo.class, aspect.getMetadata());
      assertEquals(actual, v1);
    }

    InOrder inOrder = inOrder(_mockProducer);
    inOrder.verify(_mockProducer, times(1)).produceMetadataAuditEvent(urn, null, v1);
    inOrder.verify(_mockProducer, times(1)).produceMetadataAuditEvent(urn, v1, v0);
    verifyNoMoreInteractions(_mockProducer);
  }

  @Test
  public void testAddTwoInTestMode() throws URISyntaxException {
    if (_schemaConfig == SchemaConfig.NEW_SCHEMA_ONLY && !_enableChangeLog) {
      EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
      FooUrn urn = makeFooUrn(1);
      AspectFoo foo = new AspectFoo().setValue("foo");
      AspectBar bar = new AspectBar().setValue("bar");
      IngestionParams ingestionParams = new IngestionParams().setTestMode(true);
      dao.setAlwaysEmitAuditEvent(false);
      dao.setAlwaysEmitAspectSpecificAuditEvent(false);

      dao.add(urn, foo, _dummyAuditStamp, null, ingestionParams);
      dao.add(urn, bar, _dummyAuditStamp, null, ingestionParams);

      // no MAE should be emitted in test mode
      verifyNoMoreInteractions(_mockProducer);

      BaseLocalDAO.AspectEntry<AspectFoo> aspectFooEntry = dao.getLatest(urn, AspectFoo.class, true);
      assertEquals(aspectFooEntry.getAspect().getValue(), "foo");
      BaseLocalDAO.AspectEntry<AspectBar> aspectBarEntry = dao.getLatest(urn, AspectBar.class, true);
      assertEquals(aspectBarEntry.getAspect().getValue(), "bar");
    }
  }

  @Test
  public void testAddWithIngestionAnnotation() throws URISyntaxException {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    FooUrn urn = makeFooUrn(1);
    AspectFoo foo = new AspectFoo().setValue("foo");

    IngestionParams ingestionParams = new IngestionParams().setIngestionMode(IngestionMode.LIVE);
    long t1 = 1704067200000L; // 2024-01-01 00:00:00.0 GMT
    dao.add(urn, foo, new AuditStamp().setTime(t1).setActor(Urn.createFromString("urn:li:corpuser:tester")), null, ingestionParams);

    long t2 = 1706745600000L; // 2024-02-01 00:00:00.0 GMT
    dao.add(urn, foo, new AuditStamp().setTime(t2).setActor(Urn.createFromString("urn:li:corpuser:tester")), null, ingestionParams);

    // make sure that the update still went through by checking the aspect's lastmodifiedon
    if (_schemaConfig == SchemaConfig.NEW_SCHEMA_ONLY) {
      AspectKey<FooUrn, AspectFoo> aspectKey = new AspectKey<>(AspectFoo.class, urn, 0L);
      long aspectFooLastModifiedOn = dao.getWithExtraInfo(aspectKey).get().getExtraInfo().getAudit().getTime();
      assertEquals(aspectFooLastModifiedOn, t2);
    } else {
      String aspectName = ModelUtils.getAspectName(AspectFoo.class);
      EbeanMetadataAspect aspect = getMetadata(urn, aspectName, 0);
      long time = aspect.getCreatedOn().getTime();
      assertEquals(time, t2);
    }
  }

  @Test
  public void testAddWithIngestionAnnotationWithOneFilter() throws URISyntaxException {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    FooUrn urn = makeFooUrn(2);
    AspectFoo foo = new AspectFoo().setValue("foo");

    IngestionParams ingestionParams = new IngestionParams().setIngestionMode(IngestionMode.LIVE);
    long t1 = 1704067200000L; // 2024-01-01 00:00:00.0 GMT
    dao.add(urn, foo, new AuditStamp().setTime(t1).setActor(Urn.createFromString("urn:li:corpuser:tester")), null, ingestionParams);

    long t2 = 1706745600000L; // 2024-02-01 00:00:00.0 GMT
    dao.add(urn, foo, new AuditStamp().setTime(t2).setActor(Urn.createFromString("urn:li:corpuser:tester")), null, ingestionParams);

    // Even though the aspect is annotated with FORCE_UPDATE annotation, the filter does not match so the update is not persisted.
    if (_schemaConfig == SchemaConfig.NEW_SCHEMA_ONLY) {
      AspectKey<FooUrn, AspectFoo> aspectKey = new AspectKey<>(AspectFoo.class, urn, 0L);
      long aspectFooLastModifiedOn = dao.getWithExtraInfo(aspectKey).get().getExtraInfo().getAudit().getTime();
      assertEquals(aspectFooLastModifiedOn, t1);
    } else {
      String aspectName = ModelUtils.getAspectName(AspectFoo.class);
      EbeanMetadataAspect aspect = getMetadata(urn, aspectName, 0);
      long time = aspect.getCreatedOn().getTime();
      // update not persisted, timestamp should still be t1.
      assertEquals(time, t1);
    }
  }

  @Test
  public void testAddWithIngestionAnnotationWithMultipleFilters() throws URISyntaxException {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    FooUrn urn = makeFooUrn(2); // This will not match the filter {"path": "/fooId", "value": "1"}
    AspectBar foo = new AspectBar().setValue("bar");

    IngestionParams ingestionParams = new IngestionParams().setIngestionMode(IngestionMode.LIVE);
    long t1 = 1704067200000L; // 2024-01-01 00:00:00.0 GMT
    dao.add(urn, foo, new AuditStamp().setTime(t1).setActor(Urn.createFromString("urn:li:corpuser:tester")), null, ingestionParams);

    long t2 = 1706745600000L; // 2024-02-01 00:00:00.0 GMT
    dao.add(urn, foo, new AuditStamp().setTime(t2).setActor(Urn.createFromString("urn:li:corpuser:tester")), null, ingestionParams);

    // One filter (two filters in total) matched, we should persist into db.
    if (_schemaConfig == SchemaConfig.NEW_SCHEMA_ONLY) {
      AspectKey<FooUrn, AspectBar> aspectKey = new AspectKey<>(AspectBar.class, urn, 0L);
      long aspectFooLastModifiedOn = dao.getWithExtraInfo(aspectKey).get().getExtraInfo().getAudit().getTime();
      assertEquals(aspectFooLastModifiedOn, t2);
    } else {
      String aspectName = ModelUtils.getAspectName(AspectBar.class);
      EbeanMetadataAspect aspect = getMetadata(urn, aspectName, 0);
      long time = aspect.getCreatedOn().getTime();
      // update not persisted, timestamp should still be t1.
      assertEquals(time, t2);
    }
  }

  @Test
  public void testAddWithIngestionAnnotationWithMultipleFiltersButNoMatch() throws URISyntaxException {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    FooUrnPathExtractor urnPathExtractor = (FooUrnPathExtractor) dao.getUrnPathExtractor();
    urnPathExtractor.updateDummyEntry(1);
    FooUrn urn = makeFooUrn(2); // This will not match any filter.
    AspectBar foo = new AspectBar().setValue("bar");

    IngestionParams ingestionParams = new IngestionParams().setIngestionMode(IngestionMode.LIVE);
    long t1 = 1704067200000L; // 2024-01-01 00:00:00.0 GMT
    dao.add(urn, foo, new AuditStamp().setTime(t1).setActor(Urn.createFromString("urn:li:corpuser:tester")), null, ingestionParams);

    long t2 = 1706745600000L; // 2024-02-01 00:00:00.0 GMT
    dao.add(urn, foo, new AuditStamp().setTime(t2).setActor(Urn.createFromString("urn:li:corpuser:tester")), null, ingestionParams);

    // No filter, we should not persist into db.
    if (_schemaConfig == SchemaConfig.NEW_SCHEMA_ONLY) {
      AspectKey<FooUrn, AspectBar> aspectKey = new AspectKey<>(AspectBar.class, urn, 0L);
      long aspectFooLastModifiedOn = dao.getWithExtraInfo(aspectKey).get().getExtraInfo().getAudit().getTime();
      assertEquals(aspectFooLastModifiedOn, t1);
    } else {
      String aspectName = ModelUtils.getAspectName(AspectBar.class);
      EbeanMetadataAspect aspect = getMetadata(urn, aspectName, 0);
      long time = aspect.getCreatedOn().getTime();
      // update not persisted, timestamp should still be t1.
      assertEquals(time, t1);
    }
  }

  @Test
  public void testAddWithOverrideIngestionMode() throws URISyntaxException {
    // this test is used to check that new metadata ingestion with the OVERRIDE write mode is still updated in
    // the database even if the metadata values are the same.
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    FooUrn urn = makeFooUrn(1);
    AspectFoo foo = new AspectFoo().setValue("foo");
    IngestionParams ingestionParams = new IngestionParams().setIngestionMode(IngestionMode.LIVE_OVERRIDE);
    dao.setAlwaysEmitAuditEvent(false);
    dao.setAlwaysEmitAspectSpecificAuditEvent(false);

    long t1 = 946713600000L; // 2000-01-01 00:00:00.0
    long t2 = 949392000000L; // 2000-02-01 00:00:00.0
    dao.add(urn, foo, new AuditStamp().setTime(t1).setActor(Urn.createFromString("urn:li:corpuser:tester")), null, ingestionParams);
    // MAE is emitted on a fresh metadata update, even with OVERRIDE write mode
    Mockito.verify(_mockProducer, times(1)).produceMetadataAuditEvent(urn, null, foo);

    dao.add(urn, foo, new AuditStamp().setTime(t2).setActor(Urn.createFromString("urn:li:corpuser:tester")), null, ingestionParams);
    // MAE is not emitted on a metadata update with the same metadata value, with OVERRIDE write mode
    verifyNoMoreInteractions(_mockProducer);

    // however, make sure that the update still went through by checking the aspect's lastmodifiedon
    if (_schemaConfig == SchemaConfig.NEW_SCHEMA_ONLY) {
      AspectKey<FooUrn, AspectFoo> aspectKey = new AspectKey<>(AspectFoo.class, urn, 0L);
      long aspectFooLastModifiedOn = dao.getWithExtraInfo(aspectKey).get().getExtraInfo().getAudit().getTime();
      assertEquals(aspectFooLastModifiedOn, t2);
    } else {
      String aspectName = ModelUtils.getAspectName(AspectFoo.class);
      EbeanMetadataAspect aspect = getMetadata(urn, aspectName, 0);
      long time = aspect.getCreatedOn().getTime();
      assertEquals(time, t2);
    }
  }

  @Test
  public void testDefaultEqualityTester() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    dao.setEqualityTester(AspectFoo.class, DefaultEqualityTester.<AspectFoo>newInstance());
    FooUrn urn = makeFooUrn(2);
    String aspectName = ModelUtils.getAspectName(AspectFoo.class);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectFoo bar = new AspectFoo().setValue("bar");

    dao.add(urn, foo, _dummyAuditStamp);
    dao.add(urn, foo, _dummyAuditStamp);
    dao.add(urn, bar, _dummyAuditStamp);

    // v0: bar
    EbeanMetadataAspect aspect = getMetadata(urn, aspectName, 0);
    AspectFoo actual = RecordUtils.toRecordTemplate(AspectFoo.class, aspect.getMetadata());
    assertEquals(actual, bar);

    if (dao.isChangeLogEnabled()) {
      // v1: foo
      aspect = getMetadata(urn, aspectName, 1);
      actual = RecordUtils.toRecordTemplate(AspectFoo.class, aspect.getMetadata());
      assertEquals(actual, foo);

      // no v2
      assertNull(getMetadata(urn, aspectName, 2));
    }


    verify(_mockProducer, times(1)).produceMetadataAuditEvent(urn, null, foo);
    verify(_mockProducer, times(1)).produceMetadataAuditEvent(urn, foo, bar);
    verifyNoMoreInteractions(_mockProducer);
  }

  @Test
  public void testAlwaysFalseEqualityTester() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    dao.setEqualityTester(AspectFoo.class, AlwaysFalseEqualityTester.<AspectFoo>newInstance());
    FooUrn urn = makeFooUrn(1);
    String aspectName = ModelUtils.getAspectName(AspectFoo.class);
    AspectFoo foo1 = new AspectFoo().setValue("foo");
    AspectFoo foo2 = new AspectFoo().setValue("foo");

    dao.add(urn, foo1, _dummyAuditStamp);
    dao.add(urn, foo2, _dummyAuditStamp);

    EbeanMetadataAspect aspect = getMetadata(urn, aspectName, 0);
    AspectFoo actual = RecordUtils.toRecordTemplate(AspectFoo.class, aspect.getMetadata());
    assertEquals(actual, foo1);

    if (dao.isChangeLogEnabled()) {
      aspect = getMetadata(urn, aspectName, 1);
      actual = RecordUtils.toRecordTemplate(AspectFoo.class, aspect.getMetadata());
      assertEquals(actual, foo2);
    }

    verify(_mockProducer, times(1)).produceMetadataAuditEvent(urn, null, foo1);
    verify(_mockProducer, times(1)).produceMetadataAuditEvent(urn, foo1, foo2);
    verifyNoMoreInteractions(_mockProducer);
  }

  @Test
  public void testVersionBasedRetention() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    dao.setRetention(AspectFoo.class, new VersionBasedRetention(2));
    FooUrn urn = makeFooUrn(1);
    String aspectName = ModelUtils.getAspectName(AspectFoo.class);
    AspectFoo v0 = new AspectFoo().setValue("baz");
    AspectFoo v1 = new AspectFoo().setValue("bar");
    AspectFoo v2 = new AspectFoo().setValue("foo");

    dao.add(urn, v1, _dummyAuditStamp);
    dao.add(urn, v2, _dummyAuditStamp);
    dao.add(urn, v0, _dummyAuditStamp);

    if (dao.isChangeLogEnabled()) {
      assertNull(getMetadata(urn, aspectName, 1));
      assertNotNull(getMetadata(urn, aspectName, 2));
    }
    assertNotNull(getMetadata(urn, aspectName, 0));
  }

  @Test
  public void testTimeBasedRetention() {
    Clock mockClock = mock(Clock.class);
    long baseTime = 946713600000L; // 2000.01.01
    when(mockClock.millis())
        // Format
        .thenReturn(baseTime + 1000L) // v1 age check
        .thenReturn(baseTime + 3000L) // v2 age check
        .thenReturn(baseTime + 5000L); // v3 age check

    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    dao.setClock(mockClock);
    dao.setRetention(AspectFoo.class, new TimeBasedRetention(2000L));
    FooUrn urn = makeFooUrn(1);
    String aspectName = ModelUtils.getAspectName(AspectFoo.class);
    AspectFoo v0 = new AspectFoo().setValue("baz");
    AspectFoo v1 = new AspectFoo().setValue("bar");
    AspectFoo v2 = new AspectFoo().setValue("foo");

    dao.add(urn, v1, makeAuditStamp("foo", baseTime + 1000L));
    dao.add(urn, v2, makeAuditStamp("foo", baseTime + 3000L));
    dao.add(urn, v0, makeAuditStamp("foo", baseTime + 5000L));

    if (dao.isChangeLogEnabled()) {
      assertNull(getMetadata(urn, aspectName, 1));
      assertNotNull(getMetadata(urn, aspectName, 2));
    }
    assertNotNull(getMetadata(urn, aspectName, 0));
  }

  @Test
  public void testAddSuccessAfterRetry() {
    if (_schemaConfig == SchemaConfig.OLD_SCHEMA_ONLY) {
      EbeanServer server = mock(EbeanServer.class);
      Transaction mockTransaction = mock(Transaction.class);
      when(server.beginTransaction()).thenReturn(mockTransaction);
      when(server.find(any(), any())).thenReturn(null);
      doThrow(RollbackException.class).doNothing().when(server).insert(any(EbeanMetadataAspect.class));

      Query mockQuery = mock(Query.class);
      when(mockQuery.findList()).thenReturn(Collections.emptyList());
      // additions for direct SQL execution
      when(server.findNative(any(), any())).thenReturn(mockQuery);
      when(mockQuery.setParameter(any(), any())).thenReturn(mockQuery);

      // additions for ebean find builder
      ExpressionList mockEList = mock(ExpressionList.class);
      OrderBy mockOrderBy = mock(OrderBy.class);
      when(server.find(any())).thenReturn(mockQuery);
      when(mockQuery.where()).thenReturn(mockEList);
      when(mockEList.eq(any(), any())).thenReturn(mockEList);
      when(mockEList.orderBy()).thenReturn(mockOrderBy);
      when(mockOrderBy.desc(any())).thenReturn(mockQuery);

      EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(server, FooUrn.class);
      when(server.find(any(), any())).thenReturn(null);
      dao.add(makeFooUrn(1), new AspectFoo().setValue("foo"), _dummyAuditStamp);
    }
  }

  @Test(expectedExceptions = RetryLimitReached.class)
  public void testAddFailedAfterRetry() {
    EbeanServer server = mock(EbeanServer.class);
    Transaction mockTransaction = mock(Transaction.class);
    SqlQuery mockSqlQuery = mock(SqlQuery.class);
    when(server.beginTransaction()).thenReturn(mockTransaction);
    when(server.find(any(), ArgumentMatchers.any(PrimaryKey.class))).thenReturn(null);
    doThrow(RollbackException.class).when(server).insert(any(EbeanMetadataAspect.class));
    doThrow(RollbackException.class).when(server).createSqlUpdate(any());
    when(server.createSqlQuery(any())).thenReturn(mockSqlQuery);
    when(mockSqlQuery.findList()).thenReturn(Collections.emptyList());

    Query mockQuery = mock(Query.class);
    when(mockQuery.findList()).thenReturn(Collections.emptyList());
    // additions for direct SQL execution
    when(server.findNative(any(), any())).thenReturn(mockQuery);
    when(mockQuery.setParameter(any(), any())).thenReturn(mockQuery);

    // additions for ebean find builder
    ExpressionList mockEList = mock(ExpressionList.class);
    OrderBy mockOrderBy = mock(OrderBy.class);
    when(server.find(any())).thenReturn(mockQuery);
    when(mockQuery.where()).thenReturn(mockEList);
    when(mockEList.eq(any(), any())).thenReturn(mockEList);
    when(mockEList.orderBy()).thenReturn(mockOrderBy);
    when(mockOrderBy.desc(any())).thenReturn(mockQuery);

    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(server, FooUrn.class);
    dao.add(makeFooUrn(1), new AspectFoo().setValue("foo"), _dummyAuditStamp);
  }

  @Test
  public void testAtomicMultipleUpdatesRollsbackOnFailure() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(_server, FooUrn.class);
    dao.enableAtomicMultipleUpdate(true);

    FooUrn fooUrn = makeFooUrn(1);

    // first, verify that we don't have anything in our DB when we start
    assertFalse(dao.get(AspectFoo.class, fooUrn).isPresent());
    assertFalse(dao.get(AspectBar.class, fooUrn).isPresent());

    BaseLocalDAO.AspectUpdateLambda<AspectFoo> goodUpdate = new BaseLocalDAO.AspectUpdateLambda<>(new AspectFoo().setValue("foo"));
    BaseLocalDAO.AspectUpdateLambda<AspectBar> badUpdate = new BaseLocalDAO.AspectUpdateLambda<>(AspectBar.class, (ignore) -> {
      throw new RuntimeException();
    });

    assertThrows(RuntimeException.class, () ->
        dao.addMany(fooUrn, Arrays.asList(goodUpdate, badUpdate), _dummyAuditStamp, 1, null));

    // because our second update lambda throws an exception, we still should not have records in our DB
    assertFalse(dao.get(AspectFoo.class, fooUrn).isPresent());
    assertFalse(dao.get(AspectBar.class, fooUrn).isPresent());
  }

  @Test
  public void testAtomicMultipleUpdateSuccess() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(_server, FooUrn.class);
    dao.enableAtomicMultipleUpdate(true);

    FooUrn fooUrn = makeFooUrn(1);

    // first, verify that we don't have anything in our DB when we start
    assertFalse(dao.get(AspectFoo.class, fooUrn).isPresent());
    assertFalse(dao.get(AspectBar.class, fooUrn).isPresent());

    BaseLocalDAO.AspectUpdateLambda<AspectFoo> firstUpdate = new BaseLocalDAO.AspectUpdateLambda<>(new AspectFoo().setValue("foo"));
    BaseLocalDAO.AspectUpdateLambda<AspectBar> secondUpdate = new BaseLocalDAO.AspectUpdateLambda<>(new AspectBar().setValue("bar"));

    dao.addMany(fooUrn, Arrays.asList(firstUpdate, secondUpdate), _dummyAuditStamp, 1, null);

    assertEquals(dao.get(AspectFoo.class, fooUrn).map(AspectFoo::getValue), Optional.of("foo"));
    assertEquals(dao.get(AspectBar.class, fooUrn).map(AspectBar::getValue), Optional.of("bar"));
  }

  @Test
  public void testGetNonExisting() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    FooUrn urn = makeFooUrn(1);

    Optional<AspectFoo> foo = dao.get(AspectFoo.class, urn);

    assertFalse(foo.isPresent());
  }

  @Test
  public void testGetCapsSensitivity() {
    final EbeanLocalDAO<EntityAspectUnion, BurgerUrn> dao = createDao(BurgerUrn.class);
    final BurgerUrn urnCaps = makeBurgerUrn("urn:li:burger:CHEESEburger");
    final BurgerUrn urnLower = makeBurgerUrn("urn:li:burger:cheeseburger");

    final AspectFoo v0 = new AspectFoo().setValue("baz");
    final AspectFoo v1 = new AspectFoo().setValue("foo");

    // expect v0 to be overwritten with v1
    dao.add(urnCaps, v0, _dummyAuditStamp);
    dao.add(urnLower, v1, _dummyAuditStamp);

    Optional<AspectFoo> caps = dao.get(AspectFoo.class, urnCaps);
    assertTrue(caps.isPresent());
    assertEquals(caps.get(), v1);

    Optional<AspectFoo> lower = dao.get(AspectFoo.class, urnLower);
    assertTrue(lower.isPresent());
    assertEquals(lower.get(), v1);
  }

  @Test
  public void testGetLatestVersion() {
    if (_schemaConfig == SchemaConfig.NEW_SCHEMA_ONLY) {
      // the new schema will always return the latest version (if it exists) since it stores only the latest version.
      // this test is mainly for the change log (i.e. old schema) which keeps track of all versions.
      return;
    }
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    FooUrn urn = makeFooUrn(1);
    AspectFoo v0 = new AspectFoo().setValue("foo");
    addMetadata(urn, AspectFoo.class, 0, v0);
    AspectFoo v1 = new AspectFoo().setValue("bar");
    addMetadata(urn, AspectFoo.class, 1, v1);

    Optional<AspectFoo> foo = dao.get(AspectFoo.class, urn);

    assertTrue(foo.isPresent());
    assertEquals(foo.get(), v0);
  }

  @Test
  public void testGetSpecificVersion() {
    if (_schemaConfig == SchemaConfig.NEW_SCHEMA_ONLY) {
      // the new schema will always return the latest version (if it exists) since it stores only the latest version.
      // this test is mainly for the change log (i.e. old schema) which keeps track of all versions.
      return;
    }
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    FooUrn urn = makeFooUrn(1);
    AspectFoo v0 = new AspectFoo().setValue("foo");
    addMetadata(urn, AspectFoo.class, 0, v0);
    AspectFoo v1 = new AspectFoo().setValue("bar");
    addMetadata(urn, AspectFoo.class, 1, v1);

    Optional<AspectFoo> foo = dao.get(AspectFoo.class, urn, 1);

    assertTrue(foo.isPresent());
    assertEquals(foo.get(), v1);
  }

  @Test
  public void testGetMultipleAspects() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    FooUrn urn = makeFooUrn(1);
    AspectFoo fooV1 = new AspectFoo().setValue("bar");
    dao.add(urn, fooV1, _dummyAuditStamp);
    AspectFoo fooV0 = new AspectFoo().setValue("foo");
    dao.add(urn, fooV0, _dummyAuditStamp);
    AspectBar barV0 = new AspectBar().setValue("bar");
    dao.add(urn, barV0, _dummyAuditStamp);

    Map<Class<? extends RecordTemplate>, Optional<? extends RecordTemplate>> result =
        dao.get(new HashSet<>(Arrays.asList(AspectBar.class, AspectFoo.class)), urn);

    assertEquals(result.size(), 2);
    assertEquals(result.get(AspectFoo.class).get(), fooV0);
    assertEquals(result.get(AspectBar.class).get(), barV0);
  }

  @Test
  public void testGetMultipleAspectsForMultipleUrns() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);

    // urn1 has both foo & bar
    FooUrn urn1 = makeFooUrn(1);
    AspectFoo foo1 = new AspectFoo().setValue("foo1");
    dao.add(urn1, foo1, _dummyAuditStamp);
    AspectBar bar1 = new AspectBar().setValue("bar1");
    dao.add(urn1, bar1, _dummyAuditStamp);

    // urn2 has only foo
    FooUrn urn2 = makeFooUrn(2);
    AspectFoo foo2 = new AspectFoo().setValue("foo2");
    dao.add(urn2, foo2, _dummyAuditStamp);

    // urn3 has nothing
    FooUrn urn3 = makeFooUrn(3);

    Map<FooUrn, Map<Class<? extends RecordTemplate>, Optional<? extends RecordTemplate>>> result =
        dao.get(ImmutableSet.of(AspectFoo.class, AspectBar.class), ImmutableSet.of(urn1, urn2, urn3));

    assertEquals(result.size(), 3);
    assertEquals(result.get(urn1).get(AspectFoo.class).get(), foo1);
    assertEquals(result.get(urn1).get(AspectBar.class).get(), bar1);
    assertEquals(result.get(urn2).get(AspectFoo.class).get(), foo2);
    assertFalse(result.get(urn2).get(AspectBar.class).isPresent());
    assertFalse(result.get(urn3).get(AspectFoo.class).isPresent());
    assertFalse(result.get(urn3).get(AspectBar.class).isPresent());
  }

  @Test
  public void testGetListResult() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    FooUrn urn = makeFooUrn(1);
    AspectFoo v1 = new AspectFoo().setValue("val1");
    AspectFoo v2 = new AspectFoo().setValue("val2");
    AspectFoo v4 = new AspectFoo().setValue("val4");
    // set v0 metadata as null
    EbeanMetadataAspect a0 = getMetadata(urn, AspectFoo.class.getCanonicalName(), 0, null);
    EbeanMetadataAspect a1 = getMetadata(urn, AspectFoo.class.getCanonicalName(), 1, v1);
    EbeanMetadataAspect a2 = getMetadata(urn, AspectFoo.class.getCanonicalName(), 2, v2);
    EbeanMetadataAspect a3 = getMetadata(urn, AspectFoo.class.getCanonicalName(), 3, null);
    EbeanMetadataAspect a4 = getMetadata(urn, AspectFoo.class.getCanonicalName(), 4, v4);
    List<EbeanMetadataAspect> listAspects = Arrays.asList(a0, a1, a2, a3, a4);

    PagedList pagedList = mock(PagedList.class);
    when(pagedList.getList()).thenReturn(listAspects);

    ListResult<AspectFoo> metadata = dao.getListResult(AspectFoo.class, pagedList, 0);
    List<Long> nonNullVersions = metadata.getMetadata().getExtraInfos().stream().map(ExtraInfo::getVersion).collect(Collectors.toList());

    assertEquals(metadata.getValues(), Arrays.asList(v1, v2, v4));
    assertEquals(nonNullVersions, Arrays.asList(1L, 2L, 4L));
  }

  @Test
  public void testBackfill() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    FooUrn urn = makeFooUrn(1);

    AspectFoo expected = new AspectFoo().setValue("foo");
    dao.add(urn, expected, _dummyAuditStamp);
    // MAE produced on successful add()
    verify(_mockProducer, times(1)).produceMetadataAuditEvent(urn, null, expected);

    Optional<AspectFoo> foo = dao.backfill(AspectFoo.class, urn);

    assertEquals(foo.get(), expected);
    verify(_mockProducer, times(1)).produceAspectSpecificMetadataAuditEvent(urn, expected, expected, null, IngestionMode.BOOTSTRAP);
    verifyNoMoreInteractions(_mockProducer);
  }

  @Test
  public void testBackfillSingleAspect() {
    // given
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    List<FooUrn> urns = ImmutableList.of(makeFooUrn(1), makeFooUrn(2), makeFooUrn(3));

    Map<FooUrn, Map<Class<? extends RecordTemplate>, RecordTemplate>> aspects = new HashMap<>();

    urns.forEach(urn -> {
      AspectFoo aspectFoo = new AspectFoo().setValue("foo");
      AspectBar aspectBar = new AspectBar().setValue("bar");
      aspects.put(urn, ImmutableMap.of(AspectFoo.class, aspectFoo, AspectBar.class, aspectBar));
      dao.add(urn, aspectFoo, _dummyAuditStamp);
      dao.add(urn, aspectBar, _dummyAuditStamp);
    });

    // when
    Map<FooUrn, Map<Class<? extends RecordTemplate>, Optional<? extends RecordTemplate>>> backfilledAspects =
        dao.backfill(Collections.singleton(AspectFoo.class), new HashSet<>(urns));

    // then
    for (Urn urn : urns) {
      RecordTemplate aspect = aspects.get(urn).get(AspectFoo.class);
      assertEquals(backfilledAspects.get(urn).get(AspectFoo.class).get(), aspect);
    }
  }

  @Test
  public void testBackfillMultipleAspectsOneUrn() {
    // given
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    List<FooUrn> urns = ImmutableList.of(makeFooUrn(1));

    Map<FooUrn, Map<Class<? extends RecordTemplate>, RecordTemplate>> aspects = new HashMap<>();

    urns.forEach(urn -> {
      AspectFoo aspectFoo = new AspectFoo().setValue("foo");
      AspectBar aspectBar = new AspectBar().setValue("bar");
      aspects.put(urn, ImmutableMap.of(AspectFoo.class, aspectFoo, AspectBar.class, aspectBar));
      dao.add(urn, aspectFoo, _dummyAuditStamp);
      dao.add(urn, aspectBar, _dummyAuditStamp);
    });

    // when
    Map<FooUrn, Map<Class<? extends RecordTemplate>, Optional<? extends RecordTemplate>>> backfilledAspects =
        dao.backfill(ImmutableSet.of(AspectFoo.class, AspectBar.class), Collections.singleton(urns.get(0)));

    // then
    for (Class<? extends RecordTemplate> clazz : aspects.get(urns.get(0)).keySet()) {
      RecordTemplate aspect = aspects.get(urns.get(0)).get(clazz);
      assertEquals(backfilledAspects.get(urns.get(0)).get(clazz).get(), aspect);
    }
  }

  @Test
  public void testBackfillMultipleAspectsMultipleUrns() {
    // given
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    List<FooUrn> urns = ImmutableList.of(makeFooUrn(1), makeFooUrn(2), makeFooUrn(3));

    Map<FooUrn, Map<Class<? extends RecordTemplate>, RecordTemplate>> aspects = new HashMap<>();

    urns.forEach(urn -> {
      AspectFoo aspectFoo = new AspectFoo().setValue("foo");
      AspectBar aspectBar = new AspectBar().setValue("bar");
      aspects.put(urn, ImmutableMap.of(AspectFoo.class, aspectFoo, AspectBar.class, aspectBar));
      dao.add(urn, aspectFoo, _dummyAuditStamp);
      dao.add(urn, aspectBar, _dummyAuditStamp);
      // MAEs produced on successful add()
      verify(_mockProducer, times(1)).produceMetadataAuditEvent(urn, null, aspectFoo);
      verify(_mockProducer, times(1)).produceMetadataAuditEvent(urn, null, aspectBar);
    });

    // when
    Map<FooUrn, Map<Class<? extends RecordTemplate>, Optional<? extends RecordTemplate>>> backfilledAspects =
        dao.backfill(ImmutableSet.of(AspectFoo.class, AspectBar.class), new HashSet<>(urns));

    // then
    for (Urn urn : urns) {
      for (Class<? extends RecordTemplate> clazz : aspects.get(urn).keySet()) {
        RecordTemplate aspect = aspects.get(urn).get(clazz);
        assertEquals(backfilledAspects.get(urn).get(clazz).get(), aspect);
        verify(_mockProducer, times(1)).produceAspectSpecificMetadataAuditEvent(urn, aspect, aspect, null, IngestionMode.BOOTSTRAP);
      }
    }
    verifyNoMoreInteractions(_mockProducer);
  }

  @Test
  public void testBackfillMAEOnlyPresentInDBSuccess() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    List<FooUrn> urns = ImmutableList.of(makeFooUrn(1), makeFooUrn(2), makeFooUrn(3));

    Map<FooUrn, Map<Class<? extends RecordTemplate>, RecordTemplate>> aspects = new HashMap<>();

    urns.forEach(urn -> {
      AspectFoo aspectFoo = new AspectFoo().setValue("foo");
      AspectBar aspectBar = new AspectBar().setValue("bar");
      aspects.put(urn, ImmutableMap.of(AspectFoo.class, aspectFoo, AspectBar.class, aspectBar));
      dao.add(urn, aspectFoo, _dummyAuditStamp);
      dao.add(urn, aspectBar, _dummyAuditStamp);
      // MAEs produced on successful add()
      verify(_mockProducer, times(1)).produceMetadataAuditEvent(urn, null, aspectFoo);
      verify(_mockProducer, times(1)).produceMetadataAuditEvent(urn, null, aspectBar);
    });

    // when
    Set<String> urnSet = urns.stream().map(Urn::toString).collect(Collectors.toSet());
    Set<String> aspectSet = ImmutableSet.of(
        // add not in db but valid aspect
        getAspectName(AspectFoo.class), getAspectName(AspectBar.class), getAspectName(AspectFooBar.class));
    Map<String, Set<String>> backfilledAspects = dao.backfillMAE(BackfillMode.BACKFILL_ALL, aspectSet, urnSet);

    // then
    for (FooUrn urn : urns) {
      for (Class<? extends RecordTemplate> clazz : aspects.get(urn).keySet()) {
        assertTrue(backfilledAspects.get(urn.toString()).contains(getAspectName(clazz)));
        RecordTemplate metadata = aspects.get(urn).get(clazz);
        verify(_mockProducer, times(1)).produceAspectSpecificMetadataAuditEvent(urn, metadata, metadata, null, IngestionMode.BOOTSTRAP);
      }
      assertFalse(backfilledAspects.get(urn.toString()).contains(getAspectName(AspectFooBar.class)));
    }
    verifyNoMoreInteractions(_mockProducer);
  }

  @Test
  public void testBackfillMAEOnlySelectedAspectsSuccess() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    List<FooUrn> urns = ImmutableList.of(makeFooUrn(1), makeFooUrn(2), makeFooUrn(3));

    Map<FooUrn, Map<Class<? extends RecordTemplate>, RecordTemplate>> aspects = new HashMap<>();

    urns.forEach(urn -> {
      AspectFoo aspectFoo = new AspectFoo().setValue("foo");
      AspectBar aspectBar = new AspectBar().setValue("bar");
      aspects.put(urn, ImmutableMap.of(AspectFoo.class, aspectFoo));
      dao.add(urn, aspectFoo, _dummyAuditStamp);
      dao.add(urn, aspectBar, _dummyAuditStamp);
      // MAEs produced on successful add()
      verify(_mockProducer, times(1)).produceMetadataAuditEvent(urn, null, aspectFoo);
      verify(_mockProducer, times(1)).produceMetadataAuditEvent(urn, null, aspectBar);
    });

    // when
    Set<String> urnSet = urns.stream().map(Urn::toString).collect(Collectors.toSet());
    Set<String> aspectSet = ImmutableSet.of(
        // only backfill one aspect
        getAspectName(AspectFoo.class));
    Map<String, Set<String>> backfilledAspects = dao.backfillMAE(BackfillMode.BACKFILL_ALL, aspectSet, urnSet);

    // then
    for (FooUrn urn : urns) {
      for (Class<? extends RecordTemplate> clazz : aspects.get(urn).keySet()) {
        assertTrue(backfilledAspects.get(urn.toString()).contains(getAspectName(clazz)));
        RecordTemplate metadata = aspects.get(urn).get(clazz);
        verify(_mockProducer, times(1)).produceAspectSpecificMetadataAuditEvent(urn, metadata, metadata, null, IngestionMode.BOOTSTRAP);
      }
      assertFalse(backfilledAspects.get(urn.toString()).contains(getAspectName(AspectBar.class)));
    }
    verifyNoMoreInteractions(_mockProducer);
  }

  @Test
  public void testBackfillMAENullAspectsSuccess() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    List<FooUrn> urns = ImmutableList.of(makeFooUrn(1), makeFooUrn(2), makeFooUrn(3));

    Map<FooUrn, Map<Class<? extends RecordTemplate>, RecordTemplate>> aspects = new HashMap<>();

    urns.forEach(urn -> {
      AspectFoo aspectFoo = new AspectFoo().setValue("foo");
      AspectBar aspectBar = new AspectBar().setValue("bar");
      aspects.put(urn, ImmutableMap.of(AspectFoo.class, aspectFoo, AspectBar.class, aspectBar));
      dao.add(urn, aspectFoo, _dummyAuditStamp);
      dao.add(urn, aspectBar, _dummyAuditStamp);
      // MAEs produced on successful add()
      verify(_mockProducer, times(1)).produceMetadataAuditEvent(urn, null, aspectFoo);
      verify(_mockProducer, times(1)).produceMetadataAuditEvent(urn, null, aspectBar);
    });

    // when
    Set<String> urnSet = urns.stream().map(Urn::toString).collect(Collectors.toSet());
    Map<String, Set<String>> backfilledAspects = dao.backfillMAE(BackfillMode.BACKFILL_ALL, null, urnSet);

    // then
    for (FooUrn urn : urns) {
      for (Class<? extends RecordTemplate> clazz : aspects.get(urn).keySet()) {
        assertTrue(backfilledAspects.get(urn.toString()).contains(getAspectName(clazz)));
        RecordTemplate metadata = aspects.get(urn).get(clazz);
        verify(_mockProducer, times(1)).produceAspectSpecificMetadataAuditEvent(urn, metadata, metadata, null, IngestionMode.BOOTSTRAP);
      }
    }
    verifyNoMoreInteractions(_mockProducer);
  }

  @Test
  public void testBackfillMAEInvalidAspectException() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    assertThrows(IllegalArgumentException.class, () -> dao.backfillMAE(BackfillMode.BACKFILL_ALL, ImmutableSet.of("com.linkedin.dummy.badAspect"),
        ImmutableSet.of(makeFooUrn(1).toString())));

    assertThrows(InvalidMetadataType.class, () -> dao.backfillMAE(BackfillMode.BACKFILL_ALL, ImmutableSet.of(getAspectName(AspectBaz.class)),
        ImmutableSet.of(makeFooUrn(1).toString())));
  }

  @Test
  public void testListVersions() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    FooUrn urn = makeFooUrn(1);
    List<Long> versions = new ArrayList<>();
    for (long i = 0; i < 6; i++) {
      AspectFoo foo = new AspectFoo().setValue("foo" + i);
      addMetadata(urn, AspectFoo.class, i, foo);
      versions.add(i);
    }

    ListResult<Long> results = dao.listVersions(AspectFoo.class, urn, 0, 5);
    if (!dao.isChangeLogEnabled()) {
      // when: change log is disabled,
      // expect: listVersion should only return 1 result which is the LATEST_VERSION
      assertFalse(results.isHavingMore());
      assertEquals(results.getTotalCount(), 1);
      assertEquals(results.getValues(), versions.subList(0, 1));
      return;
    }

    assertTrue(results.isHavingMore());
    assertEquals(results.getNextStart(), 5);
    assertEquals(results.getTotalCount(), 6);
    assertEquals(results.getPageSize(), 5);
    assertEquals(results.getTotalPageCount(), 2);
    assertEquals(results.getValues(), versions.subList(0, 5));

    // List last page
    results = dao.listVersions(AspectFoo.class, urn, 5, 10);

    assertFalse(results.isHavingMore());
    assertEquals(results.getNextStart(), ListResult.INVALID_NEXT_START);
    assertEquals(results.getTotalCount(), 6);
    assertEquals(results.getPageSize(), 10);
    assertEquals(results.getTotalPageCount(), 1);
    assertEquals(results.getValues(), versions.subList(5, 6));

    // List beyond last page
    results = dao.listVersions(AspectFoo.class, urn, 6, 1);

    assertFalse(results.isHavingMore());
    assertEquals(results.getNextStart(), ListResult.INVALID_NEXT_START);
    assertEquals(results.getTotalCount(), 6);
    assertEquals(results.getPageSize(), 1);
    assertEquals(results.getTotalPageCount(), 6);
    assertEquals(results.getValues(), new ArrayList<>());
  }

  private static IndexCriterionArray makeIndexCriterionArray(int size) {
    List<IndexCriterion> criterionArrays = new ArrayList<>();
    IntStream.range(0, size).forEach(i -> criterionArrays.add(new IndexCriterion().setAspect("aspect" + i)));
    return new IndexCriterionArray(criterionArrays);
  }

  @Test
  void testListUrnsFromIndexManyFilters() {
    if (_schemaConfig == SchemaConfig.OLD_SCHEMA_ONLY) {
      return;
    }

    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    FooUrn urn1 = makeFooUrn(1);
    FooUrn urn2 = makeFooUrn(2);
    FooUrn urn3 = makeFooUrn(3);
    String aspect1 = AspectFoo.class.getCanonicalName();
    String aspect2 = AspectBar.class.getCanonicalName();

    addIndex(urn1, aspect1, "/path1", true); // boolean
    addIndex(urn1, aspect1, "/path2", 1.534e2); // double
    addIndex(urn1, aspect1, "/path3", 123.4f); // float
    addIndex(urn1, aspect2, "/path4", 123); // int
    addIndex(urn1, aspect2, "/path5", 1234L); // long
    addIndex(urn1, aspect2, "/path6", "val"); // string
    addIndex(urn1, FooUrn.class.getCanonicalName(), "/fooId", 1);

    addIndex(urn2, aspect1, "/path1", true); // boolean
    addIndex(urn2, aspect1, "/path2", 1.534e2); // double
    addIndex(urn2, FooUrn.class.getCanonicalName(), "/fooId", 2);

    addIndex(urn3, aspect1, "/path1", true); // boolean
    addIndex(urn3, aspect1, "/path2", 1.534e2); // double
    addIndex(urn3, aspect1, "/path3", 123.4f); // float
    addIndex(urn3, aspect2, "/path4", 123); // int
    addIndex(urn3, aspect2, "/path5", 1234L); // long
    addIndex(urn3, aspect2, "/path6", "val"); // string
    addIndex(urn3, FooUrn.class.getCanonicalName(), "/fooId", 3);

    IndexValue indexValue1 = new IndexValue();
    indexValue1.setBoolean(true);
    IndexCriterion criterion1 = new IndexCriterion().setAspect(aspect1)
        .setPathParams(new IndexPathParams().setPath("/path1").setValue(indexValue1));
    IndexValue indexValue2 = new IndexValue();
    indexValue2.setDouble(1.534e2);
    IndexCriterion criterion2 = new IndexCriterion().setAspect(aspect1)
        .setPathParams(new IndexPathParams().setPath("/path2").setValue(indexValue2));
    IndexValue indexValue3 = new IndexValue();
    indexValue3.setFloat(123.4f);
    IndexCriterion criterion3 = new IndexCriterion().setAspect(aspect1)
        .setPathParams(new IndexPathParams().setPath("/path3").setValue(indexValue3));
    IndexValue indexValue4 = new IndexValue();
    indexValue4.setInt(123);
    IndexCriterion criterion4 = new IndexCriterion().setAspect(aspect2)
        .setPathParams(new IndexPathParams().setPath("/path4").setValue(indexValue4));
    IndexValue indexValue5 = new IndexValue();
    indexValue5.setLong(1234L);
    IndexCriterion criterion5 = new IndexCriterion().setAspect(aspect2)
        .setPathParams(new IndexPathParams().setPath("/path5").setValue(indexValue5));
    IndexValue indexValue6 = new IndexValue();
    indexValue6.setString("val");
    IndexCriterion criterion6 = new IndexCriterion().setAspect(aspect2)
        .setPathParams(new IndexPathParams().setPath("/path6").setValue(indexValue6));

    // cover CONDITION other than EQUAL
    // GREATER_THAN
    IndexValue indexValue7 = new IndexValue();
    indexValue7.setInt(100);
    IndexCriterion criterion7 = new IndexCriterion().setAspect(aspect2)
        .setPathParams(
            new IndexPathParams().setPath("/path4").setValue(indexValue7).setCondition(Condition.GREATER_THAN));

    // GREATER_THAN_EQUAL_TO
    IndexValue indexValue8 = new IndexValue();
    indexValue8.setFloat(100.2f);
    IndexCriterion criterion8 = new IndexCriterion().setAspect(aspect1)
        .setPathParams(new IndexPathParams().setPath("/path3")
            .setValue(indexValue8)
            .setCondition(Condition.GREATER_THAN_OR_EQUAL_TO));

    // LESS_THAN
    IndexValue indexValue9 = new IndexValue();
    indexValue9.setDouble(1.894e2);
    IndexCriterion criterion9 = new IndexCriterion().setAspect(aspect1)
        .setPathParams(new IndexPathParams().setPath("/path2").setValue(indexValue9).setCondition(Condition.LESS_THAN));

    // LESS_THAN_EQUAL_TO
    IndexValue indexValue10 = new IndexValue();
    indexValue10.setLong(1111L);
    IndexCriterion criterion10 = new IndexCriterion().setAspect(aspect2)
        .setPathParams(new IndexPathParams().setPath("/path5").setValue(indexValue10));

    // 1. with two filter conditions
    IndexCriterionArray indexCriterionArray1 = new IndexCriterionArray(Arrays.asList(criterion1, criterion2));
    final IndexFilter indexFilter1 = new IndexFilter().setCriteria(indexCriterionArray1);
    List<FooUrn> urns1 = dao.listUrns(indexFilter1, null, 3);

    assertEquals(urns1, Arrays.asList(urn1, urn2, urn3));

    // 2. with two filter conditions, check if LIMIT is working as desired i.e. totalCount is more than the page size
    List<FooUrn> urns2 = dao.listUrns(indexFilter1, null, 2);
    assertEquals(urns2, Arrays.asList(urn1, urn2));

    // 3. with six filter conditions covering all different data types that value can take
    IndexCriterionArray indexCriterionArray3 =
        new IndexCriterionArray(Arrays.asList(criterion1, criterion2, criterion3, criterion4, criterion5, criterion6));
    final IndexFilter indexFilter3 = new IndexFilter().setCriteria(indexCriterionArray3);
    List<FooUrn> urns3 = dao.listUrns(indexFilter3, urn1, 5);
    assertEquals(urns3, Collections.singletonList(urn3));

    // 4. GREATER_THAN criterion
    IndexCriterionArray indexCriterionArray4 = new IndexCriterionArray(
        Arrays.asList(criterion1, criterion2, criterion3, criterion4, criterion5, criterion6, criterion7));
    final IndexFilter indexFilter4 = new IndexFilter().setCriteria(indexCriterionArray4);
    List<FooUrn> urns4 = dao.listUrns(indexFilter4, null, 5);
    assertEquals(urns4, Arrays.asList(urn1, urn3));

    // 5. GREATER_THAN_EQUAL_TO criterion
    IndexCriterionArray indexCriterionArray5 = new IndexCriterionArray(
        Arrays.asList(criterion1, criterion2, criterion3, criterion4, criterion5, criterion6, criterion7, criterion8));
    final IndexFilter indexFilter5 = new IndexFilter().setCriteria(indexCriterionArray5);
    List<FooUrn> urns5 = dao.listUrns(indexFilter5, null, 10);
    assertEquals(urns5, Arrays.asList(urn1, urn3));

    // 6. LESS_THAN criterion
    IndexCriterionArray indexCriterionArray6 = new IndexCriterionArray(
        Arrays.asList(criterion1, criterion3, criterion4, criterion5, criterion6, criterion7, criterion8, criterion9));
    final IndexFilter indexFilter6 = new IndexFilter().setCriteria(indexCriterionArray6);
    List<FooUrn> urns6 = dao.listUrns(indexFilter6, urn1, 8);
    assertEquals(urns6, Collections.singletonList(urn3));

    // 7. LESS_THAN_EQUAL_TO
    IndexCriterionArray indexCriterionArray7 = new IndexCriterionArray(
        Arrays.asList(criterion1, criterion3, criterion4, criterion5, criterion6, criterion7, criterion8, criterion9,
            criterion10));
    final IndexFilter indexFilter7 = new IndexFilter().setCriteria(indexCriterionArray7);
    List<FooUrn> urns7 = dao.listUrns(indexFilter7, null, 4);
    assertEquals(urns7, Collections.emptyList());
  }

  @Test
  public void testStartsWith() {
    if (_schemaConfig == SchemaConfig.OLD_SCHEMA_ONLY) {
      return;
    }

    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    FooUrn urn1 = makeFooUrn(1);
    FooUrn urn2 = makeFooUrn(2);
    String aspect = AspectFoo.class.getCanonicalName();

    addIndex(urn1, aspect, "/path", "value1");
    addIndex(urn1, FooUrn.class.getCanonicalName(), "/fooId", 1);

    addIndex(urn2, aspect, "/path", "value2");
    addIndex(urn2, FooUrn.class.getCanonicalName(), "/fooId", 2);

    // starts with substring
    IndexValue indexValue1 = new IndexValue();
    indexValue1.setString("val");
    IndexCriterion criterion1 = new IndexCriterion().setAspect(aspect)
        .setPathParams(new IndexPathParams().setPath("/path").setValue(indexValue1).setCondition(Condition.START_WITH));

    IndexCriterionArray indexCriterionArray1 = new IndexCriterionArray(Collections.singletonList(criterion1));
    final IndexFilter indexFilter1 = new IndexFilter().setCriteria(indexCriterionArray1);
    List<FooUrn> urns1 = dao.listUrns(indexFilter1, null, 5);
    assertEquals(urns1, Arrays.asList(urn1, urn2));

    // full string
    IndexValue indexValue2 = new IndexValue();
    indexValue2.setString("value1");
    IndexCriterion criterion2 = new IndexCriterion().setAspect(aspect)
        .setPathParams(new IndexPathParams().setPath("/path").setValue(indexValue2).setCondition(Condition.START_WITH));

    IndexCriterionArray indexCriterionArray2 = new IndexCriterionArray(Collections.singletonList(criterion2));
    final IndexFilter indexFilter2 = new IndexFilter().setCriteria(indexCriterionArray2);
    List<FooUrn> urns2 = dao.listUrns(indexFilter2, null, 5);
    assertEquals(urns2, Collections.singletonList(urn1));
  }

  @Test
  public void testGetFieldColumn() {
    // 1. string corresponds to string column
    IndexSortCriterion indexSortCriterion1 = new IndexSortCriterion().setAspect(AspectFoo.class.getCanonicalName())
        .setPath("/value").setOrder(SortOrder.DESCENDING);
    String sortingColumn1 = EbeanLocalDAO.getFieldColumn(indexSortCriterion1.getPath(), indexSortCriterion1.getAspect());
    assertEquals(sortingColumn1, EbeanMetadataIndex.STRING_COLUMN);

    // 2. boolean corresponds to string column
    IndexSortCriterion indexSortCriterion2 = new IndexSortCriterion().setAspect(AspectBaz.class.getCanonicalName())
        .setPath("/boolField").setOrder(SortOrder.DESCENDING);
    String sortingColumn2 = EbeanLocalDAO.getFieldColumn(indexSortCriterion2.getPath(), indexSortCriterion2.getAspect());
    assertEquals(sortingColumn2, EbeanMetadataIndex.STRING_COLUMN);

    // 3. int corresponds to long column
    IndexSortCriterion indexSortCriterion3 = new IndexSortCriterion().setAspect(AspectBaz.class.getCanonicalName())
        .setPath("/intField").setOrder(SortOrder.DESCENDING);
    String sortingColumn3 = EbeanLocalDAO.getFieldColumn(indexSortCriterion3.getPath(), indexSortCriterion3.getAspect());
    assertEquals(sortingColumn3, EbeanMetadataIndex.LONG_COLUMN);

    // 4. long corresponds to long column
    IndexSortCriterion indexSortCriterion4 = new IndexSortCriterion().setAspect(AspectBaz.class.getCanonicalName())
        .setPath("/longField").setOrder(SortOrder.DESCENDING);
    String sortingColumn4 = EbeanLocalDAO.getFieldColumn(indexSortCriterion4.getPath(), indexSortCriterion4.getAspect());
    assertEquals(sortingColumn4, EbeanMetadataIndex.LONG_COLUMN);

    // 5. double corresponds to double column
    IndexSortCriterion indexSortCriterion5 = new IndexSortCriterion().setAspect(AspectBaz.class.getCanonicalName())
        .setPath("/doubleField").setOrder(SortOrder.DESCENDING);
    String sortingColumn5 = EbeanLocalDAO.getFieldColumn(indexSortCriterion5.getPath(), indexSortCriterion5.getAspect());
    assertEquals(sortingColumn5, EbeanMetadataIndex.DOUBLE_COLUMN);

    // 6. float corresponds to double column
    IndexSortCriterion indexSortCriterion6 = new IndexSortCriterion().setAspect(AspectBaz.class.getCanonicalName())
        .setPath("/floatField").setOrder(SortOrder.DESCENDING);
    String sortingColumn6 = EbeanLocalDAO.getFieldColumn(indexSortCriterion6.getPath(), indexSortCriterion6.getAspect());
    assertEquals(sortingColumn6, EbeanMetadataIndex.DOUBLE_COLUMN);

    // 7. enum corresponds to string column
    IndexSortCriterion indexSortCriterion7 = new IndexSortCriterion().setAspect(AspectBaz.class.getCanonicalName())
        .setPath("/enumField").setOrder(SortOrder.DESCENDING);
    String sortingColumn7 = EbeanLocalDAO.getFieldColumn(indexSortCriterion7.getPath(), indexSortCriterion7.getAspect());
    assertEquals(sortingColumn7, EbeanMetadataIndex.STRING_COLUMN);

    // 8. nested field
    IndexSortCriterion indexSortCriterion8 = new IndexSortCriterion().setAspect(AspectBaz.class.getCanonicalName())
        .setPath("/recordField/value").setOrder(SortOrder.DESCENDING);
    String sortingColumn8 = EbeanLocalDAO.getFieldColumn(indexSortCriterion8.getPath(), indexSortCriterion8.getAspect());
    assertEquals(sortingColumn8, EbeanMetadataIndex.STRING_COLUMN);

    // 9. invalid type
    IndexSortCriterion indexSortCriterion9 = new IndexSortCriterion().setAspect(AspectBaz.class.getCanonicalName())
        .setPath("/arrayField").setOrder(SortOrder.DESCENDING);
    assertThrows(UnsupportedOperationException.class,
        () -> EbeanLocalDAO.getFieldColumn(indexSortCriterion9.getPath(), indexSortCriterion9.getAspect()));

    // 10. array of records is invalid type
    IndexSortCriterion indexSortCriterion10 = new IndexSortCriterion().setAspect(MixedRecord.class.getCanonicalName())
        .setPath("/recordArray/*/value").setOrder(SortOrder.DESCENDING);
    assertThrows(UnsupportedOperationException.class,
        () -> EbeanLocalDAO.getFieldColumn(indexSortCriterion10.getPath(), indexSortCriterion10.getAspect()));
  }

  @Test
  public void testSorting() {
    if (_schemaConfig == SchemaConfig.OLD_SCHEMA_ONLY) {
      return;
    }

    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    FooUrn urn1 = makeFooUrn(1);
    FooUrn urn2 = makeFooUrn(2);
    FooUrn urn3 = makeFooUrn(3);
    String aspect1 = AspectFoo.class.getCanonicalName();
    String aspect2 = AspectBaz.class.getCanonicalName();

    addIndex(urn1, aspect1, "/value", "valB");
    addIndex(urn1, aspect2, "/stringField", "dolphin");
    addIndex(urn1, aspect2, "/longField", 10);
    addIndex(urn1, aspect2, "/recordField/value", "nestedC");
    addIndex(urn1, FooUrn.class.getCanonicalName(), "/fooId", 1);

    addIndex(urn2, aspect1, "/value", "valC");
    addIndex(urn2, aspect2, "/stringField", "reindeer");
    addIndex(urn2, aspect2, "/longField", 8);
    addIndex(urn2, aspect2, "/recordField/value", "nestedB");
    addIndex(urn2, FooUrn.class.getCanonicalName(), "/fooId", 2);

    addIndex(urn3, aspect1, "/value", "valA");
    addIndex(urn3, aspect2, "/stringField", "dog");
    addIndex(urn3, aspect2, "/longField", 100);
    addIndex(urn3, aspect2, "/recordField/value", "nestedA");
    addIndex(urn3, FooUrn.class.getCanonicalName(), "/fooId", 3);

    // filter and no sorting criterion
    IndexValue indexValue1 = new IndexValue();
    indexValue1.setString("val");
    IndexCriterion criterion1 = new IndexCriterion().setAspect(aspect1)
        .setPathParams(
            new IndexPathParams().setPath("/value").setValue(indexValue1).setCondition(Condition.START_WITH));

    IndexCriterionArray indexCriterionArray1 = new IndexCriterionArray(Collections.singletonList(criterion1));
    final IndexFilter indexFilter1 = new IndexFilter().setCriteria(indexCriterionArray1);

    List<FooUrn> urns1 = dao.listUrns(indexFilter1, null, null, 5);
    assertEquals(urns1, Arrays.asList(urn1, urn2, urn3));

    // filter and sort on same aspect and path
    IndexSortCriterion indexSortCriterion1 =
        new IndexSortCriterion().setAspect(aspect1).setPath("/value").setOrder(SortOrder.DESCENDING);

    List<FooUrn> urns2 = dao.listUrns(indexFilter1, indexSortCriterion1, null, 5);
    assertEquals(urns2, Arrays.asList(urn2, urn1, urn3));

    // filter and sort on different aspect and path
    IndexSortCriterion indexSortCriterion2 =
        new IndexSortCriterion().setAspect(aspect2).setPath("/stringField").setOrder(SortOrder.ASCENDING);

    List<FooUrn> urns3 = dao.listUrns(indexFilter1, indexSortCriterion2, null, 5);
    assertEquals(urns3, Arrays.asList(urn3, urn1, urn2));

    // sorting on long column
    IndexValue indexValue2 = new IndexValue();
    indexValue2.setString("do");
    IndexCriterion criterion2 = new IndexCriterion().setAspect(aspect2)
        .setPathParams(
            new IndexPathParams().setPath("/stringField").setValue(indexValue2).setCondition(Condition.START_WITH));

    IndexCriterionArray indexCriterionArray2 = new IndexCriterionArray(Collections.singletonList(criterion2));
    final IndexFilter indexFilter2 = new IndexFilter().setCriteria(indexCriterionArray2);

    IndexSortCriterion indexSortCriterion3 =
        new IndexSortCriterion().setAspect(aspect2).setPath("/longField").setOrder(SortOrder.DESCENDING);

    List<FooUrn> urns4 = dao.listUrns(indexFilter2, indexSortCriterion3, null, 5);
    assertEquals(urns4, Arrays.asList(urn3, urn1));

    // sorting on nested field
    IndexSortCriterion indexSortCriterion4 =
        new IndexSortCriterion().setAspect(aspect2).setPath("/recordField/value").setOrder(SortOrder.ASCENDING);

    List<FooUrn> urns5 = dao.listUrns(indexFilter1, indexSortCriterion4, null, 5);
    assertEquals(urns5, Arrays.asList(urn3, urn2, urn1));
  }

  @Test
  public void testIn() {
    if (_schemaConfig == SchemaConfig.OLD_SCHEMA_ONLY) {
      return;
    }

    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    FooUrn urn1 = makeFooUrn(1);
    FooUrn urn2 = makeFooUrn(2);
    FooUrn urn3 = makeFooUrn(3);
    String aspect = ModelUtils.getAspectName(AspectFoo.class);

    addIndex(urn1, aspect, "/path1", "foo");
    addIndex(urn1, FooUrn.class.getCanonicalName(), "/fooId", 1);

    addIndex(urn2, aspect, "/path1", "baz");
    addIndex(urn2, FooUrn.class.getCanonicalName(), "/fooId", 2);

    addIndex(urn3, aspect, "/path1", "val");
    addIndex(urn3, FooUrn.class.getCanonicalName(), "/fooId", 3);

    IndexValue indexValue1 = new IndexValue();
    indexValue1.setArray(new StringArray("foo", "baz"));
    IndexCriterion criterion1 = new IndexCriterion().setAspect(aspect)
        .setPathParams(new IndexPathParams().setPath("/path1").setValue(indexValue1).setCondition(Condition.IN));

    IndexCriterionArray indexCriterionArray1 = new IndexCriterionArray(Collections.singletonList(criterion1));
    final IndexFilter indexFilter1 = new IndexFilter().setCriteria(indexCriterionArray1);
    List<FooUrn> urns1 = dao.listUrns(indexFilter1, null, 5);
    assertEquals(urns1, Arrays.asList(urn1, urn2));

    IndexValue indexValue2 = new IndexValue();
    indexValue2.setArray(new StringArray("a", "b", "c"));
    IndexCriterion criterion2 = new IndexCriterion().setAspect(aspect)
        .setPathParams(new IndexPathParams().setPath("/path1").setValue(indexValue2).setCondition(Condition.IN));

    IndexCriterionArray indexCriterionArray2 = new IndexCriterionArray(Collections.singletonList(criterion2));
    final IndexFilter indexFilter2 = new IndexFilter().setCriteria(indexCriterionArray2);
    List<FooUrn> urns2 = dao.listUrns(indexFilter2, null, 5);
    assertEquals(urns2, Arrays.asList());

    IndexValue indexValue3 = new IndexValue();
    indexValue3.setString("test");
    IndexCriterion criterion3 = new IndexCriterion().setAspect(aspect)
        .setPathParams(new IndexPathParams().setPath("/path1").setValue(indexValue3).setCondition(Condition.IN));

    IndexCriterionArray indexCriterionArray3 = new IndexCriterionArray(Collections.singletonList(criterion3));
    final IndexFilter indexFilter3 = new IndexFilter().setCriteria(indexCriterionArray3);
    assertThrows(IllegalArgumentException.class, () -> dao.listUrns(indexFilter3, null, 5));

    IndexValue indexValue4 = new IndexValue();
    indexValue4.setArray(new StringArray());
    IndexCriterion criterion4 = new IndexCriterion().setAspect(aspect)
        .setPathParams(new IndexPathParams().setPath("/path1").setValue(indexValue4).setCondition(Condition.IN));

    IndexCriterionArray indexCriterionArray4 = new IndexCriterionArray(Collections.singletonList(criterion4));
    final IndexFilter indexFilter4 = new IndexFilter().setCriteria(indexCriterionArray4);
    assertThrows(IllegalArgumentException.class, () -> dao.listUrns(indexFilter4, null, 5));
  }

  @Test
  public void testListUrnsOffsetPagination() {
    if (_schemaConfig == SchemaConfig.OLD_SCHEMA_ONLY) {
      return;
    }

    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    FooUrn urn1 = makeFooUrn(1);
    FooUrn urn2 = makeFooUrn(2);
    FooUrn urn3 = makeFooUrn(3);
    String aspect1 = AspectFoo.class.getCanonicalName();
    String aspect2 = AspectBaz.class.getCanonicalName();

    addIndex(urn1, aspect1, "/value", "valB");
    addIndex(urn1, aspect2, "/stringField", "dolphin");
    addIndex(urn1, FooUrn.class.getCanonicalName(), "/fooId", 1);

    addIndex(urn2, aspect1, "/value", "valC");
    addIndex(urn2, aspect2, "/stringField", "reindeer");
    addIndex(urn2, FooUrn.class.getCanonicalName(), "/fooId", 2);

    addIndex(urn3, aspect1, "/value", "valA");
    addIndex(urn3, aspect2, "/stringField", "dog");
    addIndex(urn3, FooUrn.class.getCanonicalName(), "/fooId", 3);

    IndexValue indexValue = new IndexValue();
    indexValue.setString("val");
    IndexCriterion criterion = new IndexCriterion().setAspect(aspect1)
        .setPathParams(new IndexPathParams().setPath("/value").setValue(indexValue).setCondition(Condition.START_WITH));

    IndexCriterionArray indexCriterionArray = new IndexCriterionArray(Collections.singletonList(criterion));
    final IndexFilter indexFilter = new IndexFilter().setCriteria(indexCriterionArray);

    IndexSortCriterion indexSortCriterion =
        new IndexSortCriterion().setAspect(aspect1).setPath("/value").setOrder(SortOrder.DESCENDING);

    // first page
    ListResult<FooUrn> results1 = dao.listUrns(indexFilter, indexSortCriterion, 0, 2);
    assertEquals(results1.getValues(), Arrays.asList(urn2, urn1));
    assertTrue(results1.isHavingMore());
    assertEquals(results1.getNextStart(), 2);
    assertEquals(results1.getTotalCount(), 3);
    assertEquals(results1.getPageSize(), 2);
    assertEquals(results1.getTotalPageCount(), 2);

    // last page
    ListResult<FooUrn> results2 = dao.listUrns(indexFilter, indexSortCriterion, 2, 2);
    assertEquals(results2.getValues(), Arrays.asList(urn3));
    assertFalse(results2.isHavingMore());
    assertEquals(results2.getNextStart(), ListResult.INVALID_NEXT_START);
    assertEquals(results2.getTotalCount(), 3);
    assertEquals(results2.getPageSize(), 2);
    assertEquals(results2.getTotalPageCount(), 2);

    // beyond last page
    ListResult<FooUrn> results3 = dao.listUrns(indexFilter, indexSortCriterion, 4, 2);
    assertEquals(results3.getValues(), new ArrayList<>());
    assertFalse(results3.isHavingMore());
    assertEquals(results3.getNextStart(), ListResult.INVALID_NEXT_START);
    assertEquals(results3.getTotalCount(), 3);
    assertEquals(results3.getPageSize(), 2);
    assertEquals(results3.getTotalPageCount(), 2);
  }

  @Test
  public void testListUrns() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    AspectFoo foo = new AspectFoo().setValue("foo");
    List<FooUrn> urns = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      FooUrn urn = makeFooUrn(i);
      for (int j = 0; j < 3; j++) {
        dao.add(urn, foo, _dummyAuditStamp);
      }
      urns.add(urn);
    }

    ListResult<FooUrn> results = dao.listUrns(AspectFoo.class, 0, 1);

    assertTrue(results.isHavingMore());
    assertEquals(results.getNextStart(), 1);
    assertEquals(results.getTotalCount(), 3);
    assertEquals(results.getPageSize(), 1);
    assertEquals(results.getTotalPageCount(), 3);
    assertEquals(results.getValues(), urns.subList(0, 1));

    // List next page
    results = dao.listUrns(AspectFoo.class, 1, 1);

    assertTrue(results.isHavingMore());
    assertEquals(results.getNextStart(), 2);
    assertEquals(results.getTotalCount(), 3);
    assertEquals(results.getPageSize(), 1);
    assertEquals(results.getTotalPageCount(), 3);
    assertEquals(results.getValues(), urns.subList(1, 2));

    // Test List result sorted by Urns
    results = dao.listUrns(AspectFoo.class, 0, 5);
    assertEquals(results.getValues().size(), 3);
    assertEquals(results.getValues(), urns.subList(0, 3));
    assertEquals(results.getValues().get(0), makeFooUrn(0));
    assertEquals(results.getValues().get(1), makeFooUrn(1));
    assertEquals(results.getValues().get(2), makeFooUrn(2));
  }

  @Test
  public void testListUrnsPaginatedByLastUrn() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    if (_schemaConfig != SchemaConfig.OLD_SCHEMA_ONLY) {
      assertThrows(UnsupportedOperationException.class, () -> dao.listUrnsPaginatedByLastUrn(null, 1));
      return;
    }

    AspectFoo foo = new AspectFoo().setValue("foo");
    List<FooUrn> urns = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      FooUrn urn = makeFooUrn(i);
      for (int j = 0; j < 3; j++) {
        dao.add(urn, foo, _dummyAuditStamp);
      }
      urns.add(urn);
    }

    // initial pagination
    List<FooUrn> results = dao.listUrnsPaginatedByLastUrn(null, 1);
    assertEquals(results, urns.subList(0, 1));

    // next pagination
    results = dao.listUrnsPaginatedByLastUrn(results.get(0), 2);
    assertEquals(results, urns.subList(1, 3));

    // sort order
    results = dao.listUrnsPaginatedByLastUrn(null, 5);
    assertEquals(results, urns.subList(0, 3));
    assertEquals(results.get(0), makeFooUrn(0));
    assertEquals(results.get(1), makeFooUrn(1));
    assertEquals(results.get(2), makeFooUrn(2));

    // distinct urns
    AspectBar bar = new AspectBar().setValue("bar");
    dao.add(urns.get(0), bar, _dummyAuditStamp);
    results = dao.listUrnsPaginatedByLastUrn(null, 5);
    assertEquals(results.size(), 3);
    assertEquals(results.get(0), makeFooUrn(0));
    assertEquals(results.get(1), makeFooUrn(1));
    assertEquals(results.get(2), makeFooUrn(2));
  }

  @Test
  public void testList() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    List<AspectFoo> foos = new LinkedList<>();
    for (int i = 0; i < 3; i++) {
      FooUrn urn = makeFooUrn(i);

      for (int j = 0; j < 10; j++) {
        AspectFoo foo = new AspectFoo().setValue("foo" + j);
        addMetadata(urn, AspectFoo.class, j, foo);
        if (i == 0) {
          foos.add(foo);
        }
      }
    }

    FooUrn urn0 = makeFooUrn(0);

    ListResult<AspectFoo> results = dao.list(AspectFoo.class, urn0, 0, 5);

    if (dao.isChangeLogEnabled()) {
      assertTrue(results.isHavingMore());
      assertEquals(results.getNextStart(), 5);
      assertEquals(results.getTotalCount(), 10);
      assertEquals(results.getPageSize(), 5);
      assertEquals(results.getTotalPageCount(), 2);
      assertEquals(results.getValues(), foos.subList(0, 5));

      assertNotNull(results.getMetadata());
      List<Long> expectedVersions = Arrays.asList(0L, 1L, 2L, 3L, 4L);
      List<Urn> expectedUrns = Collections.singletonList(urn0);
      assertVersionMetadata(results.getMetadata(), expectedVersions, expectedUrns, _now,
          Urns.createFromTypeSpecificString("test", "foo"),
          Urns.createFromTypeSpecificString("test", "bar"));

      // List next page
      results = dao.list(AspectFoo.class, urn0, 5, 9);

      assertFalse(results.isHavingMore());
      assertEquals(results.getNextStart(), ListResult.INVALID_NEXT_START);
      assertEquals(results.getTotalCount(), 10);
      assertEquals(results.getPageSize(), 9);
      assertEquals(results.getTotalPageCount(), 2);
      assertEquals(results.getValues(), foos.subList(5, 10));
      assertNotNull(results.getMetadata());
    } else {
      // when: change log is not enabled
      // expect:
      assertFalse(results.isHavingMore());
      assertEquals(results.getNextStart(), ListResult.INVALID_NEXT_START);
      assertEquals(results.getTotalCount(), 1);
      assertEquals(results.getPageSize(), 5);
      assertEquals(results.getTotalPageCount(), 1);
      // expect: only the latest version is loaded
      assertEquals(results.getValues(), foos.subList(0, 1));
      assertNotNull(results.getMetadata());
      List<Long> expectedVersions = Arrays.asList(0L);
      List<Urn> expectedUrns = Collections.singletonList(urn0);
      assertVersionMetadata(results.getMetadata(), expectedVersions, expectedUrns, _now,
          Urns.createFromTypeSpecificString("test", "foo"),
          Urns.createFromTypeSpecificString("test", "bar"));
    }
  }

  private static LocalDAOStorageConfig makeLocalDAOStorageConfig(Class<? extends RecordTemplate> aspectClass,
      List<String> pegasusPaths) {
    Map<Class<? extends RecordTemplate>, LocalDAOStorageConfig.AspectStorageConfig> aspectStorageConfigMap =
        new HashMap<>();
    aspectStorageConfigMap.put(aspectClass, getAspectStorageConfig(pegasusPaths));
    LocalDAOStorageConfig storageConfig =
        LocalDAOStorageConfig.builder().aspectStorageConfigMap(aspectStorageConfigMap).build();
    return storageConfig;
  }

  private static LocalDAOStorageConfig makeLocalDAOStorageConfig(Class<? extends RecordTemplate> aspectClass1,
      List<String> pegasusPaths1, Class<? extends RecordTemplate> aspectClass2, List<String> pegasusPaths2) {
    Map<Class<? extends RecordTemplate>, LocalDAOStorageConfig.AspectStorageConfig> aspectStorageConfigMap =
        new HashMap<>();
    aspectStorageConfigMap.put(aspectClass1, getAspectStorageConfig(pegasusPaths1));
    aspectStorageConfigMap.put(aspectClass2, getAspectStorageConfig(pegasusPaths2));
    LocalDAOStorageConfig storageConfig =
        LocalDAOStorageConfig.builder().aspectStorageConfigMap(aspectStorageConfigMap).build();
    return storageConfig;
  }

  private static LocalDAOStorageConfig.AspectStorageConfig getAspectStorageConfig(List<String> pegasusPaths) {
    Map<String, LocalDAOStorageConfig.PathStorageConfig> pathStorageConfigMap = new HashMap<>();
    pegasusPaths.forEach(path -> pathStorageConfigMap.put(path,
        LocalDAOStorageConfig.PathStorageConfig.builder().strongConsistentSecondaryIndex(true).build()));
    return LocalDAOStorageConfig.AspectStorageConfig.builder().pathStorageConfigMap(pathStorageConfigMap).build();
  }

  @Test
  void testStrongConsistentIndexPaths() {
    // construct LocalDAOStorageConfig object
    LocalDAOStorageConfig storageConfig =
        makeLocalDAOStorageConfig(AspectFoo.class, Collections.singletonList("/value"));

    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao =
        new EbeanLocalDAO<EntityAspectUnion, FooUrn>(_mockProducer, _server, storageConfig, FooUrn.class);
    Map<Class<? extends RecordTemplate>, LocalDAOStorageConfig.AspectStorageConfig> aspectToPaths =
        dao.getStrongConsistentIndexPaths();

    assertNotNull(aspectToPaths);
    Set<Class<? extends RecordTemplate>> setAspects = aspectToPaths.keySet();
    assertEquals(setAspects, new HashSet<>(Arrays.asList(AspectFoo.class)));
    LocalDAOStorageConfig.AspectStorageConfig config = aspectToPaths.get(AspectFoo.class);
    assertTrue(config.getPathStorageConfigMap().get("/value").isStrongConsistentSecondaryIndex());
  }

  @Test
  public void testListAspectsForAllUrns() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);

    for (int i = 0; i < 3; i++) {
      FooUrn urn = makeFooUrn(i);
      for (int j = 0; j < 10; j++) {
        AspectFoo foo = new AspectFoo().setValue("foo" + i + j);
        addMetadata(urn, AspectFoo.class, j, foo);
      }
    }

    ListResult<AspectFoo> results = dao.list(AspectFoo.class, 0, 0, 2);

    assertTrue(results.isHavingMore());
    assertEquals(results.getNextStart(), 2);
    assertEquals(results.getTotalCount(), 3);
    assertEquals(results.getPageSize(), 2);
    assertEquals(results.getTotalPageCount(), 2);

    assertNotNull(results.getMetadata());
    assertVersionMetadata(results.getMetadata(), Collections.singletonList(0L), Arrays.asList(makeFooUrn(0), makeFooUrn(1)), _now,
        Urns.createFromTypeSpecificString("test", "foo"), Urns.createFromTypeSpecificString("test", "bar"));

    // Test list latest aspects
    ListResult<AspectFoo> latestResults = dao.list(AspectFoo.class, 0, 2);
    assertEquals(results, latestResults);

    // List next page
    results = dao.list(AspectFoo.class, 0, 2, 2);

    assertFalse(results.isHavingMore());
    assertEquals(results.getNextStart(), ListResult.INVALID_NEXT_START);
    assertEquals(results.getTotalCount(), 3);
    assertEquals(results.getPageSize(), 2);
    assertEquals(results.getTotalPageCount(), 2);
    assertNotNull(results.getMetadata());

    if (dao.isChangeLogEnabled()) {
      // Test list for a non-zero version
      results = dao.list(AspectFoo.class, 1, 0, 5);

      assertFalse(results.isHavingMore());
      assertEquals(results.getNextStart(), ListResult.INVALID_NEXT_START);
      assertEquals(results.getTotalCount(), 3);
      assertEquals(results.getPageSize(), 5);
      assertEquals(results.getTotalPageCount(), 1);

      assertNotNull(results.getMetadata());
      assertVersionMetadata(results.getMetadata(), Collections.singletonList(1L),
          Arrays.asList(makeFooUrn(0), makeFooUrn(1), makeFooUrn(2)), _now,
          Urns.createFromTypeSpecificString("test", "foo"), Urns.createFromTypeSpecificString("test", "bar"));
    } else {
      try {
        dao.list(AspectFoo.class, 1, 0, 5);
        fail("list aspect by non-0 version is not supported when change log is disabled");
      } catch (UnsupportedOperationException uoe) {
      }
    }
  }

  @Test
  void testNewStringId() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    String id1 = dao.newStringId();
    String id2 = dao.newStringId();

    assertNotNull(id1);
    assertTrue(id1.length() > 0);
    assertNotNull(id2);
    assertTrue(id2.length() > 0);
    assertNotEquals(id1, id2);
  }

  @Test
  void testNewNumericId() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    long id1 = dao.newNumericId("namespace");
    long id2 = dao.newNumericId("namespace");
    long id3 = dao.newNumericId("another namespace");

    assertEquals(id1, 1);
    assertEquals(id2, 2);
    assertEquals(id3, 1);
  }

  @Test
  void testExists() {
    // given
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    FooUrn urn = makeFooUrn(1);

    assertFalse(dao.exists(urn));

    // add metadata
    AspectFoo fooV1 = new AspectFoo().setValue("foo");
    dao.add(urn, fooV1, _dummyAuditStamp);

    assertTrue(dao.exists(urn));
  }

  @Test(expectedExceptions = NullPointerException.class)
  void testNullAspectStorageConfigMap() {
    // null aspect storage config map should throw an exception
    LocalDAOStorageConfig.builder().aspectStorageConfigMap(null).build();
  }

  @Test
  void testGetGMAIndexPair() {
    IndexValue indexValue = new IndexValue();
    String aspect = "aspect" + System.currentTimeMillis();
    IndexPathParams indexPathParams = new IndexPathParams().setPath("/path1").setValue(indexValue);
    IndexCriterion indexCriterion = new IndexCriterion().setAspect(aspect).setPathParams(indexPathParams);

    // 1. IndexValue pair corresponds to boolean
    indexValue.setBoolean(false);
    EbeanLocalDAO.GMAIndexPair gmaIndexPair = EbeanLocalDAO.getGMAIndexPair(indexCriterion);
    assertEquals(EbeanMetadataIndex.STRING_COLUMN, gmaIndexPair.valueType);
    assertEquals("false", gmaIndexPair.value);
    // 2. IndexValue pair corresponds to double
    double dVal = 0.000001;
    indexValue.setDouble(dVal);
    gmaIndexPair = EbeanLocalDAO.getGMAIndexPair(indexCriterion);
    assertEquals(EbeanMetadataIndex.DOUBLE_COLUMN, gmaIndexPair.valueType);
    assertEquals(dVal, gmaIndexPair.value);
    // 3. IndexValue pair corresponds to float
    float fVal = 0.0001f;
    double doubleVal = fVal;
    indexValue.setFloat(fVal);
    gmaIndexPair = EbeanLocalDAO.getGMAIndexPair(indexCriterion);
    assertEquals(EbeanMetadataIndex.DOUBLE_COLUMN, gmaIndexPair.valueType);
    assertEquals(doubleVal, gmaIndexPair.value);
    // 4. IndexValue pair corresponds to int
    int iVal = 100;
    long longVal = iVal;
    indexValue.setInt(iVal);
    gmaIndexPair = EbeanLocalDAO.getGMAIndexPair(indexCriterion);
    assertEquals(EbeanMetadataIndex.LONG_COLUMN, gmaIndexPair.valueType);
    assertEquals(longVal, gmaIndexPair.value);
    // 5. IndexValue pair corresponds to long
    long lVal = 1L;
    indexValue.setLong(lVal);
    gmaIndexPair = EbeanLocalDAO.getGMAIndexPair(indexCriterion);
    assertEquals(EbeanMetadataIndex.LONG_COLUMN, gmaIndexPair.valueType);
    assertEquals(lVal, gmaIndexPair.value);
    // 6. IndexValue pair corresponds to string
    String sVal = "testVal";
    indexValue.setString(sVal);
    gmaIndexPair = EbeanLocalDAO.getGMAIndexPair(indexCriterion);
    assertEquals(EbeanMetadataIndex.STRING_COLUMN, gmaIndexPair.valueType);
    assertEquals(sVal, gmaIndexPair.value);
    // 7. IndexValue pair corresponds to string array
    StringArray sArrVal = new StringArray();
    sArrVal.add("testVal");
    indexValue.setArray(sArrVal);
    gmaIndexPair = EbeanLocalDAO.getGMAIndexPair(indexCriterion);
    assertEquals(EbeanMetadataIndex.STRING_COLUMN, gmaIndexPair.valueType);
    assertEquals(sArrVal, gmaIndexPair.value);
  }

  @Test
  void testListUrnsFromIndex() {
    if (_schemaConfig == SchemaConfig.OLD_SCHEMA_ONLY) {
      return;
    }

    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    FooUrn urn1 = makeFooUrn(1);
    FooUrn urn2 = makeFooUrn(2);
    FooUrn urn3 = makeFooUrn(3);
    String aspect = ModelUtils.getAspectName(AspectFoo.class);

    addIndex(urn1, aspect, "/path1", "val1");
    addIndex(urn1, aspect, "/path2", "val2");
    addIndex(urn1, aspect, "/path3", "val3");
    addIndex(urn1, FooUrn.class.getCanonicalName(), "/fooId", 1);
    addIndex(urn2, aspect, "/path1", "val1");
    addIndex(urn2, FooUrn.class.getCanonicalName(), "/fooId", 2);
    addIndex(urn3, aspect, "/path1", "val1");
    addIndex(urn3, FooUrn.class.getCanonicalName(), "/fooId", 3);

    IndexCriterion indexCriterion = new IndexCriterion().setAspect(aspect);
    final IndexFilter indexFilter1 = new IndexFilter().setCriteria(new IndexCriterionArray(indexCriterion));

    // 1. only aspect and not path or value is provided in Index Filter
    indexCriterion = new IndexCriterion().setAspect(aspect);
    final IndexFilter indexFilter4 = new IndexFilter().setCriteria(new IndexCriterionArray(indexCriterion));
    List<FooUrn> urns = dao.listUrns(indexFilter4, null, 2);
    assertEquals(urns, Arrays.asList(urn1, urn2));

    // 2. aspect with path and value is provided in index filter
    IndexValue indexValue = new IndexValue();
    indexValue.setString("val1");
    IndexPathParams indexPathParams = new IndexPathParams().setPath("/path1").setValue(indexValue);
    indexCriterion = new IndexCriterion().setAspect(aspect).setPathParams(indexPathParams);
    final IndexFilter indexFilter5 = new IndexFilter().setCriteria(new IndexCriterionArray(indexCriterion));
    urns = dao.listUrns(indexFilter5, urn1, 2);
    assertEquals(urns, Arrays.asList(urn2, urn3));

    // 3. aspect with correct path but incorrect value
    indexValue.setString("valX");
    indexPathParams = new IndexPathParams().setPath("/path1").setValue(indexValue);
    indexCriterion = new IndexCriterion().setAspect(aspect).setPathParams(indexPathParams);
    final IndexFilter indexFilter6 = new IndexFilter().setCriteria(new IndexCriterionArray(indexCriterion));
    urns = dao.listUrns(indexFilter6, urn1, 2);
    assertEquals(urns, Collections.emptyList());
  }

  @Test
  void testListUrnsFromIndexZeroSize() {
    if (_schemaConfig == SchemaConfig.OLD_SCHEMA_ONLY) {
      return;
    }

    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    FooUrn urn1 = makeFooUrn(1);
    FooUrn urn2 = makeFooUrn(2);
    String aspect = ModelUtils.getAspectName(AspectFoo.class);

    addIndex(urn1, aspect, "/path1", "val1");
    addIndex(urn1, aspect, "/path2", "val2");
    addIndex(urn1, FooUrn.class.getCanonicalName(), "/fooId", 1);
    addIndex(urn2, aspect, "/path1", "val1");
    addIndex(urn2, FooUrn.class.getCanonicalName(), "/fooId", 2);

    IndexCriterion indexCriterion = new IndexCriterion().setAspect(aspect);
    final IndexFilter indexFilter = new IndexFilter().setCriteria(new IndexCriterionArray(indexCriterion));

    List<FooUrn> urns = dao.listUrns(indexFilter, null, 0);

    assertEquals(urns, Collections.emptyList());
  }

  @Test
  void testListUrnsFromIndexForAnEntity() {
    // listUrns can only be supported with new schema
    if (_schemaConfig == SchemaConfig.OLD_SCHEMA_ONLY) {
      return;
    }

    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao1 = createDao(FooUrn.class);
    EbeanLocalDAO<EntityAspectUnion, BarUrn> dao2 = createDao(BarUrn.class);
    dao1.setUrnPathExtractor(new FooUrnPathExtractor());
    dao2.setUrnPathExtractor(new BarUrnPathExtractor());

    FooUrn urn1 = makeFooUrn(1);
    FooUrn urn2 = makeFooUrn(2);
    FooUrn urn3 = makeFooUrn(3);
    BarUrn urn4 = makeBarUrn(4);

    addIndex(urn1, FooUrn.class.getCanonicalName(), "/path1", "0");
    addIndex(urn2, FooUrn.class.getCanonicalName(), "/path2", "0");
    addIndex(urn3, FooUrn.class.getCanonicalName(), "/path3", "0");
    addIndex(urn4, BarUrn.class.getCanonicalName(), "/path4", "0");

    // List foo urns
    List<FooUrn> urns1 = dao1.listUrns(FooUrn.class, null, 2);
    assertEquals(urns1, Arrays.asList(urn1, urn2));

    // List bar urns
    List<BarUrn> urns2 = dao2.listUrns(BarUrn.class, null, 1);
    assertEquals(urns2, Collections.singletonList(urn4));
  }

  @Test
  void testIndexedListUrnsInOldSchema() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    if (_schemaConfig == SchemaConfig.OLD_SCHEMA_ONLY) {
      assertThrows(UnsupportedOperationException.class, () -> dao.listUrns(
          new IndexFilter(),
          new IndexSortCriterion(),
          makeFooUrn(1),
          1
      ));

      try {
        dao.listUrns(null, null, makeFooUrn(1), 1);
      } catch (Exception ex) {
        Assert.fail("Unexpected exception thrown in calling indexedListUrns with null filters");
      }
    }
  }

  @Test
  void testGetUrn() {
    // case 1: valid urn
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    String urn1 = "urn:li:foo:1";
    FooUrn fooUrn = makeFooUrn(1);

    assertEquals(fooUrn, dao.getUrn(urn1));

    // case 2: invalid entity type, correct id
    String urn2 = "urn:li:test:1";
    assertThrows(IllegalArgumentException.class, () -> dao.getUrn(urn2));

    // case 3: invalid urn
    String urn3 = "badUrn";
    assertThrows(IllegalArgumentException.class, () -> dao.getUrn(urn3));
  }

  @Test
  public void testGetWithExtraInfoLatestVersion() {
    if (_schemaConfig == SchemaConfig.NEW_SCHEMA_ONLY) {
      // the new schema will always return the latest version (if it exists) since it stores only the latest version.
      // this test is mainly for the change log (i.e. old schema) which keeps track of all versions.
      return;
    }
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    FooUrn urn = makeFooUrn(1);
    AspectFoo v0 = new AspectFoo().setValue("foo");
    Urn creator1 = Urns.createFromTypeSpecificString("test", "testCreator1");
    Urn impersonator1 = Urns.createFromTypeSpecificString("test", "testImpersonator1");
    Urn creator2 = Urns.createFromTypeSpecificString("test", "testCreator2");
    Urn impersonator2 = Urns.createFromTypeSpecificString("test", "testImpersonator2");
    addMetadataWithAuditStamp(urn, AspectFoo.class, 0, v0, _now, creator1.toString(),
        impersonator1.toString());
    AspectFoo v1 = new AspectFoo().setValue("bar");
    addMetadataWithAuditStamp(urn, AspectFoo.class, 1, v1, _now, creator2.toString(),
        impersonator2.toString());

    Optional<AspectWithExtraInfo<AspectFoo>> foo = dao.getWithExtraInfo(AspectFoo.class, urn);

    assertTrue(foo.isPresent());
    assertEquals(foo.get(), new AspectWithExtraInfo<>(v0,
        new ExtraInfo().setAudit(makeAuditStamp(creator1, impersonator1, _now)).setUrn(urn).setVersion(0)));
  }

  @Test
  public void testGetWithExtraInfoSpecificVersion() {
    if (_schemaConfig == SchemaConfig.NEW_SCHEMA_ONLY) {
      // the new schema will always return the latest version (if it exists) since it stores only the latest version.
      // this test is mainly for the change log (i.e. old schema) which keeps track of all versions.
      return;
    }
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    FooUrn urn = makeFooUrn(1);
    AspectFoo v0 = new AspectFoo().setValue("foo");
    Urn creator1 = Urns.createFromTypeSpecificString("test", "testCreator1");
    Urn impersonator1 = Urns.createFromTypeSpecificString("test", "testImpersonator1");
    Urn creator2 = Urns.createFromTypeSpecificString("test", "testCreator2");
    Urn impersonator2 = Urns.createFromTypeSpecificString("test", "testImpersonator2");
    addMetadataWithAuditStamp(urn, AspectFoo.class, 0, v0, _now, creator1.toString(),
        impersonator1.toString());
    AspectFoo v1 = new AspectFoo().setValue("bar");
    addMetadataWithAuditStamp(urn, AspectFoo.class, 1, v1, _now, creator2.toString(),
        impersonator2.toString());

    Optional<AspectWithExtraInfo<AspectFoo>> foo = dao.getWithExtraInfo(AspectFoo.class, urn, 1);

    assertTrue(foo.isPresent());
    assertEquals(foo.get(), new AspectWithExtraInfo<>(v1,
        new ExtraInfo().setAudit(makeAuditStamp(creator2, impersonator2, _now)).setVersion(1).setUrn(urn)));
  }

  @Test
  public void testGetLatestVersionForSoftDeletedAspect() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    FooUrn urn = makeFooUrn(1);
    AspectFoo v1 = new AspectFoo().setValue("foo");
    Urn creator1 = Urns.createFromTypeSpecificString("test", "testCreator1");
    Urn impersonator1 = Urns.createFromTypeSpecificString("test", "testImpersonator1");
    Urn creator2 = Urns.createFromTypeSpecificString("test", "testCreator2");
    Urn impersonator2 = Urns.createFromTypeSpecificString("test", "testImpersonator2");
    addMetadataWithAuditStamp(urn, AspectFoo.class, 0, null, _now, creator1.toString(),
        impersonator1.toString());
    addMetadataWithAuditStamp(urn, AspectFoo.class, 1, v1, _now, creator2.toString(),
        impersonator2.toString());

    Optional<AspectFoo> foo = dao.get(AspectFoo.class, urn);

    assertFalse(foo.isPresent());
  }

  @Test
  public void testGetNonLatestVersionForSoftDeletedAspect() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    FooUrn urn = makeFooUrn(1);
    AspectFoo v0 = new AspectFoo().setValue("foo");
    Urn creator1 = Urns.createFromTypeSpecificString("test", "testCreator1");
    Urn impersonator1 = Urns.createFromTypeSpecificString("test", "testImpersonator1");
    Urn creator2 = Urns.createFromTypeSpecificString("test", "testCreator2");
    Urn impersonator2 = Urns.createFromTypeSpecificString("test", "testImpersonator2");
    addMetadataWithAuditStamp(urn, AspectFoo.class, 0, v0, _now, creator1.toString(),
        impersonator1.toString());
    addMetadataWithAuditStamp(urn, AspectFoo.class, 1, null, _now, creator2.toString(),
        impersonator2.toString());

    Optional<AspectWithExtraInfo<AspectFoo>> foo = dao.getWithExtraInfo(AspectFoo.class, urn, 1);

    assertFalse(foo.isPresent());
  }

  @Test
  public void testListSoftDeletedAspectGivenUrn() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    List<AspectFoo> foos = new LinkedList<>();
    for (int i = 0; i < 3; i++) {
      FooUrn urn = makeFooUrn(i);

      for (int j = 0; j < 10; j++) {
        AspectFoo foo = new AspectFoo().setValue("foo" + j);
        addMetadata(urn, AspectFoo.class, j, foo);
        if (i == 0) {
          foos.add(foo);
        }
      }
      // soft delete the latest version
      dao.delete(urn, AspectFoo.class, _dummyAuditStamp);
    }

    FooUrn urn0 = makeFooUrn(0);

    ListResult<AspectFoo> results = dao.list(AspectFoo.class, urn0, 0, 5);

    if (!dao.isChangeLogEnabled()) {
      // if change log is not enabled, and the entity has been soft deleted, expect no more change history
      assertFalse(results.isHavingMore());
      return;
    }

    assertTrue(results.isHavingMore());
    assertEquals(results.getNextStart(), 5);
    assertEquals(results.getTotalCount(), 10);
    assertEquals(results.getPageSize(), 5);
    assertEquals(results.getTotalPageCount(), 2);
    assertEquals(results.getValues(), foos.subList(1, 6));

    assertNotNull(results.getMetadata());
    // latest version i.e. version=0 is soft deleted, hence will not be present in the metadata
    List<Long> expectedVersions = Arrays.asList(1L, 2L, 3L, 4L, 5L);
    List<Urn> expectedUrns = Collections.singletonList(urn0);
    assertVersionMetadata(results.getMetadata(), expectedVersions, expectedUrns, _now,
        Urns.createFromTypeSpecificString("test", "foo"), Urns.createFromTypeSpecificString("test", "bar"));

    // List next page
    results = dao.list(AspectFoo.class, urn0, 5, 9);

    assertFalse(results.isHavingMore());
    assertEquals(results.getNextStart(), ListResult.INVALID_NEXT_START);
    assertEquals(results.getTotalCount(), 10);
    assertEquals(results.getPageSize(), 9);
    assertEquals(results.getTotalPageCount(), 2);
    assertEquals(results.getValues().subList(0, 4), foos.subList(6, 10));
    assertEquals(results.getValues().get(4), foos.get(0));
    assertNotNull(results.getMetadata());
  }

  @Test
  public void testListSpecificVersionSoftDeletedAspect() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    for (int i = 0; i < 3; i++) {
      FooUrn urn = makeFooUrn(i);

      for (int j = 0; j < 9; j++) {
        AspectFoo foo = new AspectFoo().setValue("foo" + j);
        dao.add(urn, foo, _dummyAuditStamp);
      }
      // soft delete metadata
      dao.delete(urn, AspectFoo.class, _dummyAuditStamp);
      // add again
      AspectFoo latest = new AspectFoo().setValue("latest");
      dao.add(urn, latest, _dummyAuditStamp);
    }

    // version=10 corresponds to soft deleted aspect
    if (!dao.isChangeLogEnabled()) {
      // version based query is not applicable if
      try {
        dao.list(AspectFoo.class, 10, 0, 2);
        fail("UnsupportedOperationException should be thrown");
      } catch (UnsupportedOperationException uoe) {
        // expected, do nothing
      }
      return;
    } else {
      ListResult<AspectFoo> results = dao.list(AspectFoo.class, 10, 0, 2);
      assertFalse(results.isHavingMore());
      assertEquals(results.getNextStart(), -1);
      assertEquals(results.getTotalCount(), 0);
      assertEquals(results.getPageSize(), 2);
      assertEquals(results.getTotalPageCount(), 0);
      assertEquals(results.getValues().size(), 0);
    }
  }

  @Test
  public void testGetSoftDeletedAspect() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    FooUrn urn = makeFooUrn(1);
    String aspectName = ModelUtils.getAspectName(AspectFoo.class);
    AspectFoo v1 = new AspectFoo().setValue("foo");
    AspectFoo v0 = new AspectFoo().setValue("bar");

    dao.add(urn, v1, _dummyAuditStamp);
    dao.add(urn, v0, _dummyAuditStamp);
    dao.delete(urn, AspectFoo.class, _dummyAuditStamp);

    // latest version of metadata should be null
    EbeanMetadataAspect aspect = getMetadata(urn, aspectName, 0);
    assertTrue(isSoftDeletedAspect(aspect, AspectFoo.class));
    Optional<AspectFoo> fooOptional = dao.get(AspectFoo.class, urn);
    assertFalse(fooOptional.isPresent());

    if (dao.isChangeLogEnabled()) {
      // version=1 should be non-null
      fooOptional = dao.get(AspectFoo.class, urn, 1);
      assertTrue(fooOptional.isPresent());
      assertEquals(fooOptional.get(), v1);

      // version=2 should be non-null
      fooOptional = dao.get(AspectFoo.class, urn, 2);
      assertTrue(fooOptional.isPresent());
      assertEquals(fooOptional.get(), v0);
    }

    InOrder inOrder = inOrder(_mockProducer);
    inOrder.verify(_mockProducer, times(1)).produceMetadataAuditEvent(urn, null, v1);
    inOrder.verify(_mockProducer, times(1)).produceMetadataAuditEvent(urn, v1, v0);
    // TODO: verify that MAE was produced with newValue set as null for soft deleted aspect
    verifyNoMoreInteractions(_mockProducer);
  }

  @Test
  public void testSoftDeletedAspectWithNoExistingMetadata() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    FooUrn urn = makeFooUrn(2);
    String aspectName = ModelUtils.getAspectName(AspectFoo.class);

    // no metadata already exists
    dao.delete(urn, AspectFoo.class, _dummyAuditStamp);

    // since there is nothing to delete, no metadata will be saved
    EbeanMetadataAspect aspect = getMetadata(urn, aspectName, 0);
    assertNull(aspect);
    Optional<AspectFoo> fooOptional = dao.get(AspectFoo.class, urn);
    assertFalse(fooOptional.isPresent());

    // no MAE will be produced
    verifyNoMoreInteractions(_mockProducer);
  }

  @Test
  public void testListVersionsForSoftDeletedAspect() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    FooUrn urn = makeFooUrn(1);
    for (long i = 0; i < 6; i++) {
      AspectFoo foo = new AspectFoo().setValue("foo" + i);
      addMetadata(urn, AspectFoo.class, i, foo);
    }
    // soft delete the latest version
    dao.delete(urn, AspectFoo.class, _dummyAuditStamp);

    ListResult<Long> results = dao.listVersions(AspectFoo.class, urn, 0, 5);

    if (!dao.isChangeLogEnabled()) {
      // When: change log is disabled,
      // Expect list version will return empty if the entity has been soft-deleted.
      assertFalse(results.isHavingMore());
      assertTrue(results.getValues().isEmpty());
      return;
    }

    assertTrue(results.isHavingMore());
    assertEquals(results.getNextStart(), 5);
    assertEquals(results.getTotalCount(), 6);
    assertEquals(results.getPageSize(), 5);
    assertEquals(results.getTotalPageCount(), 2);
    assertEquals(results.getValues(), Arrays.asList(1L, 2L, 3L, 4L, 5L));

    // List last page
    results = dao.listVersions(AspectFoo.class, urn, 5, 10);

    assertFalse(results.isHavingMore());
    assertEquals(results.getNextStart(), ListResult.INVALID_NEXT_START);
    assertEquals(results.getTotalCount(), 6);
    assertEquals(results.getPageSize(), 10);
    assertEquals(results.getTotalPageCount(), 1);
    assertEquals(results.getValues(), Collections.singletonList(6L));
  }

  @Test
  public void testListUrnsForSoftDeletedAspect() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    for (int i = 0; i < 3; i++) {
      FooUrn urn = makeFooUrn(i);
      for (int j = 0; j < 3; j++) {
        AspectFoo foo = new AspectFoo().setValue("foo" + j);
        dao.add(urn, foo, _dummyAuditStamp);
      }
      // soft delete the latest version of aspect
      dao.delete(urn, AspectFoo.class, _dummyAuditStamp);
    }

    ListResult<FooUrn> results = dao.listUrns(AspectFoo.class, 0, 1);

    assertFalse(results.isHavingMore());
    assertEquals(results.getNextStart(), -1);
    assertEquals(results.getTotalCount(), 0);
    assertEquals(results.getPageSize(), 1);
    assertEquals(results.getTotalPageCount(), 0);
    assertEquals(results.getValues().size(), 0);
  }

  @Test
  public void testListUrnsAfterUndeleteSoftDeletedAspect() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);

    List<FooUrn> urns = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      FooUrn urn = makeFooUrn(i);
      for (int j = 0; j < 3; j++) {
        AspectFoo foo = new AspectFoo().setValue("foo" + j);
        dao.add(urn, foo, _dummyAuditStamp);
      }
      // soft delete the latest version of aspect
      dao.delete(urn, AspectFoo.class, _dummyAuditStamp);
      AspectFoo latest = new AspectFoo().setValue("val");
      dao.add(urn, latest, _dummyAuditStamp);
      urns.add(urn);
    }

    ListResult<FooUrn> results = dao.listUrns(AspectFoo.class, 0, 1);

    assertTrue(results.isHavingMore());
    assertEquals(results.getNextStart(), 1);
    assertEquals(results.getTotalCount(), 3);
    assertEquals(results.getPageSize(), 1);
    assertEquals(results.getTotalPageCount(), 3);
    assertEquals(results.getValues(), urns.subList(0, 1));
  }

  @Test
  public void testGetWithKeysSoftDeletedAspect() {
    // given
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    FooUrn fooUrn = makeFooUrn(1);

    // both aspect keys exist
    AspectKey<FooUrn, AspectFoo> aspectKey1 = new AspectKey<>(AspectFoo.class, fooUrn, 1L);
    AspectKey<FooUrn, AspectBar> aspectKey2 = new AspectKey<>(AspectBar.class, fooUrn, 0L);

    // add metadata
    addMetadata(fooUrn, AspectFoo.class, 1, null);
    AspectBar barV0 = new AspectBar().setValue("bar");
    addMetadata(fooUrn, AspectBar.class, 0, barV0);

    // when
    Map<AspectKey<FooUrn, ? extends RecordTemplate>, Optional<? extends RecordTemplate>> records =
        dao.get(new HashSet<>(Arrays.asList(aspectKey1, aspectKey2)));

    // then
    assertEquals(records.size(), 2);
    assertEquals(records.get(aspectKey1), Optional.empty());
    assertEquals(records.get(aspectKey2), Optional.of(barV0));
  }

  @Test
  public void testUndeleteSoftDeletedAspect() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    FooUrn urn = makeFooUrn(1);
    String aspectName = ModelUtils.getAspectName(AspectFoo.class);
    AspectFoo v1 = new AspectFoo().setValue("foo");
    AspectFoo v0 = new AspectFoo().setValue("bar");

    dao.add(urn, v1, _dummyAuditStamp);
    dao.add(urn, v0, _dummyAuditStamp);
    // soft delete the aspect
    dao.delete(urn, AspectFoo.class, _dummyAuditStamp);

    // next undelete the soft deleted aspect
    AspectFoo foo = new AspectFoo().setValue("baz");
    dao.add(urn, foo, _dummyAuditStamp);
    Optional<AspectFoo> fooOptional = dao.get(AspectFoo.class, urn);
    // latest version of metadata should be non-null and correspond to the metadata added after soft deleting the aspect

    if (!dao.isChangeLogEnabled() && _schemaConfig != SchemaConfig.NEW_SCHEMA_ONLY) {
      // skip if change log is disabled and schemaConfig is not NEW_SCHEMA_ONLY
    } else {
      assertTrue(fooOptional.isPresent());
      assertEquals(fooOptional.get(), foo);
    }

    if (dao.isChangeLogEnabled()) {
      // version=3 should correspond to soft deleted metadata
      EbeanMetadataAspect aspect = getMetadata(urn, aspectName, 3);
      assertTrue(isSoftDeletedAspect(aspect, AspectFoo.class));
      fooOptional = dao.get(AspectFoo.class, urn, 3);
      assertFalse(fooOptional.isPresent());

      // version=2 should be non-null
      fooOptional = dao.get(AspectFoo.class, urn, 2);
      assertTrue(fooOptional.isPresent());
      assertEquals(fooOptional.get(), v0);

      // version=1 should be non-null again
      fooOptional = dao.get(AspectFoo.class, urn, 1);
      assertTrue(fooOptional.isPresent());
      assertEquals(fooOptional.get(), v1);
    }

    InOrder inOrder = inOrder(_mockProducer);
    inOrder.verify(_mockProducer, times(1)).produceMetadataAuditEvent(urn, null, v1);
    inOrder.verify(_mockProducer, times(1)).produceMetadataAuditEvent(urn, v1, v0);
    inOrder.verify(_mockProducer, times(1)).produceMetadataAuditEvent(urn, null, foo);
    // TODO: verify that MAE was produced with newValue set as null for soft deleted aspect
    verifyNoMoreInteractions(_mockProducer);
  }

  // common setup logic to the next two tests for relationship removal
  private void setupAspectsAndRelationships(
      FooUrn fooUrn,
      EbeanLocalDAO<EntityAspectUnion, FooUrn> fooDao) throws URISyntaxException {
    // necessary flag to prevent removal of existing same-type relationships in "another aspect"
    fooDao.setUseAspectColumnForRelationshipRemoval(true);

    EbeanLocalDAO<EntityAspectUnion, BarUrn> barDao = createDao(BarUrn.class);

    // add an aspect (AspectFooBar) which includes BelongsTo relationships and ReportsTo relationships
    BarUrn barUrn1 = BarUrn.createFromString("urn:li:bar:1");
    BelongsToV2 belongsTo1 = new BelongsToV2().setDestination(BelongsToV2.Destination.create(barUrn1.toString()));
    BarUrn barUrn2 = BarUrn.createFromString("urn:li:bar:2");
    BelongsToV2 belongsTo2 = new BelongsToV2().setDestination(BelongsToV2.Destination.create(barUrn2.toString()));
    BarUrn barUrn3 = BarUrn.createFromString("urn:li:bar:3");
    BelongsToV2 belongsTo3 = new BelongsToV2().setDestination(BelongsToV2.Destination.create(barUrn3.toString()));
    BelongsToV2Array belongsToArray = new BelongsToV2Array(belongsTo1, belongsTo2, belongsTo3);
    ReportsTo reportsTo = new ReportsTo().setSource(fooUrn).setDestination(barUrn1);
    ReportsToArray reportsToArray = new ReportsToArray(reportsTo);
    AspectFooBar aspectFooBar = new AspectFooBar()
        .setBars(new BarUrnArray(barUrn1, barUrn2, barUrn3)).setBelongsTos(belongsToArray).setReportsTos(reportsToArray);
    AuditStamp auditStamp = makeAuditStamp("foo", System.currentTimeMillis());

    fooDao.add(fooUrn, aspectFooBar, auditStamp);
    barDao.add(barUrn1, new AspectFoo().setValue("1"), auditStamp);
    barDao.add(barUrn2, new AspectFoo().setValue("2"), auditStamp);
    barDao.add(barUrn3, new AspectFoo().setValue("3"), auditStamp);

    // add an aspect (AspectFooBaz) which includes BelongsTo relationships
    BarUrn barUrn4 = BarUrn.createFromString("urn:li:bar:4");
    BelongsToV2 belongsTo4 = new BelongsToV2().setDestination(BelongsToV2.Destination.create(barUrn4.toString()));
    BelongsToV2Array belongsToArray2 = new BelongsToV2Array(belongsTo4);
    AspectFooBaz aspectFooBaz = new AspectFooBaz().setBars(new BarUrnArray(barUrn4)).setBelongsTos(belongsToArray2);

    fooDao.add(fooUrn, aspectFooBaz, auditStamp);
    barDao.add(barUrn4, new AspectFoo().setValue("4"), auditStamp);
  }

  @Test
  public void testRemoveRelationshipsDuringAspectSoftDeletion() throws URISyntaxException {
    FooUrn fooUrn = makeFooUrn(1);
    EbeanLocalDAO<EntityAspectUnion, FooUrn> fooDao = createDao(FooUrn.class);

    setupAspectsAndRelationships(fooUrn, fooDao);

    // Verify local relationships and entities are added.
    EbeanLocalRelationshipQueryDAO ebeanLocalRelationshipQueryDAO = new EbeanLocalRelationshipQueryDAO(_server);
    ebeanLocalRelationshipQueryDAO.setSchemaConfig(_schemaConfig);

    List<BelongsToV2> resultBelongsTos =
        ebeanLocalRelationshipQueryDAO.findRelationships(FooSnapshot.class, EMPTY_FILTER, BarSnapshot.class,
            EMPTY_FILTER, BelongsToV2.class, OUTGOING_FILTER, 0, 10);

    assertEquals(resultBelongsTos.size(), 4);

    List<ReportsTo> resultReportsTos =
        ebeanLocalRelationshipQueryDAO.findRelationships(FooSnapshot.class, EMPTY_FILTER, BarSnapshot.class,
            EMPTY_FILTER, ReportsTo.class, OUTGOING_FILTER, 0, 10);

    assertEquals(resultReportsTos.size(), 1);

    AspectKey<FooUrn, AspectFooBar> key = new AspectKey<>(AspectFooBar.class, fooUrn, 0L);
    List<EbeanMetadataAspect> aspects = fooDao.batchGetHelper(Collections.singletonList(key), 1, 0);

    assertEquals(aspects.size(), 1);

    // soft delete the AspectFooBar aspect
    fooDao.delete(fooUrn, AspectFooBar.class, _dummyAuditStamp);

    // check that the belongsTo relationships 1, 2, & 3 were soft deleted
    resultBelongsTos = ebeanLocalRelationshipQueryDAO.findRelationships(FooSnapshot.class, EMPTY_FILTER, BarSnapshot.class,
            EMPTY_FILTER, BelongsToV2.class, OUTGOING_FILTER, 0, 10);

    // since we only deleted 1 of the 2 Aspects with BelongsTo relationships, we should still have 1 BelongsTo relationship
    assertEquals(resultBelongsTos.size(), 1);

    // check that the reportsTo relationship was soft deleted
    resultReportsTos =
        ebeanLocalRelationshipQueryDAO.findRelationships(FooSnapshot.class, EMPTY_FILTER, BarSnapshot.class,
            EMPTY_FILTER, ReportsTo.class, OUTGOING_FILTER, 0, 10);

    assertEquals(resultReportsTos.size(), 0);

    // check that the AspectFooBar aspect was soft deleted
    Optional<AspectWithExtraInfo<AspectFooBar>> optionalAspect = fooDao.getWithExtraInfo(AspectFooBar.class, fooUrn, 0L);
    assertFalse(optionalAspect.isPresent());
  }

  // basically a copy of the above test but makes use of the deleteMany() call
  @Test
  public void testDeleteManyWithRelationshipRemoval() throws URISyntaxException {
    FooUrn fooUrn = makeFooUrn(1);
    EbeanLocalDAO<EntityAspectUnion, FooUrn> fooDao = createDao(FooUrn.class);
    // necessary flag to prevent removal of existing same-type relationships in "another aspect"
    fooDao.setUseAspectColumnForRelationshipRemoval(true);

    EbeanLocalDAO<EntityAspectUnion, BarUrn> barDao = createDao(BarUrn.class);

    // add an aspect (AspectFooBar) which includes BelongsTo relationships and ReportsTo relationships
    BarUrn barUrn1 = BarUrn.createFromString("urn:li:bar:1");
    BelongsToV2 belongsTo1 = new BelongsToV2().setDestination(BelongsToV2.Destination.create(barUrn1.toString()));
    BarUrn barUrn2 = BarUrn.createFromString("urn:li:bar:2");
    BelongsToV2 belongsTo2 = new BelongsToV2().setDestination(BelongsToV2.Destination.create(barUrn2.toString()));
    BarUrn barUrn3 = BarUrn.createFromString("urn:li:bar:3");
    BelongsToV2 belongsTo3 = new BelongsToV2().setDestination(BelongsToV2.Destination.create(barUrn3.toString()));
    BelongsToV2Array belongsToArray = new BelongsToV2Array(belongsTo1, belongsTo2, belongsTo3);
    ReportsTo reportsTo = new ReportsTo().setSource(fooUrn).setDestination(barUrn1);
    ReportsToArray reportsToArray = new ReportsToArray(reportsTo);
    AspectFooBar aspectFooBar = new AspectFooBar()
        .setBars(new BarUrnArray(barUrn1, barUrn2, barUrn3)).setBelongsTos(belongsToArray).setReportsTos(reportsToArray);
    AuditStamp auditStamp = makeAuditStamp("foo", System.currentTimeMillis());

    fooDao.add(fooUrn, aspectFooBar, auditStamp);
    barDao.add(barUrn1, new AspectFoo().setValue("1"), auditStamp);
    barDao.add(barUrn2, new AspectFoo().setValue("2"), auditStamp);
    barDao.add(barUrn3, new AspectFoo().setValue("3"), auditStamp);

    // add an aspect (AspectFooBaz) which includes BelongsTo relationships
    BarUrn barUrn4 = BarUrn.createFromString("urn:li:bar:4");
    BelongsToV2 belongsTo4 = new BelongsToV2().setDestination(BelongsToV2.Destination.create(barUrn4.toString()));
    BelongsToV2Array belongsToArray2 = new BelongsToV2Array(belongsTo4);
    AspectFooBaz aspectFooBaz = new AspectFooBaz().setBars(new BarUrnArray(barUrn4)).setBelongsTos(belongsToArray2);

    fooDao.add(fooUrn, aspectFooBaz, auditStamp);
    barDao.add(barUrn4, new AspectFoo().setValue("4"), auditStamp);

    // Verify local relationships and entities are added.
    EbeanLocalRelationshipQueryDAO ebeanLocalRelationshipQueryDAO = new EbeanLocalRelationshipQueryDAO(_server);
    ebeanLocalRelationshipQueryDAO.setSchemaConfig(_schemaConfig);

    List<BelongsToV2> resultBelongsTos =
        ebeanLocalRelationshipQueryDAO.findRelationships(FooSnapshot.class, EMPTY_FILTER, BarSnapshot.class,
            EMPTY_FILTER, BelongsToV2.class, OUTGOING_FILTER, 0, 10);

    assertEquals(resultBelongsTos.size(), 4);

    List<ReportsTo> resultReportsTos =
        ebeanLocalRelationshipQueryDAO.findRelationships(FooSnapshot.class, EMPTY_FILTER, BarSnapshot.class,
            EMPTY_FILTER, ReportsTo.class, OUTGOING_FILTER, 0, 10);

    assertEquals(resultReportsTos.size(), 1);

    AspectKey<FooUrn, AspectFooBar> key = new AspectKey<>(AspectFooBar.class, fooUrn, 0L);
    List<EbeanMetadataAspect> aspects = fooDao.batchGetHelper(Collections.singletonList(key), 1, 0);

    assertEquals(aspects.size(), 1);

    // soft delete the AspectFooBar and AspectFooBaz aspects
    Collection<EntityAspectUnion> deletedAspects =
        fooDao.deleteMany(fooUrn, new HashSet<>(Arrays.asList(AspectFooBar.class, AspectFooBaz.class)), _dummyAuditStamp);

    assertEquals(deletedAspects.size(), 2);

    // check that the AspectFooBar content returned matches the pre-deletion content
    Optional<EntityAspectUnion> aspectFooBarDeleted = deletedAspects.stream()
        .filter(EntityAspectUnion::isAspectFooBar)
        .findFirst();
    assertTrue(aspectFooBarDeleted.isPresent());
    assertEquals(aspectFooBarDeleted.get().getAspectFooBar(), aspectFooBar);

    // check that the AspectFooBaz content returned matches the pre-deletion content
    Optional<EntityAspectUnion> aspectFooBazDeleted = deletedAspects.stream()
        .filter(EntityAspectUnion::isAspectFooBaz)
        .findFirst();
    assertTrue(aspectFooBazDeleted.isPresent());
    assertEquals(aspectFooBazDeleted.get().getAspectFooBaz(), aspectFooBaz);

    // check that the belongsTo relationships 1, 2, 3, and 4 were soft deleted
    resultBelongsTos = ebeanLocalRelationshipQueryDAO.findRelationships(FooSnapshot.class, EMPTY_FILTER, BarSnapshot.class,
        EMPTY_FILTER, BelongsToV2.class, OUTGOING_FILTER, 0, 10);

    assertEquals(resultBelongsTos.size(), 0);

    // check that the reportsTo relationship was soft deleted
    resultReportsTos =
        ebeanLocalRelationshipQueryDAO.findRelationships(FooSnapshot.class, EMPTY_FILTER, BarSnapshot.class,
            EMPTY_FILTER, ReportsTo.class, OUTGOING_FILTER, 0, 10);

    assertEquals(resultReportsTos.size(), 0);

    // check that the AspectFooBar aspect was soft deleted
    Optional<AspectWithExtraInfo<AspectFooBar>> optionalAspect = fooDao.getWithExtraInfo(AspectFooBar.class, fooUrn, 0L);
    assertFalse(optionalAspect.isPresent());

    // check that the AspectFooBaz aspect was soft deleted
    Optional<AspectWithExtraInfo<AspectFooBaz>> optionalAspect2 = fooDao.getWithExtraInfo(AspectFooBaz.class, fooUrn, 0L);
    assertFalse(optionalAspect2.isPresent());
  }

  @Test
  public void testDeleteWithReturnOnNonexistentAsset() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    FooUrn urn = makeFooUrn(1);

    AspectFoo foo = dao.deleteWithReturn(urn, AspectFoo.class, _dummyAuditStamp, 3, null);
    assertNull(foo);
  }

  @Test
  public void testDeleteWithReturnOnNullAspect() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    FooUrn urn = makeFooUrn(1);

    // add aspect so the row exists in the entity table, but the column for other aspects will be empty
    AspectFoo v0 = new AspectFoo().setValue("foo");
    dao.add(urn, v0, _dummyAuditStamp);

    // attempt to delete an aspect that doesn't exist
    AspectBar foo = dao.deleteWithReturn(urn, AspectBar.class, _dummyAuditStamp, 3, null);
    assertNull(foo);
  }

  @Test
  public void testDeleteWithReturnOnAlreadyDeletedAspect() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    FooUrn urn = makeFooUrn(1);
    AspectFoo v0 = new AspectFoo().setValue("foo");
    dao.add(urn, v0, _dummyAuditStamp);
    AspectFoo foo = dao.deleteWithReturn(urn, AspectFoo.class, _dummyAuditStamp, 3, null);

    // make sure that the content matches the original
    assertEquals(foo, v0);

    // attempt to delete an aspect that has already been deleted
    AspectFoo fooAgain = dao.deleteWithReturn(urn, AspectFoo.class, _dummyAuditStamp, 3, null);
    assertNull(fooAgain);
  }

  @Test
  public void testDeleteManyOnNonexistentAsset() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    FooUrn urn = makeFooUrn(1);

    Collection<EntityAspectUnion> deletionResults =
        dao.deleteMany(urn, new HashSet<>(Collections.singletonList(AspectFoo.class)), _dummyAuditStamp, 3, null);

    // make sure return collection is empty
    assertEquals(deletionResults.size(), 0);
  }

  @Test
  public void testDeleteManyOnNullAspect() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    FooUrn urn = makeFooUrn(1);

    // add aspect so the row exists in the entity table, but the column for other aspects will be empty
    AspectFoo v0 = new AspectFoo().setValue("foo");
    dao.add(urn, v0, _dummyAuditStamp);

    // attempt to delete an aspect that doesn't exist
    Collection<EntityAspectUnion> deletionResults =
        dao.deleteMany(urn, new HashSet<>(Collections.singletonList(AspectBar.class)), _dummyAuditStamp, 3, null);

    // make sure return collection is empty
    assertEquals(deletionResults.size(), 0);
  }

  @Test
  public void testDeleteManyOnAlreadyDeletedAspect() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    FooUrn urn = makeFooUrn(1);
    AspectFoo v0 = new AspectFoo().setValue("foo");
    dao.add(urn, v0, _dummyAuditStamp);

    // delete the aspect
    Collection<EntityAspectUnion> deletionResults =
        dao.deleteMany(urn, new HashSet<>(Collections.singletonList(AspectFoo.class)), _dummyAuditStamp, 3, null);
    assertEquals(deletionResults.size(), 1);

    // make sure that the content matches the original
    Optional<EntityAspectUnion> aspectFooDeleted = deletionResults.stream()
        .filter(EntityAspectUnion::isAspectFoo)
        .findFirst();
    assertTrue(aspectFooDeleted.isPresent());
    assertEquals(aspectFooDeleted.get().getAspectFoo(), v0);

    // attempt to delete an aspect that has already been deleted
    Collection<EntityAspectUnion> deletionResultsAgain =
        dao.deleteMany(urn, new HashSet<>(Collections.singletonList(AspectFoo.class)), _dummyAuditStamp, 3, null);

    // make sure return collection is empty
    assertEquals(deletionResultsAgain.size(), 0);
  }

  @Test
  public void testGetWithExtraInfoMultipleKeys() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    FooUrn urn = makeFooUrn(1);
    Urn creator1 = Urns.createFromTypeSpecificString("test", "testCreator1");
    Urn impersonator1 = Urns.createFromTypeSpecificString("test", "testImpersonator1");
    Urn creator2 = Urns.createFromTypeSpecificString("test", "testCreator2");
    Urn impersonator2 = Urns.createFromTypeSpecificString("test", "testImpersonator2");
    Urn creator3 = Urns.createFromTypeSpecificString("test", "testCreator3");
    Urn impersonator3 = Urns.createFromTypeSpecificString("test", "testImpersonator3");
    AspectFoo fooV0 = new AspectFoo().setValue("foo");
    addMetadataWithAuditStamp(urn, AspectFoo.class, 0, fooV0, _now, creator1.toString(),
        impersonator1.toString());
    AspectFoo fooV1 = new AspectFoo().setValue("bar");
    addMetadataWithAuditStamp(urn, AspectFoo.class, 1, fooV1, _now, creator2.toString(),
        impersonator2.toString());
    AspectBar barV0 = new AspectBar().setValue("bar");
    addMetadataWithAuditStamp(urn, AspectBar.class, 0, barV0, _now, creator3.toString(),
        impersonator3.toString());

    // both aspect keys exist
    AspectKey<FooUrn, AspectFoo> aspectKey1 = new AspectKey<>(AspectFoo.class, urn, 1L);
    AspectKey<FooUrn, AspectBar> aspectKey2 = new AspectKey<>(AspectBar.class, urn, 0L);

    Map<AspectKey<FooUrn, ? extends RecordTemplate>, AspectWithExtraInfo<? extends RecordTemplate>> result =
        dao.getWithExtraInfo(new HashSet<>(Arrays.asList(aspectKey1, aspectKey2)));

    assertEquals(result.keySet().size(), 2);
    assertEquals(result.get(aspectKey1), new AspectWithExtraInfo<>(fooV1,
        new ExtraInfo().setAudit(makeAuditStamp(creator2, impersonator2, _now)).setVersion(1).setUrn(urn)));
    assertEquals(result.get(aspectKey2), new AspectWithExtraInfo<>(barV0,
        new ExtraInfo().setAudit(makeAuditStamp(creator3, impersonator3, _now)).setVersion(0).setUrn(urn)));

    // one of the aspect keys does not exist
    AspectKey<FooUrn, AspectBar> aspectKey3 = new AspectKey<>(AspectBar.class, urn, 1L);

    result = dao.getWithExtraInfo(new HashSet<>(Arrays.asList(aspectKey1, aspectKey3)));

    assertEquals(result.keySet().size(), 1);
    assertEquals(result.get(aspectKey1), new AspectWithExtraInfo<>(fooV1,
        new ExtraInfo().setAudit(makeAuditStamp(creator2, impersonator2, _now)).setVersion(1).setUrn(urn)));
  }

  @Test
  public void testGetWithKeysCount() {
    // given
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);

    FooUrn fooUrn = makeFooUrn(1);

    // both aspect keys exist
    AspectKey<FooUrn, AspectFoo> aspectKey1 = new AspectKey<>(AspectFoo.class, fooUrn, 1L);
    AspectKey<FooUrn, AspectBar> aspectKey2 = new AspectKey<>(AspectBar.class, fooUrn, 0L);

    // add metadata
    AspectFoo fooV1 = new AspectFoo().setValue("foo");
    addMetadata(fooUrn, AspectFoo.class, 1, fooV1);
    AspectBar barV0 = new AspectBar().setValue("bar");
    addMetadata(fooUrn, AspectBar.class, 0, barV0);

    // batch get without query keys count set
    // when
    Map<AspectKey<FooUrn, ? extends RecordTemplate>, Optional<? extends RecordTemplate>> records =
        dao.get(new HashSet<>(Arrays.asList(aspectKey1, aspectKey2)));

    // then
    assertEquals(records.size(), 2);
  }

  @Test
  public void testCountAggregate() {
    if (_schemaConfig == SchemaConfig.OLD_SCHEMA_ONLY) {
      return;
    }

    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    FooUrn urn1 = makeFooUrn(1);
    FooUrn urn2 = makeFooUrn(2);
    FooUrn urn3 = makeFooUrn(3);
    String aspect1 = AspectFoo.class.getCanonicalName();
    String aspect2 = AspectBaz.class.getCanonicalName();

    Set<String> addedColumns = new HashSet<>();
    addIndex(urn1, aspect1, "/value", "valB");
    addIndex(urn1, aspect2, "/stringField", "valC");
    addIndex(urn1, aspect2, "/longField", 10);
    addIndex(urn1, aspect2, "/doubleField", 1.2);
    addIndex(urn1, aspect2, "/recordField/value", "nestedC");
    addIndex(urn1, FooUrn.class.getCanonicalName(), "/fooId", 1);

    addIndex(urn2, aspect1, "/value", "valB");
    addIndex(urn2, aspect2, "/stringField", "valC");
    addIndex(urn2, aspect2, "/longField", 8);
    addIndex(urn2, aspect2, "/doubleField", 1.2);
    addIndex(urn2, aspect2, "/recordField/value", "nestedB");
    addIndex(urn2, FooUrn.class.getCanonicalName(), "/fooId", 2);

    addIndex(urn3, aspect1, "/value", "valA");
    addIndex(urn3, aspect2, "/stringField", "valC");
    addIndex(urn3, aspect2, "/longField", 100);
    addIndex(urn3, aspect2, "/doubleField", 1.2);
    addIndex(urn3, aspect2, "/recordField/value", "nestedA");
    addIndex(urn3, FooUrn.class.getCanonicalName(), "/fooId", 3);

    // group by string
    IndexValue indexValue1 = new IndexValue();
    indexValue1.setString("val");
    IndexCriterion criterion1 = new IndexCriterion().setAspect(aspect1)
        .setPathParams(new IndexPathParams().setPath("/value").setValue(indexValue1).setCondition(Condition.START_WITH));

    IndexCriterionArray indexCriterionArray1 = new IndexCriterionArray(Collections.singletonList(criterion1));
    final IndexFilter indexFilter1 = new IndexFilter().setCriteria(indexCriterionArray1);
    IndexGroupByCriterion indexGroupByCriterion1 = new IndexGroupByCriterion().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value");

    Map<String, Long> result = dao.countAggregate(indexFilter1, indexGroupByCriterion1);
    assertEquals(result.size(), 2);
    assertEquals(result.get("valB").longValue(), 2);
    assertEquals(result.get("valA").longValue(), 1);

    // group by string and filter
    IndexValue indexValue2 = new IndexValue();
    indexValue2.setInt(10);
    IndexCriterion criterion2 = new IndexCriterion().setAspect(aspect2)
        .setPathParams(new IndexPathParams().setPath("/longField").setValue(indexValue2).setCondition(Condition.GREATER_THAN_OR_EQUAL_TO));

    IndexCriterionArray indexCriterionArray2 = new IndexCriterionArray(Collections.singletonList(criterion2));
    final IndexFilter indexFilter2 = new IndexFilter().setCriteria(indexCriterionArray2);
    result = dao.countAggregate(indexFilter2, indexGroupByCriterion1);
    assertEquals(result.size(), 2);
    assertEquals(result.get("valB").longValue(), 1);
    assertEquals(result.get("valA").longValue(), 1);

    // group by nested field
    IndexGroupByCriterion indexGroupByCriterion2 = new IndexGroupByCriterion().setAspect(AspectBaz.class.getCanonicalName())
        .setPath("/recordField/value");

    result = dao.countAggregate(indexFilter1, indexGroupByCriterion2);
    assertEquals(result.size(), 3);
    assertEquals(result.get("nestedA").longValue(), 1);
    assertEquals(result.get("nestedB").longValue(), 1);
    assertEquals(result.get("nestedC").longValue(), 1);

    // group by long field
    IndexGroupByCriterion indexGroupByCriterion3 = new IndexGroupByCriterion().setAspect(AspectBaz.class.getCanonicalName())
        .setPath("/longField");

    result = dao.countAggregate(indexFilter1, indexGroupByCriterion3);
    assertEquals(result.size(), 3);
    assertEquals(result.get("10").longValue(), 1);
    assertEquals(result.get("8").longValue(), 1);
    assertEquals(result.get("100").longValue(), 1);

    // group by double field
    IndexGroupByCriterion indexGroupByCriterion4 = new IndexGroupByCriterion().setAspect(AspectBaz.class.getCanonicalName())
        .setPath("/doubleField");

    result = dao.countAggregate(indexFilter1, indexGroupByCriterion4);
    assertEquals(result.size(), 1);
    assertEquals(result.get("1.2").longValue(), 3);

    // group by an aspect and path that the filtered results do not contain
    IndexGroupByCriterion indexGroupByCriterion5 = new IndexGroupByCriterion().setAspect(AspectBaz.class.getCanonicalName())
        .setPath("/enumField");

    result = dao.countAggregate(indexFilter1, indexGroupByCriterion5);
    assertEquals(result.size(), 0);

    // filter by condition that are both strings and group by 1 of them
    IndexValue indexValue3 = new IndexValue();
    indexValue3.setString("valC");
    IndexCriterion criterion3 = new IndexCriterion().setAspect(aspect2)
        .setPathParams(new IndexPathParams().setPath("/stringField").setValue(indexValue3).setCondition(Condition.START_WITH));

    IndexValue indexValue4 = new IndexValue();
    indexValue4.setString("valB");
    IndexCriterion criterion4 = new IndexCriterion().setAspect(aspect1)
        .setPathParams(new IndexPathParams().setPath("/value").setValue(indexValue4).setCondition(Condition.START_WITH));

    IndexCriterionArray indexCriterionArray3 = new IndexCriterionArray(Arrays.asList(criterion3, criterion4));
    final IndexFilter indexFilter3 = new IndexFilter().setCriteria(indexCriterionArray3);

    result = dao.countAggregate(indexFilter3, indexGroupByCriterion1);
    assertEquals(result.size(), 1);
    assertEquals(result.get("valB").longValue(), 2);

    // in filter
    IndexValue indexValue5 = new IndexValue();
    indexValue5.setArray(new StringArray("valA", "valB"));
    IndexCriterion criterion5 = new IndexCriterion().setAspect(aspect1)
        .setPathParams(new IndexPathParams().setPath("/value").setValue(indexValue5).setCondition(Condition.IN));

    IndexCriterionArray indexCriterionArray4 = new IndexCriterionArray(Collections.singletonList(criterion5));
    final IndexFilter indexFilter4 = new IndexFilter().setCriteria(indexCriterionArray4);

    result = dao.countAggregate(indexFilter4, indexGroupByCriterion1);
    List<FooUrn> test = dao.listUrns(indexFilter4, null, null, 10);
    assertEquals(test.size(), 3);
    assertEquals(result.size(), 2);
    assertEquals(result.get("valB").longValue(), 2);
    assertEquals(result.get("valA").longValue(), 1);
  }

  @Test
  public void testNegativeIsInvalidKeyCount() {
    // given
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);

    // expect
    assertThrows(IllegalArgumentException.class, () -> dao.setQueryKeysCount(-1));
  }

  public void testGetWithQuerySize(int querySize) {
    // given
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    FooUrn fooUrn = makeFooUrn(1);

    // both aspect keys exist
    AspectKey<FooUrn, AspectFoo> aspectKey1 = new AspectKey<>(AspectFoo.class, fooUrn, 1L);
    AspectKey<FooUrn, AspectBar> aspectKey2 = new AspectKey<>(AspectBar.class, fooUrn, 0L);

    // add metadata
    AspectFoo fooV1 = new AspectFoo().setValue("foo");
    addMetadata(fooUrn, AspectFoo.class, 1, fooV1);
    AspectBar barV0 = new AspectBar().setValue("bar");
    addMetadata(fooUrn, AspectBar.class, 0, barV0);

    FooUrn fooUrn2 = makeFooUrn(2);
    AspectKey<FooUrn, AspectFoo> aspectKey3 = new AspectKey<>(AspectFoo.class, fooUrn2, 0L);
    AspectKey<FooUrn, AspectFoo> aspectKey4 = new AspectKey<>(AspectFoo.class, fooUrn2, 1L);
    AspectKey<FooUrn, AspectBar> aspectKey5 = new AspectKey<>(AspectBar.class, fooUrn2, 0L);

    // add metadata
    AspectFoo fooV3 = new AspectFoo().setValue("foo3");
    addMetadata(fooUrn2, AspectFoo.class, 0, fooV3);
    AspectFoo fooV4 = new AspectFoo().setValue("foo4");
    addMetadata(fooUrn2, AspectFoo.class, 1, fooV4);
    AspectBar barV5 = new AspectBar().setValue("bar5");
    addMetadata(fooUrn2, AspectBar.class, 0, barV5);

    dao.setQueryKeysCount(querySize);

    // when
    Map<AspectKey<FooUrn, ? extends RecordTemplate>, Optional<? extends RecordTemplate>> fiveRecords =
        dao.get(new HashSet<>(Arrays.asList(aspectKey1, aspectKey2, aspectKey3, aspectKey4, aspectKey5)));

    // then
    assertEquals(fiveRecords.size(), 5);
  }

  @Test
  public void testNoPaging() {
    testGetWithQuerySize(0);
  }

  @Test
  public void testPageSizeOne() {
    testGetWithQuerySize(1);
  }

  @Test
  public void testPageSizeTwo() {
    testGetWithQuerySize(2);
  }

  @Test
  public void testPageSizeThree() {
    testGetWithQuerySize(3);
  }

  @Test
  public void testPageSizeFour() {
    testGetWithQuerySize(4);
  }

  @Test
  public void testPageSizeSameAsResultSize() {
    testGetWithQuerySize(5);
  }

  @Test
  public void testPageSizeGreaterThanResultsSize() {
    testGetWithQuerySize(1000);
  }

  @Test(expectedExceptions = OptimisticLockException.class)
  public void testOptimisticLockException() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    FooUrn fooUrn = makeFooUrn(1);
    AspectFoo fooAspect = new AspectFoo().setValue("foo");

    // create bean
    EbeanMetadataAspect aspect = new EbeanMetadataAspect();
    aspect.setKey(new EbeanMetadataAspect.PrimaryKey(fooUrn.toString(), AspectFoo.class.getCanonicalName(), 0));
    aspect.setMetadata(RecordUtils.toJsonString(fooAspect));
    aspect.setCreatedOn(new Timestamp(_now - 100));
    aspect.setCreatedBy("fooActor");

    if (_schemaConfig == SchemaConfig.OLD_SCHEMA_ONLY) {
      // add aspect to the db
      _server.insert(aspect);

      // change timestamp and update the inserted row. this simulates a change in the version 0 row by a concurrent transaction
      aspect.setCreatedOn(new Timestamp(_now));
      _server.update(aspect);

      // call save method with timestamp (_now - 100) but timestamp is already changed to _now
      // expect OptimisticLockException if optimistic locking is enabled
      dao.updateWithOptimisticLocking(fooUrn, fooAspect, AspectFoo.class, makeAuditStamp("fooActor", _now + 100),
          0, new Timestamp(_now - 100), null, false);

    } else if (_schemaConfig == SchemaConfig.DUAL_SCHEMA) {
      // in DUAL SCHEMA, the aspect table is the SOT even though it also writes to the entity table
      // Given:
      //  1. in DUAL SCHEMA mode
      //  2. (foo:1, lastmodified(_now + 1)) in entity table (discrepancy)
      //  3. (foo:1, lastmodified(_now), version=0) in aspect table

      dao.insert(fooUrn, fooAspect, AspectFoo.class, makeAuditStamp("fooActor", _now), 0, null, false);

      // make inconsistent timestamp only on the entity table
      dao.setSchemaConfig(SchemaConfig.NEW_SCHEMA_ONLY);
      dao.setChangeLogEnabled(false);
      dao.insert(fooUrn, fooAspect, AspectFoo.class, makeAuditStamp("fooActor", _now + 1), 0, null, false);
      dao.setChangeLogEnabled(true);
      dao.setSchemaConfig(_schemaConfig);

      // When: update with old timestamp matches the lastmodifiedon time in entity table
      try {
        fooAspect.setValue("bar");
        dao.updateWithOptimisticLocking(fooUrn, fooAspect, AspectFoo.class, makeAuditStamp("fooActor", _now + 1000L), 0,
            new Timestamp(_now), null, false);
      } catch (OptimisticLockException e) {
        fail("Expect the update pass since the old timestamp matches the lastmodifiedon in aspect table");
      }
      // Expect: update succeed and the values are updated
      BaseLocalDAO.AspectEntry<AspectFoo> result = dao.getLatest(fooUrn, AspectFoo.class, false);
      assertEquals(result.getAspect().getValue(), "bar");
      assertEquals(result.getExtraInfo().getAudit().getTime(), Long.valueOf(_now + 1000L)); // need to set by at least 1

      // When: update with old timestamp does not match the lastmodifiedon in the aspect table
      // Expect: OptimisticLockException.
      dao.updateWithOptimisticLocking(fooUrn, fooAspect, AspectFoo.class, makeAuditStamp("fooActor", _now + 400), 0,
          new Timestamp(_now + 100), null, false);
    } else if (_enableChangeLog) {
      // either NEW SCHEMA, the entity table is the SOT and the aspect table is the log table
      // Given:
      //  1. in NEW SCHEMA mode
      //  2. (foo:1, lastmodifiedon(_now + 1), version=0) in aspect table (discrepancy)
      //  3. (foo:1, lastmodifiedon(_now)) in entity table

      dao.insert(fooUrn, fooAspect, AspectFoo.class, makeAuditStamp("fooActor", _now), 0, null, false);
      // make inconsistent timestamp on aspect table
      aspect.setCreatedOn(new Timestamp(_now + 1));
      _server.update(aspect);

      // When: update with old timestamp matches the lastmodifiedon time in entity table
      try {
        fooAspect.setValue("bar");
        dao.updateWithOptimisticLocking(fooUrn, fooAspect, AspectFoo.class, makeAuditStamp("fooActor", _now + 200), 0,
            new Timestamp(_now), null, false);
      } catch (OptimisticLockException e) {
        fail("Expect the update pass since the old timestamp matches the lastmodifiedon in entity table");
      }
      // Expect: update succeed and the values are updated
      assertEquals(dao.getLatest(fooUrn, AspectFoo.class, false).getAspect().getValue(), "bar");
      assertEquals(dao.getLatest(fooUrn, AspectFoo.class, false).getExtraInfo().getAudit().getTime(), Long.valueOf(_now + 200L));

      // When: update with old timestamp does not match the lastmodifiedon in the entity table
      // Expect: OptimisticLockException.
      dao.updateWithOptimisticLocking(fooUrn, fooAspect, AspectFoo.class, makeAuditStamp("fooActor", _now + 400), 0,
          new Timestamp(_now + 100), null, false);
    } else {
      // Given: changeLog is disabled
      assertFalse(_enableChangeLog);
      // When: updateWithOptimisticLocking is called
      try {
        dao.updateWithOptimisticLocking(fooUrn, fooAspect, AspectFoo.class, makeAuditStamp("fooActor", _now + 400), 0,
            new Timestamp(_now + 100), null, false);
        fail("UnsupportedOperationException should be thrown");
      } catch (UnsupportedOperationException uoe) {
        // Expect: UnsupportedOperationException is thrown
        throw new OptimisticLockException("skip: when _changeLog is enabled: " + uoe);
      }
    }
  }

  @Test
  public void testBackfillEntityTables() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = mock(EbeanLocalDAO.class);

    FooUrn urn1 = makeFooUrn(1);
    FooUrn urn2 = makeFooUrn(2);
    List<FooUrn> urns = new ArrayList<>();
    urns.add(urn1);
    urns.add(urn2);

    // make sure to actually call the backfill() method when using our mock dao
    doCallRealMethod().when(dao).backfillEntityTables(any(), any());

    // when
    dao.backfillEntityTables(Collections.singleton(AspectFoo.class), new HashSet<>(urns));

    // then
    verify(dao, times(1)).updateEntityTables(urn1, AspectFoo.class);
    verify(dao, times(1)).updateEntityTables(urn2, AspectFoo.class);
  }

  @Test
  public void testUpdateEntityTables() throws URISyntaxException {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);

    // fill in old schema
    FooUrn urn1 = new FooUrn(1);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectFooBar fooBar = new AspectFooBar().setBars(new BarUrnArray(BarUrn.createFromString("urn:li:bar:1")));
    // this function only adds to old schema
    EbeanMetadataAspect ema1 = getMetadata(urn1, AspectFoo.class.getCanonicalName(), 0, foo);
    _server.save(ema1);
    EbeanMetadataAspect ema2 = getMetadata(urn1, AspectFooBar.class.getCanonicalName(), 0, fooBar);
    _server.save(ema2);

    // check that there is nothing in the entity table right now
    if (_schemaConfig != SchemaConfig.OLD_SCHEMA_ONLY) {
      List<SqlRow> initial = _server.createSqlQuery("SELECT * FROM metadata_entity_foo").findList();
      assertEquals(initial.size(), 0);
    }

    // perform the migration
    try {
      dao.setLocalRelationshipBuilderRegistry(new SampleLocalRelationshipRegistryImpl());
      dao.updateEntityTables(urn1, AspectFoo.class);
      dao.updateEntityTables(urn1, AspectFooBar.class);
      if (_schemaConfig == SchemaConfig.OLD_SCHEMA_ONLY) {
        // expect an exception here since there is no new schema to update
        fail();
      }
    } catch (UnsupportedOperationException e) {
      if (_schemaConfig == SchemaConfig.OLD_SCHEMA_ONLY) {
        // pass since an exception is expected when using only the old schema
        return;
      }
      fail();
    }

    // check new schema
    List<SqlRow> result = _server.createSqlQuery("SELECT * FROM metadata_entity_foo").findList();
    assertEquals(result.size(), 1);
    assertEquals(result.get(0).get("urn"), "urn:li:foo:1");
    assertNotNull(result.get(0).get("a_aspectfoo"));
    assertNotNull(result.get(0).get("a_aspectfoobar"));
    assertNull(result.get(0).get("a_aspectbar"));

    // make sure relationships are ingested too
    result = _server.createSqlQuery("SELECT * FROM metadata_relationship_belongsto").findList();
    assertEquals(result.size(), 1);
    assertEquals(result.get(0).get("source"), "urn:li:foo:1");
    assertEquals(result.get(0).get("destination"), "urn:li:bar:1");
  }

  @Test
  public void testBackfillLocalRelationshipsFromEntityTables() throws URISyntaxException {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    dao.setLocalRelationshipBuilderRegistry(new SampleLocalRelationshipRegistryImpl());
    FooUrn fooUrn = makeFooUrn(1);
    BarUrn barUrn1 = BarUrn.createFromString("urn:li:bar:1");
    BarUrn barUrn2 = BarUrn.createFromString("urn:li:bar:2");
    BarUrn barUrn3 = BarUrn.createFromString("urn:li:bar:3");
    BarUrnArray barUrns = new BarUrnArray(barUrn1, barUrn2, barUrn3);
    AspectFooBar aspectFooBar = new AspectFooBar().setBars(barUrns);
    dao.add(fooUrn, aspectFooBar, _dummyAuditStamp);

    // clear local relationship table
    _server.createSqlUpdate("delete from metadata_relationship_belongsto").execute();

    List<BaseLocalRelationshipBuilder.LocalRelationshipUpdates> relationshipUpdates =
    dao.backfillLocalRelationships(fooUrn, AspectFooBar.class);

    List<SqlRow> results = _server.createSqlQuery("select * from metadata_relationship_belongsto").findList();
    assertEquals(results.size(), 3);
    assertEquals(relationshipUpdates.size(), 1);
    assertEquals(relationshipUpdates.get(0).getRemovalOption(), REMOVE_ALL_EDGES_FROM_SOURCE);

    for (int i = 0; i < results.size(); i++) {
      try {
        RecordTemplate relationship = (RecordTemplate) relationshipUpdates.get(0).getRelationships().get(i);
        Urn source = (Urn) relationship.getClass().getMethod("getSource").invoke(relationship);
        Urn dest = (Urn) relationship.getClass().getMethod("getDestination").invoke(relationship);
        assertEquals(source.toString(), "urn:li:foo:1");
        assertEquals(dest.toString(), "urn:li:bar:" + (i + 1));
        assertEquals(relationshipUpdates.get(0).getRelationships().get(i).getClass().getSimpleName(), "BelongsTo");
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Test
  public void testAddRelationships() throws URISyntaxException {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> fooDao = createDao(FooUrn.class);
    EbeanLocalDAO<EntityAspectUnion, BarUrn> barDao = createDao(BarUrn.class);
    FooUrn fooUrn = makeFooUrn(1);
    BarUrn barUrn1 = BarUrn.createFromString("urn:li:bar:1");
    BarUrn barUrn2 = BarUrn.createFromString("urn:li:bar:2");
    BarUrn barUrn3 = BarUrn.createFromString("urn:li:bar:3");
    AspectFooBar aspectFooBar = new AspectFooBar().setBars(new BarUrnArray(barUrn1, barUrn2, barUrn3));
    AuditStamp auditStamp = makeAuditStamp("foo", System.currentTimeMillis());

    // Turn off local relationship ingestion first, to fill only the entity tables.
    fooDao.setLocalRelationshipBuilderRegistry(null);
    barDao.setLocalRelationshipBuilderRegistry(null);

    fooDao.add(fooUrn, aspectFooBar, auditStamp);
    barDao.add(barUrn1, new AspectFoo().setValue("1"), auditStamp);
    barDao.add(barUrn2, new AspectFoo().setValue("2"), auditStamp);
    barDao.add(barUrn3, new AspectFoo().setValue("3"), auditStamp);

    // Verify that NO local relationships were added
    EbeanLocalRelationshipQueryDAO ebeanLocalRelationshipQueryDAO = new EbeanLocalRelationshipQueryDAO(_server);
    ebeanLocalRelationshipQueryDAO.setSchemaConfig(_schemaConfig);
    List<BelongsTo> relationships = ebeanLocalRelationshipQueryDAO.findRelationships(
        FooSnapshot.class, EMPTY_FILTER, BarSnapshot.class, EMPTY_FILTER, BelongsTo.class, OUTGOING_FILTER, 0, 10);
    assertEquals(relationships.size(), 0);

    // Turn on local relationship ingestion now
    fooDao.setLocalRelationshipBuilderRegistry(new SampleLocalRelationshipRegistryImpl());

    // Add only the local relationships
    fooDao.handleRelationshipIngestion(fooUrn, aspectFooBar, null, AspectFooBar.class, false);

    // Verify that the local relationships were added
    relationships = ebeanLocalRelationshipQueryDAO.findRelationships(
        FooSnapshot.class, EMPTY_FILTER, BarSnapshot.class, EMPTY_FILTER, BelongsTo.class, OUTGOING_FILTER, 0, 10);

    assertEquals(relationships.size(), 3);
  }

  @Test
  public void testAddRelationshipsWithAspectColumn() throws URISyntaxException {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> fooDao = createDao(FooUrn.class);
    EbeanLocalDAO<EntityAspectUnion, BarUrn> barDao = createDao(BarUrn.class);
    FooUrn fooUrn = makeFooUrn(1);
    BarUrn barUrn1 = BarUrn.createFromString("urn:li:bar:1");
    BarUrn barUrn2 = BarUrn.createFromString("urn:li:bar:2");
    BarUrn barUrn3 = BarUrn.createFromString("urn:li:bar:3");
    AspectFooBar aspectFooBar = new AspectFooBar().setBars(new BarUrnArray(barUrn1, barUrn2, barUrn3));
    AuditStamp auditStamp = makeAuditStamp("foo", System.currentTimeMillis());

    // Turn off local relationship ingestion first, to fill only the entity tables.
    fooDao.setLocalRelationshipBuilderRegistry(null);
    barDao.setLocalRelationshipBuilderRegistry(null);

    fooDao.add(fooUrn, aspectFooBar, auditStamp);
    barDao.add(barUrn1, new AspectFoo().setValue("1"), auditStamp);
    barDao.add(barUrn2, new AspectFoo().setValue("2"), auditStamp);
    barDao.add(barUrn3, new AspectFoo().setValue("3"), auditStamp);

    // Verify that NO local relationships were added
    EbeanLocalRelationshipQueryDAO ebeanLocalRelationshipQueryDAO = new EbeanLocalRelationshipQueryDAO(_server);
    ebeanLocalRelationshipQueryDAO.setSchemaConfig(_schemaConfig);
    List<BelongsTo> relationships = ebeanLocalRelationshipQueryDAO.findRelationships(
        FooSnapshot.class, EMPTY_FILTER, BarSnapshot.class, EMPTY_FILTER, BelongsTo.class, OUTGOING_FILTER, 0, 10);
    assertEquals(relationships.size(), 0);

    // Turn on local relationship ingestion now
    fooDao.setLocalRelationshipBuilderRegistry(new SampleLocalRelationshipRegistryImpl());
    fooDao.setUseAspectColumnForRelationshipRemoval(true);

    // Add only the local relationships
    fooDao.handleRelationshipIngestion(fooUrn, aspectFooBar, null, AspectFooBar.class, false);

    // Verify that the local relationships were added
    relationships = ebeanLocalRelationshipQueryDAO.findRelationships(
        FooSnapshot.class, EMPTY_FILTER, BarSnapshot.class, EMPTY_FILTER, BelongsTo.class, OUTGOING_FILTER, 0, 10);

    assertEquals(relationships.size(), 3);

    // Verify that all 3 relationships added have non-null aspect values
    List<SqlRow> results = _server.createSqlQuery("select aspect from metadata_relationship_belongsto").findList();
    assertEquals(results.size(), 3);
    results.forEach(row -> assertEquals(row.getString("aspect"), AspectFooBar.class.getCanonicalName()));
  }

  @Test
  public void testAddWithLocalRelationshipBuilder() throws URISyntaxException {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> fooDao = createDao(FooUrn.class);
    EbeanLocalDAO<EntityAspectUnion, BarUrn> barDao = createDao(BarUrn.class);
    fooDao.setLocalRelationshipBuilderRegistry(new SampleLocalRelationshipRegistryImpl());
    barDao.setLocalRelationshipBuilderRegistry(new SampleLocalRelationshipRegistryImpl());

    FooUrn fooUrn = makeFooUrn(1);
    BarUrn barUrn1 = BarUrn.createFromString("urn:li:bar:1");
    BarUrn barUrn2 = BarUrn.createFromString("urn:li:bar:2");
    BarUrn barUrn3 = BarUrn.createFromString("urn:li:bar:3");
    AspectFooBar aspectFooBar = new AspectFooBar().setBars(new BarUrnArray(barUrn1, barUrn2, barUrn3));
    AuditStamp auditStamp = makeAuditStamp("foo", System.currentTimeMillis());

    fooDao.add(fooUrn, aspectFooBar, auditStamp);
    barDao.add(barUrn1, new AspectFoo().setValue("1"), auditStamp);
    barDao.add(barUrn2, new AspectFoo().setValue("2"), auditStamp);
    barDao.add(barUrn3, new AspectFoo().setValue("3"), auditStamp);

    // Verify local relationships and entity are added.
    EbeanLocalRelationshipQueryDAO ebeanLocalRelationshipQueryDAO = new EbeanLocalRelationshipQueryDAO(_server);
    ebeanLocalRelationshipQueryDAO.setSchemaConfig(_schemaConfig);

    List<BelongsTo> relationships = ebeanLocalRelationshipQueryDAO.findRelationships(
        FooSnapshot.class, EMPTY_FILTER, BarSnapshot.class, EMPTY_FILTER, BelongsTo.class, OUTGOING_FILTER, 0, 10);

    AspectKey<FooUrn, AspectFooBar> key = new AspectKey<>(AspectFooBar.class, fooUrn, 0L);
    List<EbeanMetadataAspect> aspects = fooDao.batchGetHelper(Collections.singletonList(key), 1, 0);

    assertEquals(relationships.size(), 3);
    assertEquals(aspects.size(), 1);
  }

  @Test
  public void testAddRelationshipsFromAspect() throws URISyntaxException {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> fooDao = createDao(FooUrn.class);
    EbeanLocalDAO<EntityAspectUnion, BarUrn> barDao = createDao(BarUrn.class);

    FooUrn fooUrn = makeFooUrn(1);
    BarUrn barUrn1 = BarUrn.createFromString("urn:li:bar:1");
    BelongsToV2 belongsTo1 = new BelongsToV2().setDestination(BelongsToV2.Destination.create(barUrn1.toString()));
    BarUrn barUrn2 = BarUrn.createFromString("urn:li:bar:2");
    BelongsToV2 belongsTo2 = new BelongsToV2().setDestination(BelongsToV2.Destination.create(barUrn2.toString()));
    BarUrn barUrn3 = BarUrn.createFromString("urn:li:bar:3");
    BelongsToV2 belongsTo3 = new BelongsToV2().setDestination(BelongsToV2.Destination.create(barUrn3.toString()));
    BelongsToV2Array belongsToArray = new BelongsToV2Array(belongsTo1, belongsTo2, belongsTo3);
    AspectFooBar aspectFooBar = new AspectFooBar().setBelongsTos(belongsToArray);
    AuditStamp auditStamp = makeAuditStamp("foo", System.currentTimeMillis());

    fooDao.add(fooUrn, aspectFooBar, auditStamp);
    barDao.add(barUrn1, new AspectFoo().setValue("1"), auditStamp);
    barDao.add(barUrn2, new AspectFoo().setValue("2"), auditStamp);
    barDao.add(barUrn3, new AspectFoo().setValue("3"), auditStamp);

    // Verify local relationships and entity are added.
    EbeanLocalRelationshipQueryDAO ebeanLocalRelationshipQueryDAO = new EbeanLocalRelationshipQueryDAO(_server);
    ebeanLocalRelationshipQueryDAO.setSchemaConfig(_schemaConfig);

    List<BelongsToV2> relationships =
        ebeanLocalRelationshipQueryDAO.findRelationships(FooSnapshot.class, EMPTY_FILTER, BarSnapshot.class,
            EMPTY_FILTER, BelongsToV2.class, OUTGOING_FILTER, 0, 10);

    AspectKey<FooUrn, AspectFooBar> key = new AspectKey<>(AspectFooBar.class, fooUrn, 0L);
    List<EbeanMetadataAspect> aspects = fooDao.batchGetHelper(Collections.singletonList(key), 1, 0);

    assertEquals(relationships.size(), 3);
    assertEquals(aspects.size(), 1);
  }

  @Test
  public void testAddRelationshipsV2WithRelationshipBuilders() throws URISyntaxException {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> fooDao = createDao(FooUrn.class);
    EbeanLocalDAO<EntityAspectUnion, BarUrn> barDao = createDao(BarUrn.class);

    // create AspectFooBaz without setting relationship-type fields
    FooUrn fooUrn = makeFooUrn(1);
    BarUrn barUrn1 = BarUrn.createFromString("urn:li:bar:1");
    BarUrn barUrn2 = BarUrn.createFromString("urn:li:bar:2");
    BarUrn barUrn3 = BarUrn.createFromString("urn:li:bar:3");
    AspectFooBaz aspectFooBaz = new AspectFooBaz().setBars(new BarUrnArray(barUrn1, barUrn2, barUrn3));
    AuditStamp auditStamp = makeAuditStamp("foo", System.currentTimeMillis());

    fooDao.setLocalRelationshipBuilderRegistry(new SampleLocalRelationshipRegistryImpl());

    fooDao.add(fooUrn, aspectFooBaz, auditStamp);
    barDao.add(barUrn1, new AspectFoo().setValue("1"), auditStamp);
    barDao.add(barUrn2, new AspectFoo().setValue("2"), auditStamp);
    barDao.add(barUrn3, new AspectFoo().setValue("3"), auditStamp);

    // Verify local relationships and entity are added.
    EbeanLocalRelationshipQueryDAO ebeanLocalRelationshipQueryDAO = new EbeanLocalRelationshipQueryDAO(_server);
    ebeanLocalRelationshipQueryDAO.setSchemaConfig(_schemaConfig);

    List<BelongsToV2> relationships =
        ebeanLocalRelationshipQueryDAO.findRelationships(FooSnapshot.class, EMPTY_FILTER, BarSnapshot.class,
            EMPTY_FILTER, BelongsToV2.class, OUTGOING_FILTER, 0, 10);

    AspectKey<FooUrn, AspectFooBaz> key = new AspectKey<>(AspectFooBaz.class, fooUrn, 0L);
    List<EbeanMetadataAspect> aspects = fooDao.batchGetHelper(Collections.singletonList(key), 1, 0);

    assertEquals(relationships.size(), 3);
    assertEquals(aspects.size(), 1);
  }

  @Test
  public void testAddRelationshipsV2WithRegisteredButEmptyRelationshipBuilders() throws URISyntaxException {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> fooDao = createDao(FooUrn.class);
    EbeanLocalDAO<EntityAspectUnion, BarUrn> barDao = createDao(BarUrn.class);

    // create AspectFooBaz with null bar array and non-null relationship-type fields.
    // the relationship builder will return an empty list of relationship updates so the DAO should try
    // to extract relationships from aspect metadata
    FooUrn fooUrn = makeFooUrn(1);
    BarUrn barUrn1 = BarUrn.createFromString("urn:li:bar:1");
    BelongsToV2 belongsTo1 = new BelongsToV2().setDestination(BelongsToV2.Destination.create(barUrn1.toString()));
    BarUrn barUrn2 = BarUrn.createFromString("urn:li:bar:2");
    BelongsToV2 belongsTo2 = new BelongsToV2().setDestination(BelongsToV2.Destination.create(barUrn2.toString()));
    BarUrn barUrn3 = BarUrn.createFromString("urn:li:bar:3");
    BelongsToV2 belongsTo3 = new BelongsToV2().setDestination(BelongsToV2.Destination.create(barUrn3.toString()));
    BelongsToV2Array belongsToArray = new BelongsToV2Array(belongsTo1, belongsTo2, belongsTo3);
    AspectFooBaz aspectFooBaz = new AspectFooBaz().setBelongsTos(belongsToArray);
    AuditStamp auditStamp = makeAuditStamp("foo", System.currentTimeMillis());

    fooDao.setLocalRelationshipBuilderRegistry(new SampleLocalRelationshipRegistryImpl());

    fooDao.add(fooUrn, aspectFooBaz, auditStamp);
    barDao.add(barUrn1, new AspectFoo().setValue("1"), auditStamp);
    barDao.add(barUrn2, new AspectFoo().setValue("2"), auditStamp);
    barDao.add(barUrn3, new AspectFoo().setValue("3"), auditStamp);

    // Verify local relationships and entity are added.
    EbeanLocalRelationshipQueryDAO ebeanLocalRelationshipQueryDAO = new EbeanLocalRelationshipQueryDAO(_server);
    ebeanLocalRelationshipQueryDAO.setSchemaConfig(_schemaConfig);

    List<BelongsToV2> relationships =
        ebeanLocalRelationshipQueryDAO.findRelationships(FooSnapshot.class, EMPTY_FILTER, BarSnapshot.class,
            EMPTY_FILTER, BelongsToV2.class, OUTGOING_FILTER, 0, 10);

    AspectKey<FooUrn, AspectFooBaz> key = new AspectKey<>(AspectFooBaz.class, fooUrn, 0L);
    List<EbeanMetadataAspect> aspects = fooDao.batchGetHelper(Collections.singletonList(key), 1, 0);

    assertEquals(relationships.size(), 3);
    assertEquals(aspects.size(), 1);
  }

  @Test
  public void testAddRelationshipsV2DefaultToRelationshipBuilders() throws URISyntaxException {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> fooDao = createDao(FooUrn.class);
    EbeanLocalDAO<EntityAspectUnion, BarUrn> barDao = createDao(BarUrn.class);

    // create AspectFooBaz with non-null bar array and non-null relationship-type fields but with different values.
    // bars: 1, 2, 3 -RelationshipBuilder-> foo1->bar1, foo1->bar2, foo1->bar3
    // belongsTos: foo1->bar4
    // the DAO should default to using the relationships from the relationship builders over the relationships
    // extracted from the aspect.
    FooUrn fooUrn = makeFooUrn(1);
    BarUrn barUrn1 = BarUrn.createFromString("urn:li:bar:1");
    BarUrn barUrn2 = BarUrn.createFromString("urn:li:bar:2");
    BarUrn barUrn3 = BarUrn.createFromString("urn:li:bar:3");
    BarUrn barUrn4 = BarUrn.createFromString("urn:li:bar:4");
    BelongsToV2 belongsTo4 = new BelongsToV2().setDestination(BelongsToV2.Destination.create(barUrn4.toString()));
    BelongsToV2Array belongsToArray = new BelongsToV2Array(belongsTo4);
    AspectFooBaz aspectFooBaz = new AspectFooBaz().setBars(new BarUrnArray(barUrn1, barUrn2, barUrn3)).setBelongsTos(belongsToArray);
    AuditStamp auditStamp = makeAuditStamp("foo", System.currentTimeMillis());

    fooDao.setLocalRelationshipBuilderRegistry(new SampleLocalRelationshipRegistryImpl());

    fooDao.add(fooUrn, aspectFooBaz, auditStamp);
    barDao.add(barUrn1, new AspectFoo().setValue("1"), auditStamp);
    barDao.add(barUrn2, new AspectFoo().setValue("2"), auditStamp);
    barDao.add(barUrn3, new AspectFoo().setValue("3"), auditStamp);

    // Verify local relationships and entity are added.
    EbeanLocalRelationshipQueryDAO ebeanLocalRelationshipQueryDAO = new EbeanLocalRelationshipQueryDAO(_server);
    ebeanLocalRelationshipQueryDAO.setSchemaConfig(_schemaConfig);

    List<BelongsToV2> relationships =
        ebeanLocalRelationshipQueryDAO.findRelationships(FooSnapshot.class, EMPTY_FILTER, BarSnapshot.class,
            EMPTY_FILTER, BelongsToV2.class, OUTGOING_FILTER, 0, 10);

    AspectKey<FooUrn, AspectFooBaz> key = new AspectKey<>(AspectFooBaz.class, fooUrn, 0L);
    List<EbeanMetadataAspect> aspects = fooDao.batchGetHelper(Collections.singletonList(key), 1, 0);

    assertEquals(relationships.size(), 3);
    assertFalse(relationships.contains(belongsTo4));
    assertEquals(aspects.size(), 1);
  }

  @Test
  public void testNewSchemaFilterByArray() {
    if (_schemaConfig == SchemaConfig.NEW_SCHEMA_ONLY) {

      // Prepare data with attributes being ["foo", "bar", "baz"]
      EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
      FooUrn fooUrn = makeFooUrn(1);
      AspectAttributes attributes = new AspectAttributes().setAttributes(new StringArray("foo", "bar", "baz"));

      // Create index filter with value "bar"
      IndexPathParams indexPathParams = new IndexPathParams().setPath("/attributes").setValue(IndexValue.create("bar")).setCondition(Condition.CONTAIN);
      IndexCriterion criterion = new IndexCriterion().setAspect(AspectAttributes.class.getCanonicalName()).setPathParams(indexPathParams);
      IndexFilter indexFilter = new IndexFilter().setCriteria(new IndexCriterionArray(criterion));
      dao.add(fooUrn, attributes, _dummyAuditStamp);
      List<FooUrn> urns = dao.listUrns(indexFilter, null, 5);

      // Verify find one
      assertEquals(urns, Collections.singletonList(fooUrn));

      // Create index filter with value "zoo"
      IndexPathParams indexPathParams2 = new IndexPathParams().setPath("/attributes").setValue(IndexValue.create("zoo")).setCondition(Condition.CONTAIN);
      IndexCriterion criterion2 = new IndexCriterion().setAspect(AspectAttributes.class.getCanonicalName()).setPathParams(indexPathParams2);
      IndexFilter indexFilter2 = new IndexFilter().setCriteria(new IndexCriterionArray(criterion2));
      dao.add(fooUrn, attributes, _dummyAuditStamp);
      List<FooUrn> empty = dao.listUrns(indexFilter2, null, 5);

      // Verify nothing found
      assertTrue(empty.isEmpty());
    }
  }

  @Test
  public void testNewSchemaExactMatchArray() {
    if (_schemaConfig == SchemaConfig.NEW_SCHEMA_ONLY) {

      // Prepare data with attributes being ["foo", "bar", "baz"]
      EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
      FooUrn fooUrn = makeFooUrn(1);
      AspectAttributes attributes = new AspectAttributes().setAttributes(new StringArray("foo", "bar", "baz"));
      dao.add(fooUrn, attributes, _dummyAuditStamp);

      // Create index filter with value ["foo", "bar", "baz"]
      IndexValue arrayValue = IndexValue.create(new StringArray("foo", "bar", "baz"));
      IndexPathParams indexPathParams = new IndexPathParams().setPath("/attributes").setValue(arrayValue).setCondition(Condition.EQUAL);
      IndexCriterion criterion = new IndexCriterion().setAspect(AspectAttributes.class.getCanonicalName()).setPathParams(indexPathParams);
      IndexFilter indexFilter = new IndexFilter().setCriteria(new IndexCriterionArray(criterion));

      List<FooUrn> urns = dao.listUrns(indexFilter, null, 5);

      // Verify find one
      assertEquals(urns, Collections.singletonList(fooUrn));

      // Create index filter with value ["bar", "foo", "baz"]. Order is different.
      IndexValue arrayValue2 = IndexValue.create(new StringArray("bar", "foo", "baz"));
      IndexPathParams indexPathParams2 = new IndexPathParams().setPath("/attributes").setValue(arrayValue2).setCondition(Condition.EQUAL);
      IndexCriterion criterion2 = new IndexCriterion().setAspect(AspectAttributes.class.getCanonicalName()).setPathParams(indexPathParams2);
      IndexFilter indexFilter2 = new IndexFilter().setCriteria(new IndexCriterionArray(criterion2));
      List<FooUrn> empty1 = dao.listUrns(indexFilter2, null, 5);

      // Verify nothing found
      assertTrue(empty1.isEmpty());

      // Create index filter with value ["foo", "bar"]. Missing baz element.
      IndexValue arrayValue3 = IndexValue.create(new StringArray("foo", "bar"));
      IndexPathParams indexPathParams3 = new IndexPathParams().setPath("/attributes").setValue(arrayValue3).setCondition(Condition.EQUAL);
      IndexCriterion criterion3 = new IndexCriterion().setAspect(AspectAttributes.class.getCanonicalName()).setPathParams(indexPathParams3);
      IndexFilter indexFilter3 = new IndexFilter().setCriteria(new IndexCriterionArray(criterion3));
      List<FooUrn> empty2 = dao.listUrns(indexFilter3, null, 5);

      // Verify nothing found
      assertTrue(empty2.isEmpty());
    }
  }

  @Test
  public void testNewSchemaExactMatchEmptyArray() {
    if (_schemaConfig == SchemaConfig.NEW_SCHEMA_ONLY) {
      // Prepare data with attributes being empty array
      EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
      FooUrn fooUrn = makeFooUrn(2);
      AspectAttributes emptyAttr = new AspectAttributes().setAttributes(new StringArray());
      dao.add(fooUrn, emptyAttr, _dummyAuditStamp);

      // Create index filter with empty array
      IndexValue arrayValue = IndexValue.create(new StringArray());
      IndexPathParams indexPathParams =
          new IndexPathParams().setPath("/attributes").setValue(arrayValue).setCondition(Condition.EQUAL);
      IndexCriterion criterion =
          new IndexCriterion().setAspect(AspectAttributes.class.getCanonicalName()).setPathParams(indexPathParams);
      IndexFilter indexFilter = new IndexFilter().setCriteria(new IndexCriterionArray(criterion));

      List<FooUrn> urns = dao.listUrns(indexFilter, null, 5);

      // Verify find one
      assertEquals(urns, Collections.singletonList(fooUrn));
    }
  }

  @Test
  public void testDataNotWrittenIntoOldSchemaWhenChangeLogIsDisabled() {
    EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
    if (dao.isChangeLogEnabled()) {
      // this test is only applicable when changeLog is disabled
      return;
    }

    // Given: an empty old schema and empty new schema and changelog is disabled
    EbeanLocalDAO<EntityAspectUnion, FooUrn> legacyDao = createDao(FooUrn.class);
    legacyDao.setSchemaConfig(SchemaConfig.OLD_SCHEMA_ONLY);
    legacyDao.setChangeLogEnabled(false);

    // When: AspectFoo is written into the dao.
    FooUrn fooUrn = makeFooUrn(1);
    AspectFoo v1 = new AspectFoo().setValue("foo");
    dao.add(fooUrn, v1, _dummyAuditStamp);

    // Expect: the aspect foo is only written into the new schema.
    assertTrue(dao.get(AspectFoo.class, fooUrn).isPresent());
    assertFalse(legacyDao.get(AspectFoo.class, fooUrn).isPresent());
  }

  @Test
  public void testOverwriteLatestVersion() {
    if (!_enableChangeLog || _schemaConfig != SchemaConfig.NEW_SCHEMA_ONLY) {
      // skip this test if the change log is not even enabled and/or if we are not operating in new schema mode.
      return;
    }

    // new schema DAO, used for inserts and reads
    EbeanLocalDAO<EntityAspectUnion, FooUrn> newSchemaDao = createDao(FooUrn.class);
    newSchemaDao.setOverwriteLatestVersionEnabled(true);

    // old schema DAO, used for reads only
    EbeanLocalDAO<EntityAspectUnion, FooUrn> oldSchemaDao = createDao(FooUrn.class);
    oldSchemaDao.setSchemaConfig(SchemaConfig.OLD_SCHEMA_ONLY);

    // Given: first version of metadata is inserted
    FooUrn fooUrn = makeFooUrn(1);
    AspectFoo v1 = new AspectFoo().setValue("foo");
    newSchemaDao.add(fooUrn, v1, _dummyAuditStamp);

    // When: second version of metadata is inserted
    AspectFoo v2 = new AspectFoo().setValue("bar");
    newSchemaDao.add(fooUrn, v2, _dummyAuditStamp);

    // Expect: second version of metadata inserted overwrote the first version in the metadata_aspect table
    Optional<AspectFoo> newSchemaResult = newSchemaDao.get(AspectFoo.class, fooUrn);
    Optional<AspectFoo> oldSchemaResultLatest = oldSchemaDao.get(AspectFoo.class, fooUrn, 0);
    Optional<AspectFoo> oldSchemaResultNonLatest = oldSchemaDao.get(AspectFoo.class, fooUrn, 1);

    assertTrue(newSchemaResult.isPresent());
    assertEquals(newSchemaResult.get().getValue(), "bar");
    assertTrue(oldSchemaResultLatest.isPresent());
    assertEquals(oldSchemaResultLatest.get().getValue(), "bar");
    assertFalse(oldSchemaResultNonLatest.isPresent());
  }

  @Test
  public void testGetWithExtraInfoFromNewSchema() {
    if (_schemaConfig == SchemaConfig.NEW_SCHEMA_ONLY) {
      EbeanLocalDAO<EntityAspectUnion, FooUrn> dao = createDao(FooUrn.class);
      FooUrn urn = makeFooUrn(1);
      AspectFoo aspectFoo = new AspectFoo().setValue("foo");
      IngestionTrackingContext context = new IngestionTrackingContext().setEmitter("testEmitter");

      Urn creator1 = Urns.createFromTypeSpecificString("test", "testCreator1");
      Urn impersonator1 = Urns.createFromTypeSpecificString("test", "testImpersonator1");
      dao.add(urn, aspectFoo, makeAuditStamp(creator1, impersonator1, _now), context, null);
      Optional<AspectWithExtraInfo<AspectFoo>> foo = dao.getWithExtraInfo(AspectFoo.class, urn);

      assertTrue(foo.isPresent());
      assertEquals(foo.get(), new AspectWithExtraInfo<>(aspectFoo,
          new ExtraInfo().setAudit(makeAuditStamp(creator1, impersonator1, _now))
              .setUrn(urn)
              .setVersion(0)
              .setEmitter("testEmitter")));
    }
  }

  @Nonnull
  private EbeanMetadataAspect getMetadata(Urn urn, String aspectName, long version, @Nullable RecordTemplate metadata) {
    EbeanMetadataAspect aspect = new EbeanMetadataAspect();
    aspect.setKey(new EbeanMetadataAspect.PrimaryKey(urn.toString(), aspectName, version));
    if (metadata != null) {
      aspect.setMetadata(RecordUtils.toJsonString(metadata));
    } else {
      aspect.setMetadata(DELETED_VALUE);
    }
    aspect.setCreatedOn(new Timestamp(_now));
    aspect.setCreatedBy("urn:li:test:foo");
    aspect.setCreatedFor("urn:li:test:bar");
    return aspect;
  }

  private <ASPECT extends RecordTemplate> void addMetadata(Urn urn, Class<ASPECT> aspectClass, long version, @Nullable RecordTemplate metadata) {
    String aspectName = aspectClass.getCanonicalName();
    EbeanMetadataAspect ema = getMetadata(urn, aspectName, version, metadata);
    _server.save(ema);

    if (_schemaConfig == SchemaConfig.NEW_SCHEMA_ONLY || _schemaConfig == SchemaConfig.DUAL_SCHEMA) {
      addMetadataEntityTable(urn, aspectClass, metadata, version, _now, ema.getCreatedBy(), ema.getCreatedFor());
    }
  }

  private <ASPECT extends RecordTemplate> void addMetadataEntityTable(Urn urn, Class<ASPECT> aspectClass, @Nullable RecordTemplate metadata, long version,
      long createdOn, String createdBy, String createdFor) {
    if (version != 0) {
      return;
    }
    String aspectName = aspectClass.getCanonicalName();
    String columnName = SQLSchemaUtils.getAspectColumnName(urn.getEntityType(), aspectName);
    String template = "insert into metadata_entity_%s (urn, %s, lastmodifiedon, lastmodifiedby, createdfor) value"
        + "('%s', '%s', '%s', '%s', '%s') ON DUPLICATE KEY UPDATE %s = '%s';";
    String query = String.format(template, urn.getEntityType(), columnName, urn, createAuditedAspect(metadata, aspectClass, createdOn, createdBy, createdFor),
        new Timestamp(createdOn), createdBy, createdFor, columnName, createAuditedAspect(metadata, aspectClass, createdOn, createdBy, createdFor));
    _server.createSqlUpdate(query).execute();
  }

  private <ASPECT extends RecordTemplate> String createAuditedAspect(RecordTemplate metadata, Class<ASPECT> aspectClass,
      long createdOn, String createdBy, String createdFor) {
    return metadata == null ? DELETED_VALUE : EbeanLocalAccess.toJsonString(new AuditedAspect()
        .setAspect(RecordUtils.toJsonString(metadata))
        .setCanonicalName(aspectClass.getCanonicalName())
        .setLastmodifiedby(createdBy)
        .setLastmodifiedon(new Timestamp(createdOn).toString())
        .setCreatedfor(createdFor, SetMode.IGNORE_NULL));
  }

  private <ASPECT extends RecordTemplate> void addMetadataWithAuditStamp(Urn urn, Class<ASPECT> aspectClass, long version, RecordTemplate metadata,
      long timeStamp, String creator, String impersonator) {
    EbeanMetadataAspect aspect = getMetadata(urn, aspectClass.getCanonicalName(), version, metadata);
    aspect.setCreatedOn(new Timestamp(timeStamp));
    aspect.setCreatedBy(creator);
    aspect.setCreatedFor(impersonator);
    _server.save(aspect);

    if (_schemaConfig == SchemaConfig.NEW_SCHEMA_ONLY || _schemaConfig == SchemaConfig.DUAL_SCHEMA) {
      addMetadataEntityTable(urn, aspectClass, metadata, version, timeStamp, creator, impersonator);
    }
  }

  private void addIndex(Urn urn, String aspectName, String pathName, Object val) {
    Object trueVal;
    if (val instanceof String) {
      trueVal = "'" + val + "'";
    } else if (val instanceof Boolean) {
      trueVal = "'" + val + "'";
    } else if (val instanceof Double) {
      trueVal = val;
    } else if (val instanceof Float) {
      trueVal = ((Float) val).doubleValue();
    } else if (val instanceof Integer) {
      trueVal = Long.valueOf((Integer) val);
    } else if (val instanceof Long) {
      trueVal = val;
    } else {
      return;
    }

    /*
    we will have
    metadata_entity_foo:
    urn  | lastmodifiedon   | lastmodifiedby |  a_aspectfoo                              | i_aspectfoo$longval | i_aspectfoo$stringval
    urn:1| <some_timestamp> | "actor"        | "{..."longval":3, "stringval":"hello"...} |          3          |       "hello"
    urn:2| <some_timestamp> | "actor"        | "{..."longval":5...}                      |          5          |       <empty>
    */

    String aspectColumnName = isUrn(aspectName) ? null : SQLSchemaUtils.getAspectColumnName(urn.getEntityType(), aspectName); // e.g. a_aspectfoo;
    String fullIndexColumnName = SQLSchemaUtils.getGeneratedColumnName(urn.getEntityType(), aspectName, pathName,
        _eBeanDAOConfig.isNonDollarVirtualColumnsEnabled()); // e.g. i_aspectfoo$path1$value1

    String checkColumnExistance = String.format("SELECT * FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '%s' AND"
        + " TABLE_NAME = '%s' AND COLUMN_NAME = '%s'", _server.getName(), getTableName(urn), fullIndexColumnName);

    if (_server.createSqlQuery(checkColumnExistance).findList().isEmpty()) {
      String sqlUpdate = String.format("ALTER TABLE %s ADD COLUMN %s VARCHAR(255);", getTableName(urn), fullIndexColumnName);
      _server.execute(Ebean.createSqlUpdate(sqlUpdate));
    }

    checkColumnExistance = String.format("SELECT * FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '%s' AND"
        + " TABLE_NAME = '%s' AND COLUMN_NAME = '%s'", _server.getName(), getTableName(urn), aspectColumnName);
    // similarly for index columns (i_*), we need to add any new aspect columns (a_*)
    if (aspectColumnName != null && _server.createSqlQuery(checkColumnExistance).findList().isEmpty()) {
      String sqlUpdate = String.format("ALTER TABLE %s ADD COLUMN %s VARCHAR(255);", getTableName(urn), aspectColumnName);
      _server.execute(Ebean.createSqlUpdate(sqlUpdate));
    }

    // finally, we need to update the newly added column with the passed-in value.
    String sqlUpdate;
    if (aspectColumnName != null) {
      final String dummyAspectValue = "{\"value\": \"dummy_value\"}";
      sqlUpdate = String.format("INSERT INTO %s (urn, a_urn, lastmodifiedon, lastmodifiedby, %s, %s) "
              + "VALUES ('%s', '{}','00-01-01 00:00:00.000000', 'tester', '%s', %s) ON DUPLICATE KEY UPDATE %s = %s, %s = '%s';", getTableName(urn),
          aspectColumnName, fullIndexColumnName, urn, dummyAspectValue, trueVal, fullIndexColumnName, trueVal, aspectColumnName, dummyAspectValue);
    } else {
      sqlUpdate = String.format("INSERT INTO %s (urn, a_urn, lastmodifiedon, lastmodifiedby, %s) "
              + "VALUES ('%s', '{}', '00-01-01 00:00:00.000000', 'tester', %s) ON DUPLICATE KEY UPDATE %s = %s;", getTableName(urn),
          fullIndexColumnName, urn, trueVal, fullIndexColumnName, trueVal);
    }

    _server.execute(Ebean.createSqlUpdate(sqlUpdate));
  }

  private EbeanMetadataAspect getMetadata(Urn urn, String aspectName, long version) {
    if (_schemaConfig == SchemaConfig.NEW_SCHEMA_ONLY && version == 0) {
      String aspectColumn = getAspectColumnName(urn.getEntityType(), aspectName);
      String template = "select urn, lastmodifiedon, lastmodifiedby, createdfor, %s from metadata_entity_%s";
      String query = String.format(template, aspectColumn, urn.getEntityType());
      SqlRow result = _server.createSqlQuery(query).findOne();
      if (result != null) {
        EbeanMetadataAspect ema = new EbeanMetadataAspect();
        String metadata = extractAspectJsonString(result.getString(aspectColumn));
        if (metadata == null) {
          metadata = DELETED_VALUE;
        }
        ema.setMetadata(metadata);
        ema.setKey(new PrimaryKey(urn.toString(), aspectName, version));
        ema.setCreatedOn(result.getTimestamp("lastmodifiedon"));
        ema.setCreatedBy(result.getString("lastmodifiedby"));
        ema.setCreatedFor(result.getString("creatdfor"));
        return ema;
      }
      return null;
    }
    return _server.find(EbeanMetadataAspect.class,
        new EbeanMetadataAspect.PrimaryKey(urn.toString(), aspectName, version));
  }

  private EbeanMetadataAspect getTestMetadata(Urn urn, String aspectName, long version) {
    if (_schemaConfig == SchemaConfig.NEW_SCHEMA_ONLY && version == 0) {
      String aspectColumn = getAspectColumnName(urn.getEntityType(), aspectName);
      String template = "select urn, lastmodifiedon, lastmodifiedby, createdfor, %s from metadata_entity_%s_test";
      String query = String.format(template, aspectColumn, urn.getEntityType());
      SqlRow result = _server.createSqlQuery(query).findOne();
      if (result != null) {
        EbeanMetadataAspect ema = new EbeanMetadataAspect();
        String metadata = extractAspectJsonString(result.getString(aspectColumn));
        if (metadata == null) {
          metadata = DELETED_VALUE;
        }
        ema.setMetadata(metadata);
        ema.setKey(new PrimaryKey(urn.toString(), aspectName, version));
        ema.setCreatedOn(result.getTimestamp("lastmodifiedon"));
        ema.setCreatedBy(result.getString("lastmodifiedby"));
        ema.setCreatedFor(result.getString("creatdfor"));
        return ema;
      }
      return null;
    }
    return _server.find(EbeanMetadataAspect.class,
        new EbeanMetadataAspect.PrimaryKey(urn.toString(), aspectName, version));
  }

  private void assertVersionMetadata(ListResultMetadata listResultMetadata, List<Long> versions, List<Urn> urns,
      Long time, Urn actor, Urn impersonator) {
    List<ExtraInfo> extraInfos = listResultMetadata.getExtraInfos();
    assertEquals(extraInfos.stream().map(ExtraInfo::getVersion).collect(Collectors.toSet()), versions);
    assertEquals(extraInfos.stream().map(ExtraInfo::getUrn).collect(Collectors.toSet()), urns);

    extraInfos.forEach(v -> {
      assertEquals(v.getAudit().getTime(), time);
      assertEquals(v.getAudit().getActor(), actor);
      assertEquals(v.getAudit().getImpersonator(), impersonator);
    });
  }
}
