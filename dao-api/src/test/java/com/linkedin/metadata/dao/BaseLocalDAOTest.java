package com.linkedin.metadata.dao;

import com.linkedin.common.AuditStamp;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.metadata.dao.builder.BaseLocalRelationshipBuilder.LocalRelationshipUpdates;
import com.linkedin.metadata.dao.ingestion.AspectCallbackMapKey;
import com.linkedin.metadata.dao.ingestion.AspectCallbackRoutingClient;
import com.linkedin.metadata.dao.ingestion.SampleAspectCallbackRoutingClient;
import com.linkedin.metadata.dao.ingestion.SampleLambdaFunctionRegistryImpl;
import com.linkedin.metadata.dao.ingestion.AspectCallbackRegistry;
import com.linkedin.metadata.dao.producer.BaseMetadataEventProducer;
import com.linkedin.metadata.dao.producer.BaseTrackingMetadataEventProducer;
import com.linkedin.metadata.dao.retention.TimeBasedRetention;
import com.linkedin.metadata.dao.retention.VersionBasedRetention;
import com.linkedin.metadata.dao.tracking.BaseTrackingManager;
import com.linkedin.metadata.dao.urnpath.EmptyPathExtractor;
import com.linkedin.metadata.events.IngestionMode;
import com.linkedin.metadata.events.IngestionTrackingContext;
import com.linkedin.metadata.internal.IngestionParams;
import com.linkedin.metadata.query.ExtraInfo;
import com.linkedin.metadata.query.IndexFilter;
import com.linkedin.metadata.query.IndexGroupByCriterion;
import com.linkedin.metadata.query.IndexSortCriterion;
import com.linkedin.testing.AspectBar;
import com.linkedin.testing.AspectFoo;
import com.linkedin.testing.BarUrnArray;
import com.linkedin.testing.EntityAspectUnion;
import com.linkedin.testing.localrelationship.AspectFooBar;
import com.linkedin.testing.urn.BarUrn;
import com.linkedin.testing.urn.FooUrn;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.mockito.stubbing.OngoingStubbing;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.linkedin.common.AuditStamps.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class BaseLocalDAOTest {

  static class DummyTransactionRunner {
    public <T> T run(Supplier<T> block) {
      return block.get();
    }
  }

  static class DummyLocalDAO<ENTITY_ASPECT_UNION extends UnionTemplate> extends BaseLocalDAO<ENTITY_ASPECT_UNION, FooUrn> {

    private final BiFunction<FooUrn, Class<? extends RecordTemplate>, AspectEntry> _getLatestFunction;
    private final DummyTransactionRunner _transactionRunner;

    public DummyLocalDAO(Class<ENTITY_ASPECT_UNION> aspectClass, BiFunction<FooUrn, Class<? extends RecordTemplate>, AspectEntry> getLatestFunction,
        BaseMetadataEventProducer eventProducer, DummyTransactionRunner transactionRunner) {
      super(aspectClass, eventProducer, FooUrn.class, new EmptyPathExtractor<>());
      _getLatestFunction = getLatestFunction;
      _transactionRunner = transactionRunner;
    }

    public DummyLocalDAO(Class<ENTITY_ASPECT_UNION> aspectClass, BiFunction<FooUrn, Class<? extends RecordTemplate>, AspectEntry> getLatestFunction,
        BaseTrackingMetadataEventProducer eventProducer, BaseTrackingManager trackingManager, DummyTransactionRunner transactionRunner) {
      super(aspectClass, eventProducer, trackingManager, FooUrn.class, new EmptyPathExtractor<>());
      _getLatestFunction = getLatestFunction;
      _transactionRunner = transactionRunner;
    }

    @Override
    protected <ASPECT extends RecordTemplate> long saveLatest(FooUrn urn, Class<ASPECT> aspectClass, ASPECT oldEntry,
        AuditStamp oldAuditStamp, ASPECT newEntry, AuditStamp newAuditStamp, boolean isSoftDeleted,
        @Nullable IngestionTrackingContext trackingContext, boolean isTestMode) {
      return 0;
    }

    @Override
    protected <ASPECT_UNION extends RecordTemplate> int createNewAspect(@Nonnull FooUrn urn,
        @Nonnull List<AspectCreateLambda<? extends RecordTemplate>> aspectCreateLambdas,
        @Nonnull List<? extends RecordTemplate> aspectValues, @Nonnull AuditStamp newAuditStamp,
        @Nullable IngestionTrackingContext trackingContext, boolean isTestMode) {
      return aspectValues.size();
    }

    @Override
    public <ASPECT extends RecordTemplate> void updateEntityTables(@Nonnull FooUrn urn, @Nonnull Class<ASPECT> aspectClass) {

    }

    @Override
    public <ASPECT extends RecordTemplate> List<LocalRelationshipUpdates> backfillLocalRelationships(
        @Nonnull FooUrn urn, @Nonnull Class<ASPECT> aspectClass) {
      return null;
    }

    @Nonnull
    @Override
    protected <T> T runInTransactionWithRetry(Supplier<T> block, int maxTransactionRetry) {
      return _transactionRunner.run(block);
    }

    @Override
    protected <ASPECT extends RecordTemplate> AspectEntry<ASPECT> getLatest(FooUrn urn, Class<ASPECT> aspectClass,
        boolean isTestMode) {
      return _getLatestFunction.apply(urn, aspectClass);
    }

    @Override
    protected <ASPECT extends RecordTemplate> long getNextVersion(FooUrn urn, Class<ASPECT> aspectClass) {
      return 0;
    }

    @Override
    protected <ASPECT extends RecordTemplate> void insert(FooUrn urn, RecordTemplate value, Class<ASPECT> aspectClass,
        AuditStamp auditStamp, long version, @Nullable IngestionTrackingContext trackingContext, boolean isTestMode) {

    }

    @Override
    protected <ASPECT extends RecordTemplate> void updateWithOptimisticLocking(@Nonnull FooUrn urn,
        @Nullable RecordTemplate value, @Nonnull Class<ASPECT> aspectClass, @Nonnull AuditStamp newAuditStamp,
        long version, @Nonnull Timestamp oldTimestamp, @Nullable IngestionTrackingContext trackingContext,
        boolean isTestMode) {

    }

    @Override
    public boolean exists(FooUrn urn) {
      return true;
    }

    @Override
    protected <ASPECT extends RecordTemplate> void applyVersionBasedRetention(Class<ASPECT> aspectClass, FooUrn urn,
        VersionBasedRetention retention, long largestVersion) {

    }

    @Override
    protected <ASPECT extends RecordTemplate> void applyTimeBasedRetention(Class<ASPECT> aspectClass, FooUrn urn,
        TimeBasedRetention retention, long currentTime) {

    }

    @Override
    public <ASPECT extends RecordTemplate> ListResult<Long> listVersions(Class<ASPECT> aspectClass, FooUrn urn,
        int start, int pageSize) {
      return null;
    }

    @Override
    public <ASPECT extends RecordTemplate> ListResult<FooUrn> listUrns(Class<ASPECT> aspectClass, int start,
        int pageSize) {
      return null;
    }

    @Override
    public List<FooUrn> listUrns(@Nonnull IndexFilter indexFilter, @Nullable IndexSortCriterion indexSortCriterion,
        @Nullable FooUrn lastUrn, int pageSize) {
      return null;
    }

    @Override
    public ListResult<FooUrn> listUrns(@Nonnull IndexFilter indexFilter,
        @Nullable IndexSortCriterion indexSortCriterion, int start, int pageSize) {
      return ListResult.<FooUrn>builder().build();
    }

    @Override
    public <ASPECT extends RecordTemplate> ListResult<ASPECT> list(Class<ASPECT> aspectClass, FooUrn urn, int start,
        int pageSize) {
      return null;
    }

    @Override
    public <ASPECT extends RecordTemplate> ListResult<ASPECT> list(Class<ASPECT> aspectClass, long version, int start,
        int pageSize) {
      return null;
    }

    @Override
    public <ASPECT extends RecordTemplate> ListResult<ASPECT> list(Class<ASPECT> aspectClass, int start, int pageSize) {
      return null;
    }

    @Override
    public Map<String, Long> countAggregate(@Nonnull IndexFilter indexFilter, @Nonnull IndexGroupByCriterion groupCriterion) {
      return Collections.emptyMap();
    }

    @Override
    public long newNumericId(String namespace, int maxTransactionRetry) {
      return 0;
    }

    @Override
    @Nonnull
    public Map<AspectKey<FooUrn, ? extends RecordTemplate>, Optional<? extends RecordTemplate>> get(
        Set<AspectKey<FooUrn, ? extends RecordTemplate>> aspectKeys) {
      return Collections.emptyMap();
    }

    @Override
    @Nonnull
    public Map<AspectKey<FooUrn, ? extends RecordTemplate>, AspectWithExtraInfo<? extends RecordTemplate>> getWithExtraInfo(
        @Nonnull Set<AspectKey<FooUrn, ? extends RecordTemplate>> keys) {
      return Collections.emptyMap();
    }
  }

  private DummyLocalDAO<EntityAspectUnion> _dummyLocalDAO;
  private AuditStamp _dummyAuditStamp;
  private BaseMetadataEventProducer _mockEventProducer;
  private BaseTrackingMetadataEventProducer _mockTrackingEventProducer;
  private BaseTrackingManager _mockTrackingManager;
  private BiFunction<FooUrn, Class<? extends RecordTemplate>, BaseLocalDAO.AspectEntry> _mockGetLatestFunction;
  private DummyTransactionRunner _mockTransactionRunner;

  @BeforeMethod
  public void setup() {
    _mockGetLatestFunction = mock(BiFunction.class);
    _mockEventProducer = mock(BaseMetadataEventProducer.class);
    _mockTrackingEventProducer = mock(BaseTrackingMetadataEventProducer.class);
    _mockTrackingManager = mock(BaseTrackingManager.class);
    _mockTransactionRunner = spy(DummyTransactionRunner.class);
    _dummyLocalDAO = new DummyLocalDAO<>(EntityAspectUnion.class, _mockGetLatestFunction, _mockEventProducer,
        _mockTransactionRunner);
    _dummyLocalDAO.setEmitAuditEvent(true);
    _dummyLocalDAO.setEmitAspectSpecificAuditEvent(true);
    _dummyAuditStamp = makeAuditStamp("foo", 1234);
  }

  private <T extends RecordTemplate> BaseLocalDAO.AspectEntry<T> makeAspectEntry(T aspect,
      AuditStamp auditStamp) {
    ExtraInfo extraInfo = null;
    if (auditStamp != null) {
      extraInfo = new ExtraInfo().setAudit(auditStamp);
    }
    return new BaseLocalDAO.AspectEntry<>(aspect, extraInfo);
  }

  private <T extends RecordTemplate> void expectGetLatest(FooUrn urn, Class<T> aspectClass,
      List<BaseLocalDAO.AspectEntry<T>> returnValues) {
    OngoingStubbing<BaseLocalDAO.AspectEntry<T>> ongoing = when(_mockGetLatestFunction.apply(urn, aspectClass));
    for (BaseLocalDAO.AspectEntry<T> value : returnValues) {
      ongoing = ongoing.thenReturn(value);
    }
  }

  @Test
  public void testMAEEmissionAlways() throws URISyntaxException {
    FooUrn urn = new FooUrn(1);
    AspectFoo foo = new AspectFoo().setValue("foo");
    _dummyLocalDAO.setAlwaysEmitAuditEvent(true);
    expectGetLatest(urn, AspectFoo.class,
        Arrays.asList(makeAspectEntry(null, null), makeAspectEntry(foo, _dummyAuditStamp)));

    _dummyLocalDAO.add(urn, foo, _dummyAuditStamp);
    _dummyLocalDAO.add(urn, foo, _dummyAuditStamp);

    verify(_mockEventProducer, times(1)).produceMetadataAuditEvent(urn, null, foo);
    verify(_mockEventProducer, times(1)).produceAspectSpecificMetadataAuditEvent(urn, null, foo, _dummyAuditStamp, IngestionMode.LIVE);
    verify(_mockEventProducer, times(1)).produceMetadataAuditEvent(urn, foo, foo);
    verifyNoMoreInteractions(_mockEventProducer);
  }

  @Test
  public void testMAEEmissionOnValueChange() throws URISyntaxException {
    FooUrn urn = new FooUrn(1);
    AspectFoo foo1 = new AspectFoo().setValue("foo1");
    AspectFoo foo2 = new AspectFoo().setValue("foo2");
    _dummyLocalDAO.setAlwaysEmitAuditEvent(false);
    expectGetLatest(urn, AspectFoo.class,
        Arrays.asList(makeAspectEntry(null, null), makeAspectEntry(foo1, _dummyAuditStamp)));

    _dummyLocalDAO.add(urn, foo1, _dummyAuditStamp);
    AuditStamp auditStamp2 = makeAuditStamp("tester", 5678L);
    _dummyLocalDAO.add(urn, foo2, auditStamp2);

    verify(_mockEventProducer, times(1)).produceMetadataAuditEvent(urn, null, foo1);
    verify(_mockEventProducer, times(1)).produceAspectSpecificMetadataAuditEvent(urn, null, foo1, _dummyAuditStamp, IngestionMode.LIVE);
    verify(_mockEventProducer, times(1)).produceMetadataAuditEvent(urn, foo1, foo2);
    verify(_mockEventProducer, times(1)).produceAspectSpecificMetadataAuditEvent(urn, foo1, foo2, auditStamp2, IngestionMode.LIVE);
    verifyNoMoreInteractions(_mockEventProducer);
  }

  @Test
  public void testMAEEmissionNoValueChange() throws URISyntaxException {
    FooUrn urn = new FooUrn(1);
    AspectFoo foo1 = new AspectFoo().setValue("foo");
    AspectFoo foo2 = new AspectFoo().setValue("foo");
    AspectFoo foo3 = new AspectFoo().setValue("foo");
    _dummyLocalDAO.setAlwaysEmitAuditEvent(false);
    expectGetLatest(urn, AspectFoo.class,
        Arrays.asList(makeAspectEntry(null, null), makeAspectEntry(foo1, _dummyAuditStamp)));

    _dummyLocalDAO.add(urn, foo1, _dummyAuditStamp);
    _dummyLocalDAO.add(urn, foo2, _dummyAuditStamp);
    _dummyLocalDAO.add(urn, foo3, _dummyAuditStamp);

    verify(_mockEventProducer, times(1)).produceMetadataAuditEvent(urn, null, foo1);
    verify(_mockEventProducer, times(1)).produceAspectSpecificMetadataAuditEvent(urn, null, foo1, _dummyAuditStamp, IngestionMode.LIVE);
    verifyNoMoreInteractions(_mockEventProducer);
  }

  @Test
  public void testMAEWithNullValue() throws URISyntaxException {
    FooUrn urn = new FooUrn(1);
    AspectFoo foo = new AspectFoo().setValue("foo");
    _dummyLocalDAO.setAlwaysEmitAuditEvent(true);
    expectGetLatest(urn, AspectFoo.class,
        Arrays.asList(makeAspectEntry(null, null), makeAspectEntry(foo, _dummyAuditStamp)));

    _dummyLocalDAO.add(urn, foo, _dummyAuditStamp);
    _dummyLocalDAO.delete(urn, AspectFoo.class, _dummyAuditStamp);

    verify(_mockEventProducer, times(1)).produceMetadataAuditEvent(urn, null, foo);
    verify(_mockEventProducer, times(1)).produceAspectSpecificMetadataAuditEvent(urn, null, foo, _dummyAuditStamp, IngestionMode.LIVE);
    // TODO: ensure MAE is produced with newValue set as null for soft deleted aspect
    // verify(_mockEventProducer, times(1)).produceMetadataAuditEvent(urn, foo, null);
    verifyNoMoreInteractions(_mockEventProducer);
  }

  @Test
  public void testMAEv5WithTracking() throws URISyntaxException {
    FooUrn urn = new FooUrn(1);
    AspectFoo foo = new AspectFoo().setValue("foo");
    IngestionTrackingContext mockTrackingContext = mock(IngestionTrackingContext.class);
    DummyLocalDAO<EntityAspectUnion> dummyLocalDAO = new DummyLocalDAO<>(EntityAspectUnion.class,
        _mockGetLatestFunction, _mockTrackingEventProducer, _mockTrackingManager,
        _dummyLocalDAO._transactionRunner);
    dummyLocalDAO.setEmitAuditEvent(true);
    dummyLocalDAO.setAlwaysEmitAuditEvent(true);
    dummyLocalDAO.setEmitAspectSpecificAuditEvent(true);
    dummyLocalDAO.setAlwaysEmitAspectSpecificAuditEvent(true);
    expectGetLatest(urn, AspectFoo.class,
        Arrays.asList(makeAspectEntry(null, null), makeAspectEntry(foo, _dummyAuditStamp)));

    dummyLocalDAO.add(urn, foo, _dummyAuditStamp, mockTrackingContext, null);
    dummyLocalDAO.add(urn, foo, _dummyAuditStamp, mockTrackingContext, null);

    verify(_mockTrackingEventProducer, times(1)).produceMetadataAuditEvent(urn, null, foo);
    verify(_mockTrackingEventProducer, times(1)).produceMetadataAuditEvent(urn, foo, foo);
    verify(_mockTrackingEventProducer, times(1)).produceAspectSpecificMetadataAuditEvent(urn, null,
        foo, _dummyAuditStamp, mockTrackingContext, IngestionMode.LIVE);
    verify(_mockTrackingEventProducer, times(1)).produceAspectSpecificMetadataAuditEvent(urn, foo,
        foo, _dummyAuditStamp, mockTrackingContext, IngestionMode.LIVE);
    verifyNoMoreInteractions(_mockTrackingEventProducer);
  }

  @Test
  public void testMAEv5WithOverride() throws URISyntaxException {
    FooUrn urn = new FooUrn(1);
    AspectFoo foo = new AspectFoo().setValue("foo");
    IngestionTrackingContext mockTrackingContext = mock(IngestionTrackingContext.class);
    DummyLocalDAO<EntityAspectUnion> dummyLocalDAO = new DummyLocalDAO<>(EntityAspectUnion.class,
        _mockGetLatestFunction, _mockTrackingEventProducer, _mockTrackingManager,
        _dummyLocalDAO._transactionRunner);

    // pretend there is already foo in the database
    when(dummyLocalDAO.getLatest(urn, AspectFoo.class, false))
        .thenReturn(new BaseLocalDAO.AspectEntry<>(foo, null, false));

    // try to add foo again but with the OVERRIDE write mode
    dummyLocalDAO.add(urn, foo, _dummyAuditStamp, mockTrackingContext, new IngestionParams().setIngestionMode(IngestionMode.LIVE_OVERRIDE));

    // verify that there are no MAE emissions
    verifyNoMoreInteractions(_mockTrackingEventProducer);
  }

  @Test
  public void testAddSamePreUpdateHookTwice() {
    BiConsumer<FooUrn, AspectFoo> hook = (urn, foo) -> {
      // do nothing;
    };

    _dummyLocalDAO.addPreUpdateHook(AspectFoo.class, hook);

    try {
      _dummyLocalDAO.addPreUpdateHook(AspectFoo.class, hook);
    } catch (IllegalArgumentException e) {
      // expected
      return;
    }

    fail("No IllegalArgumentException thrown");
  }

  @Test
  public void testPreUpdateHookInvoked() throws URISyntaxException {
    FooUrn urn = new FooUrn(1);
    AspectFoo foo = new AspectFoo().setValue("foo");
    BiConsumer<FooUrn, AspectFoo> hook = mock(BiConsumer.class);
    expectGetLatest(urn, AspectFoo.class,
        Collections.singletonList(makeAspectEntry(null, null)));

    _dummyLocalDAO.addPreUpdateHook(AspectFoo.class, hook);
    _dummyLocalDAO.add(urn, foo, _dummyAuditStamp);

    verify(hook, times(1)).accept(urn, foo);
    verifyNoMoreInteractions(hook);
  }

  @Test
  public void testAddSamePostUpdateHookTwice() {
    BiConsumer<FooUrn, AspectFoo> hook = (urn, foo) -> {
      // do nothing;
    };

    _dummyLocalDAO.addPostUpdateHook(AspectFoo.class, hook);

    try {
      _dummyLocalDAO.addPostUpdateHook(AspectFoo.class, hook);
    } catch (IllegalArgumentException e) {
      // expected
      return;
    }

    fail("No IllegalArgumentException thrown");
  }

  @Test
  public void testPostUpdateHookInvoked() throws URISyntaxException {
    FooUrn urn = new FooUrn(1);
    AspectFoo foo = new AspectFoo().setValue("foo");
    BiConsumer<FooUrn, AspectFoo> hook = mock(BiConsumer.class);
    expectGetLatest(urn, AspectFoo.class,
        Collections.singletonList(makeAspectEntry(null, null)));

    _dummyLocalDAO.addPostUpdateHook(AspectFoo.class, hook);
    _dummyLocalDAO.add(urn, foo, _dummyAuditStamp);

    verify(hook, times(1)).accept(urn, foo);
    verifyNoMoreInteractions(hook);
  }

  @Test
  public void testAtomicUpdateEnableUsesOneTransaction() throws URISyntaxException {
    FooUrn urn = new FooUrn(1);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectBar bar = new AspectBar().setValue("bar");

    _dummyLocalDAO.enableAtomicMultipleUpdate(true);
    when(_mockGetLatestFunction.apply(any(), eq(AspectFoo.class))).thenReturn(new BaseLocalDAO.AspectEntry<AspectFoo>(null, null));
    when(_mockGetLatestFunction.apply(any(), eq(AspectBar.class))).thenReturn(new BaseLocalDAO.AspectEntry<AspectBar>(null, null));

    _dummyLocalDAO.addMany(urn, Arrays.asList(foo, bar), _dummyAuditStamp);

    verify(_mockTransactionRunner, times(1)).run(any());
  }

  @Test
  public void testAtomicUpdateDisabledUsesMultipleTransactions() throws URISyntaxException {
    FooUrn urn = new FooUrn(1);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectBar bar = new AspectBar().setValue("bar");

    _dummyLocalDAO.enableAtomicMultipleUpdate(false);
    when(_mockGetLatestFunction.apply(any(), eq(AspectFoo.class))).thenReturn(new BaseLocalDAO.AspectEntry<AspectFoo>(null, null));
    when(_mockGetLatestFunction.apply(any(), eq(AspectBar.class))).thenReturn(new BaseLocalDAO.AspectEntry<AspectBar>(null, null));

    _dummyLocalDAO.addMany(urn, Arrays.asList(foo, bar), _dummyAuditStamp);

    verify(_mockTransactionRunner, times(2)).run(any());
  }

  @DataProvider(name = "addBackfillForNoopCases")
  public Object[][] addBackfillForNoopCases() {
    AuditStamp oldAuditStamp = makeAuditStamp("susActor", 6L);

    // case 1 - emitTime doesn't exist
    IngestionTrackingContext context1 = new IngestionTrackingContext();
    context1.setBackfill(true);

    // case 2 - new emit time < old emit time
    IngestionTrackingContext context2 = new IngestionTrackingContext();
    context2.setBackfill(true);
    context2.setEmitTime(4L);
    long oldEmitTime2 = 5L;

    // case 3 - new emit time < old emit time (same as case 2, but old stamp < new emit time)
    IngestionTrackingContext context3 = new IngestionTrackingContext();
    context3.setBackfill(true);
    context3.setEmitTime(10L);
    long oldEmitTime3 = 11L;

    // case 4 - old emit time = null, new emit time < old audit stamp
    IngestionTrackingContext context4 = new IngestionTrackingContext();
    context4.setBackfill(true);
    context4.setEmitTime(3L);

    return new Object[][] {
        { context1, oldAuditStamp, null },
        { context2, oldAuditStamp, oldEmitTime2 },
        { context3, oldAuditStamp, oldEmitTime3 },
        { context4, oldAuditStamp, null }
    };
  }

  @Test(description = "Each test case represents a scenario where a backfill event should NOT be backfilled",
      dataProvider = "addBackfillForNoopCases")
  public void testAddForBackfillEventsWhenWeShouldNotDoBackfill(
      IngestionTrackingContext ingestionTrackingContext, AuditStamp oldAuditStamp, Long oldEmitTime
  ) throws URISyntaxException {
    FooUrn urn = new FooUrn(1);
    AspectFoo oldFoo = new AspectFoo().setValue("oldFoo");
    AspectFoo newFoo = new AspectFoo().setValue("newFoo");

    ExtraInfo extraInfo = new ExtraInfo();
    extraInfo.setAudit(oldAuditStamp);
    extraInfo.setEmitTime(oldEmitTime, SetMode.IGNORE_NULL);

    DummyLocalDAO<EntityAspectUnion> dummyLocalDAO = new DummyLocalDAO<>(EntityAspectUnion.class,
        _mockGetLatestFunction, _mockTrackingEventProducer, _mockTrackingManager,
        _dummyLocalDAO._transactionRunner);
    dummyLocalDAO.setEmitAuditEvent(true);
    dummyLocalDAO.setAlwaysEmitAuditEvent(true);
    dummyLocalDAO.setEmitAspectSpecificAuditEvent(true);
    dummyLocalDAO.setAlwaysEmitAspectSpecificAuditEvent(true);
    BaseLocalDAO.AspectEntry<AspectFoo> aspectEntry = new BaseLocalDAO.AspectEntry<>(oldFoo, extraInfo);
    expectGetLatest(urn, AspectFoo.class, Collections.singletonList(aspectEntry));

    dummyLocalDAO.add(urn, newFoo, _dummyAuditStamp, ingestionTrackingContext, null);

    verify(_mockTrackingEventProducer, times(1)).produceMetadataAuditEvent(urn, oldFoo, oldFoo);
    verify(_mockTrackingEventProducer, times(1)).produceAspectSpecificMetadataAuditEvent(
        urn, oldFoo, oldFoo, _dummyAuditStamp, ingestionTrackingContext, IngestionMode.LIVE);
    verifyNoMoreInteractions(_mockTrackingEventProducer);
  }

  @DataProvider(name = "addBackfillForCasesThatShouldBackfill")
  public Object[][] addBackfillForCasesThatShouldBackfill() {
    AuditStamp oldAuditStamp = makeAuditStamp("susActor", 6L);

    // case 1 - emitTime exists and is larger than old emit time
    IngestionTrackingContext context1 = new IngestionTrackingContext();
    context1.setBackfill(true);
    context1.setEmitTime(5L);
    long oldEmitTime1 = 4L;

    // case 2 - emitTime exists and is larger than old emit time
    IngestionTrackingContext context2 = new IngestionTrackingContext();
    context2.setBackfill(true);
    context2.setEmitTime(10L);
    long oldEmitTime2 = 4L;

    // case 3 - emitTime exists, old emitTime doesn't exist, emitTime > old audit stamp
    IngestionTrackingContext context3 = new IngestionTrackingContext();
    context3.setBackfill(true);
    context3.setEmitTime(7L);

    return new Object[][] {
        { context1, oldAuditStamp, oldEmitTime1 },
        { context2, oldAuditStamp, oldEmitTime2 },
        { context3, oldAuditStamp, null }
    };
  }

  @Test(description = "Event should be processed for backfill event", dataProvider = "addBackfillForCasesThatShouldBackfill")
  public void testAddForBackfill(
      IngestionTrackingContext ingestionTrackingContext, AuditStamp oldAuditStamp, Long oldEmitTime
  ) throws URISyntaxException {
    FooUrn urn = new FooUrn(1);
    AspectFoo oldFoo = new AspectFoo().setValue("oldFoo");
    AspectFoo newFoo = new AspectFoo().setValue("newFoo");

    ExtraInfo extraInfo = new ExtraInfo();
    extraInfo.setAudit(oldAuditStamp);
    extraInfo.setEmitTime(oldEmitTime, SetMode.IGNORE_NULL);

    DummyLocalDAO<EntityAspectUnion> dummyLocalDAO = new DummyLocalDAO<>(EntityAspectUnion.class,
        _mockGetLatestFunction, _mockTrackingEventProducer, _mockTrackingManager,
        _dummyLocalDAO._transactionRunner);
    dummyLocalDAO.setEmitAuditEvent(true);
    dummyLocalDAO.setAlwaysEmitAuditEvent(true);
    dummyLocalDAO.setEmitAspectSpecificAuditEvent(true);
    dummyLocalDAO.setAlwaysEmitAspectSpecificAuditEvent(true);
    BaseLocalDAO.AspectEntry<AspectFoo> aspectEntry = new BaseLocalDAO.AspectEntry<>(oldFoo, extraInfo);
    expectGetLatest(urn, AspectFoo.class, Collections.singletonList(aspectEntry));

    dummyLocalDAO.add(urn, newFoo, _dummyAuditStamp, ingestionTrackingContext, null);

    verify(_mockTrackingEventProducer, times(1)).produceMetadataAuditEvent(urn, oldFoo, newFoo);
    verify(_mockTrackingEventProducer, times(1)).produceAspectSpecificMetadataAuditEvent(
        urn, oldFoo, newFoo, _dummyAuditStamp, ingestionTrackingContext, IngestionMode.LIVE);
    verifyNoMoreInteractions(_mockTrackingEventProducer);
  }

  @Test(description = "Event should be processed for backfill event since latest aspect is null")
  public void testAddForBackfillWhenLatestIsNull() throws URISyntaxException {
    FooUrn urn = new FooUrn(1);
    AspectFoo newFoo = new AspectFoo().setValue("newFoo");

    ExtraInfo extraInfo = new ExtraInfo();
    AuditStamp oldAuditStamp = makeAuditStamp("nonSusActor", 5L);
    extraInfo.setAudit(oldAuditStamp);

    DummyLocalDAO<EntityAspectUnion> dummyLocalDAO = new DummyLocalDAO<>(EntityAspectUnion.class,
        _mockGetLatestFunction, _mockTrackingEventProducer, _mockTrackingManager,
        _dummyLocalDAO._transactionRunner);
    dummyLocalDAO.setEmitAuditEvent(true);
    dummyLocalDAO.setAlwaysEmitAuditEvent(true);
    dummyLocalDAO.setEmitAspectSpecificAuditEvent(true);
    dummyLocalDAO.setAlwaysEmitAspectSpecificAuditEvent(true);
    expectGetLatest(urn, AspectFoo.class, Collections.singletonList(makeAspectEntry(null, oldAuditStamp)));

    IngestionTrackingContext ingestionTrackingContext = new IngestionTrackingContext();
    ingestionTrackingContext.setBackfill(true);
    // intentionally set it to be smaller than old audit stamp to make sure that if latest aspect is null,
    // we always proceed with backfill
    // Although this should not happen in real life
    ingestionTrackingContext.setEmitTime(4L);

    dummyLocalDAO.add(urn, newFoo, _dummyAuditStamp, ingestionTrackingContext, null);

    verify(_mockTrackingEventProducer, times(1)).produceMetadataAuditEvent(urn, null, newFoo);
    verify(_mockTrackingEventProducer, times(1)).produceAspectSpecificMetadataAuditEvent(
        urn, null, newFoo, _dummyAuditStamp, ingestionTrackingContext, IngestionMode.LIVE);
    verifyNoMoreInteractions(_mockTrackingEventProducer);
  }

  @Test
  public void testPreIngestionLambda() throws URISyntaxException {
    FooUrn urn = new FooUrn(1);
    AspectFooBar fooBar1 = new AspectFooBar().setBars(new BarUrnArray(new BarUrn(1), new BarUrn(2)));
    AspectFooBar fooBar2 = new AspectFooBar().setBars(new BarUrnArray(new BarUrn(3), new BarUrn(4)));
    AspectFooBar mergedFooBar =
        new AspectFooBar().setBars(new BarUrnArray(new BarUrn(1), new BarUrn(2), new BarUrn(3), new BarUrn(4)));
    DummyLocalDAO<EntityAspectUnion> dummyLocalDAO =
        new DummyLocalDAO<>(EntityAspectUnion.class, _mockGetLatestFunction, _mockTrackingEventProducer,
            _mockTrackingManager, _dummyLocalDAO._transactionRunner);
    dummyLocalDAO.setEmitAuditEvent(true);
    dummyLocalDAO.setAlwaysEmitAuditEvent(true);
    dummyLocalDAO.setEmitAspectSpecificAuditEvent(true);
    dummyLocalDAO.setAlwaysEmitAspectSpecificAuditEvent(true);
    dummyLocalDAO.setLambdaFunctionRegistry(new SampleLambdaFunctionRegistryImpl());
    expectGetLatest(urn, AspectFooBar.class,
        Arrays.asList(makeAspectEntry(null, null), makeAspectEntry(fooBar1, _dummyAuditStamp)));

    dummyLocalDAO.add(urn, fooBar1, _dummyAuditStamp);
    verify(_mockTrackingEventProducer, times(1)).produceMetadataAuditEvent(urn, null, fooBar1);
    verify(_mockTrackingEventProducer, times(1)).produceAspectSpecificMetadataAuditEvent(urn, null, fooBar1,
        _dummyAuditStamp, null, IngestionMode.LIVE);

    AuditStamp auditStamp2 = makeAuditStamp("tester", 5678L);
    dummyLocalDAO.add(urn, fooBar2, auditStamp2);
    verify(_mockTrackingEventProducer, times(1)).produceMetadataAuditEvent(urn, fooBar1, mergedFooBar);
    verify(_mockTrackingEventProducer, times(1)).produceAspectSpecificMetadataAuditEvent(urn, fooBar1, mergedFooBar,
        auditStamp2, null, IngestionMode.LIVE);

    verifyNoMoreInteractions(_mockTrackingEventProducer);
  }

  @Test
  public void testAspectCallbackHelperFromFooToBar() throws URISyntaxException {
    // Setup test data
    FooUrn urn = new FooUrn(1);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectFoo bar = new AspectFoo().setValue("bar");

    Map<AspectCallbackMapKey, AspectCallbackRoutingClient> aspectCallbackMap = new HashMap<>();
    AspectCallbackMapKey aspectCallbackMapKey = new AspectCallbackMapKey(AspectFoo.class, urn.getEntityType());
    aspectCallbackMap.put(aspectCallbackMapKey, new SampleAspectCallbackRoutingClient());

    AspectCallbackRegistry aspectCallbackRegistry = new AspectCallbackRegistry(aspectCallbackMap);
    _dummyLocalDAO.setAspectCallbackRegistry(aspectCallbackRegistry);
    BaseLocalDAO.AspectUpdateResult result = _dummyLocalDAO.aspectCallbackHelper(urn, foo, Optional.empty(), null);
    AspectFoo newAspect = (AspectFoo) result.getUpdatedAspect();
    assertEquals(newAspect, bar);
  }

  @Test
  public void testCreateAspectWithCallbacks() throws URISyntaxException {
    // Setup test data
    FooUrn urn = new FooUrn(1);
    RecordTemplate foo = new AspectFoo().setValue("foo");
    RecordTemplate bar = new AspectBar().setValue("bar");

    BaseLocalDAO.AspectCreateLambda<RecordTemplate>
        fooCreateLambda = new BaseLocalDAO.AspectCreateLambda<>(foo);
    BaseLocalDAO.AspectCreateLambda<RecordTemplate>
        barCreateLambda = new BaseLocalDAO.AspectCreateLambda<>(bar);

    Map<AspectCallbackMapKey, AspectCallbackRoutingClient> aspectCallbackMap = new HashMap<>();
    AspectCallbackMapKey aspectCallbackMapKey = new AspectCallbackMapKey(AspectFoo.class, urn.getEntityType());
    aspectCallbackMap.put(aspectCallbackMapKey, new SampleAspectCallbackRoutingClient());

    AspectCallbackRegistry aspectCallbackRegistry = new AspectCallbackRegistry(aspectCallbackMap);
    _dummyLocalDAO.setAspectCallbackRegistry(aspectCallbackRegistry);

    List<BaseLocalDAO.AspectCreateLambda<? extends RecordTemplate>> aspectCreateLambdas = new ArrayList<>();
    aspectCreateLambdas.add(fooCreateLambda);
    aspectCreateLambdas.add(barCreateLambda);

    List<RecordTemplate> aspectValues = new ArrayList<>();
    aspectValues.add(foo);
    aspectValues.add(bar);

    FooUrn result = _dummyLocalDAO.createAspectsWithCallbacks(urn, aspectValues, aspectCreateLambdas, _dummyAuditStamp, null);
    assertEquals(result, urn);
  }

  @Test
  public void testMAEEmissionForAspectCallbackHelper() throws URISyntaxException {
    FooUrn urn = new FooUrn(1);
    AspectFoo foo = new AspectFoo().setValue("foo");
    AspectFoo bar = new AspectFoo().setValue("bar");
    _dummyLocalDAO.setAlwaysEmitAuditEvent(true);
    Map<AspectCallbackMapKey, AspectCallbackRoutingClient> aspectCallbackMap = new HashMap<>();
    aspectCallbackMap.put(new AspectCallbackMapKey(AspectFoo.class, urn.getEntityType()), new SampleAspectCallbackRoutingClient());
    AspectCallbackRegistry aspectCallbackRegistry = new AspectCallbackRegistry(aspectCallbackMap);
    _dummyLocalDAO.setAspectCallbackRegistry(aspectCallbackRegistry);
    expectGetLatest(urn, AspectFoo.class,
        Arrays.asList(makeAspectEntry(null, null), makeAspectEntry(foo, _dummyAuditStamp)));

    _dummyLocalDAO.add(urn, foo, _dummyAuditStamp);

    verify(_mockEventProducer, times(1)).produceMetadataAuditEvent(urn, null, bar);
    verify(_mockEventProducer, times(1)).produceAspectSpecificMetadataAuditEvent(urn, null, bar, _dummyAuditStamp, IngestionMode.LIVE);
    verifyNoMoreInteractions(_mockEventProducer);
  }

  @Test
  public void testAspectCallbackHelperWithUnregisteredAspect() throws URISyntaxException {
    // Setup test data
    FooUrn urn = new FooUrn(1);
    AspectBar foo = new AspectBar().setValue("foo");

    // Inject RestliPreIngestionAspectRegistry with no registered aspect
    Map<AspectCallbackMapKey, AspectCallbackRoutingClient> aspectCallbackMap = new HashMap<>();
    aspectCallbackMap.put(new AspectCallbackMapKey(AspectFoo.class, urn.getEntityType()), new SampleAspectCallbackRoutingClient());
    AspectCallbackRegistry aspectCallbackRegistry = new AspectCallbackRegistry(aspectCallbackMap);
    _dummyLocalDAO.setAspectCallbackRegistry(aspectCallbackRegistry);

    // Call the add method
    BaseLocalDAO.AspectUpdateResult result = _dummyLocalDAO.aspectCallbackHelper(urn, foo, Optional.empty(), null);

    // Verify that the result is the same as the input aspect since it's not registered
    assertEquals(result.getUpdatedAspect(), foo);
  }
}
