package com.linkedin.metadata.dao;

import com.linkedin.common.AuditStamp;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.builder.BaseLocalRelationshipBuilder.LocalRelationshipUpdates;
import com.linkedin.metadata.dao.producer.BaseMetadataEventProducer;
import com.linkedin.metadata.dao.producer.BaseTrackingMetadataEventProducer;
import com.linkedin.metadata.dao.retention.TimeBasedRetention;
import com.linkedin.metadata.dao.retention.VersionBasedRetention;
import com.linkedin.metadata.dao.tracking.BaseTrackingManager;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.events.IngestionTrackingContext;
import com.linkedin.metadata.query.ExtraInfo;
import com.linkedin.metadata.query.IndexFilter;
import com.linkedin.metadata.query.IndexGroupByCriterion;
import com.linkedin.metadata.query.IndexSortCriterion;
import com.linkedin.testing.AspectVersioned;
import com.linkedin.testing.EntityAspectUnionVersioned;
import com.linkedin.testing.urn.FooUrn;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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


public class BaseLocalDAOAspectVersionTest {

  static class DummyTransactionRunner {
    public <T> T run(Supplier<T> block) {
      return block.get();
    }
  }

  static class DummyLocalDAO extends BaseLocalDAO<EntityAspectUnionVersioned, FooUrn> {

    private final BiFunction<FooUrn, Class<? extends RecordTemplate>, AspectEntry> _getLatestFunction;
    private final DummyTransactionRunner _transactionRunner;

    public DummyLocalDAO(BiFunction<FooUrn, Class<? extends RecordTemplate>, AspectEntry> getLatestFunction,
        BaseMetadataEventProducer eventProducer, DummyTransactionRunner transactionRunner) {
      super(EntityAspectUnionVersioned.class, eventProducer, FooUrn.class);
      _getLatestFunction = getLatestFunction;
      _transactionRunner = transactionRunner;
    }

    public DummyLocalDAO(BiFunction<FooUrn, Class<? extends RecordTemplate>, AspectEntry> getLatestFunction,
        BaseTrackingMetadataEventProducer eventProducer, BaseTrackingManager trackingManager, DummyTransactionRunner transactionRunner) {
      super(EntityAspectUnionVersioned.class, eventProducer, trackingManager, FooUrn.class);
      _getLatestFunction = getLatestFunction;
      _transactionRunner = transactionRunner;
    }

    @Override
    protected <ASPECT extends RecordTemplate> long saveLatest(FooUrn urn, Class<ASPECT> aspectClass, ASPECT oldEntry,
        AuditStamp oldAuditStamp, ASPECT newEntry, AuditStamp newAuditStamp, boolean isSoftDeleted,
        @Nullable IngestionTrackingContext trackingContext) {
      return 0;
    }

    @Override
    public <ASPECT extends RecordTemplate> void updateEntityTables(@Nonnull FooUrn urn, @Nonnull Class<ASPECT> aspectClass) {

    }

    @Override
    public <ASPECT extends RecordTemplate> List<LocalRelationshipUpdates> backfillLocalRelationshipsFromEntityTables(
        @Nonnull FooUrn urn, @Nonnull Class<ASPECT> aspectClass) {
      return null;
    }

    @Nonnull
    @Override
    protected <T> T runInTransactionWithRetry(Supplier<T> block, int maxTransactionRetry) {
      return _transactionRunner.run(block);
    }

    @Override
    protected <ASPECT extends RecordTemplate> AspectEntry<ASPECT> getLatest(FooUrn urn, Class<ASPECT> aspectClass) {
      return _getLatestFunction.apply(urn, aspectClass);
    }

    @Override
    protected <ASPECT extends RecordTemplate> long getNextVersion(FooUrn urn, Class<ASPECT> aspectClass) {
      return 0;
    }

    @Override
    protected <ASPECT extends RecordTemplate> void insert(FooUrn urn, RecordTemplate value, Class<ASPECT> aspectClass,
        AuditStamp auditStamp, long version, @Nullable IngestionTrackingContext trackingContext) {

    }

    @Override
    protected <ASPECT extends RecordTemplate> void updateWithOptimisticLocking(@Nonnull FooUrn urn,
        @Nullable RecordTemplate value, @Nonnull Class<ASPECT> aspectClass, @Nonnull AuditStamp newAuditStamp,
        long version, @Nonnull Timestamp oldTimestamp, @Nullable IngestionTrackingContext trackingContext) {

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

  private DummyLocalDAO _dummyLocalDAO;
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
    _dummyLocalDAO = new DummyLocalDAO(_mockGetLatestFunction, _mockEventProducer, _mockTransactionRunner);
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



  @Test(description = "Test MAE emission triggered by incoming aspects with higher versions")
  public void testMAEEmissionOnVerChange() throws URISyntaxException {
    FooUrn urn = new FooUrn(1);
    AspectVersioned foo1 = new AspectVersioned().setValue("foo1");
    AspectVersioned ver010101 = RecordUtils.toRecordTemplate(AspectVersioned.class, createVersionDataMap(1, 1, 1, "ver1"));
    AspectVersioned ver020101 = RecordUtils.toRecordTemplate(AspectVersioned.class, createVersionDataMap(2, 1, 1, "ver2"));

    // Test that a version bump without a value change will still cause aspect to be written
    AspectVersioned ver020201OldValue = RecordUtils.toRecordTemplate(AspectVersioned.class, createVersionDataMap(2, 2, 1, "ver2"));

    AuditStamp auditStamp2 = makeAuditStamp("tester", 5678L);
    AuditStamp auditStamp3 = makeAuditStamp("tester", 5679L);
    AuditStamp auditStamp4 = makeAuditStamp("tester", 5680L);

    _dummyLocalDAO.setAlwaysEmitAuditEvent(false);
    expectGetLatest(urn, AspectVersioned.class,
        Arrays.asList(makeAspectEntry(null, null), makeAspectEntry(foo1, _dummyAuditStamp),
            makeAspectEntry(ver010101, auditStamp2), makeAspectEntry(ver020101, auditStamp3), makeAspectEntry(ver020201OldValue, auditStamp4)));

    _dummyLocalDAO.add(urn, foo1, _dummyAuditStamp);
    _dummyLocalDAO.add(urn, ver010101, auditStamp2);
    _dummyLocalDAO.add(urn, ver020101, auditStamp3);
    _dummyLocalDAO.add(urn, ver020201OldValue, auditStamp4);

    verify(_mockEventProducer, times(1)).produceMetadataAuditEvent(urn, null, foo1);
    verify(_mockEventProducer, times(1)).produceAspectSpecificMetadataAuditEvent(urn, null, foo1, _dummyAuditStamp);
    verify(_mockEventProducer, times(1)).produceMetadataAuditEvent(urn, foo1, ver010101);
    verify(_mockEventProducer, times(1)).produceAspectSpecificMetadataAuditEvent(urn, foo1, ver010101, auditStamp2);
    verify(_mockEventProducer, times(1)).produceMetadataAuditEvent(urn, ver010101, ver020101);
    verify(_mockEventProducer, times(1)).produceAspectSpecificMetadataAuditEvent(urn, ver010101, ver020101, auditStamp3);
    verify(_mockEventProducer, times(1)).produceMetadataAuditEvent(urn, ver020101, ver020201OldValue);
    verify(_mockEventProducer, times(1)).produceAspectSpecificMetadataAuditEvent(urn, ver020101, ver020201OldValue, auditStamp4);
    verifyNoMoreInteractions(_mockEventProducer);
  }


  @Test(description = "Test that no MAEs are emitted if incoming aspect has a lower version than existing aspect")
  public void testMAEEmissionVerNoChange() throws URISyntaxException {
    FooUrn urn = new FooUrn(1);
    AspectVersioned ver020101 = RecordUtils.toRecordTemplate(AspectVersioned.class, createVersionDataMap(2, 1, 1, "ver2"));
    AspectVersioned foo1 = new AspectVersioned().setValue("foo");
    AspectVersioned ver010101 = RecordUtils.toRecordTemplate(AspectVersioned.class, createVersionDataMap(1, 1, 1, "ver1"));

    _dummyLocalDAO.setAlwaysEmitAuditEvent(false);
    expectGetLatest(urn, AspectVersioned.class,
        Arrays.asList(makeAspectEntry(null, null), makeAspectEntry(ver020101, _dummyAuditStamp)));

    _dummyLocalDAO.add(urn, ver020101, _dummyAuditStamp);
    _dummyLocalDAO.add(urn, foo1, _dummyAuditStamp);
    _dummyLocalDAO.add(urn, ver010101, _dummyAuditStamp);
    _dummyLocalDAO.add(urn, ver020101, _dummyAuditStamp);

    verify(_mockEventProducer, times(1)).produceMetadataAuditEvent(urn, null, ver020101);
    verify(_mockEventProducer, times(1)).produceAspectSpecificMetadataAuditEvent(urn, null, ver020101, _dummyAuditStamp);
    verifyNoMoreInteractions(_mockEventProducer);
  }





  @Test(description = "Test aspectVersionSkipWrite")
  public void testAspectVersionSkipWrite() throws URISyntaxException {
    AspectVersioned ver010101 = RecordUtils.toRecordTemplate(AspectVersioned.class, createVersionDataMap(1, 1, 1, "testValue1"));
    AspectVersioned ver020101 = RecordUtils.toRecordTemplate(AspectVersioned.class, createVersionDataMap(2, 1, 1, "testValue2"));
    AspectVersioned noVer = new AspectVersioned().setValue("noVer");

    // Cases where the version check will force writing to be skipped
    assertEquals(_dummyLocalDAO.aspectVersionSkipWrite(ver010101, ver020101), true);
    assertEquals(_dummyLocalDAO.aspectVersionSkipWrite(noVer, ver010101), true);
    assertEquals(_dummyLocalDAO.aspectVersionSkipWrite(null, ver010101), true);

    // Cases where the version check will NOT force writing to be skipped
    assertEquals(_dummyLocalDAO.aspectVersionSkipWrite(ver010101, ver010101), false);
    assertEquals(_dummyLocalDAO.aspectVersionSkipWrite(ver020101, ver010101), false);
    assertEquals(_dummyLocalDAO.aspectVersionSkipWrite(noVer, noVer), false);
    assertEquals(_dummyLocalDAO.aspectVersionSkipWrite(ver010101, noVer), false);
    assertEquals(_dummyLocalDAO.aspectVersionSkipWrite(ver010101, null), false);
    assertEquals(_dummyLocalDAO.aspectVersionSkipWrite(null, null), false);
  }

  // Helper function to create DataMap with fields baseSemanticVersion and value
  private DataMap createVersionDataMap(int major, int minor, int patch, String value) {
    Map<String, Integer> versionMap = new HashMap<>();
    versionMap.put("major", major);
    versionMap.put("minor", minor);
    versionMap.put("patch", patch);
    DataMap innerMap = new DataMap(versionMap);
    Map<String, Object> recordMap = new HashMap<>();
    recordMap.put("baseSemanticVersion", innerMap);
    recordMap.put("value", value);

    return new DataMap(recordMap);
  }

}
