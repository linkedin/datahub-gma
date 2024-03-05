package com.linkedin.metadata.dao;

import static com.linkedin.metadata.dao.BaseLocalDAOTest.DummyLocalDAO;
import static com.linkedin.metadata.dao.BaseLocalDAOTest.DummyTransactionRunner;
import com.linkedin.common.AuditStamp;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.producer.BaseMetadataEventProducer;
import com.linkedin.metadata.dao.producer.BaseTrackingMetadataEventProducer;
import com.linkedin.metadata.dao.tracking.BaseTrackingManager;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.events.IngestionMode;
import com.linkedin.metadata.query.ExtraInfo;
import com.linkedin.testing.AspectVersioned;
import com.linkedin.testing.EntityAspectUnionVersioned;
import com.linkedin.testing.urn.FooUrn;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import org.mockito.stubbing.OngoingStubbing;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.common.AuditStamps.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class BaseLocalDAOAspectVersionTest {

  private DummyLocalDAO<EntityAspectUnionVersioned> _dummyLocalDAO;
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
    _dummyLocalDAO = new DummyLocalDAO<EntityAspectUnionVersioned>(EntityAspectUnionVersioned.class,
        _mockGetLatestFunction, _mockEventProducer, _mockTransactionRunner);
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
    verify(_mockEventProducer, times(1)).produceAspectSpecificMetadataAuditEvent(urn, null, foo1, _dummyAuditStamp, IngestionMode.LIVE);
    verify(_mockEventProducer, times(1)).produceMetadataAuditEvent(urn, foo1, ver010101);
    verify(_mockEventProducer, times(1)).produceAspectSpecificMetadataAuditEvent(urn, foo1, ver010101, auditStamp2, IngestionMode.LIVE);
    verify(_mockEventProducer, times(1)).produceMetadataAuditEvent(urn, ver010101, ver020101);
    verify(_mockEventProducer, times(1)).produceAspectSpecificMetadataAuditEvent(urn, ver010101, ver020101, auditStamp3, IngestionMode.LIVE);
    verify(_mockEventProducer, times(1)).produceMetadataAuditEvent(urn, ver020101, ver020201OldValue);
    verify(_mockEventProducer, times(1)).produceAspectSpecificMetadataAuditEvent(urn, ver020101, ver020201OldValue, auditStamp4, IngestionMode.LIVE);
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
    verify(_mockEventProducer, times(1)).produceAspectSpecificMetadataAuditEvent(urn, null, ver020101, _dummyAuditStamp, IngestionMode.LIVE);
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
