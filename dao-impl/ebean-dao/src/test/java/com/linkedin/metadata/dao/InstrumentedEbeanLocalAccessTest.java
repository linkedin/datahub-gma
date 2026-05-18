package com.linkedin.metadata.dao;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.tracking.BaseDaoBenchmarkMetrics;
import com.linkedin.testing.AspectFoo;
import com.linkedin.metadata.query.IndexFilter;
import com.linkedin.metadata.query.IndexGroupByCriterion;
import com.linkedin.metadata.query.IndexSortCriterion;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


/**
 * Tests for {@link InstrumentedEbeanLocalAccess}. Verifies:
 * <ul>
 *   <li>Delegation to the wrapped implementation</li>
 *   <li>Per-method dimension wiring (operation, aspect, count bucket) into recordOperation</li>
 *   <li>Status and error class capture on success and failure</li>
 *   <li>Direct delegation (no timing) when metrics are disabled</li>
 *   <li>bucketCount boundary behavior</li>
 * </ul>
 */
public class InstrumentedEbeanLocalAccessTest {

  // Use a simple URN subclass name for testing entity type extraction
  // "TestUrn" -> "test"
  private static abstract class TestUrn extends Urn {
    TestUrn(String rawUrn) throws Exception {
      super(rawUrn);
    }
  }

  private IEbeanLocalAccess<TestUrn> _mockDelegate;
  private BaseDaoBenchmarkMetrics _mockMetrics;
  private InstrumentedEbeanLocalAccess<TestUrn> _instrumented;

  @SuppressWarnings("unchecked")
  @BeforeMethod
  public void setUp() {
    _mockDelegate = mock(IEbeanLocalAccess.class);
    _mockMetrics = mock(BaseDaoBenchmarkMetrics.class);
    when(_mockMetrics.isEnabled()).thenReturn(true);

    _instrumented = new InstrumentedEbeanLocalAccess<>(_mockDelegate, _mockMetrics, TestUrn.class);
  }

  @Test
  public void testBucketCountBoundaries() {
    assertEquals(InstrumentedEbeanLocalAccess.bucketCount(0), "0");
    assertEquals(InstrumentedEbeanLocalAccess.bucketCount(-1), "0");
    assertEquals(InstrumentedEbeanLocalAccess.bucketCount(1), "1");
    assertEquals(InstrumentedEbeanLocalAccess.bucketCount(5), "5");
    assertEquals(InstrumentedEbeanLocalAccess.bucketCount(9), "9");
    assertEquals(InstrumentedEbeanLocalAccess.bucketCount(10), "10+");
    assertEquals(InstrumentedEbeanLocalAccess.bucketCount(100), "10+");
  }

  @Test
  public void testEntityTypeExtraction() {
    // "TestUrn" -> "test"; trigger any instrumented call to verify entity type
    when(_mockDelegate.exists(any())).thenReturn(true);
    _instrumented.exists(null);

    ArgumentCaptor<String> entityCaptor = ArgumentCaptor.forClass(String.class);
    verify(_mockMetrics).recordOperation(eq("exists"), entityCaptor.capture(),
        isNull(), isNull(), eq("success"), isNull(), anyLong());
    assertEquals(entityCaptor.getValue(), "test");
  }

  @Test
  public void testAddDelegatesAndRecordsOperation() {
    when(_mockDelegate.add(any(), any(), any(), any(), any(), anyBoolean())).thenReturn(1);

    int result = _instrumented.add(null, null, AspectFoo.class, mock(AuditStamp.class), null, false);

    assertEquals(result, 1);
    verify(_mockDelegate).add(any(), any(), any(), any(), any(), anyBoolean());
    verify(_mockMetrics).recordOperation(eq("add"), eq("test"), eq("AspectFoo"),
        isNull(), eq("success"), isNull(), anyLong());
  }

  @Test
  public void testAddWithOptimisticLockingDelegatesAndRecordsOperation() {
    when(_mockDelegate.addWithOptimisticLocking(any(), any(), any(), any(), any(), any(),
        anyBoolean(), anyBoolean())).thenReturn(1);

    int result = _instrumented.addWithOptimisticLocking(null, null, AspectFoo.class,
        mock(AuditStamp.class), null, null, false, false);

    assertEquals(result, 1);
    verify(_mockDelegate).addWithOptimisticLocking(any(), any(), any(), any(), any(), any(),
        anyBoolean(), anyBoolean());
    verify(_mockMetrics).recordOperation(eq("addWithOptimisticLocking"), eq("test"),
        eq("AspectFoo"), isNull(), eq("success"), isNull(), anyLong());
  }

  @Test
  public void testCreateRecordsCountBucket() {
    when(_mockDelegate.create(any(), any(), any(), any(), any(), anyBoolean())).thenReturn(1);

    int result = _instrumented.create(null, Collections.emptyList(), Collections.emptyList(),
        mock(AuditStamp.class), null, false);

    assertEquals(result, 1);
    // Empty list -> bucket "0" (defensive: shouldn't happen in real usage, but still bucketed)
    verify(_mockMetrics).recordOperation(eq("create"), eq("test"), isNull(),
        eq("0"), eq("success"), isNull(), anyLong());
  }

  @Test
  public void testBatchGetUnionRecordsCountBucket() {
    List<EbeanMetadataAspect> expected = Collections.emptyList();
    when(_mockDelegate.batchGetUnion(any(), anyInt(), anyInt(), anyBoolean(), anyBoolean()))
        .thenReturn(expected);

    List<EbeanMetadataAspect> result =
        _instrumented.batchGetUnion(Collections.emptyList(), 10, 0, false, false);

    assertSame(result, expected);
    verify(_mockMetrics).recordOperation(eq("batchGetUnion"), eq("test"), isNull(),
        eq("0"), eq("success"), isNull(), anyLong());
  }

  @Test
  public void testSoftDeleteAssetRecordsOperation() {
    when(_mockDelegate.softDeleteAsset(any(), anyBoolean())).thenReturn(3);

    int result = _instrumented.softDeleteAsset(null, false);

    assertEquals(result, 3);
    verify(_mockMetrics).recordOperation(eq("softDeleteAsset"), eq("test"), isNull(),
        isNull(), eq("success"), isNull(), anyLong());
  }

  @Test
  public void testExistsRecordsOperation() {
    when(_mockDelegate.exists(any())).thenReturn(true);

    boolean result = _instrumented.exists(null);

    assertTrue(result);
    verify(_mockMetrics).recordOperation(eq("exists"), eq("test"), isNull(),
        isNull(), eq("success"), isNull(), anyLong());
  }

  @Test
  public void testCountAggregateRecordsOperation() {
    Map<String, Long> expected = new HashMap<>();
    expected.put("key", 5L);
    when(_mockDelegate.countAggregate(any(), any())).thenReturn(expected);

    Map<String, Long> result = _instrumented.countAggregate(null, mock(IndexGroupByCriterion.class));

    assertSame(result, expected);
    verify(_mockMetrics).recordOperation(eq("countAggregate"), eq("test"), isNull(),
        isNull(), eq("success"), isNull(), anyLong());
  }

  @Test
  public void testListRecordsOperation() {
    ListResult<RecordTemplate> expected = mock(ListResult.class);
    when(_mockDelegate.list(any(Class.class), anyInt(), anyInt())).thenReturn(expected);

    ListResult<RecordTemplate> result = _instrumented.list(RecordTemplate.class, 0, 10);

    assertSame(result, expected);
    verify(_mockMetrics).recordOperation(eq("list"), eq("test"), isNull(),
        isNull(), eq("success"), isNull(), anyLong());
  }

  @Test
  public void testListUrnsOffsetRecordsOperation() {
    ListResult<TestUrn> expected = mock(ListResult.class);
    when(_mockDelegate.listUrns(any(IndexFilter.class), any(IndexSortCriterion.class),
        anyInt(), anyInt())).thenReturn(expected);

    ListResult<TestUrn> result = _instrumented.listUrns(
        mock(IndexFilter.class), mock(IndexSortCriterion.class), 0, 10);

    assertSame(result, expected);
    verify(_mockMetrics).recordOperation(eq("listUrns.offset"), eq("test"), isNull(),
        isNull(), eq("success"), isNull(), anyLong());
  }

  @Test
  public void testFailureRecordsStatusAndErrorClassAndRethrows() {
    RuntimeException error = new IllegalStateException("DB error");
    when(_mockDelegate.exists(any())).thenThrow(error);

    try {
      _instrumented.exists(null);
      fail("Expected exception to be re-thrown");
    } catch (IllegalStateException ex) {
      assertSame(ex, error);
    }

    verify(_mockMetrics).recordOperation(eq("exists"), eq("test"), isNull(), isNull(),
        eq("failure"), eq("IllegalStateException"), anyLong());
  }

  @Test
  public void testDisabledMetricsBypassesTiming() {
    when(_mockMetrics.isEnabled()).thenReturn(false);
    when(_mockDelegate.exists(any())).thenReturn(true);

    boolean result = _instrumented.exists(null);

    assertTrue(result);
    verify(_mockDelegate).exists(any());
    // No record* calls should be made
    verify(_mockMetrics, never()).recordOperation(anyString(), anyString(), any(), any(),
        anyString(), any(), anyLong());
  }

  @Test
  public void testEnsureSchemaUpToDateNotInstrumented() {
    _instrumented.ensureSchemaUpToDate();

    verify(_mockDelegate).ensureSchemaUpToDate();
    verify(_mockMetrics, never()).recordOperation(anyString(), anyString(), any(), any(),
        anyString(), any(), anyLong());
  }

  @Test
  public void testSetUrnPathExtractorDelegates() {
    _instrumented.setUrnPathExtractor(null);
    verify(_mockDelegate).setUrnPathExtractor(null);
  }

  @Test
  public void testConfigureOptionalForceIndexDelegates() {
    Map<Class<?>, String> criteria =
        Collections.singletonMap(AspectFoo.class, "/value");
    _instrumented.configureOptionalForceIndex("PRIMARY", criteria);
    verify(_mockDelegate).configureOptionalForceIndex("PRIMARY", criteria);
  }

  @Test
  public void testReadDeletionInfoBatchRecordsCountBucket() {
    Map<TestUrn, EntityDeletionInfo> expected = new HashMap<>();
    when(_mockDelegate.readDeletionInfoBatch(any(), anyBoolean())).thenReturn(expected);

    List<TestUrn> urns = Collections.singletonList(null);
    Map<TestUrn, EntityDeletionInfo> result = _instrumented.readDeletionInfoBatch(urns, false);

    assertSame(result, expected);
    verify(_mockDelegate).readDeletionInfoBatch(urns, false);
    verify(_mockMetrics).recordOperation(eq("readDeletionInfoBatch"), eq("test"), isNull(),
        eq("1"), eq("success"), isNull(), anyLong());
  }

  @Test
  public void testBatchSoftDeleteAssetsRecordsCountBucket() {
    when(_mockDelegate.batchSoftDeleteAssets(any(), any(), anyBoolean())).thenReturn(5);

    List<TestUrn> urns = Collections.singletonList(null);
    int result = _instrumented.batchSoftDeleteAssets(urns, "2026-01-01", false);

    assertEquals(result, 5);
    verify(_mockDelegate).batchSoftDeleteAssets(urns, "2026-01-01", false);
    verify(_mockMetrics).recordOperation(eq("batchSoftDeleteAssets"), eq("test"), isNull(),
        eq("1"), eq("success"), isNull(), anyLong());
  }
}
