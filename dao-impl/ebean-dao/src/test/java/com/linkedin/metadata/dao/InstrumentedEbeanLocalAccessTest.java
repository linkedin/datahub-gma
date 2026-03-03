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
 *   <li>Latency recording on success</li>
 *   <li>Latency + error recording on failure</li>
 *   <li>Direct delegation (no timing) when metrics are disabled</li>
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
  public void testEntityTypeExtraction() {
    // "TestUrn" -> "test"
    InstrumentedEbeanLocalAccess<TestUrn> access =
        new InstrumentedEbeanLocalAccess<>(_mockDelegate, _mockMetrics, TestUrn.class);

    // Trigger any instrumented call to verify the entity type passed to metrics
    when(_mockDelegate.exists(any())).thenReturn(true);
    access.exists(null);

    ArgumentCaptor<String> entityCaptor = ArgumentCaptor.forClass(String.class);
    verify(_mockMetrics).recordOperation(eq("exists"), entityCaptor.capture(), anyLong());
    assertEquals(entityCaptor.getValue(), "test");
  }

  @Test
  public void testAddDelegatesAndRecordsLatency() {
    when(_mockDelegate.add(any(), any(), any(), any(), any(), anyBoolean())).thenReturn(1);

    int result = _instrumented.add(null, null, AspectFoo.class, mock(AuditStamp.class), null, false);

    assertEquals(result, 1);
    verify(_mockDelegate).add(any(), any(), any(), any(), any(), anyBoolean());
    verify(_mockMetrics).recordOperation(eq("add.AspectFoo"), eq("test"), anyLong());
    verify(_mockMetrics, never()).recordOperationError(anyString(), anyString(), anyString());
  }

  @Test
  public void testAddWithOptimisticLockingDelegatesAndRecordsLatency() {
    when(_mockDelegate.addWithOptimisticLocking(any(), any(), any(), any(), any(), any(), anyBoolean(), anyBoolean()))
        .thenReturn(1);

    int result = _instrumented.addWithOptimisticLocking(null, null, AspectFoo.class,
        mock(AuditStamp.class), null, null, false, false);

    assertEquals(result, 1);
    verify(_mockDelegate).addWithOptimisticLocking(any(), any(), any(), any(), any(), any(), anyBoolean(), anyBoolean());
    verify(_mockMetrics).recordOperation(eq("addWithOptimisticLocking.AspectFoo"), eq("test"), anyLong());
  }

  @Test
  public void testCreateDelegatesAndRecordsLatency() {
    when(_mockDelegate.create(any(), any(), any(), any(), any(), anyBoolean())).thenReturn(1);

    int result = _instrumented.create(null, Collections.emptyList(), Collections.emptyList(),
        mock(AuditStamp.class), null, false);

    assertEquals(result, 1);
    verify(_mockMetrics).recordOperation(eq("create.aspects_0"), eq("test"), anyLong());
  }

  @Test
  public void testBatchGetUnionDelegatesAndRecordsLatency() {
    List<EbeanMetadataAspect> expected = Collections.emptyList();
    when(_mockDelegate.batchGetUnion(any(), anyInt(), anyInt(), anyBoolean(), anyBoolean()))
        .thenReturn(expected);

    List<EbeanMetadataAspect> result = _instrumented.batchGetUnion(Collections.emptyList(), 10, 0, false, false);

    assertSame(result, expected);
    verify(_mockMetrics).recordOperation(eq("batchGetUnion.keys_0"), eq("test"), anyLong());
  }

  @Test
  public void testSoftDeleteAssetDelegatesAndRecordsLatency() {
    when(_mockDelegate.softDeleteAsset(any(), anyBoolean())).thenReturn(3);

    int result = _instrumented.softDeleteAsset(null, false);

    assertEquals(result, 3);
    verify(_mockMetrics).recordOperation(eq("softDeleteAsset"), eq("test"), anyLong());
  }

  @Test
  public void testExistsDelegatesAndRecordsLatency() {
    when(_mockDelegate.exists(any())).thenReturn(true);

    boolean result = _instrumented.exists(null);

    assertTrue(result);
    verify(_mockMetrics).recordOperation(eq("exists"), eq("test"), anyLong());
  }

  @Test
  public void testCountAggregateDelegatesAndRecordsLatency() {
    Map<String, Long> expected = new HashMap<>();
    expected.put("key", 5L);
    when(_mockDelegate.countAggregate(any(), any())).thenReturn(expected);

    Map<String, Long> result = _instrumented.countAggregate(null, mock(IndexGroupByCriterion.class));

    assertSame(result, expected);
    verify(_mockMetrics).recordOperation(eq("countAggregate"), eq("test"), anyLong());
  }

  @Test
  public void testListDelegatesAndRecordsLatency() {
    ListResult<RecordTemplate> expected = mock(ListResult.class);
    when(_mockDelegate.list(any(Class.class), anyInt(), anyInt())).thenReturn(expected);

    ListResult<RecordTemplate> result = _instrumented.list(RecordTemplate.class, 0, 10);

    assertSame(result, expected);
    verify(_mockMetrics).recordOperation(eq("list"), eq("test"), anyLong());
  }

  @Test
  public void testListUrnsWithPaginationDelegatesAndRecordsLatency() {
    ListResult<TestUrn> expected = mock(ListResult.class);
    when(_mockDelegate.listUrns(any(IndexFilter.class), any(IndexSortCriterion.class), anyInt(), anyInt()))
        .thenReturn(expected);

    ListResult<TestUrn> result = _instrumented.listUrns(
        mock(IndexFilter.class), mock(IndexSortCriterion.class), 0, 10);

    assertSame(result, expected);
    verify(_mockMetrics).recordOperation(eq("listUrns.offset"), eq("test"), anyLong());
  }

  @Test
  public void testErrorRecordingAndRethrow() {
    RuntimeException error = new IllegalStateException("DB error");
    when(_mockDelegate.exists(any())).thenThrow(error);

    try {
      _instrumented.exists(null);
      fail("Expected exception to be re-thrown");
    } catch (IllegalStateException ex) {
      assertSame(ex, error);
    }

    verify(_mockMetrics).recordOperation(eq("exists"), eq("test"), anyLong());
    verify(_mockMetrics).recordOperationError("exists", "test", "IllegalStateException");
  }

  @Test
  public void testDisabledMetricsBypassesTiming() {
    when(_mockMetrics.isEnabled()).thenReturn(false);
    when(_mockDelegate.exists(any())).thenReturn(true);

    boolean result = _instrumented.exists(null);

    assertTrue(result);
    verify(_mockDelegate).exists(any());
    // No metrics calls should be made
    verify(_mockMetrics, never()).recordOperation(anyString(), anyString(), anyLong());
    verify(_mockMetrics, never()).recordOperationError(anyString(), anyString(), anyString());
  }

  @Test
  public void testEnsureSchemaUpToDateNotInstrumented() {
    _instrumented.ensureSchemaUpToDate();

    verify(_mockDelegate).ensureSchemaUpToDate();
    verify(_mockMetrics, never()).recordOperation(anyString(), anyString(), anyLong());
  }

  @Test
  public void testSetUrnPathExtractorDelegates() {
    _instrumented.setUrnPathExtractor(null);
    verify(_mockDelegate).setUrnPathExtractor(null);
  }
}
