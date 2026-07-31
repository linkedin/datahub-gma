package com.linkedin.metadata.dao;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.tracking.BaseDaoUsageEmitter;
import com.linkedin.metadata.dao.tracking.DaoReadContext;
import com.linkedin.metadata.dao.tracking.DaoUsageBuffer;
import com.linkedin.metadata.dao.tracking.DaoUsageTarget;
import com.linkedin.metadata.events.IngestionTrackingContext;
import com.linkedin.metadata.query.IndexFilter;
import com.linkedin.metadata.query.IndexGroupByCriterion;
import com.linkedin.metadata.query.IndexSortCriterion;
import com.linkedin.testing.AspectBar;
import com.linkedin.testing.AspectFoo;
import com.linkedin.testing.urn.FooUrn;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.testing.TestUtils.makeFooUrn;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


/**
 * Tests for {@link UsageTrackingEbeanLocalAccess}. Verifies:
 * <ul>
 *   <li>Each captured operation maps to the right operationType, entityType, source and targets</li>
 *   <li>batchGetUnion groups aspects per URN</li>
 *   <li>Writes/deletes carry the caller from the audit stamp; reads and DELETE_ALL do not</li>
 *   <li>Excluded, test-mode and backfill operations do not emit</li>
 *   <li>The emitter is fail-open (its exceptions never propagate) and disabled = zero interaction</li>
 *   <li>The delegate's return value is always passed through unchanged</li>
 * </ul>
 */
public class UsageTrackingEbeanLocalAccessTest {

  private IEbeanLocalAccess<FooUrn> _mockDelegate;
  private BaseDaoUsageEmitter _mockEmitter;
  private UsageTrackingEbeanLocalAccess<FooUrn> _usage;

  private FooUrn _urn1;
  private FooUrn _urn2;
  private FooUrn _actor;
  private AuditStamp _auditStamp;

  @SuppressWarnings("unchecked")
  @BeforeMethod
  public void setUp() {
    _mockDelegate = mock(IEbeanLocalAccess.class);
    _mockEmitter = mock(BaseDaoUsageEmitter.class);
    when(_mockEmitter.isEnabled()).thenReturn(true);

    _usage = new UsageTrackingEbeanLocalAccess<>(_mockDelegate, _mockEmitter);

    _urn1 = makeFooUrn(1);
    _urn2 = makeFooUrn(2);
    _actor = makeFooUrn(99);
    _auditStamp = new AuditStamp().setActor(_actor).setTime(0L);
  }

  @SuppressWarnings("unchecked")
  private ArgumentCaptor<List<DaoUsageTarget>> targetsCaptor() {
    return ArgumentCaptor.forClass(List.class);
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testInternalReadIsNotEmitted() {
    when(_mockDelegate.batchGetUnion(any(), anyInt(), anyInt(), anyBoolean(), anyBoolean()))
        .thenReturn(Collections.emptyList());
    List<AspectKey<FooUrn, ? extends RecordTemplate>> keys =
        (List) Collections.singletonList(new AspectKey<>(AspectFoo.class, _urn1, 0L));

    try (DaoReadContext.Scope ignored = DaoReadContext.markInternalRead()) {
      _usage.batchGetUnion(keys, 1, 0, true, false);
    }

    // Read-before-write is marked internal, so it is not counted as a consumer read.
    verify(_mockEmitter, never()).emit(anyString(), anyString(), anyString(), any(), any(), any());
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testConsumerReadStillEmittedAfterInternalScopeCloses() {
    when(_mockDelegate.batchGetUnion(any(), anyInt(), anyInt(), anyBoolean(), anyBoolean()))
        .thenReturn(Collections.emptyList());
    List<AspectKey<FooUrn, ? extends RecordTemplate>> keys =
        (List) Collections.singletonList(new AspectKey<>(AspectFoo.class, _urn1, 0L));

    // A read-before-write inside the scope is skipped...
    try (DaoReadContext.Scope ignored = DaoReadContext.markInternalRead()) {
      _usage.batchGetUnion(keys, 1, 0, true, false);
    }
    // ...and once the scope closes, a genuine consumer read is emitted as normal.
    _usage.batchGetUnion(keys, 1, 0, false, false);

    verify(_mockEmitter, times(1))
        .emit(eq("READ"), eq("foo"), eq("batchGetUnion"), isNull(), isNull(), any());
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testNestedInternalScopesDoNotUnmarkOuterRead() {
    when(_mockDelegate.batchGetUnion(any(), anyInt(), anyInt(), anyBoolean(), anyBoolean()))
        .thenReturn(Collections.emptyList());
    List<AspectKey<FooUrn, ? extends RecordTemplate>> keys =
        (List) Collections.singletonList(new AspectKey<>(AspectFoo.class, _urn1, 0L));

    try (DaoReadContext.Scope outer = DaoReadContext.markInternalRead()) {
      try (DaoReadContext.Scope inner = DaoReadContext.markInternalRead()) {
        _usage.batchGetUnion(keys, 1, 0, true, false);
      }
      // The inner scope closing must not unmark the outer region, so this read is still internal.
      _usage.batchGetUnion(keys, 1, 0, true, false);
    }

    verify(_mockEmitter, never()).emit(anyString(), anyString(), anyString(), any(), any(), any());
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testEntityTypeComesFromUrnNotUrnClassName() throws Exception {
    // A camelCase entity type distinguishes urn.getEntityType() from the old derivation, which
    // lowercased the URN class name and would have produced "mlmodel" (and "" for a raw Urn).
    IEbeanLocalAccess<Urn> mockDelegate = mock(IEbeanLocalAccess.class);
    UsageTrackingEbeanLocalAccess<Urn> usage =
        new UsageTrackingEbeanLocalAccess<>(mockDelegate, _mockEmitter);
    Urn mlModelUrn = new Urn("mlModel", "some-model");

    when(mockDelegate.add(any(), any(), any(), any(), any(), anyBoolean())).thenReturn(1);

    usage.add(mlModelUrn, new AspectFoo().setValue("v"), AspectFoo.class, _auditStamp, null, false);

    verify(_mockEmitter).emit(eq("WRITE"), eq("mlModel"), eq("add"), any(), any(), any());
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testWriteIsHeldUntilTransactionCommits() {
    when(_mockDelegate.add(any(), any(), any(), any(), any(), anyBoolean())).thenReturn(1);

    final int mark = DaoUsageBuffer.enter();
    try {
      _usage.add(_urn1, new AspectFoo().setValue("v"), AspectFoo.class, _auditStamp, null, false);
      // The write has run but the transaction has not committed, so nothing is reported yet.
      verify(_mockEmitter, never()).emit(anyString(), anyString(), anyString(), any(), any(), any());
    } finally {
      DaoUsageBuffer.exit(mark, true).forEach(Runnable::run);
    }

    verify(_mockEmitter, times(1)).emit(eq("WRITE"), eq("foo"), eq("add"), any(), any(), any());
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testWriteIsNotEmittedWhenTransactionRollsBack() {
    when(_mockDelegate.add(any(), any(), any(), any(), any(), anyBoolean())).thenReturn(1);

    final int mark = DaoUsageBuffer.enter();
    try {
      _usage.add(_urn1, new AspectFoo().setValue("v"), AspectFoo.class, _auditStamp, null, false);
    } finally {
      DaoUsageBuffer.exit(mark, false).forEach(Runnable::run);
    }

    // A rolled-back write never happened, so it must not be reported.
    verify(_mockEmitter, never()).emit(anyString(), anyString(), anyString(), any(), any(), any());
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testReadIsEmittedImmediatelyInsideTransaction() {
    when(_mockDelegate.batchGetUnion(any(), anyInt(), anyInt(), anyBoolean(), anyBoolean()))
        .thenReturn(Collections.emptyList());
    List<AspectKey<FooUrn, ? extends RecordTemplate>> keys =
        (List) Collections.singletonList(new AspectKey<>(AspectFoo.class, _urn1, 0L));

    final int mark = DaoUsageBuffer.enter();
    try {
      _usage.batchGetUnion(keys, 1, 0, false, false);
      // A read that was served happened, whether or not a later write in the same transaction
      // rolls back, so reads are not deferred.
      verify(_mockEmitter, times(1))
          .emit(eq("READ"), eq("foo"), eq("batchGetUnion"), isNull(), isNull(), any());
    } finally {
      DaoUsageBuffer.exit(mark, false);
    }
  }

  // ---------------------------------------------------------------------------------------
  // Reads
  // ---------------------------------------------------------------------------------------

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testBatchGetUnionEmitsReadGroupedByUrn() {
    List<AspectKey<FooUrn, ? extends RecordTemplate>> keys = (List) Arrays.asList(
        new AspectKey<>(AspectFoo.class, _urn1, 0L),
        new AspectKey<>(AspectBar.class, _urn1, 0L),
        new AspectKey<>(AspectFoo.class, _urn2, 0L));
    List<EbeanMetadataAspect> expected = Collections.emptyList();
    when(_mockDelegate.batchGetUnion(any(), anyInt(), anyInt(), anyBoolean(), anyBoolean()))
        .thenReturn(expected);

    List<EbeanMetadataAspect> result = _usage.batchGetUnion(keys, 3, 0, false, false);

    assertSame(result, expected);
    ArgumentCaptor<List<DaoUsageTarget>> targets = targetsCaptor();
    verify(_mockEmitter).emit(eq("READ"), eq("foo"), eq("batchGetUnion"), isNull(), isNull(),
        targets.capture());
    List<DaoUsageTarget> captured = targets.getValue();
    assertEquals(captured.size(), 2);
    assertEquals(captured.get(0).getUrn(), _urn1.toString());
    assertEquals(captured.get(0).getAspects(), Arrays.asList("AspectFoo", "AspectBar"));
    assertEquals(captured.get(1).getUrn(), _urn2.toString());
    assertEquals(captured.get(1).getAspects(), Collections.singletonList("AspectFoo"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testListByUrnEmitsRead() {
    ListResult<AspectFoo> expected = mock(ListResult.class);
    when(_mockDelegate.list(any(Class.class), any(), anyInt(), anyInt())).thenReturn(expected);

    ListResult<AspectFoo> result = _usage.list(AspectFoo.class, _urn1, 0, 10);

    assertSame(result, expected);
    ArgumentCaptor<List<DaoUsageTarget>> targets = targetsCaptor();
    verify(_mockEmitter).emit(eq("READ"), eq("foo"), eq("list"), isNull(), isNull(),
        targets.capture());
    assertEquals(targets.getValue().get(0).getUrn(), _urn1.toString());
    assertEquals(targets.getValue().get(0).getAspects(), Collections.singletonList("AspectFoo"));
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testListByUrnIsNotEmittedInsideInternalScope() {
    ListResult<AspectFoo> expected = mock(ListResult.class);
    when(_mockDelegate.list(any(Class.class), any(), anyInt(), anyInt())).thenReturn(expected);

    // Every captured read honors the marker, not just batchGetUnion, so a read taken inside a
    // marked region (e.g. backfill) is never billed to a consumer.
    ListResult<AspectFoo> result;
    try (DaoReadContext.Scope ignored = DaoReadContext.markInternalRead()) {
      result = _usage.list(AspectFoo.class, _urn1, 0, 10);
    }

    assertSame(result, expected);
    verify(_mockEmitter, never()).emit(anyString(), anyString(), anyString(), any(), any(), any());
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testBatchGetUnionEmitsOnlyThePagedWindow() {
    // Callers page over the SAME full key list, advancing only position (see
    // EbeanLocalDAO#batchGet). Each page must report only the window the delegate reads,
    // otherwise every key is re-reported on every page.
    final int totalKeys = 120;
    final int keysCount = 50;
    List<AspectKey<FooUrn, ? extends RecordTemplate>> keys = new ArrayList<>();
    for (int i = 0; i < totalKeys; i++) {
      keys.add(new AspectKey<>(AspectFoo.class, makeFooUrn(i), 0L));
    }
    when(_mockDelegate.batchGetUnion(any(), anyInt(), anyInt(), anyBoolean(), anyBoolean()))
        .thenReturn(Collections.emptyList());

    for (int position = 0; position < totalKeys; position += keysCount) {
      _usage.batchGetUnion((List) keys, keysCount, position, false, false);
    }

    ArgumentCaptor<List<DaoUsageTarget>> targets = targetsCaptor();
    verify(_mockEmitter, times(3)).emit(eq("READ"), eq("foo"), eq("batchGetUnion"), isNull(),
        isNull(), targets.capture());

    List<List<DaoUsageTarget>> pages = targets.getAllValues();
    assertEquals(pages.get(0).size(), keysCount);
    assertEquals(pages.get(1).size(), keysCount);
    assertEquals(pages.get(2).size(), totalKeys - (2 * keysCount));

    // Every key reported exactly once across all pages.
    Set<String> seen = new HashSet<>();
    for (List<DaoUsageTarget> page : pages) {
      for (DaoUsageTarget target : page) {
        assertTrue(seen.add(target.getUrn()), "urn reported on more than one page: " + target.getUrn());
      }
    }
    assertEquals(seen.size(), totalKeys);
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testBatchGetUnionWindowIsClampedToKeyList() {
    List<AspectKey<FooUrn, ? extends RecordTemplate>> keys = (List) Arrays.asList(
        new AspectKey<>(AspectFoo.class, _urn1, 0L),
        new AspectKey<>(AspectFoo.class, _urn2, 0L));
    when(_mockDelegate.batchGetUnion(any(), anyInt(), anyInt(), anyBoolean(), anyBoolean()))
        .thenReturn(Collections.emptyList());

    // Position past the end: nothing was read, so nothing is emitted.
    _usage.batchGetUnion((List) keys, 50, 100, false, false);
    verify(_mockEmitter, never()).emit(anyString(), anyString(), anyString(), any(), any(), any());

    // keysCount larger than the list: clamps to the available keys.
    _usage.batchGetUnion((List) keys, 50, 0, false, false);
    ArgumentCaptor<List<DaoUsageTarget>> targets = targetsCaptor();
    verify(_mockEmitter).emit(eq("READ"), eq("foo"), eq("batchGetUnion"), isNull(), isNull(),
        targets.capture());
    assertEquals(targets.getValue().size(), 2);
  }

  // ---------------------------------------------------------------------------------------
  // Writes / deletes
  // ---------------------------------------------------------------------------------------

  @Test
  public void testAddWithValueEmitsWriteWithActor() {
    when(_mockDelegate.add(any(), any(), any(), any(), any(), anyBoolean())).thenReturn(1);

    int result = _usage.add(_urn1, new AspectFoo(), AspectFoo.class, _auditStamp, null, false);

    assertEquals(result, 1);
    ArgumentCaptor<List<DaoUsageTarget>> targets = targetsCaptor();
    verify(_mockEmitter).emit(eq("WRITE"), eq("foo"), eq("add"), eq(_actor.toString()), isNull(),
        targets.capture());
    assertEquals(targets.getValue().get(0).getUrn(), _urn1.toString());
    assertEquals(targets.getValue().get(0).getAspects(), Collections.singletonList("AspectFoo"));
  }

  @Test
  public void testAddNullValueEmitsDelete() {
    when(_mockDelegate.add(any(), any(), any(), any(), any(), anyBoolean())).thenReturn(1);

    _usage.add(_urn1, null, AspectFoo.class, _auditStamp, null, false);

    verify(_mockEmitter).emit(eq("DELETE"), eq("foo"), eq("add"), eq(_actor.toString()), isNull(),
        any());
  }

  @Test
  public void testAddWithOptimisticLockingEmitsWrite() {
    when(_mockDelegate.addWithOptimisticLocking(any(), any(), any(), any(), any(), any(),
        anyBoolean(), anyBoolean())).thenReturn(1);

    _usage.addWithOptimisticLocking(_urn1, new AspectFoo(), AspectFoo.class, _auditStamp, null,
        null, false, false);

    verify(_mockEmitter).emit(eq("WRITE"), eq("foo"), eq("addWithOptimisticLocking"),
        eq(_actor.toString()), isNull(), any());
  }

  @Test
  public void testCreateEmitsWrite() {
    when(_mockDelegate.create(any(), any(), any(), any(), any(), anyBoolean())).thenReturn(1);

    int result = _usage.create(_urn1, Collections.singletonList(new AspectFoo()),
        Collections.emptyList(), _auditStamp, null, false);

    assertEquals(result, 1);
    ArgumentCaptor<List<DaoUsageTarget>> targets = targetsCaptor();
    verify(_mockEmitter).emit(eq("WRITE"), eq("foo"), eq("create"), eq(_actor.toString()), isNull(),
        targets.capture());
    assertEquals(targets.getValue().get(0).getAspects(), Collections.singletonList("AspectFoo"));
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testBatchUpsertEmitsWrite() {
    AspectFoo value = new AspectFoo();
    BaseLocalDAO.AspectUpdateLambda lambda = new BaseLocalDAO.AspectUpdateLambda(value);
    BaseLocalDAO.AspectUpdateContext ctx = new BaseLocalDAO.AspectUpdateContext(null, value, lambda);
    List<BaseLocalDAO.AspectUpdateContext<RecordTemplate>> contexts =
        (List) Collections.singletonList(ctx);
    when(_mockDelegate.batchUpsert(any(), any(), any(), any(), anyBoolean())).thenReturn(1);

    int result = _usage.batchUpsert(_urn1, contexts, _auditStamp, null, false);

    assertEquals(result, 1);
    ArgumentCaptor<List<DaoUsageTarget>> targets = targetsCaptor();
    verify(_mockEmitter).emit(eq("WRITE"), eq("foo"), eq("batchUpsert"), eq(_actor.toString()),
        isNull(), targets.capture());
    assertEquals(targets.getValue().get(0).getUrn(), _urn1.toString());
    assertEquals(targets.getValue().get(0).getAspects(), Collections.singletonList("AspectFoo"));
  }

  @Test
  public void testSoftDeleteAssetEmitsDeleteAllWithEmptyAspects() {
    when(_mockDelegate.softDeleteAsset(any(), anyBoolean())).thenReturn(1);

    _usage.softDeleteAsset(_urn1, false);

    ArgumentCaptor<List<DaoUsageTarget>> targets = targetsCaptor();
    verify(_mockEmitter).emit(eq("DELETE_ALL"), eq("foo"), eq("softDeleteAsset"), isNull(),
        isNull(), targets.capture());
    assertEquals(targets.getValue().get(0).getUrn(), _urn1.toString());
    assertTrue(targets.getValue().get(0).getAspects().isEmpty());
  }

  @Test
  public void testImpersonatorPopulatedOnWrite() {
    FooUrn impersonator = makeFooUrn(500);
    AuditStamp stamp = new AuditStamp().setActor(_actor).setImpersonator(impersonator).setTime(0L);
    when(_mockDelegate.add(any(), any(), any(), any(), any(), anyBoolean())).thenReturn(1);

    _usage.add(_urn1, new AspectFoo(), AspectFoo.class, stamp, null, false);

    verify(_mockEmitter).emit(eq("WRITE"), eq("foo"), eq("add"), eq(_actor.toString()),
        eq(impersonator.toString()), any());
  }

  // ---------------------------------------------------------------------------------------
  // Exclusions and safety
  // ---------------------------------------------------------------------------------------

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testTestModeSkipsEmission() {
    when(_mockDelegate.add(any(), any(), any(), any(), any(), anyBoolean())).thenReturn(1);
    when(_mockDelegate.batchGetUnion(any(), anyInt(), anyInt(), anyBoolean(), anyBoolean()))
        .thenReturn(Collections.emptyList());

    _usage.add(_urn1, new AspectFoo(), AspectFoo.class, _auditStamp, null, true);
    List<AspectKey<FooUrn, ? extends RecordTemplate>> keys =
        (List) Collections.singletonList(new AspectKey<>(AspectFoo.class, _urn1, 0L));
    _usage.batchGetUnion(keys, 1, 0, false, true);

    verify(_mockEmitter, never()).emit(anyString(), anyString(), anyString(), any(), any(), any());
  }

  @Test
  public void testBackfillSkipsEmission() {
    IngestionTrackingContext backfillContext = new IngestionTrackingContext().setBackfill(true);
    when(_mockDelegate.add(any(), any(), any(), any(), any(), anyBoolean())).thenReturn(1);

    _usage.add(_urn1, new AspectFoo(), AspectFoo.class, _auditStamp, backfillContext, false);

    verify(_mockEmitter, never()).emit(anyString(), anyString(), anyString(), any(), any(), any());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testExcludedMethodsDoNotEmit() {
    when(_mockDelegate.exists(any())).thenReturn(true);
    when(_mockDelegate.list(any(Class.class), anyInt(), anyInt())).thenReturn(mock(ListResult.class));
    when(_mockDelegate.countAggregate(any(), any())).thenReturn(Collections.emptyMap());
    when(_mockDelegate.batchSoftDeleteAssets(any(), any(), anyBoolean())).thenReturn(0);
    when(_mockDelegate.readDeletionInfoBatch(any(), anyBoolean())).thenReturn(Collections.emptyMap());
    when(_mockDelegate.listUrns(any(IndexFilter.class), any(IndexSortCriterion.class), anyInt(),
        anyInt())).thenReturn(mock(ListResult.class));

    _usage.exists(_urn1);
    _usage.list(AspectFoo.class, 0, 10);
    _usage.countAggregate(null, mock(IndexGroupByCriterion.class));
    _usage.batchSoftDeleteAssets(Collections.singletonList(_urn1), "2026-01-01", false);
    _usage.readDeletionInfoBatch(Collections.singletonList(_urn1), false);
    _usage.listUrns(mock(IndexFilter.class), mock(IndexSortCriterion.class), 0, 10);
    _usage.ensureSchemaUpToDate();

    verify(_mockEmitter, never()).emit(anyString(), anyString(), anyString(), any(), any(), any());
  }

  @Test
  public void testDisabledEmitterSkipsEmissionButDelegates() {
    when(_mockEmitter.isEnabled()).thenReturn(false);
    when(_mockDelegate.add(any(), any(), any(), any(), any(), anyBoolean())).thenReturn(7);

    int result = _usage.add(_urn1, new AspectFoo(), AspectFoo.class, _auditStamp, null, false);

    assertEquals(result, 7);
    verify(_mockDelegate).add(any(), any(), any(), any(), any(), anyBoolean());
    verify(_mockEmitter, never()).emit(anyString(), anyString(), anyString(), any(), any(), any());
  }

  @Test
  public void testEmitterExceptionDoesNotPropagate() {
    when(_mockDelegate.add(any(), any(), any(), any(), any(), anyBoolean())).thenReturn(3);
    doThrow(new RuntimeException("boom")).when(_mockEmitter)
        .emit(anyString(), anyString(), anyString(), any(), any(), any());

    int result = _usage.add(_urn1, new AspectFoo(), AspectFoo.class, _auditStamp, null, false);

    assertEquals(result, 3);
  }

  @Test
  public void testPartialAuditStampDoesNotPropagate() {
    // actor is a required field: AuditStamp#getActor() uses GetMode.STRICT and throws on a
    // partial record. Deriving the caller must stay inside the emitter's failure boundary so a
    // malformed stamp cannot surface after the write has already succeeded.
    when(_mockDelegate.add(any(), any(), any(), any(), any(), anyBoolean())).thenReturn(5);
    AuditStamp partial = new AuditStamp().setTime(0L);

    int result = _usage.add(_urn1, new AspectFoo(), AspectFoo.class, partial, null, false);

    assertEquals(result, 5);
    verify(_mockDelegate).add(any(), any(), any(), any(), any(), anyBoolean());
    verify(_mockEmitter, never()).emit(anyString(), anyString(), anyString(), any(), any(), any());
  }

  @Test
  public void testConfigAndPassThroughMethodsDelegate() {
    _usage.setUrnPathExtractor(null);
    verify(_mockDelegate).setUrnPathExtractor(null);

    _usage.configureOptionalForceIndex("PRIMARY", null);
    verify(_mockDelegate).configureOptionalForceIndex("PRIMARY", null);

    verify(_mockEmitter, never()).emit(anyString(), anyString(), anyString(), any(), any(), any());
  }
}
