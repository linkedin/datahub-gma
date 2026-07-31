package com.linkedin.metadata.dao.tracking;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class DaoUsageBufferTest {

  @Test
  public void testRecordRunsImmediatelyWhenNoTransaction() {
    final List<String> emitted = new ArrayList<>();

    DaoUsageBuffer.record(() -> emitted.add("write"));

    // Non-transactional paths behave exactly as before.
    assertEquals(emitted, Collections.singletonList("write"));
    assertFalse(DaoUsageBuffer.isBuffering());
  }

  @Test
  public void testCommitReleasesBufferedEmission() {
    final List<String> emitted = new ArrayList<>();

    final int mark = DaoUsageBuffer.enter();
    DaoUsageBuffer.record(() -> emitted.add("write"));
    // Nothing is emitted while the transaction is still open.
    assertTrue(emitted.isEmpty());
    final List<Runnable> pending = DaoUsageBuffer.exit(mark, true);

    assertEquals(pending.size(), 1);
    pending.forEach(Runnable::run);
    assertEquals(emitted, Collections.singletonList("write"));
    assertFalse(DaoUsageBuffer.isBuffering());
  }

  @Test
  public void testRollbackDiscardsBufferedEmission() {
    final List<String> emitted = new ArrayList<>();

    final int mark = DaoUsageBuffer.enter();
    DaoUsageBuffer.record(() -> emitted.add("write"));
    final List<Runnable> pending = DaoUsageBuffer.exit(mark, false);

    // The write never became durable, so it must not be reported.
    assertTrue(pending.isEmpty());
    pending.forEach(Runnable::run);
    assertTrue(emitted.isEmpty());
    assertFalse(DaoUsageBuffer.isBuffering());
  }

  @Test
  public void testRetryReportsTheWriteOnce() {
    final List<String> emitted = new ArrayList<>();

    final int mark = DaoUsageBuffer.enter();
    // Attempt 1 buffers, then fails.
    DaoUsageBuffer.truncateTo(mark);
    DaoUsageBuffer.record(() -> emitted.add("write"));
    // Attempt 2 discards attempt 1's emission, buffers its own, and commits.
    DaoUsageBuffer.truncateTo(mark);
    DaoUsageBuffer.record(() -> emitted.add("write"));
    final List<Runnable> pending = DaoUsageBuffer.exit(mark, true);

    pending.forEach(Runnable::run);
    assertEquals(emitted, Collections.singletonList("write"));
  }

  @Test
  public void testInnerCommitIsNotReleasedUntilOuterCommits() {
    final List<String> emitted = new ArrayList<>();

    final int outerMark = DaoUsageBuffer.enter();
    final int innerMark = DaoUsageBuffer.enter();
    DaoUsageBuffer.record(() -> emitted.add("inner-write"));
    final List<Runnable> innerPending = DaoUsageBuffer.exit(innerMark, true);

    // Ebean implements the inner transaction with a savepoint, so its commit is not durable yet.
    assertTrue(innerPending.isEmpty());
    assertTrue(DaoUsageBuffer.isBuffering());

    final List<Runnable> outerPending = DaoUsageBuffer.exit(outerMark, true);
    outerPending.forEach(Runnable::run);
    assertEquals(emitted, Collections.singletonList("inner-write"));
    assertFalse(DaoUsageBuffer.isBuffering());
  }

  @Test
  public void testInnerCommitIsDiscardedWhenOuterRollsBack() {
    final List<String> emitted = new ArrayList<>();

    final int outerMark = DaoUsageBuffer.enter();
    final int innerMark = DaoUsageBuffer.enter();
    DaoUsageBuffer.record(() -> emitted.add("inner-write"));
    DaoUsageBuffer.exit(innerMark, true);
    final List<Runnable> outerPending = DaoUsageBuffer.exit(outerMark, false);

    // The outer rollback takes the inner write with it, so nothing is reported.
    assertTrue(outerPending.isEmpty());
    outerPending.forEach(Runnable::run);
    assertTrue(emitted.isEmpty());
    assertFalse(DaoUsageBuffer.isBuffering());
  }

  @Test
  public void testInnerRollbackDoesNotDiscardOuterEmission() {
    final List<String> emitted = new ArrayList<>();

    final int outerMark = DaoUsageBuffer.enter();
    DaoUsageBuffer.record(() -> emitted.add("outer-write"));

    final int innerMark = DaoUsageBuffer.enter();
    DaoUsageBuffer.record(() -> emitted.add("inner-write"));
    DaoUsageBuffer.exit(innerMark, false);

    final List<Runnable> pending = DaoUsageBuffer.exit(outerMark, true);
    pending.forEach(Runnable::run);

    // The inner frame drops only what it buffered.
    assertEquals(emitted, Collections.singletonList("outer-write"));
  }

  @Test
  public void testEmissionsAreReleasedInOrder() {
    final List<String> emitted = new ArrayList<>();

    final int mark = DaoUsageBuffer.enter();
    DaoUsageBuffer.record(() -> emitted.add("first"));
    DaoUsageBuffer.record(() -> emitted.add("second"));
    DaoUsageBuffer.exit(mark, true).forEach(Runnable::run);

    assertEquals(emitted, Arrays.asList("first", "second"));
  }

  /**
   * A frame that is entered but never exited would leave the thread buffering forever, silently
   * dropping every later emission on it. The transaction runner pairs enter/exit in a finally, so
   * an exception escaping the block must still close the frame.
   */
  @Test
  public void testFrameIsClosedWhenBlockThrows() {
    final List<String> emitted = new ArrayList<>();

    final int mark = DaoUsageBuffer.enter();
    try {
      DaoUsageBuffer.record(() -> emitted.add("write"));
      throw new IllegalStateException("boom");
    } catch (IllegalStateException expected) {
      // expected
    } finally {
      DaoUsageBuffer.exit(mark, false);
    }

    assertFalse(DaoUsageBuffer.isBuffering());
    assertTrue(emitted.isEmpty());

    // The thread is usable again: the next emission is not swallowed by a stranded frame.
    DaoUsageBuffer.record(() -> emitted.add("later"));
    assertEquals(emitted, Collections.singletonList("later"));
  }

  @Test
  public void testExitWithoutEnterIsHarmless() {
    final List<Runnable> pending = DaoUsageBuffer.exit(0, true);

    assertTrue(pending.isEmpty());
    assertFalse(DaoUsageBuffer.isBuffering());
  }

  @Test
  public void testBufferIsNotSharedAcrossThreads() throws Exception {
    final List<String> emitted = Collections.synchronizedList(new ArrayList<>());

    final int mark = DaoUsageBuffer.enter();
    DaoUsageBuffer.record(() -> emitted.add("main-thread-write"));

    // Another thread has no frame, so its emission runs immediately.
    final Thread thread = new Thread(() -> DaoUsageBuffer.record(() -> emitted.add("other-thread-write")));
    thread.start();
    thread.join();

    assertEquals(emitted, Collections.singletonList("other-thread-write"));

    DaoUsageBuffer.exit(mark, true).forEach(Runnable::run);
    assertEquals(emitted, Arrays.asList("other-thread-write", "main-thread-write"));
  }
}
