package com.linkedin.metadata.dao.tracking;

import java.util.concurrent.atomic.AtomicBoolean;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class DaoReadContextTest {

  @Test
  public void testDefaultsToFalse() {
    assertFalse(DaoReadContext.isInternalRead());
  }

  @Test
  public void testMarkThenClose() {
    try (DaoReadContext.Scope ignored = DaoReadContext.markInternalRead()) {
      assertTrue(DaoReadContext.isInternalRead());
    }
    assertFalse(DaoReadContext.isInternalRead());
  }

  /**
   * An inner marked region must not unmark the enclosing one when it closes. Before the scope API
   * the inner {@code clear()} removed the marker outright and the rest of the outer region was
   * silently billed as consumer reads.
   */
  @Test
  public void testNestedScopesRestorePreviousState() {
    try (DaoReadContext.Scope outer = DaoReadContext.markInternalRead()) {
      assertTrue(DaoReadContext.isInternalRead());

      try (DaoReadContext.Scope inner = DaoReadContext.markInternalRead()) {
        assertTrue(DaoReadContext.isInternalRead());
      }

      // The outer region is still active.
      assertTrue(DaoReadContext.isInternalRead());
    }
    assertFalse(DaoReadContext.isInternalRead());
  }

  @Test
  public void testDeeplyNestedScopes() {
    try (DaoReadContext.Scope first = DaoReadContext.markInternalRead()) {
      try (DaoReadContext.Scope second = DaoReadContext.markInternalRead()) {
        try (DaoReadContext.Scope third = DaoReadContext.markInternalRead()) {
          assertTrue(DaoReadContext.isInternalRead());
        }
        assertTrue(DaoReadContext.isInternalRead());
      }
      assertTrue(DaoReadContext.isInternalRead());
    }
    assertFalse(DaoReadContext.isInternalRead());
  }

  @Test
  public void testScopeIsRestoredWhenBodyThrows() {
    try {
      try (DaoReadContext.Scope ignored = DaoReadContext.markInternalRead()) {
        assertTrue(DaoReadContext.isInternalRead());
        throw new IllegalStateException("boom");
      }
    } catch (IllegalStateException expected) {
      // expected
    }
    assertFalse(DaoReadContext.isInternalRead());
  }

  @Test
  public void testNestedScopeIsRestoredWhenInnerBodyThrows() {
    try (DaoReadContext.Scope outer = DaoReadContext.markInternalRead()) {
      try {
        try (DaoReadContext.Scope inner = DaoReadContext.markInternalRead()) {
          throw new IllegalStateException("boom");
        }
      } catch (IllegalStateException expected) {
        // expected
      }
      assertTrue(DaoReadContext.isInternalRead());
    }
    assertFalse(DaoReadContext.isInternalRead());
  }

  /**
   * The marker is backed by a bare {@link ThreadLocal}, so a thread that has never marked reads
   * {@code null} internally. The accessor must treat that as {@code false} rather than throwing
   * on unboxing -- it is evaluated outside the emitter's fail-open guard, so an exception here
   * would propagate into the DAO read path.
   */
  @Test
  public void testIsInternalReadIsNullSafeOnUntouchedThread() throws Exception {
    final AtomicBoolean observed = new AtomicBoolean(true);
    final AtomicBoolean threw = new AtomicBoolean(false);

    final Thread thread = new Thread(() -> {
      try {
        observed.set(DaoReadContext.isInternalRead());
      } catch (RuntimeException e) {
        threw.set(true);
      }
    });
    thread.start();
    thread.join();

    assertFalse(threw.get());
    assertFalse(observed.get());
  }

  @Test
  public void testMarkerIsNotVisibleToOtherThreads() throws Exception {
    final AtomicBoolean observedOnOtherThread = new AtomicBoolean(true);

    try (DaoReadContext.Scope ignored = DaoReadContext.markInternalRead()) {
      assertTrue(DaoReadContext.isInternalRead());

      final Thread thread = new Thread(() -> observedOnOtherThread.set(DaoReadContext.isInternalRead()));
      thread.start();
      thread.join();
    }

    assertFalse(observedOnOtherThread.get());
  }

  /**
   * Closing twice must not resurrect or corrupt the marker, since try-with-resources on a scope
   * held in a variable can be closed explicitly as well.
   */
  @Test
  public void testCloseIsIdempotent() {
    final DaoReadContext.Scope scope = DaoReadContext.markInternalRead();
    assertTrue(DaoReadContext.isInternalRead());

    scope.close();
    assertFalse(DaoReadContext.isInternalRead());

    scope.close();
    assertFalse(DaoReadContext.isInternalRead());
  }
}
