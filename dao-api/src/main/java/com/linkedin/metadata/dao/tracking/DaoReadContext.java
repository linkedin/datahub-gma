package com.linkedin.metadata.dao.tracking;

/**
 * A thread-local marker that flags the current thread as executing an internal read-before-write,
 * so usage instrumentation can distinguish it from a genuine consumer read.
 *
 * <p>Every aspect write/delete performs a read-modify-write: the write path reads the current value
 * before writing. In new-schema mode that internal read flows through the same access layer as
 * consumer reads and would otherwise be counted as one. The write path sets this flag around the
 * internal read (clearing it in a {@code finally}); the usage decorator skips emission for reads
 * while the flag is set. The set / read / clear all happen synchronously on one thread with no
 * asynchronous boundary, so the flag is reliable and cannot leak across operations.
 *
 * <p>Consumer reads take a different path and never set this flag, so they are still recorded.
 */
public final class DaoReadContext {

  private static final ThreadLocal<Boolean> INTERNAL_READ = ThreadLocal.withInitial(() -> Boolean.FALSE);

  private DaoReadContext() {
  }

  /**
   * Marks the current thread as executing an internal read-before-write.
   */
  public static void markInternalRead() {
    INTERNAL_READ.set(Boolean.TRUE);
  }

  /**
   * Clears the internal-read marker for the current thread. Callers MUST invoke this in a
   * {@code finally} block so the marker cannot leak to a subsequent operation on a pooled thread.
   */
  public static void clear() {
    INTERNAL_READ.remove();
  }

  /**
   * Returns whether the current thread is executing an internal read-before-write.
   *
   * @return {@code true} if the current thread is executing an internal read-before-write
   */
  public static boolean isInternalRead() {
    return INTERNAL_READ.get();
  }
}
