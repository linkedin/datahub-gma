package com.linkedin.metadata.dao.tracking;

/**
 * A thread-local marker that flags the current thread as executing an internal read-before-write,
 * so usage instrumentation can distinguish it from a genuine consumer read.
 *
 * <p>Every aspect write/delete performs a read-modify-write: the write path reads the current value
 * before writing. In new-schema mode that internal read flows through the same access layer as
 * consumer reads and would otherwise be counted as one. The write path marks the region around the
 * internal read; the usage decorator skips emission for reads while the marker is set. The mark /
 * read / restore all happen synchronously on one thread with no asynchronous boundary, so the
 * marker is reliable and cannot leak across operations.
 *
 * <p>Consumer reads take a different path and never set this marker, so they are still recorded.
 *
 * <p>Marked regions may safely nest: {@link #markInternalRead()} returns a {@link Scope} that
 * restores the <em>previous</em> state on close rather than unconditionally clearing, so an inner
 * region can never unmark an enclosing one. Always use it with try-with-resources:
 *
 * <pre>{@code
 * try (DaoReadContext.Scope ignored = DaoReadContext.markInternalRead()) {
 *   return readSomething();
 * }
 * }</pre>
 */
public final class DaoReadContext {

  private static final ThreadLocal<Boolean> INTERNAL_READ = new ThreadLocal<>();

  private DaoReadContext() {
  }

  /**
   * An active internal-read marker. Closing restores the marker to the state it had before the
   * corresponding {@link #markInternalRead()} call, which is what makes marked regions nestable.
   *
   * <p>{@link AutoCloseable#close()} is narrowed to declare no checked exception so
   * try-with-resources call sites do not need to catch anything.
   */
  public interface Scope extends AutoCloseable {
    @Override
    void close();
  }

  /**
   * Marks the current thread as executing an internal read-before-write until the returned scope
   * is closed. Callers MUST use try-with-resources (or close the scope in a {@code finally}) so
   * the marker cannot leak to a subsequent operation on a pooled thread.
   *
   * @return a scope that restores the previous marker state when closed
   */
  public static Scope markInternalRead() {
    final boolean previous = isInternalRead();
    INTERNAL_READ.set(Boolean.TRUE);
    return () -> {
      if (previous) {
        INTERNAL_READ.set(Boolean.TRUE);
      } else {
        INTERNAL_READ.remove();
      }
    };
  }

  /**
   * Returns whether the current thread is executing an internal read-before-write.
   *
   * @return {@code true} if the current thread is executing an internal read-before-write
   */
  public static boolean isInternalRead() {
    return Boolean.TRUE.equals(INTERNAL_READ.get());
  }
}
