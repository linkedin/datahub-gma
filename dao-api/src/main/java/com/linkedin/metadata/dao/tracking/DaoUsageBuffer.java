package com.linkedin.metadata.dao.tracking;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;


/**
 * A thread-local buffer that holds usage emissions until the enclosing transaction commits.
 *
 * <p>The usage decorator wraps the access layer, which sits below the transaction boundary: it sees
 * a write before the transaction commits. Emitting there reports writes that later roll back, and
 * reports them again on every retry. Transactional writes therefore record here instead.
 *
 * <p>Only the outermost frame releases anything: Ebean implements nested transactions with
 * savepoints, so an inner commit is undone if the outer transaction rolls back. See
 * {@code EbeanLocalDAO#runInTransactionWithRetry} for the enter/truncate/exit/flush sequence.
 *
 * <p>{@link #record} runs immediately when no frame is active, so non-transactional paths are
 * unchanged. Reads are not buffered -- a read that was served happened regardless of whether a
 * later write rolls back.
 *
 * <p>Callers MUST pair {@link #enter()} with {@link #exit} in a {@code finally}, with nothing that
 * can throw in between: a frame that is never exited leaves the thread buffering forever, silently
 * dropping every later emission on it.
 */
public final class DaoUsageBuffer {

  /** Nesting depth and the emissions waiting on the outermost commit. */
  private static final class Frame {
    private final List<Runnable> pending = new ArrayList<>();
    private int depth;
  }

  private static final ThreadLocal<Frame> STATE = new ThreadLocal<>();

  private DaoUsageBuffer() {
  }

  /**
   * Opens a transaction frame on the current thread.
   *
   * @return the mark for {@link #truncateTo} and {@link #exit}: the number of emissions already
   *         buffered by enclosing frames, which this frame must not discard
   */
  public static int enter() {
    Frame frame = STATE.get();
    if (frame == null) {
      frame = new Frame();
      STATE.set(frame);
    }
    frame.depth++;
    return frame.pending.size();
  }

  /**
   * Drops everything buffered since {@code mark}, leaving enclosing frames untouched.
   *
   * @param mark the value returned by {@link #enter()}
   */
  public static void truncateTo(int mark) {
    final Frame frame = STATE.get();
    if (frame == null || mark < 0) {
      return;
    }
    while (frame.pending.size() > mark) {
      frame.pending.remove(frame.pending.size() - 1);
    }
  }

  /**
   * Closes the current frame.
   *
   * @param mark      the value returned by the matching {@link #enter()}
   * @param committed whether this frame's transaction committed; if not, its emissions are dropped
   * @return the emissions to run, empty unless this was the outermost frame
   */
  @Nonnull
  public static List<Runnable> exit(int mark, boolean committed) {
    final Frame frame = STATE.get();
    if (frame == null) {
      return Collections.emptyList();
    }
    if (!committed) {
      truncateTo(mark);
    }
    frame.depth--;
    if (frame.depth > 0) {
      return Collections.emptyList();
    }
    // Outermost frame: clear the thread-local so pooled threads retain nothing.
    STATE.remove();
    return frame.pending.isEmpty() ? Collections.emptyList() : frame.pending;
  }

  /**
   * Buffers an emission until the outermost transaction commits, or runs it immediately when no
   * frame is active. Never throws.
   *
   * @param emission the emission to run; must not throw
   */
  public static void record(@Nonnull Runnable emission) {
    final Frame frame = STATE.get();
    if (frame == null || frame.depth <= 0) {
      emission.run();
      return;
    }
    frame.pending.add(emission);
  }

  /**
   * Whether a transaction frame is currently open on this thread. For tests and diagnostics.
   *
   * @return {@code true} if emissions on this thread are currently buffered
   */
  public static boolean isBuffering() {
    final Frame frame = STATE.get();
    return frame != null && frame.depth > 0;
  }
}
