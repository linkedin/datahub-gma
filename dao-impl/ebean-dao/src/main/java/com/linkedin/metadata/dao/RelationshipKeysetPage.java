package com.linkedin.metadata.dao;

import com.linkedin.data.template.RecordTemplate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;


/**
 * Immutable result of a single keyset (seek) pagination page returned by
 * {@link EbeanLocalRelationshipQueryDAO#findRelationshipsByKeyset}.
 *
 * @param <R> the deserialized relationship record type
 */
public final class RelationshipKeysetPage<R extends RecordTemplate> {

  @Getter
  @Nonnull
  private final List<R> relationships;
  @Getter
  private final long highWaterId;
  @Getter
  @Nullable
  private final RelationshipKeysetCursor nextCursor;

  /**
   * Creates a page.
   *
   * @param relationships the relationships in this page, in ascending {@code id} order. Copied
   *                      defensively and exposed as an unmodifiable list.
   * @param highWaterId the largest relationship row {@code id} when paging started (the
   *                    {@code maxId} captured on the first page), which bounds the scan. Must be
   *                    non-negative.
   * @param nextCursor cursor to fetch the next page, or {@code null} if this is the last page.
   *                   When non-null it must preserve the same {@code maxId} as {@code highWaterId}.
   */
  public RelationshipKeysetPage(@Nonnull List<R> relationships, long highWaterId,
      @Nullable RelationshipKeysetCursor nextCursor) {
    if (relationships == null) {
      throw new IllegalArgumentException("relationships must not be null");
    }
    if (highWaterId < 0) {
      throw new IllegalArgumentException(
          "highWaterId must be non-negative but was " + highWaterId);
    }
    if (nextCursor != null && nextCursor.getMaxId() != highWaterId) {
      throw new IllegalArgumentException(
          "nextCursor maxId (" + nextCursor.getMaxId() + ") must match highWaterId ("
              + highWaterId + ")");
    }
    this.relationships =
        Collections.unmodifiableList(new ArrayList<>(relationships));
    this.highWaterId = highWaterId;
    this.nextCursor = nextCursor;
  }
}
