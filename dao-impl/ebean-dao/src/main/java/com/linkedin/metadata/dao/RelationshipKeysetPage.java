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
  private final long maxId;
  @Getter
  @Nullable
  private final RelationshipKeysetCursor nextCursor;

  /**
   * Creates a page.
   *
   * @param relationships the relationships in this page, in ascending {@code id} order. Copied
   *                      defensively and exposed as an unmodifiable list.
   * @param maxId the largest relationship row {@code id} this page may draw from. When the scan is
   *              paged this is the largest id in the table at the time paging started, captured on
   *              the first page, which bounds the scan so rows inserted mid-scan are excluded. When
   *              the whole result was read in one statement there is no such bound and no second
   *              page to exclude anything from, so it is the largest id returned, or 0 when the
   *              result is empty. Must be non-negative.
   * @param nextCursor cursor to fetch the next page, or {@code null} if this is the last page.
   *                   When non-null its own {@code maxId} must equal this page's {@code maxId}.
   */
  public RelationshipKeysetPage(@Nonnull List<R> relationships, long maxId,
      @Nullable RelationshipKeysetCursor nextCursor) {
    if (relationships == null) {
      throw new IllegalArgumentException("relationships must not be null");
    }
    if (maxId < 0) {
      throw new IllegalArgumentException(
          "maxId must be non-negative but was " + maxId);
    }
    if (nextCursor != null && nextCursor.getMaxId() != maxId) {
      throw new IllegalArgumentException(
          "nextCursor maxId (" + nextCursor.getMaxId() + ") must match page maxId ("
              + maxId + ")");
    }
    this.relationships =
        Collections.unmodifiableList(new ArrayList<>(relationships));
    this.maxId = maxId;
    this.nextCursor = nextCursor;
  }
}
