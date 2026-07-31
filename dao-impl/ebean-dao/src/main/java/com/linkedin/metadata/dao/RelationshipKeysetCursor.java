package com.linkedin.metadata.dao;

import lombok.Getter;


/**
 * Immutable position of a keyset (seek) pagination scan over a single relationship table, used by
 * {@link EbeanLocalRelationshipQueryDAO#findRelationshipsByKeyset}.
 *
 * <p>{@code lastId} is the id of the last row returned; the next page starts strictly after it
 * ({@code rt.id > lastId}) and is {@code 0} for the first page. {@code maxId} is the largest
 * relationship row id when paging starts, captured on the first page
 * ({@code COALESCE(MAX(id), 0)}); every page is bounded by {@code rt.id <= maxId}. Later inserts get
 * larger ids and are excluded, which keeps the scan finite. The combined pages are not a
 * point-in-time snapshot: existing rows updated or soft-deleted between page calls can change which
 * rows a later page returns, so callers must not assume they see every row that was current when
 * paging started. Both values are non-negative and {@code lastId <= maxId} always holds.</p>
 */
public final class RelationshipKeysetCursor {

  @Getter
  private final long lastId;
  @Getter
  private final long maxId;

  /**
   * Creates a cursor.
   *
   * @param lastId id of the last row already returned; the next page starts strictly after it.
   *               Must be non-negative.
   * @param maxId largest relationship row id when paging starts; bounds the scan. Must be
   *              non-negative and {@code >= lastId}.
   */
  public RelationshipKeysetCursor(long lastId, long maxId) {
    if (lastId < 0) {
      throw new IllegalArgumentException("lastId must be non-negative but was " + lastId);
    }
    if (maxId < 0) {
      throw new IllegalArgumentException("maxId must be non-negative but was " + maxId);
    }
    if (lastId > maxId) {
      throw new IllegalArgumentException(
          "lastId (" + lastId + ") must not be greater than maxId (" + maxId + ")");
    }
    this.lastId = lastId;
    this.maxId = maxId;
  }
}
