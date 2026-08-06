package com.linkedin.metadata.dao;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import lombok.Getter;


/**
 * Immutable position of a keyset (seek) pagination scan over a single relationship table, used by
 * {@link EbeanLocalRelationshipQueryDAO#findRelationshipsByKeyset}.
 *
 * <p>{@code scanStartTime} is captured from the database clock when the first page starts and is
 * used by later pages to include rows that were current at scan start even if they were soft-deleted
 * before a later page is read. {@code relationshipTableName} identifies the relationship table this
 * cursor belongs to and is validated by the DAO before continuing a scan. {@code lastId} is the id
 * of the last row returned; the next page starts strictly after it
 * ({@code rt.id > lastId}) and is {@code 0} for the first page. {@code maxId} is the largest
 * relationship row id when paging starts, captured on the first page
 * ({@code COALESCE(MAX(id), 0)}); every page is bounded by {@code rt.id <= maxId}. Later inserts get
 * larger ids and are excluded, which keeps the scan finite. Numeric values are non-negative and
 * {@code lastId <= maxId} always holds.</p>
 */
public final class RelationshipKeysetCursor {
  private static final Pattern SCAN_START_TIME_PATTERN =
      Pattern.compile("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{6}");
  private static final DateTimeFormatter SCAN_START_TIME_FORMATTER =
      DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSSSSS").withResolverStyle(ResolverStyle.STRICT);

  @Getter
  private final long lastId;
  @Getter
  private final long maxId;
  @Getter
  @Nonnull
  private final String relationshipTableName;
  @Getter
  @Nonnull
  private final String scanStartTime;

  /**
   * Creates a cursor.
   *
   * @param lastId id of the last row already returned; the next page starts strictly after it.
   *               Must be non-negative.
   * @param maxId largest relationship row id when paging starts; bounds the scan. Must be
   *              non-negative and {@code >= lastId}.
   * @param scanStartTime database time when the scan started. Must not be null.
   * @param relationshipTableName relationship table this cursor belongs to. Must not be null or
   *                              empty.
   */
  public RelationshipKeysetCursor(long lastId, long maxId, @Nonnull String scanStartTime,
      @Nonnull String relationshipTableName) {
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
    if (scanStartTime == null) {
      throw new IllegalArgumentException("scanStartTime must not be null");
    }
    if (scanStartTime.trim().isEmpty()) {
      throw new IllegalArgumentException("scanStartTime must not be empty");
    }
    validateScanStartTime(scanStartTime);
    if (relationshipTableName == null || relationshipTableName.trim().isEmpty()) {
      throw new IllegalArgumentException("relationshipTableName must not be null or empty");
    }
    this.lastId = lastId;
    this.maxId = maxId;
    this.scanStartTime = scanStartTime;
    this.relationshipTableName = relationshipTableName;
  }

  private static void validateScanStartTime(@Nonnull String scanStartTime) {
    if (!SCAN_START_TIME_PATTERN.matcher(scanStartTime).matches()) {
      throw new IllegalArgumentException("scanStartTime must use yyyy-MM-dd HH:mm:ss.SSSSSS");
    }
    try {
      LocalDateTime.parse(scanStartTime, SCAN_START_TIME_FORMATTER);
    } catch (DateTimeParseException e) {
      throw new IllegalArgumentException("scanStartTime must use yyyy-MM-dd HH:mm:ss.SSSSSS", e);
    }
  }
}
