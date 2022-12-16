package com.linkedin.metadata.dao;

import com.linkedin.metadata.query.ListResultMetadata;
import java.util.List;
import lombok.Builder;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;


/**
 * An immutable value class that holds the result of a list operation and other pagination information.
 *
 * @param <T> the result type
 */
@Builder
@Slf4j
@Value
public class ListResult<T> {

  public static final int INVALID_NEXT_START = -1;

  // A single page of results
  List<T> values;

  // Related search result metadata
  ListResultMetadata metadata;

  // Offset from the next page
  int nextStart;

  // Whether there's more results
  boolean havingMore;

  // Total number of hits
  int totalCount;

  // Total number of pages
  int totalPageCount;

  // Size of each page
  int pageSize;

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (o.getClass() != this.getClass()) {
      return false;
    }
    ListResult<T> other = (ListResult<T>) o;

    if (this.values.size() != other.values.size()) {
      log.warn("ListResults have different sizes.");
      return false;
    }

    final boolean samePrimitiveValues = this.nextStart == other.nextStart
            && this.havingMore == other.havingMore
            && this.totalCount == other.totalCount
            && this.totalPageCount == other.totalPageCount
            && this.pageSize == other.pageSize;
    if (!samePrimitiveValues) {
      log.warn("ListResults have different values for primitive fields.");
      return false;
    }

    // either both metadata fields are null or both are equal (need to check this.metadata != null to avoid NPE)
    if (!((this.metadata == null && other.metadata == null) || (this.metadata != null && this.metadata.equals(other.metadata)))) {
      log.warn("ListResults have different search result metadata values.");
      return false;
    }

    if (!this.values.containsAll(other.values)) {
      log.warn("ListResults contain different elements.");
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }
}
