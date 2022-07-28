package com.linkedin.metadata.dao;

import com.linkedin.metadata.query.ListResultMetadata;
import java.util.List;
import lombok.Builder;
import lombok.Value;


/**
 * An immutable value class that holds the result of a list operation and other pagination information.
 *
 * @param <T> the result type
 */
@Builder
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
    if (o == this) {
      return true;
    }

    if (!(o instanceof ListResult)) {
      return false;
    }

    ListResult<T> other = (ListResult<T>) o;

    if (this.values.size() != other.values.size()) {
      return false;
    }

    // TODO: this comparison has worst case O(n^2) runtime. find a more efficient way.
    // TODO: need to add .equals method for all T values possible.
    return this.values.containsAll(other.values)
        && other.values.containsAll(this.values)
        && this.nextStart == other.nextStart
        && this.havingMore == other.havingMore
        && this.totalCount == other.totalCount
        && this.totalPageCount == other.totalPageCount
        && this.pageSize == other.pageSize;
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }
}
