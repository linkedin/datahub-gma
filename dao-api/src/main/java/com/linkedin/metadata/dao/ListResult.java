package com.linkedin.metadata.dao;

import com.linkedin.metadata.query.ListResultMetadata;
import java.util.ArrayList;
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

  // Assume this will be called when comparing old schema vs new schema results (in EbeanDAOUtils::compareResults).
  // The logs are written under the assumption that we are calling oldSchemaListResult.equals(newSchemaListResult).
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
      log.warn(String.format("The old schema returned %d result(s) while the new schema returned %d result(s).", this.values.size(), other.values.size()));
      return false;
    }

    final boolean samePrimitiveValues = this.nextStart == other.nextStart
            && this.havingMore == other.havingMore
            && this.totalCount == other.totalCount
            && this.totalPageCount == other.totalPageCount
            && this.pageSize == other.pageSize;
    if (!samePrimitiveValues) {
      log.warn(String.format("ListResults have different values for primitive fields."
          + "\nOld schema: {nextStart: %d, havingMore: %b, totalCount: %d, totalPageCount: %d, pageSize: %d}"
          + "\nNew schema: {nextStart: %d, havingMore: %b, totalCount: %d, totalPageCount: %d, pageSize: %d}",
          this.nextStart, this.havingMore, this.totalCount, this.totalPageCount, this.pageSize,
          other.nextStart, other.havingMore, other.totalCount, other.totalPageCount, other.pageSize));
      return false;
    }

    // either both metadata fields are null or both are equal (need to check this.metadata != null to avoid NPE)
    if (!((this.metadata == null && other.metadata == null) || (this.metadata != null && this.metadata.equals(other.metadata)))) {
      log.warn(String.format("ListResults have different search result metadata values."
          + "\nOld schema ListResult metadata: %s\nNew schema ListResult metadata: %s", this.metadata, other.metadata));
      return false;
    }

    // assuming this = old schema result and other = new schema result
    if (!this.values.containsAll(other.values)) {
      List<T> onlyInOldSchema = new ArrayList<>(this.values);
      List<T> onlyInNewSchema = new ArrayList<>(other.values);
      onlyInOldSchema.removeAll(other.values);
      onlyInNewSchema.removeAll(this.values);
      final String message = String.format("ListResults contain different elements."
          + "\nExists in old schema but not in new: %s\nExists in new schema but not in old: %s", onlyInOldSchema, onlyInNewSchema);
      log.warn(message);
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }
}
