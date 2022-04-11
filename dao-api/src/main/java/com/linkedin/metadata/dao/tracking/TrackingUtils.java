package com.linkedin.metadata.dao.tracking;

import java.util.concurrent.ThreadLocalRandom;
import javax.annotation.Nonnull;


/**
 * Constants used in metadata tracking.
 */
public class TrackingUtils {
  private TrackingUtils() {
  }

  public enum ProcessType {
    AUTOCOMPLETE_QUERY_END("autocompleteQuery.end"),
    AUTOCOMPLETE_QUERY_FAIL("autocompleteQuery.fail"),
    AUTOCOMPLETE_QUERY_START("autocompleteQuery.start"),
    CONVERT_FAIL("convert.fail"),
    FEED_BACKUP_INDEX_END("feedBackupIndex.end"),
    FEED_BACKUP_INDEX_START("feedBackupIndex.start"),
    FEED_LIVE_INDEX_END("feedLiveIndex.end"),
    FEED_LIVE_INDEX_START("feedLiveIndex.start"),
    FILTER_QUERY_END("filterQuery.end"),
    FILTER_QUERY_FAIL("filterQuery.fail"),
    FILTER_QUERY_START("filterQuery.start"),
    PROCESS_END("process.end"),
    PROCESS_FAIL("process.fail"),
    PROCESS_START("process.start"),
    RECEIVE_END("receive.end"),
    RECEIVE_START("receive.start"),
    SEARCH_QUERY_END("searchQuery.end"),
    SEARCH_QUERY_FAIL("searchQuery.fail"),
    SEARCH_QUERY_START("searchQuery.start");

    private final String _name;

    ProcessType(String name) {
      _name = name;
    }

    public String getName() {
      return _name;
    }
  }

  @Nonnull
  public static byte[] random(@Nonnull byte[] output) {
    ThreadLocalRandom.current().nextBytes(output);
    return output;
  }
}

