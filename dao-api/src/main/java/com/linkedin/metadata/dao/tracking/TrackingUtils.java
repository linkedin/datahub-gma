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
    BUILD_FILTER_QUERY_END("buildFilterQuery.end"),
    BUILD_FILTER_QUERY_START("buildFilterQuery.start"),
    BUILD_SEARCH_QUERY_END("buildSearchQuery.end"),
    BUILD_SEARCH_QUERY_START("buildSearchQuery.start"),
    CONVERT_FAIL("convert.fail"),
    FEED_BACKUP_INDEX_END("feedBackupIndex.end"),
    FEED_BACKUP_INDEX_START("feedBackupIndex.start"),
    FEED_LIVE_INDEX_END("feedLiveIndex.end"),
    FEED_LIVE_INDEX_START("feedLiveIndex.start"),
    PROCESS_END("process.end"),
    PROCESS_FAIL("process.fail"),
    PROCESS_START("process.start"),
    RECEIVE_END("receive.end"),
    RECEIVE_START("receive.start"),
    EXECUTE_AND_EXTRACT_END("executeAndExtract.end"),
    EXECUTE_AND_EXTRACT_FAIL("executeAndExtract.fail"),
    EXECUTE_AND_EXTRACT_START("executeAndExtract.start");

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

