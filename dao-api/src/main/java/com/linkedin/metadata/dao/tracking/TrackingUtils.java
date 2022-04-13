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
    // Process States in ESSearchDAO.
    // End point of autocomplete request.
    AUTOCOMPLETE_QUERY_END("autocompleteQuery.end"),
    // Fail state of autocomplete request.
    AUTOCOMPLETE_QUERY_FAIL("autocompleteQuery.fail"),
    // Start point of autocomplete request.
    AUTOCOMPLETE_QUERY_START("autocompleteQuery.start"),
    // End point of filter request.
    FILTER_QUERY_END("filterQuery.end"),
    // Fail state of filter request.
    FILTER_QUERY_FAIL("filterQuery.fail"),
    // Start point of filter request.
    FILTER_QUERY_START("filterQuery.start"),
    // End point of search request.
    SEARCH_QUERY_END("searchQuery.end"),
    // Fail state of search request.
    SEARCH_QUERY_FAIL("searchQuery.fail"),
    // Start point of search request.
    SEARCH_QUERY_START("searchQuery.start"),

    // Process States in ES-MAE-Consumer Job.
    // Fail state of convert snapshot/document.
    CONVERT_FAIL("convert.fail"),
    // End point of feed backup index.
    FEED_BACKUP_INDEX_END("feedBackupIndex.end"),
    // Start point of feed backup index.
    FEED_BACKUP_INDEX_START("feedBackupIndex.start"),
    // End point of feed live index.
    FEED_LIVE_INDEX_END("feedLiveIndex.end"),
    // Start point of feed live index.
    FEED_LIVE_INDEX_START("feedLiveIndex.start"),
    // End point of process event.
    PROCESS_END("process.end"),
    // Fail state of process event.
    PROCESS_FAIL("process.fail"),
    // Start point of process event.
    PROCESS_START("process.start"),
    // End point of receive event.
    RECEIVE_END("receive.end"),
    // Start point of receive event.
    RECEIVE_START("receive.start");

    private final String _name;

    ProcessType(String name) {
      _name = name;
    }

    public String getName() {
      return _name;
    }
  }

  /**
   * Create a fixed 16 size random byte array for trackingID.
   * @return the fixed 16 size random trackingID.
   */
  @Nonnull
  public static byte[] getRandomTrackingId() {
    return random(new byte[16]);
  }

  @Nonnull
  private static byte[] random(@Nonnull byte[] output) {
    ThreadLocalRandom.current().nextBytes(output);
    return output;
  }
}

