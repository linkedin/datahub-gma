package com.linkedin.metadata.dao.tracking;

import com.linkedin.avro2pegasus.events.UUID;
import com.linkedin.data.ByteString;
import java.util.concurrent.ThreadLocalRandom;
import javax.annotation.Nonnull;


/**
 * Constants used in metadata tracking.
 */
public class TrackingUtils {
  private TrackingUtils() {
  }

  public enum ProcessType {
    // Process states in ESSearchDAO.
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

    // LI: Please refer to http://go/mg/healthmonitoring for definitions on each of these process states.
    // Process states in Local DAO.
    MYSQL_WRITE_SUCCESS("dao.mysqlWrite.success"),
    MYSQL_WRITE_FAILURE("dao.mySqlWrite.failure"),
    DAO_PROCESS_START("dao.process.start"),
    DAO_PROCESS_FAILURE("dao.process.failure"),
    DAO_PROCESS_SKIPPED("dao.process.skipped"),
    MAEV5_EMISSION_SUCCESS("dao.maev5.success"),
    MAEV5_EMISSION_FAILURE("dao.maev5.failure"),

    // Process states in MCEv2 Consumer Job.

    // MCEv2 events submitted to Kafka topic
    MCEV2_SUBMITTED("mce-v2-consumer.mcev2.submitted"),
    MCEV2_RECEIVED("mce-v2-consumer.mcev2.received"),
    MCEV2_PROCESS_SUCCESS("mce-v2-consumer.process.success"),
    MCEV2_PROCESS_FAILURE("mce-v2-consumer.process.failure"),
    MCEV2_FAILURE_EVENT_SUCCESS("mce-v2-consumer.failureEvent.success"),
    MCEV2_FAILURE_EVENT_FAILURE("mce-v2-consumer.failureEvent.failure"),

    // Process states in MCEv5 Consumer Job.

    // MCEv5 events submitted to Kafka topic
    MCEV5_SUBMITTED("mce-v5-consumer.mcev5.submitted"),
    MCEV5_RECEIVED("mce-v5-consumer.mcev5.received"),
    MCEV5_PROCESS_SUCCESS("mce-v5-consumer.process.success"),
    MCEV5_PROCESS_FAILURE("mce-v5-consumer.process.failure"),
    MCEV5_FAILURE_EVENT_SUCCESS("mce-v5-consumer.failureEvent.success"),
    MCEV5_FAILURE_EVENT_FAILURE("mce-v5-consumer.failureEvent.failure"),

    // Process states in MAEv5 Consumer Search Job

    // MAEv5 events submitted to Kafka topic
    MAEV5_SUBMITTED_SEARCH("maev5-elasticsearch-job.maev5.submitted"),
    // MAEv5 events received by es-graph-job consumer
    MAEV5_RECEIVED_SEARCH("maev5-elasticsearch-job.maev5.received"),
    PREPROCESS_SEARCH_SUCCESS("maev5-elasticsearch-job.preprocess.success"),
    PREPROCESS_SEARCH_FAILURE("maev5-elasticsearch-job.preprocess.failure"),
    BULKPROCESSOR_SEARCH_SUCCESS("maev5-elasticsearch-job.bulkProcessor.success"),
    BULKPROCESSOR_SEARCH_FAILURE("maev5-elasticsearch-job.bulkProcessor.failure"),
    SEARCH_FAILURE_EVENT_SUCCESS("maev5-elasticsearch-job.failureEvent.success"),
    SEARCH_FAILURE_EVENT_FAILURE("maev5-elasticsearch-job.failureEvent.failure"),

    // Process states in MAEv5 Consumer Graph Job

    // MAEv5 events submitted to Kafka topic
    MAEV5_SUBMITTED_GRAPH("maev5-es-graph-job.maev5.submitted"),
    // MAEv5 events received by es-graph-job consumer
    MAEV5_RECEIVED_GRAPH("maev5-es-graph-job.maev5.received"),
    PREPROCESS_GRAPH_SUCCESS("maev5-es-graph-job.preprocess.success"),
    PREPROCESS_GRAPH_FAILURE("maev5-es-graph-job.preprocess.failure"),
    BULKPROCESSOR_GRAPH_SUCCESS("maev5-es-graph-job.bulkProcessor.success"),
    BULKPROCESSOR_GRAPH_FAILURE("maev5-es-graph-job.bulkProcessor.failure"),
    GRAPH_FAILURE_EVENT_SUCCESS("maev5-es-graph-job.failureEvent.success"),
    GRAPH_FAILURE_EVENT_FAILURE("maev5-es-graph-job.failureEvent.failure"),

    // Process states in MLE Consumer Job
    MLE_RECEIVED("mle-consumer-job.mle.received"),
    MLE_PROCESS_SUCCESS("mle-consumer-job.process.success"),
    MLE_PROCESS_FAILURE("mle-consumer-job.process.failure");

    private final String _name;

    ProcessType(String name) {
      _name = name;
    }

    public String getName() {
      return _name;
    }
  }

  /*
   * LI: http://go/metadata/tracking/dimensions
   */
  public enum Dimension {
    ASPECT_TYPE("aspectType"),
    ERROR_TYPE("errorType"),
    ORIGINAL_EMIT_TIME("originalEmitTime"),
    PLATFORM_TYPE("platformType");

    private final String _name;

    Dimension(String name) {
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
  @Deprecated
  @Nonnull
  public static byte[] getRandomTrackingId() {
    return random(new byte[16]);
  }

  @Nonnull
  public static UUID getRandomUUID() {
    return new UUID(ByteString.copy(getRandomTrackingId()));
  }

  @Nonnull
  private static byte[] random(@Nonnull byte[] output) {
    ThreadLocalRandom.current().nextBytes(output);
    return output;
  }

  @Nonnull
  public static byte[] convertUUID(UUID uuid) {
    return uuid.data().copyBytes();
  }
}

