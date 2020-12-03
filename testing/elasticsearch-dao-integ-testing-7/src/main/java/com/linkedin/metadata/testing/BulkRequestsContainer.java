package com.linkedin.metadata.testing;

import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;


/**
 * Records the results of all requests and also has the capability to flush and then block for all results via the
 * {@link #flushAndSettle()} method.
 *
 * <p>Implementation wise, this just a super light wrapper around {@link BulkProcessor} and {@link TestBulkListener} so
 * that limited functionality is exposed to tests.
 */
public final class BulkRequestsContainer {
  private final BulkProcessor _bulkProcessor;
  private final TestBulkListener _testBulkListener;

  BulkRequestsContainer(@Nonnull BulkProcessor bulkProcessor, @Nonnull TestBulkListener testBulkListener) {
    _bulkProcessor = bulkProcessor;
    _testBulkListener = testBulkListener;
  }

  /**
   * Resets the state of this container, clearing the recorded requests and responses.
   */
  public void reset() {
    _testBulkListener.reset();
  }

  /**
   * Flushes any queued requests and then waits for them to settle.
   */
  public void flushAndSettle() throws InterruptedException {
    _bulkProcessor.flush();
    _testBulkListener.settle();
  }

  /**
   * All requests (executing, successful, and errored) that this bulk processor saw.
   *
   * <p>The returned set has no guaranteed order.
   */
  public Set<BulkRequest> getAllRequests() {
    return _testBulkListener.getAllRequests();
  }

  /**
   * The currently executing requests, in the order they were kicked off.
   */
  public List<BulkRequest> getExecutingRequests() {
    return _testBulkListener.getExecutingRequests();
  }

  /**
   * A map from request to the error result.
   *
   * <p>The entries are ordered in in the order they were received.
   */
  public Map<BulkRequest, Throwable> getErrors() {
    return _testBulkListener.getErrors();
  }

  /**
   * A map from the request to the successful response result.
   *
   * <p>The entries are ordered in in the order they were received.
   */
  public Map<BulkRequest, BulkResponse> getResponses() {
    return _testBulkListener.getResponses();
  }
}
