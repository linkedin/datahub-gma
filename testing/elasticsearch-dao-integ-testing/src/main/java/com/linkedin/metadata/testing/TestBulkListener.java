package com.linkedin.metadata.testing;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.support.WriteRequest;


/**
 * A thread safe bulk listener for use in tests.
 *
 * <p>This class records the results of all requests and also has the capability to block for all results via the {@link
 * #settle()} method.</p>
 *
 * <p>This class also changes the behavior of bulk requests by making them all trigger a refresh of the index so that
 * tests can immediately query the results.
 */
final class TestBulkListener implements BulkProcessor.Listener {
  private final Object _lock = new Object();
  private final Set<BulkRequest> _executingRequests = new HashSet<>();
  private final Map<BulkRequest, BulkResponse> _responses = new HashMap<>();
  private final Map<BulkRequest, Throwable> _errors = new HashMap<>();

  /**
   * Resets the state of this bulk listener, clearing the recorded requests and responses.
   */
  public void reset() {
    synchronized (_lock) {
      if (!_executingRequests.isEmpty()) {
        throw new IllegalStateException("Cannot reset while requests are being executed!");
      }
      _responses.clear();
      _errors.clear();
    }
  }

  /**
   * The set of currently executing requests.
   */
  public Set<BulkRequest> getExecutingRequests() {
    synchronized (_lock) {
      return new HashSet<>(_executingRequests);
    }
  }

  /**
   * All requests (executing, successful, and errored) that this bulk processor saw.
   */
  public Set<BulkRequest> getAllRequests() {
    synchronized (_lock) {
      Set<BulkRequest> requests = new HashSet<>();
      requests.addAll(_executingRequests);
      requests.addAll(_responses.keySet());
      requests.addAll(_errors.keySet());
      return requests;
    }
  }

  /**
   * A map from the request to the successful response result.
   */
  public Map<BulkRequest, BulkResponse> getResponses() {
    synchronized (_lock) {
      return new HashMap<>(_responses);
    }
  }

  /**
   * A map from request to the error result.
   */
  public Map<BulkRequest, Throwable> getErrors() {
    synchronized (_lock) {
      return new HashMap<>(_errors);
    }
  }

  /**
   * Waits for all currently executing requests to settle (return successfully or with an error).
   *
   * <p>Note: This does not call flush first. The caller is responsible for doing that.
   */
  public void settle() throws InterruptedException {
    boolean settled = false;
    while (!settled) {
      synchronized (_lock) {
        if (_executingRequests.isEmpty()) {
          settled = true;
        } else {
          _lock.wait();
        }
      }
    }
  }

  @Override
  public void beforeBulk(long executionId, BulkRequest request) {
    synchronized (_lock) {
      // For testing purposes we want update to reflect immediately so that they can be queried.
      request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
      _executingRequests.add(request);
    }
  }

  @Override
  public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
    synchronized (_lock) {
      _executingRequests.remove(request);
      _responses.put(request, response);
      _lock.notifyAll();
    }
  }

  @Override
  public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
    synchronized (_lock) {
      _executingRequests.remove(request);
      _errors.put(request, failure);
      _lock.notifyAll();
    }
  }
}