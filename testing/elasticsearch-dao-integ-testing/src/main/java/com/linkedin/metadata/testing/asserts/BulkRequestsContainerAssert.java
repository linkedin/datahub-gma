package com.linkedin.metadata.testing.asserts;

import com.linkedin.metadata.testing.BulkRequestsContainer;
import java.util.Collection;
import java.util.TreeSet;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.IterableAssert;
import org.assertj.core.api.MapAssert;
import org.elasticsearch.action.bulk.BulkRequest;


/**
 * Assertions for {@link BulkRequestsContainer}.
 */
public class BulkRequestsContainerAssert extends AbstractAssert<BulkRequestsContainerAssert, BulkRequestsContainer> {
  public BulkRequestsContainerAssert(BulkRequestsContainer requestsContainer) {
    super(requestsContainer, BulkRequestsContainerAssert.class);
  }

  public static BulkRequestsContainerAssert assertThat(@Nonnull BulkRequestsContainer actual) {
    return new BulkRequestsContainerAssert(actual);
  }

  private static Collection<String> getIds(Collection<BulkRequest> requests) {
    return requests.stream()
        .flatMap(request -> request.requests().stream())
        .map(request -> request.id())
        .collect(Collectors.toList());
  }

  /**
   * Allows easy access to assert the document IDs that were written.
   */
  public IterableAssert<String> documentIds() {
    return Assertions.assertThat(getIds(actual.getResponses().keySet()));
  }

  /**
   * Allows easy access to assert the errors during writing, if any.
   */
  public MapAssert<BulkRequest, Throwable> errors() {
    return Assertions.assertThat(actual.getErrors());
  }

  /**
   * Asserts that requests made were successful.
   */
  public BulkRequestsContainerAssert hadNoErrors() {
    if (!actual.getErrors().isEmpty()) {
      for (Throwable t : actual.getErrors().values()) {
        t.printStackTrace();
      }

      failWithMessage("%s requests failed with errors. See console for more details.", actual.getErrors().size());
    }

    return this;
  }

  /**
   * Asserts that no requests are currently executing.
   */
  public BulkRequestsContainerAssert allRequestsSettled() {
    if (!actual.getExecutingRequests().isEmpty()) {
      final Collection<String> inFlightIds = new TreeSet<>(getIds(actual.getExecutingRequests()));
      failWithMessage("Expected no executing requests, but found that requests for IDs [%s] were in flight.",
          String.join(", ", inFlightIds));
    }

    return this;
  }
}
