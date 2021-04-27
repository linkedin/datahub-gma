package com.linkedin.metadata.dao.internal;

import com.linkedin.metadata.dao.exception.RetryLimitReached;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.commons.lang.time.StopWatch;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Query;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.exceptions.Neo4jException;


public final class Neo4jQueryExecutor {
  private static final int MAX_TRANSACTION_RETRY = 3;
  private final Driver _driver;
  private final SessionConfig _sessionConfig;

  public Neo4jQueryExecutor(@Nonnull Driver driver, @Nonnull SessionConfig sessionConfig) {
    _driver = driver;
    _sessionConfig = sessionConfig;
  }

  public Neo4jQueryExecutor(@Nonnull Driver driver) {
    this(driver, SessionConfig.defaultConfig());
  }

  /**
   * Executes a list of queries with parameters in one transaction.
   *
   * @param queries List of queries with parameters to be executed in order
   */
  @Nonnull
  public Neo4jQueryResult execute(@Nonnull List<Query> queries) {
    int retry = 0;
    final StopWatch stopWatch = new StopWatch();
    stopWatch.start();
    Exception lastException;
    try (final Session session = _driver.session(_sessionConfig)) {
      do {
        try {
          session.writeTransaction(tx -> {
            for (Query query : queries) {
              tx.run(query);
            }
            return null;
          });
          lastException = null;
          break;
        } catch (Neo4jException e) {
          lastException = e;
        }
      } while (++retry <= MAX_TRANSACTION_RETRY);
    }

    if (lastException != null) {
      throw new RetryLimitReached(
          "Failed to execute Neo4j write transaction after " + MAX_TRANSACTION_RETRY + " retries", lastException);
    }

    stopWatch.stop();
    return Neo4jQueryResult.builder().tookMs(stopWatch.getTime()).retries(retry).build();
  }
}
