package com.linkedin.metadata.dao.internal;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Query;
import org.neo4j.driver.SessionConfig;

import static com.linkedin.metadata.dao.Neo4jUtil.*;
import static com.linkedin.metadata.dao.utils.ModelUtils.*;
import static com.linkedin.metadata.dao.utils.RecordUtils.*;


/**
 * An Neo4j implementation of {@link BaseGraphWriterDAO}.
 */
@Slf4j
public class Neo4jGraphWriterDAO extends BaseGraphWriterDAO {
  /**
   * Event listening interface to consume certain neo4j events and report metrics to some specific metric recording
   * framework.
   *
   * <p>This allows for recording lower level metrics than just recording how long these method calls take; there is
   * other overhead associated with the methods in this class, and these callbacks attempt to more accurately reflect
   * how long neo4j transactions are taking.
   */
  public interface MetricListener {
    /**
     * Event when entities are successfully added to the neo4j graph.
     *
     * @param entityCount how many entities were added in this transaction
     * @param updateTimeMs how long the update took in total (across all retries)
     * @param retries how many retries were needed before the update was successful (0 means first attempt was a
     *     success)
     */
    void onEntitiesAdded(int entityCount, long updateTimeMs, int retries);

    /**
     * Event when relationships are successfully added to the neo4j graph.
     *
     * @param relationshipCount how many relationships were added in this transaction
     * @param updateTimeMs how long the update took in total (across all retries)
     * @param retries how many retries were needed before the update was successful (0 means first attempt was a
     *     success)
     */
    void onRelationshipsAdded(int relationshipCount, long updateTimeMs, int retries);

    /**
     * Event when entities are successfully removed from the neo4j graph.
     *
     * @param entityCount how many entities were removed in this transaction
     * @param updateTimeMs how long the update took in total (across all retries)
     * @param retries how many retries were needed before the update was successful (0 means first attempt was a
     *     success)
     */
    void onEntitiesRemoved(int entityCount, long updateTimeMs, int retries);

    /**
     * Event when relationships are successfully removed from the neo4j graph.
     *
     * @param relationshipCount how many relationships were added in this transaction
     * @param updateTimeMs how long the update took in total (across all retries)
     * @param retries how many retries were needed before the update was successful (0 means first attempt was a
     *     success)
     */
    void onRelationshipsRemoved(int relationshipCount, long updateTimeMs, int retries);
  }

  private static final class DelegateMetricListener implements MetricListener {
    private final Set<MetricListener> _metricListeners = new HashSet<>();

    void addMetricListener(@Nonnull MetricListener metricListener) {
      _metricListeners.add(metricListener);
    }

    @Override
    public void onEntitiesAdded(int entityCount, long updateTimeMs, int retries) {
      for (MetricListener m : _metricListeners) {
        m.onEntitiesAdded(entityCount, updateTimeMs, retries);
      }
    }

    @Override
    public void onRelationshipsAdded(int relationshipCount, long updateTimeMs, int retries) {
      for (MetricListener m : _metricListeners) {
        m.onRelationshipsAdded(relationshipCount, updateTimeMs, retries);
      }
    }

    @Override
    public void onEntitiesRemoved(int entityCount, long updateTimeMs, int retries) {
      for (MetricListener m : _metricListeners) {
        m.onEntitiesRemoved(entityCount, updateTimeMs, retries);
      }
    }

    @Override
    public void onRelationshipsRemoved(int relationshipCount, long updateTimeMs, int retries) {
      for (MetricListener m : _metricListeners) {
        m.onRelationshipsRemoved(relationshipCount, updateTimeMs, retries);
      }
    }
  }

  private DelegateMetricListener _metricListener = new DelegateMetricListener();
  private final Neo4jQueriesTransformer _queriesTransformer;
  private final Neo4jQueryExecutor _queryExecutor;

  private Neo4jGraphWriterDAO(@Nonnull Neo4jQueriesTransformer queriesTransformer,
      @Nonnull Neo4jQueryExecutor queryExecutor) {
    _queriesTransformer = queriesTransformer;
    _queryExecutor = queryExecutor;
  }

  public Neo4jGraphWriterDAO(@Nonnull Driver driver) {
    this(new Neo4jQueriesTransformer(), new Neo4jQueryExecutor(driver));
  }

  /* Should only be used for testing */
  public Neo4jGraphWriterDAO(@Nonnull Driver driver, @Nonnull Set<Class<? extends RecordTemplate>> allEntities) {
    this(new Neo4jQueriesTransformer(allEntities), new Neo4jQueryExecutor(driver));
  }

  /**
   * WARNING: Do NOT use this! This is not tested yet.
   * Multi-DB support comes with Neo4j 4+.
   * Although DAO works with Neo4j 4+, we can't bump Neo4j test harness to 4+ to test this because it needs Java 11
   * And Java 11 build is blocked by ES7 migration.
   */
  public Neo4jGraphWriterDAO(@Nonnull Driver driver, @Nonnull String databaseName) {
    this(new Neo4jQueriesTransformer(), new Neo4jQueryExecutor(driver, SessionConfig.forDatabase(databaseName)));
  }

  public void addMetricListener(@Nonnull MetricListener metricListener) {
    _metricListener.addMetricListener(metricListener);
  }

  @Override
  public <ENTITY extends RecordTemplate> void addEntities(@Nonnull List<ENTITY> entities) {
    final List<Query> list = new ArrayList<>();

    for (ENTITY entity : entities) {
      list.add(_queriesTransformer.addEntityQuery(entity));
    }

    final Neo4jQueryResult result = _queryExecutor.execute(list);
    log.trace("Added {} entities over {} retries, which took {} millis", entities.size(), result.getTookMs(),
        result.getRetries());
    _metricListener.onEntitiesAdded(entities.size(), result.getTookMs(), result.getRetries());
  }

  @Override
  public <URN extends Urn> void removeEntities(@Nonnull List<URN> urns) {
    final List<Query> list = new ArrayList<>();
    for (URN urn : urns) {
      list.add(_queriesTransformer.removeEntityQuery(urn));
    }

    final Neo4jQueryResult result = _queryExecutor.execute(list);
    log.trace("Removed {} entities over {} retries, which took {} millis", urns.size(), result.getTookMs(),
        result.getRetries());
    _metricListener.onEntitiesRemoved(urns.size(), result.getTookMs(), result.getRetries());
  }

  @Override
  public <RELATIONSHIP extends RecordTemplate> void addRelationships(@Nonnull List<RELATIONSHIP> relationships,
      @Nonnull RemovalOption removalOption) {
    if (relationships.isEmpty()) {
      return;
    }

    final List<Query> list = new ArrayList<>();

    _queriesTransformer.relationshipRemovalOptionQuery(relationships.get(0), removalOption).ifPresent(list::add);

    checkSameUrn(relationships, removalOption);

    for (RELATIONSHIP relationship : relationships) {
      list.add(_queriesTransformer.addRelationshipQuery(relationship));
    }

    final Neo4jQueryResult result = _queryExecutor.execute(list);
    log.trace("Added {} relationships over {} retries, which took {} millis", relationships.size(), result.getTookMs(),
        result.getRetries());
    _metricListener.onRelationshipsAdded(relationships.size(), result.getTookMs(), result.getRetries());
  }

  @Override
  public <RELATIONSHIP extends RecordTemplate> void removeRelationships(@Nonnull List<RELATIONSHIP> relationships) {
    if (relationships.isEmpty()) {
      return;
    }

    final List<Query> list = new ArrayList<>();
    for (RELATIONSHIP relationship : relationships) {
      list.add(_queriesTransformer.removeEdge(relationship));
    }

    final Neo4jQueryResult result = _queryExecutor.execute(list);
    log.trace("Removed {} relationships over {} retries, which took {} millis", relationships.size(),
        result.getTookMs(), result.getRetries());
    _metricListener.onRelationshipsRemoved(relationships.size(), result.getTookMs(), result.getRetries());
  }

  private void checkSameUrn(@Nonnull List<? extends RecordTemplate> relationships,
      @Nonnull RemovalOption removalOption) {
    final Urn source0Urn = getSourceUrnFromRelationship(relationships.get(0));
    final Urn destination0Urn = getDestinationUrnFromRelationship(relationships.get(0));

    if (removalOption == RemovalOption.REMOVE_ALL_EDGES_FROM_SOURCE) {
      checkSameUrn(relationships, SOURCE_FIELD, source0Urn);
    } else if (removalOption == RemovalOption.REMOVE_ALL_EDGES_TO_DESTINATION) {
      checkSameUrn(relationships, DESTINATION_FIELD, destination0Urn);
    } else if (removalOption == RemovalOption.REMOVE_ALL_EDGES_FROM_SOURCE_TO_DESTINATION) {
      checkSameUrn(relationships, SOURCE_FIELD, source0Urn);
      checkSameUrn(relationships, DESTINATION_FIELD, destination0Urn);
    }
  }

  private void checkSameUrn(@Nonnull List<? extends RecordTemplate> records, @Nonnull String field,
      @Nonnull Urn compare) {
    for (RecordTemplate relation : records) {
      if (!compare.equals(getRecordTemplateField(relation, field, Urn.class))) {
        throw new IllegalArgumentException("Records have different " + field + " urn");
      }
    }
  }
}
