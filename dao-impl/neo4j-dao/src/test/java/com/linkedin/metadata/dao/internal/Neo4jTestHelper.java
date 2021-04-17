package com.linkedin.metadata.dao.internal;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Query;
import org.neo4j.driver.Session;

import static com.linkedin.metadata.dao.Neo4jUtil.*;
import static com.linkedin.metadata.dao.utils.ModelUtils.*;


/**
 * Helper for making queries in unit tests.
 */
public final class Neo4jTestHelper {
  private final Driver _driver;
  private final Neo4jQueriesTransformer _neo4jQueriesTransformer;

  private Neo4jTestHelper(@Nonnull Driver driver, @Nonnull Neo4jQueriesTransformer neo4jQueriesTransformer) {
    _driver = driver;
    _neo4jQueriesTransformer = neo4jQueriesTransformer;
  }

  public Neo4jTestHelper(@Nonnull Driver driver) {
    this(driver, new Neo4jQueriesTransformer());
  }

  public Neo4jTestHelper(@Nonnull Driver driver, @Nonnull Set<Class<? extends RecordTemplate>> entitiesSet) {
    this(driver, new Neo4jQueriesTransformer(entitiesSet));
  }

  private List<Map<String, Object>> execute(@Nonnull Query query) {
    try (Session session = _driver.session()) {
      return session.run(query)
          .list()
          .stream()
          .map(record -> record.values().get(0).asMap())
          .collect(Collectors.toList());
    }
  }

  @Nonnull
  public Optional<Map<String, Object>> getNode(@Nonnull Urn urn) {
    List<Map<String, Object>> nodes = getAllNodes(urn);
    if (nodes.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(nodes.get(0));
  }

  @Nonnull
  public List<Map<String, Object>> getAllNodes(@Nonnull Urn urn) {
    final String matchTemplate = "MATCH (node%s {urn: $urn}) RETURN node";

    final String sourceType = _neo4jQueriesTransformer.getNodeType(urn);
    final String statement = String.format(matchTemplate, sourceType);

    final Map<String, Object> params = new HashMap<>();
    params.put("urn", urn.toString());

    return execute(new Query(statement, params));
  }

  @Nonnull
  public List<Map<String, Object>> getEdges(@Nonnull RecordTemplate relationship) {
    final Urn sourceUrn = getSourceUrnFromRelationship(relationship);
    final Urn destinationUrn = getDestinationUrnFromRelationship(relationship);
    final String relationshipType = getType(relationship);

    final String sourceType = _neo4jQueriesTransformer.getNodeType(sourceUrn);
    final String destinationType = _neo4jQueriesTransformer.getNodeType(destinationUrn);

    final String matchTemplate =
        "MATCH (source%s {urn: $sourceUrn})-[r:%s]->(destination%s {urn: $destinationUrn}) RETURN r";
    final String statement = String.format(matchTemplate, sourceType, relationshipType, destinationType);

    final Map<String, Object> params = new HashMap<>();
    params.put("sourceUrn", sourceUrn.toString());
    params.put("destinationUrn", destinationUrn.toString());

    return execute(new Query(statement, params));
  }

  @Nonnull
  public List<Map<String, Object>> getEdgesFromSource(@Nonnull Urn sourceUrn,
      @Nonnull Class<? extends RecordTemplate> relationshipClass) {
    final String relationshipType = getType(relationshipClass);
    final String sourceType = _neo4jQueriesTransformer.getNodeType(sourceUrn);

    final String matchTemplate = "MATCH (source%s {urn: $sourceUrn})-[r:%s]->() RETURN r";
    final String statement = String.format(matchTemplate, sourceType, relationshipType);

    final Map<String, Object> params = new HashMap<>();
    params.put("sourceUrn", sourceUrn.toString());

    return execute(new Query(statement, params));
  }
}
