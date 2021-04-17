package com.linkedin.metadata.dao.internal;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.validator.EntityValidator;
import com.linkedin.metadata.validator.RelationshipValidator;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import org.neo4j.driver.Query;

import static com.linkedin.metadata.dao.Neo4jUtil.*;
import static com.linkedin.metadata.dao.utils.ModelUtils.*;


/**
 * Can transform GMA entities and relationships into Neo4j queries for upserting.
 *
 * <p>This separates out transformation logic from query execution logic ({@link Neo4jGraphWriterDAO}).
 */
public final class Neo4jQueriesTransformer {
  private static final Map<String, String> DEFAULT_URN_TO_ENTITY_MAP = buildUrnToEntityMap(getAllEntities());
  private final Map<String, String> _urnToEntityMap;

  public Neo4jQueriesTransformer() {
    this(DEFAULT_URN_TO_ENTITY_MAP);
  }

  /**
   * For use in unit testing.
   */
  public Neo4jQueriesTransformer(@Nonnull Set<Class<? extends RecordTemplate>> entitiesSet) {
    this(buildUrnToEntityMap(entitiesSet));
  }

  private Neo4jQueriesTransformer(@Nonnull Map<String, String> urnToEntityMap) {
    _urnToEntityMap = urnToEntityMap;
  }

  @Nonnull
  private static Map<String, String> buildUrnToEntityMap(@Nonnull Set<Class<? extends RecordTemplate>> entitiesSet) {
    Map<String, String> map = new HashMap<>();
    for (Class<? extends RecordTemplate> entity : entitiesSet) {
      if (map.put(getEntityTypeFromUrnClass(urnClassForEntity(entity)), getType(entity)) != null) {
        throw new IllegalStateException("Duplicate key");
      }
    }
    return map;
  }

  @Nonnull
  private Object toPropertyValue(@Nonnull Object obj) {
    if (obj instanceof Urn) {
      return obj.toString();
    }
    return obj;
  }

  // visible for testing
  @Nonnull
  String getNodeType(@Nonnull Urn urn) {
    return ":" + _urnToEntityMap.getOrDefault(urn.getEntityType(), "UNKNOWN");
  }

  @Nonnull
  private Query buildQuery(@Nonnull String queryTemplate, @Nonnull Map<String, Object> params) {
    for (Map.Entry<String, Object> entry : params.entrySet()) {
      String k = entry.getKey();
      Object v = entry.getValue();
      params.put(k, toPropertyValue(v));
    }
    return new Query(queryTemplate, params);
  }

  @Nonnull
  public Query addEntityQuery(@Nonnull RecordTemplate entity) {
    EntityValidator.validateEntitySchema(entity.getClass());

    final Urn urn = getUrnFromEntity(entity);
    final String nodeType = getNodeType(urn);

    // Use += to ensure this doesn't override the node but merges in the new properties to allow for partial updates.
    final String mergeTemplate = "MERGE (node%s {urn: $urn}) SET node += $properties RETURN node";
    final String statement = String.format(mergeTemplate, nodeType);

    final Map<String, Object> params = new HashMap<>();
    params.put("urn", urn.toString());
    final Map<String, Object> props = entityToNode(entity);
    props.remove("urn"); // no need to set twice (this is implied by MERGE), and they can be quite long.
    params.put("properties", props);

    return buildQuery(statement, params);
  }

  @Nonnull
  public Query removeEntityQuery(@Nonnull Urn urn) {
    // also delete any relationship going to or from it
    final String nodeType = getNodeType(urn);

    final String matchTemplate = "MATCH (node%s {urn: $urn}) DETACH DELETE node";
    final String statement = String.format(matchTemplate, nodeType);

    final Map<String, Object> params = new HashMap<>();
    params.put("urn", urn.toString());

    return buildQuery(statement, params);
  }

  @Nonnull
  public Optional<Query> relationshipRemovalOptionQuery(@Nonnull RecordTemplate relationship,
      BaseGraphWriterDAO.RemovalOption removalOption) {
    // remove existing edges according to RemovalOption
    final Urn source0Urn = getSourceUrnFromRelationship(relationship);
    final Urn destination0Urn = getDestinationUrnFromRelationship(relationship);
    final String relationType = getType(relationship);

    final String sourceType = getNodeType(source0Urn);
    final String destinationType = getNodeType(destination0Urn);

    final Map<String, Object> params = new HashMap<>();

    if (removalOption == BaseGraphWriterDAO.RemovalOption.REMOVE_ALL_EDGES_FROM_SOURCE) {
      final String removeTemplate = "MATCH (source%s {urn: $urn})-[relation:%s]->() DELETE relation";
      final String statement = String.format(removeTemplate, sourceType, relationType);

      params.put("urn", source0Urn.toString());

      return Optional.of(buildQuery(statement, params));
    } else if (removalOption == BaseGraphWriterDAO.RemovalOption.REMOVE_ALL_EDGES_TO_DESTINATION) {
      final String removeTemplate = "MATCH ()-[relation:%s]->(destination%s {urn: $urn}) DELETE relation";
      final String statement = String.format(removeTemplate, relationType, destinationType);

      params.put("urn", destination0Urn.toString());

      return Optional.of(buildQuery(statement, params));
    } else if (removalOption == BaseGraphWriterDAO.RemovalOption.REMOVE_ALL_EDGES_FROM_SOURCE_TO_DESTINATION) {
      final String removeTemplate =
          "MATCH (source%s {urn: $sourceUrn})-[relation:%s]->(destination%s {urn: $destinationUrn}) DELETE relation";
      final String statement = String.format(removeTemplate, sourceType, relationType, destinationType);

      params.put("sourceUrn", source0Urn.toString());
      params.put("destinationUrn", destination0Urn.toString());

      return Optional.of(buildQuery(statement, params));
    }

    return Optional.empty();
  }

  @Nonnull
  public Query addRelationshipQuery(@Nonnull RecordTemplate relationship) {
    RelationshipValidator.validateRelationshipSchema(relationship.getClass());
    final Urn srcUrn = getSourceUrnFromRelationship(relationship);
    final Urn destUrn = getDestinationUrnFromRelationship(relationship);
    final String sourceNodeType = getNodeType(srcUrn);
    final String destinationNodeType = getNodeType(destUrn);

    // Add/Update relationship. Use MERGE on nodes to prevent needing to have separate queries to create them.
    final String mergeRelationshipTemplate =
        "MERGE (source%s {urn: $sourceUrn}) " + "MERGE (destination%s {urn: $destinationUrn}) "
            + "MERGE (source)-[r:%s]->(destination) SET r += $properties";
    final String statement =
        String.format(mergeRelationshipTemplate, sourceNodeType, destinationNodeType, getType(relationship));

    final Map<String, Object> paramsMerge = new HashMap<>();
    paramsMerge.put("sourceUrn", srcUrn.toString());
    paramsMerge.put("destinationUrn", destUrn.toString());
    paramsMerge.put("properties", relationshipToEdge(relationship));

    return new Query(statement, paramsMerge);
  }

  @Nonnull
  public Query removeEdge(@Nonnull RecordTemplate relationship) {
    final Urn sourceUrn = getSourceUrnFromRelationship(relationship);
    final Urn destinationUrn = getDestinationUrnFromRelationship(relationship);

    final String sourceType = getNodeType(sourceUrn);
    final String destinationType = getNodeType(destinationUrn);

    final String removeMatchTemplate =
        "MATCH (source%s {urn: $sourceUrn})-[relation:%s %s]->(destination%s {urn: $destinationUrn}) DELETE relation";
    final String criteria = relationshipToCriteria(relationship);
    final String statement =
        String.format(removeMatchTemplate, sourceType, getType(relationship), criteria, destinationType);

    final Map<String, Object> params = new HashMap<>();
    params.put("sourceUrn", sourceUrn.toString());
    params.put("destinationUrn", destinationUrn.toString());

    return buildQuery(statement, params);
  }
}
