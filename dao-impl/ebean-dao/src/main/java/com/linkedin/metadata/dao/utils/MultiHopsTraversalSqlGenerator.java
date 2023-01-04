package com.linkedin.metadata.dao.utils;

import com.linkedin.metadata.query.Condition;
import com.linkedin.metadata.query.LocalRelationshipFilter;
import com.linkedin.metadata.query.RelationshipDirection;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import org.javatuples.Pair;


/**
 * Multi-hops traversal on graph backed by local relationship tables.
 */
public class MultiHopsTraversalSqlGenerator {
  private static Map<Condition, String> _supportedConditions;

  public MultiHopsTraversalSqlGenerator(Map<Condition, String> supportedConditions) {
    _supportedConditions = Collections.unmodifiableMap(supportedConditions);
  }

  /**
   * Construct a SQL query which finds entities by traversing the graph based on the given filters.
   */
  @Nonnull
  @ParametersAreNonnullByDefault
  public String multiHopTraversalSql(int minHop, int maxHop, int count, int offset, String relationshipTable,
      String srcEntityTable, String destEntityTable, LocalRelationshipFilter relationshipFilter, LocalRelationshipFilter srcFilter,
      LocalRelationshipFilter destFilter) {

    /*
     * For now, only one-hop traversal is supported because multi-hops traversal using SQL is expensive
     * and implementation is error-prone. Application side can also perform multi-hops traversal with one-hop traversal.
     * We might consider to support multi-hop traversal in the future if strong use case appears.
     */
    if (minHop != 1 || maxHop != 1) {
      throw new UnsupportedOperationException("Only one-hop traversal is supported.");
    }

    if (relationshipFilter.getDirection() == RelationshipDirection.UNDIRECTED && !srcEntityTable.equals(destEntityTable)) {
      throw new IllegalArgumentException("Source and destination entity table must be same if direction is undirected.");
    }

    final String entityTable = relationshipFilter.getDirection() == RelationshipDirection.INCOMING ? srcEntityTable : destEntityTable;

    // If relationship direction is directed.
    if (relationshipFilter.getDirection() == RelationshipDirection.INCOMING
        || relationshipFilter.getDirection() == RelationshipDirection.OUTGOING) {
      String urnSql = firstHopUrnsDirected(relationshipTable, srcEntityTable, destEntityTable, relationshipFilter, srcFilter,
          destFilter, relationshipFilter.getDirection());
      return String.format("SELECT * FROM %s WHERE urn IN (%s) ORDER BY urn LIMIT %d OFFSET %d", entityTable, urnSql, count, offset);
    }

    // Relationship is undirected.
    String urnSql = firstHopUrnsUndirected(relationshipTable, entityTable, relationshipFilter, srcFilter);
    return findEntitiesUndirected(entityTable, relationshipTable, urnSql, destFilter);
  }

  /**
   * Construct a SQL query which finds URNs of entities that are one hop away for directed relationship.
   * Direction must be INCOMING or OUTGOING.
   */
  @Nonnull
  @ParametersAreNonnullByDefault
  private String firstHopUrnsDirected(String relationshipTable, String srcEntityTable, String destEntityTable,
      LocalRelationshipFilter relationshipFilter, LocalRelationshipFilter srcFilter, LocalRelationshipFilter destFilter, RelationshipDirection direction) {

    String urnColumn = "destination";
    if (direction == RelationshipDirection.INCOMING) {
      urnColumn = "source";
    }

    StringBuilder sqlBuilder = new StringBuilder(
        String.format("SELECT rt.%s FROM %s rt INNER JOIN %s dt ON rt.destination=dt.urn INNER JOIN %s st ON rt.source=st.urn",
            urnColumn, relationshipTable, destEntityTable, srcEntityTable));

    String whereClause = SQLStatementUtils.whereClause(_supportedConditions,
        new Pair<>(relationshipFilter, "rt"),
        new Pair<>(destFilter, "dt"),
        new Pair<>(srcFilter, "st"));

    if (whereClause != null) {
      sqlBuilder.append(" WHERE ").append(whereClause);
    }

    return sqlBuilder.toString();
  }

  /**
   * Construct a SQL query which finds URNs of entities that are one hop away for undirected relationship.
   */
  @Nonnull
  @ParametersAreNonnullByDefault
  private String firstHopUrnsUndirected(String relationshipTable, String entityTable, LocalRelationshipFilter relationshipFilter,
      LocalRelationshipFilter srcFilter) {

    StringBuilder sourceUrnsSql = new StringBuilder(
        String.format("SELECT rt.source FROM %s rt INNER JOIN %s et ON rt.source=et.urn", relationshipTable, entityTable));

    StringBuilder destUrnsSql = new StringBuilder(
        String.format("SELECT rt.destination FROM %s rt INNER JOIN %s et ON rt.destination=et.urn", relationshipTable, entityTable));

    String whereClause = SQLStatementUtils.whereClause(_supportedConditions,
        new Pair<>(relationshipFilter, "rt"),
        new Pair<>(srcFilter, "et"));

    if (whereClause != null) {
      sourceUrnsSql.append(" WHERE ").append(whereClause);
      destUrnsSql.append(" WHERE ").append(whereClause);
    }

    return String.format("%s UNION %s", sourceUrnsSql, destUrnsSql);
  }

  /**
   * Construct SQL that finds entities for undirected relationship.
   */
  @Nonnull
  @ParametersAreNonnullByDefault
  private String findEntitiesUndirected(String entityTable, String relationshipTable, String firstHopUrnSql, LocalRelationshipFilter destFilter) {
    String whereClause = SQLStatementUtils.whereClause(_supportedConditions, new Pair<>(destFilter, "et"));

    StringBuilder sourceEntitySql = new StringBuilder(
        String.format("SELECT et.* FROM %s et INNER JOIN %s rt ON et.urn=rt.source WHERE rt.destination IN (%s)",
            entityTable, relationshipTable, firstHopUrnSql));

    StringBuilder destEntitySql = new StringBuilder(
        String.format("SELECT et.* FROM %s et INNER JOIN %s rt ON et.urn=rt.destination WHERE rt.source IN (%s)",
            entityTable, relationshipTable, firstHopUrnSql));

    if (whereClause != null) {
      sourceEntitySql.append(" AND ").append(String.format("(%s)", whereClause));
      destEntitySql.append(" AND ").append(String.format("(%s)", whereClause));
    }

    return String.format("%s UNION %s", sourceEntitySql, destEntitySql);
  }
}
