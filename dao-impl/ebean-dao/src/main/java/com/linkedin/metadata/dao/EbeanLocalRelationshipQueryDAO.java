package com.linkedin.metadata.dao;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.metadata.aspect.AuditedAspect;
import com.linkedin.metadata.dao.utils.ClassUtils;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.dao.utils.SQLSchemaUtils;
import com.linkedin.metadata.dao.utils.SQLStatementUtils;
import com.linkedin.metadata.dao.utils.Statement;
import com.linkedin.metadata.query.Condition;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.RelationshipFilter;
import io.ebean.EbeanServer;
import io.ebean.SqlRow;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.javatuples.Pair;
import org.javatuples.Triplet;


public class EbeanLocalRelationshipQueryDAO extends BaseQueryDAO {
  private final EbeanServer _server;
  private static final int DEFAULT_COUNT = 10;
  private static final int DEFAULT_OFFSET = 0;

  public EbeanLocalRelationshipQueryDAO(EbeanServer server) {
    _server = server;
  }

  static final Map<Condition, String> SUPPORTED_CONDITIONS =
      Collections.unmodifiableMap(new HashMap<Condition, String>() {
        {
          put(Condition.EQUAL, "=");
          put(Condition.GREATER_THAN, ">");
          put(Condition.GREATER_THAN_OR_EQUAL_TO, ">=");
          put(Condition.IN, "IN");
          put(Condition.LESS_THAN, "<");
          put(Condition.LESS_THAN_OR_EQUAL_TO, "<=");
        }
      });

  /**
   * Finds a list of entities of a specific type based on the given filter on the entity.
   * The SNAPSHOT class must be defined within com.linkedin.metadata.snapshot package in metadata-models.
   * @param snapshotClass the snapshot class to query.
   * @param filter the filter to apply when querying.
   * @param offset the offset query should start at. Ignored if set to a negative value.
   * @param count the maximum number of entities to return. Ignored if set to a non-positive value.
   * @return A list of entity records of class SNAPSHOT.
   */
  @Nonnull
  @Override
  public <SNAPSHOT extends RecordTemplate> List<SNAPSHOT> findEntities(@Nonnull Class<SNAPSHOT> snapshotClass,
      @Nonnull Filter filter, int offset, int count) {
    count = count <= 0 ? DEFAULT_COUNT : count;
    offset = offset < 0 ? DEFAULT_OFFSET : offset;
    validateFilterForEntity(filter, snapshotClass);

    // Build SQL
    final String tableName = SQLSchemaUtils.getTableName(ModelUtils.getUrnTypeFromSnapshot(snapshotClass));
    final StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder.append("SELECT * FROM ").append(tableName);
    if (filter.hasCriteria() && filter.getCriteria().size() > 0) {
      sqlBuilder.append(" WHERE ").append(SQLStatementUtils.whereClause(filter, SUPPORTED_CONDITIONS, null));
    }
    sqlBuilder.append(" ORDER BY source LIMIT ").append(count).append(" OFFSET ").append(offset);

    // Execute SQL
    return _server.createSqlQuery(sqlBuilder.toString()).findList().stream()
        .map(sqlRow -> constructSnapshot(sqlRow, snapshotClass))
        .collect(Collectors.toList());
  }

  @Nonnull
  @Override
  public <SRC_ENTITY extends RecordTemplate, DEST_ENTITY extends RecordTemplate, RELATIONSHIP extends RecordTemplate> List<RecordTemplate> findEntities(
      @Nullable Class<SRC_ENTITY> sourceEntityClass, @Nonnull Filter sourceEntityFilter,
      @Nullable Class<DEST_ENTITY> destinationEntityClass, @Nonnull Filter destinationEntityFilter,
      @Nonnull Class<RELATIONSHIP> relationshipType, @Nonnull RelationshipFilter relationshipFilter, int minHops,
      int maxHops, int offset, int count) {
    return null;
  }

  @Nonnull
  @Override
  public <SRC_ENTITY extends RecordTemplate, RELATIONSHIP extends RecordTemplate, INTER_ENTITY extends RecordTemplate> List<RecordTemplate> findEntities(
      @Nullable Class<SRC_ENTITY> sourceEntityClass, @Nonnull Filter sourceEntityFilter,
      @Nonnull List<Triplet<Class<RELATIONSHIP>, RelationshipFilter, Class<INTER_ENTITY>>> traversePaths, int offset,
      int count) {
    return null;
  }

  /**
   * Finds a list of relationships of a specific type based on the given filters.
   * The SRC_SNAPSHOT and DEST_SNAPSHOT class must be defined within com.linkedin.metadata.snapshot package in metadata-models.
   *
   * @param sourceEntityClass the source entity class to query
   * @param sourceEntityFilter the filter to apply to the source entity when querying
   * @param destinationEntityClass the destination entity class
   * @param destinationEntityFilter the filter to apply to the destination entity when querying
   * @param relationshipType the type of relationship to query
   * @param relationshipFilter the filter to apply to relationship when querying
   * @param offset the offset query should start at. Ignored if set to a negative value.
   * @param count the maximum number of entities to return. Ignored if set to a non-positive value.
   * @return A list of relationship records.
   */
  @Nonnull
  @Override
  public <SRC_SNAPSHOT extends RecordTemplate, DEST_SNAPSHOT extends RecordTemplate, RELATIONSHIP extends RecordTemplate> List<RELATIONSHIP> findRelationships(
      @Nullable Class<SRC_SNAPSHOT> sourceEntityClass, @Nonnull Filter sourceEntityFilter,
      @Nullable Class<DEST_SNAPSHOT> destinationEntityClass, @Nonnull Filter destinationEntityFilter,
      @Nonnull Class<RELATIONSHIP> relationshipType, @Nonnull Filter relationshipFilter, int offset, int count) {
    validateFilterForEntity(sourceEntityFilter, sourceEntityClass);
    validateFilterForEntity(destinationEntityFilter, destinationEntityClass);
    validateFilterForEntity(relationshipFilter, relationshipType);

    final String destTableName = destinationEntityClass != null ? SQLSchemaUtils.getTableName(ModelUtils.getUrnTypeFromSnapshot(destinationEntityClass)) : null;
    final String sourceTableName = sourceEntityClass != null ? SQLSchemaUtils.getTableName(ModelUtils.getUrnTypeFromSnapshot(sourceEntityClass)) : null;
    final String relationshipTableName = SQLSchemaUtils.getRelationshipTableName(relationshipType);

    final String sql = buildFindRelationshipSQL(
        destTableName,
        sourceTableName,
        relationshipTableName,
        sourceEntityFilter,
        destinationEntityFilter,
        relationshipFilter);

    return _server.createSqlQuery(sql).findList().stream()
        .map(row -> RecordUtils.toRecordTemplate(relationshipType, row.getString("metadata")))
        .collect(Collectors.toList());
  }

  @Nonnull
  @Override
  public <ENTITY extends RecordTemplate> List<ENTITY> findEntities(@Nonnull Class<ENTITY> entityClass,
      @Nonnull Statement queryStatement) {
    throw new UnsupportedOperationException("Local relationship does not support query statement.");
  }

  @Nonnull
  @Override
  public List<RecordTemplate> findMixedTypesEntities(@Nonnull Statement queryStatement) {
    throw new UnsupportedOperationException("Local relationship does not support query statement.");
  }

  @Nonnull
  @Override
  public <RELATIONSHIP extends RecordTemplate> List<RELATIONSHIP> findRelationships(
      @Nonnull Class<RELATIONSHIP> relationshipClass, @Nonnull Statement queryStatement) {
    throw new UnsupportedOperationException("Local relationship does not support query statement.");
  }

  @Nonnull
  @Override
  public List<RecordTemplate> findMixedTypesRelationships(@Nonnull Statement queryStatement) {
    throw new UnsupportedOperationException("Local relationship does not support query statement.");
  }

  /**
   * Validate:
   * 1. The entity filter only contains supported condition.
   * 2. if entity class is null, then filter should be emtpy.
   * If any of above is violated, throw IllegalArgumentException.
   */
  private <ENTITY extends RecordTemplate> void validateFilterForEntity(@Nonnull Filter filter, @Nullable Class<ENTITY> entityClass) {
    if (entityClass == null) {
      if (filter.hasCriteria() && filter.getCriteria().size() > 0) {
        throw new IllegalArgumentException("snapshotClass is null but filter is not empty");
      }
    }

    filter.getCriteria().forEach(criterion -> {
      if (!SUPPORTED_CONDITIONS.containsKey(criterion.getCondition())) {
        throw new IllegalArgumentException(String.format("Condition %s is not supported by local relationship DAO",
            criterion.getCondition()));
      }
    });
  }

  /**
   * Construct a SNAPSHOT from a SqlRow.
   *
   * @param sqlRow one row from entity table
   * @param snapshotClass The snapshot class for the entity.
   * @return A snapshot instance containing all aspects extracted from SqlRow
   */
  @Nonnull
  private <SNAPSHOT extends RecordTemplate> SNAPSHOT constructSnapshot(@Nonnull final SqlRow sqlRow, @Nonnull final Class<SNAPSHOT> snapshotClass) {
    final Class<UnionTemplate> unionTemplateClass = ModelUtils.getUnionClassFromSnapshot(snapshotClass);
    final List<UnionTemplate> aspects = new ArrayList<>();

    for (String aspectCanonicalName : ModelUtils.getAspectClassNames(unionTemplateClass)) {
      String colName = SQLSchemaUtils.getColumnNameFromAnnotation(aspectCanonicalName);
      String auditedAspectStr = sqlRow.getString(colName);

      if (auditedAspectStr != null) {
        AuditedAspect auditedAspect = RecordUtils.toRecordTemplate(AuditedAspect.class, auditedAspectStr);
        RecordTemplate aspect = RecordUtils.toRecordTemplate(ClassUtils.loadClass(aspectCanonicalName), auditedAspect.getAspect());
        aspects.add(ModelUtils.newAspectUnion(ModelUtils.getUnionClassFromSnapshot(snapshotClass), aspect));
      }
    }

    return ModelUtils.newSnapshot(snapshotClass, sqlRow.getString("urn"), aspects);
  }

  /**
   * Construct SQL similar to following:
   *
   * <p>SELECT rt.* FROM relationship_table rt
   * INNER JOIN destination_entity_table dt ON dt.urn = rt.destinationEntityUrn
   * INNER JOIN source_entity_table st ON st.urn = rt.sourceEntityUrn
   * WHERE destination entity filters AND source entity filters AND relationship filters
   */
  @Nonnull
  private String buildFindRelationshipSQL(@Nullable final String destTableName, @Nullable final String sourceTableName,
      @Nonnull final String relationshipTableName, @Nonnull final Filter sourceEntityFilter, @Nonnull final Filter destinationEntityFilter,
      @Nonnull final Filter relationshipFilter) {

    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder.append("SELECT rt.* FROM ").append(relationshipTableName).append(" rt ");

    if (destTableName != null) {
      sqlBuilder.append("INNER JOIN ").append(destTableName).append(" dt ON dt.urn=rt.destination ");
    }

    if (sourceTableName != null) {
      sqlBuilder.append("INNER JOIN ").append(sourceTableName).append(" st ON st.urn=rt.source ");
    }

    String whereClause = SQLStatementUtils.whereClause(SUPPORTED_CONDITIONS,
        new Pair<>(sourceEntityFilter, "st"),
        new Pair<>(destinationEntityFilter, "dt"),
        new Pair<>(relationshipFilter, "rt"));

    if (whereClause != null) {
      sqlBuilder.append("WHERE ").append(whereClause);
    }

    return sqlBuilder.toString();
  }
}
