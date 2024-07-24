package com.linkedin.metadata.dao;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.metadata.dao.utils.ClassUtils;
import com.linkedin.metadata.dao.utils.EBeanDAOUtils;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.dao.utils.MultiHopsTraversalSqlGenerator;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.dao.utils.SQLSchemaUtils;
import com.linkedin.metadata.dao.utils.SQLStatementUtils;
import com.linkedin.metadata.query.Condition;
import com.linkedin.metadata.query.LocalRelationshipCriterion;
import com.linkedin.metadata.query.LocalRelationshipFilter;
import com.linkedin.metadata.query.RelationshipDirection;
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
import lombok.extern.slf4j.Slf4j;
import org.javatuples.Pair;

/**
 * An Ebean implementation of {@link BaseQueryDAO} backed by local relationship tables.
 */
@Slf4j
public class EbeanLocalRelationshipQueryDAO {
  private final EbeanServer _server;
  private final MultiHopsTraversalSqlGenerator _sqlGenerator;

  private final EBeanDAOConfig _eBeanDAOConfig;

  private EbeanLocalDAO.SchemaConfig _schemaConfig = EbeanLocalDAO.SchemaConfig.NEW_SCHEMA_ONLY;

  private static final int MAX_ALLOWED_FILTERS_OLD_SCHEMA = 1;

  public EbeanLocalRelationshipQueryDAO(EbeanServer server, EBeanDAOConfig eBeanDAOConfig) {
    _server = server;
    _eBeanDAOConfig = eBeanDAOConfig;
    _sqlGenerator = new MultiHopsTraversalSqlGenerator(SUPPORTED_CONDITIONS);
  }

  public EbeanLocalRelationshipQueryDAO(EbeanServer server) {
    _server = server;
    _eBeanDAOConfig = new EBeanDAOConfig();
    _sqlGenerator = new MultiHopsTraversalSqlGenerator(SUPPORTED_CONDITIONS);
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

  public void setSchemaConfig(EbeanLocalDAO.SchemaConfig schemaConfig) {
    _schemaConfig = schemaConfig;
  }

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
  public <SNAPSHOT extends RecordTemplate> List<SNAPSHOT> findEntities(@Nonnull Class<SNAPSHOT> snapshotClass,
      @Nonnull LocalRelationshipFilter filter, int offset, int count) {
    if (_schemaConfig == EbeanLocalDAO.SchemaConfig.OLD_SCHEMA_ONLY) {
      throw new UnsupportedOperationException("findEntities is not supported in OLD_SCHEMA_MODE");
    }
    validateEntityFilter(filter, snapshotClass);

    // Build SQL
    final String tableName = SQLSchemaUtils.getTableName(ModelUtils.getUrnTypeFromSnapshot(snapshotClass));
    final StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder.append("SELECT * FROM ").append(tableName);
    if (filter.hasCriteria() && filter.getCriteria().size() > 0) {
      sqlBuilder.append(" WHERE ").append(SQLStatementUtils.whereClause(filter, SUPPORTED_CONDITIONS, null,
          _eBeanDAOConfig.isNonDollarVirtualColumnsEnabled()));
    }
    sqlBuilder.append(" ORDER BY urn LIMIT ").append(Math.max(1, count)).append(" OFFSET ").append(Math.max(0, offset));

    // Execute SQL
    return _server.createSqlQuery(sqlBuilder.toString()).findList().stream()
        .map(sqlRow -> constructSnapshot(sqlRow, snapshotClass))
        .collect(Collectors.toList());
  }

  @Nonnull
  public <SRC_SNAPSHOT extends RecordTemplate, DEST_SNAPSHOT extends RecordTemplate, RELATIONSHIP extends RecordTemplate> List<RecordTemplate> findEntities(
      @Nonnull Class<SRC_SNAPSHOT> sourceEntityClass, @Nonnull LocalRelationshipFilter sourceEntityFilter,
      @Nonnull Class<DEST_SNAPSHOT> destinationEntityClass, @Nonnull LocalRelationshipFilter destinationEntityFilter,
      @Nonnull Class<RELATIONSHIP> relationshipType, @Nonnull LocalRelationshipFilter relationshipFilter, int minHops,
      int maxHops, int offset, int count) {
    if (_schemaConfig == EbeanLocalDAO.SchemaConfig.OLD_SCHEMA_ONLY) {
      throw new UnsupportedOperationException("findEntities is not supported in OLD_SCHEMA_MODE");
    }

    validateRelationshipFilter(relationshipFilter);
    validateEntityFilter(sourceEntityFilter, sourceEntityClass);
    validateEntityFilter(destinationEntityFilter, destinationEntityClass);

    final String relationshipTable = SQLSchemaUtils.getRelationshipTableName(relationshipType);
    final String srcEntityTable = SQLSchemaUtils.getTableName(ModelUtils.getUrnTypeFromSnapshot(sourceEntityClass));
    final String destEntityTable = SQLSchemaUtils.getTableName(ModelUtils.getUrnTypeFromSnapshot(destinationEntityClass));
    final String sql = _sqlGenerator.multiHopTraversalSql(minHops, maxHops, Math.max(1, count), Math.max(0, offset), relationshipTable,
        srcEntityTable, destEntityTable, relationshipFilter, sourceEntityFilter, destinationEntityFilter,
        _eBeanDAOConfig.isNonDollarVirtualColumnsEnabled());

    final Class snapshotClass = relationshipFilter.getDirection() == RelationshipDirection.INCOMING ? sourceEntityClass : destinationEntityClass;

    // Execute SQL
    List<RecordTemplate> results = new ArrayList<>();
    _server.createSqlQuery(sql).findList().forEach(sqlRow -> results.add(constructSnapshot(sqlRow, snapshotClass)));
    return results;
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
  public <SRC_SNAPSHOT extends RecordTemplate, DEST_SNAPSHOT extends RecordTemplate, RELATIONSHIP extends RecordTemplate> List<RELATIONSHIP> findRelationships(
      @Nullable Class<SRC_SNAPSHOT> sourceEntityClass, @Nonnull LocalRelationshipFilter sourceEntityFilter,
      @Nullable Class<DEST_SNAPSHOT> destinationEntityClass, @Nonnull LocalRelationshipFilter destinationEntityFilter,
      @Nonnull Class<RELATIONSHIP> relationshipType, @Nonnull LocalRelationshipFilter relationshipFilter, int offset, int count) {
    validateEntityFilter(sourceEntityFilter, sourceEntityClass);
    validateEntityFilter(destinationEntityFilter, destinationEntityClass);
    validateRelationshipFilter(relationshipFilter);

    String destTableName = null;
    if (destinationEntityClass != null) {
      destTableName = SQLSchemaUtils.getTableName(ModelUtils.getUrnTypeFromSnapshot(destinationEntityClass));
    }

    String sourceTableName = null;
    if (sourceEntityClass != null) {
      sourceTableName = SQLSchemaUtils.getTableName(ModelUtils.getUrnTypeFromSnapshot(sourceEntityClass));
    }

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

  /**
   * Finds a list of relationships of a specific type based on the given filters if applicable.
   *
   * @param sourceEntityUrn urn of the source entity to query
   * @param sourceEntityFilter the filter to apply to the source entity when querying (not applicable to non-MG entities)
   * @param destinationEntityUrn urn of the destination entity to query. If relationship is OwnedBy, this is crew/ldap.
   * @param destinationEntityFilter the filter to apply to the destination entity when querying (not applicable to non-MG entities)
   * @param relationshipType the type of relationship to query
   * @param relationshipFilter the filter to apply to relationship when querying
   * @param offset the offset query should start at. Ignored if set to a negative value.
   * @param count the maximum number of entities to return. Ignored if set to a non-positive value.   * @return A list of relationship records.
   */
  @Nonnull
  public <RELATIONSHIP extends RecordTemplate> List<RELATIONSHIP> findRelationships(
      @Nullable String sourceEntityUrn, @Nullable LocalRelationshipFilter sourceEntityFilter,
      @Nullable String destinationEntityUrn, @Nullable LocalRelationshipFilter destinationEntityFilter,
      @Nonnull Class<RELATIONSHIP> relationshipType, @Nonnull LocalRelationshipFilter relationshipFilter,
      int offset, int count) {
    // NOTE: additional validation for sourceEntityUrn and sourceEntityUrn first.
    // for non-MG entities, filters need to be null or ignored.
    throw new RuntimeException("findRelationships is not implemented.");
  }

  /**
   * Validate:
   * 1. The entity filter only contains supported condition.
   * 2. if entity class is null, then filter should be emtpy.
   * If any of above is violated, throw IllegalArgumentException.
   */
  private <ENTITY extends RecordTemplate> void validateEntityFilter(@Nonnull LocalRelationshipFilter filter, @Nullable Class<ENTITY> entityClass) {
    if (entityClass == null && filter.hasCriteria() && filter.getCriteria().size() > 0) {
      throw new IllegalArgumentException("Entity class is null but filter is not empty.");
    }

    validateFilterCriteria(filter);
  }

  /**
   * Validate:
   * 1. The relationship filter only contains supported condition.
   * 2. Relationship direction cannot be unknown.
   * If any of above is violated, throw IllegalArgumentException.
   */
  private void validateRelationshipFilter(@Nonnull LocalRelationshipFilter filter) {
    if (filter.getDirection() == null || filter.getDirection() == RelationshipDirection.$UNKNOWN) {
      throw new IllegalArgumentException("Relationship direction cannot be null or UNKNOWN.");
    }

    if (filter.hasCriteria()) {
      validateFilterCriteria(filter);
    }
  }

  /**
   * Validate whether filter criteria contains unsupported condition.
   * @param filter the local relationship filter.
   */
  private void validateFilterCriteria(@Nonnull LocalRelationshipFilter filter) {
    filter.getCriteria().stream().map(criterion -> {
      Condition condition = criterion.getCondition();
      if (!SUPPORTED_CONDITIONS.containsKey(condition)) {
        throw new IllegalArgumentException(String.format("Condition %s is not supported by local relationship DAO.", condition));
      }
      return null; // unused
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
      String colName = SQLSchemaUtils.getAspectColumnName(aspectCanonicalName);
      String auditedAspectStr = sqlRow.getString(colName);

      if (auditedAspectStr != null) {
        RecordTemplate aspect = RecordUtils.toRecordTemplate(ClassUtils.loadClass(aspectCanonicalName),
            EBeanDAOUtils.extractAspectJsonString(auditedAspectStr));
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
      @Nonnull final String relationshipTableName, @Nonnull final LocalRelationshipFilter sourceEntityFilter,
      @Nonnull final LocalRelationshipFilter destinationEntityFilter, @Nonnull final LocalRelationshipFilter relationshipFilter) {
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder.append("SELECT rt.* FROM ").append(relationshipTableName).append(" rt ");

    if (_schemaConfig == EbeanLocalDAO.SchemaConfig.NEW_SCHEMA_ONLY || _schemaConfig == EbeanLocalDAO.SchemaConfig.DUAL_SCHEMA) {
      if (destTableName != null) {
        sqlBuilder.append("INNER JOIN ").append(destTableName).append(" dt ON dt.urn=rt.destination ");
      }

      if (sourceTableName != null) {
        sqlBuilder.append("INNER JOIN ").append(sourceTableName).append(" st ON st.urn=rt.source ");
      }

      sqlBuilder.append("WHERE deleted_ts is NULL");
      String whereClause = SQLStatementUtils.whereClause(SUPPORTED_CONDITIONS,
          _eBeanDAOConfig.isNonDollarVirtualColumnsEnabled(),
          new Pair<>(sourceEntityFilter, "st"),
          new Pair<>(destinationEntityFilter, "dt"),
          new Pair<>(relationshipFilter, "rt"));

      if (whereClause != null) {
        sqlBuilder.append(" AND ").append(whereClause);
      }
    } else if (_schemaConfig == EbeanLocalDAO.SchemaConfig.OLD_SCHEMA_ONLY) {
      validateEntityFilterOldSchema(sourceEntityFilter);
      validateEntityFilterOldSchema(destinationEntityFilter);
      sqlBuilder.append("WHERE deleted_ts IS NULL");
      if (sourceEntityFilter.hasCriteria() && sourceEntityFilter.getCriteria().size() > 0) {
        sqlBuilder.append(
            SQLStatementUtils.whereClauseOldSchema(SUPPORTED_CONDITIONS, sourceEntityFilter.getCriteria(), SQLStatementUtils.SOURCE));
      }
      if (destinationEntityFilter.hasCriteria() && destinationEntityFilter.getCriteria().size() > 0) {
        sqlBuilder.append(
            SQLStatementUtils.whereClauseOldSchema(SUPPORTED_CONDITIONS, destinationEntityFilter.getCriteria(), SQLStatementUtils.DESTINATION));
      }
    } else {
      throw new RuntimeException("The schema config must be set to OLD_SCHEMA_ONLY, DUAL_SCHEMA, or NEW_SCHEMA_ONLY.");
    }
    return sqlBuilder.toString();
  }

  /**
   * Ensure that the source and destination entity filters when running in OLD_SCHEMA_ONLY mode follow these restrictions.
   * 1) include no more than 1 criterion
   * 2) that 1 criterion must be on the urn field
   * 3) the passed in condition is supported by this DAO
   */
  private void validateEntityFilterOldSchema(LocalRelationshipFilter filter) {
    if (filter.hasCriteria() && filter.getCriteria().size() > 0) {
      if (filter.getCriteria().size() > MAX_ALLOWED_FILTERS_OLD_SCHEMA) {
        throw new IllegalArgumentException(String.format("Only %s filter is allowed in old schema mode.", MAX_ALLOWED_FILTERS_OLD_SCHEMA));
      }
      LocalRelationshipCriterion criterion = filter.getCriteria().get(0);

      if (!criterion.hasField() || !criterion.getField().isUrnField()) {
        throw new IllegalArgumentException("EbeanLocalRelationshipQueryDAO running in OLD_SCHEMA_ONLY mode only supports"
            + " local relationship filters on the 'urn' field");
      }
      Condition condition = filter.getCriteria().get(0).getCondition();
      if (!SUPPORTED_CONDITIONS.containsKey(condition)) {
        throw new IllegalArgumentException(String.format("Condition %s is not supported by local relationship DAO.", condition));
      }
    }
  }
}
