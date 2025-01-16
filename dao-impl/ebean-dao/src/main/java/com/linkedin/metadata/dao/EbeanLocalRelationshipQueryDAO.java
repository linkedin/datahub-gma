package com.linkedin.metadata.dao;

import com.google.common.annotations.VisibleForTesting;
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
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.naming.OperationNotSupportedException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.javatuples.Pair;


/**
 * An Ebean implementation of {@link BaseQueryDAO} backed by local relationship tables.
 */
@Slf4j
public class EbeanLocalRelationshipQueryDAO {
  private final EbeanServer _server;
  private final MultiHopsTraversalSqlGenerator _sqlGenerator;

  private final EBeanDAOConfig _eBeanDAOConfig;

  private Set<String> _mgEntityTypeNameSet;
  private EbeanLocalDAO.SchemaConfig _schemaConfig = EbeanLocalDAO.SchemaConfig.NEW_SCHEMA_ONLY;

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
   * This method is not supported in OLD_SCHEMA_ONLY mode.
   * @param snapshotClass the snapshot class to query.
   * @param filter the filter to apply when querying.
   * @param offset the offset the query should start at. Ignored if set to a negative value.
   * @param count the maximum number of entities to return. Ignored if set to a non-positive value.
   * @return A list of entity records of class SNAPSHOT.
   * @throws OperationNotSupportedException when called in OLD_SCHEMA_ONLY mode. This exception must be explicitly handled by the caller.
   */
  @Nonnull
  public <SNAPSHOT extends RecordTemplate> List<SNAPSHOT> findEntities(@Nonnull Class<SNAPSHOT> snapshotClass,
      @Nonnull LocalRelationshipFilter filter, int offset, int count) throws OperationNotSupportedException {
    if (_schemaConfig == EbeanLocalDAO.SchemaConfig.OLD_SCHEMA_ONLY) {
      throw new OperationNotSupportedException("findEntities is not supported in OLD_SCHEMA_MODE");
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

  /**
   * Finds a list of entities of a specific type based on the given source, destination, and relationship filters.
   * Every SNAPSHOT class must be defined within com.linkedin.metadata.snapshot package in metadata-models.
   * This method is not supported in OLD_SCHEMA_ONLY mode.
   * @param sourceEntityClass the snapshot class of the source entity to query.
   * @param sourceEntityFilter the filter to apply to the source entity when querying.
   * @param destinationEntityClass the snapshot class of the destination entity to query.
   * @param destinationEntityFilter the filter to apply to the destination entity when querying.
   * @param relationshipType the snapshot class of the relationship to query.
   * @param relationshipFilter the filter to apply to the relationship when querying.
   * @param minHops minimum number of hops to query.
   * @param maxHops maximum number of hops to query.
   * @param offset the offset the query should start at. Ignored if set to a non-positive value.
   * @param count the maximum number of entities to return. Ignored if set to a non-positive value.
   * @return A list of entity records that satisfy the query.
   * @throws OperationNotSupportedException when called in OLD_SCHEMA_ONLY mode. This exception must be explicitly handled by the caller.
   **/
  @Nonnull
  public <SRC_SNAPSHOT extends RecordTemplate, DEST_SNAPSHOT extends RecordTemplate, RELATIONSHIP extends RecordTemplate> List<RecordTemplate> findEntities(
      @Nonnull Class<SRC_SNAPSHOT> sourceEntityClass, @Nonnull LocalRelationshipFilter sourceEntityFilter,
      @Nonnull Class<DEST_SNAPSHOT> destinationEntityClass, @Nonnull LocalRelationshipFilter destinationEntityFilter,
      @Nonnull Class<RELATIONSHIP> relationshipType, @Nonnull LocalRelationshipFilter relationshipFilter, int minHops,
      int maxHops, int offset, int count) throws OperationNotSupportedException {
    if (_schemaConfig == EbeanLocalDAO.SchemaConfig.OLD_SCHEMA_ONLY) {
      throw new OperationNotSupportedException("findEntities is not supported in OLD_SCHEMA_MODE");
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
        relationshipTableName,
        relationshipFilter,
        sourceTableName,
        sourceEntityFilter,
        destTableName,
        destinationEntityFilter,
        count,
        offset);

    return _server.createSqlQuery(sql).findList().stream()
        .map(row -> RecordUtils.toRecordTemplate(relationshipType, row.getString("metadata")))
        .collect(Collectors.toList());
  }

  /**
   * Finds a list of relationships of a specific type (Urn) based on the given filters if applicable.
   *
   * @param sourceEntityType type of source entity to query (e.g. "dataset")
   * @param sourceEntityFilter the filter to apply to the source entity when querying (not applicable to non-MG entities)
   * @param destinationEntityType type of destination entity to query (e.g. "dataset")
   * @param destinationEntityFilter the filter to apply to the destination entity when querying (not applicable to non-MG entities)
   * @param relationshipType the type of relationship to query
   * @param relationshipFilter the filter to apply to relationship when querying
   * @param offset the offset query should start at. Ignored if set to a negative value.
   * @param count the maximum number of entities to return. Ignored if set to a non-positive value.
   * @return A list of relationship records.
   */
  @Nonnull
  public <RELATIONSHIP extends RecordTemplate> List<RELATIONSHIP> findRelationshipsV2(
      @Nullable String sourceEntityType, @Nullable LocalRelationshipFilter sourceEntityFilter,
      @Nullable String destinationEntityType, @Nullable LocalRelationshipFilter destinationEntityFilter,
      @Nonnull Class<RELATIONSHIP> relationshipType, @Nonnull LocalRelationshipFilter relationshipFilter,
      int offset, int count) {
    validateEntityTypeAndFilter(sourceEntityFilter, sourceEntityType);
    validateEntityTypeAndFilter(destinationEntityFilter, destinationEntityType);
    validateRelationshipFilter(relationshipFilter);

    // the assumption is we have the table for every MG entity. For non-MG entities, sourceTableName will be null.
    final String sourceTableName = getMgEntityTableName(sourceEntityType);
    final String destTableName = getMgEntityTableName(destinationEntityType);
    final String relationshipTableName = SQLSchemaUtils.getRelationshipTableName(relationshipType);

    final String sql = buildFindRelationshipSQL(
        relationshipTableName, relationshipFilter,
        sourceTableName, sourceEntityFilter,
        destTableName, destinationEntityFilter,
        count, offset);

    return _server.createSqlQuery(sql).findList().stream()
        .map(row -> RecordUtils.toRecordTemplate(relationshipType, row.getString("metadata")))
        .collect(Collectors.toList());
  }

  /**
   * Checks if a given entity type has an entity table in the db.
   */
  @VisibleForTesting
  protected boolean isMgEntityType(@Nonnull String entityType) {
    if (_schemaConfig == EbeanLocalDAO.SchemaConfig.OLD_SCHEMA_ONLY) {
      // there's no concept of MG entity or non-entity in old schema mode. always return false.
      return false;
    }

    initMgEntityTypeNameSet();

    return _mgEntityTypeNameSet.contains(StringUtils.lowerCase(entityType));
  }


  /**
   * Extracts the table name from an entity urn for MG entities. If entityUrn is not for MG entity, return null.
   * @param entityType String representing the type of entity (e.g. "dataset")
   * @return metadata_entity_entity-type or null
   */
  @Nullable
  private String getMgEntityTableName(@Nullable String entityType) {
    if (entityType == null || !isMgEntityType(entityType)) {
      return null;
    }
    return SQLSchemaUtils.getTableName(entityType);
  }

  /**
   * Validate:
   * 1. The entity filter only contains supported conditions.
   * 2. if the entity class is null, then the filter should be empty.
   * If any of above is violated, throw an IllegalArgumentException.
   */
  private <ENTITY extends RecordTemplate> void validateEntityFilter(@Nonnull LocalRelationshipFilter filter, @Nullable Class<ENTITY> entityClass) {
    if (entityClass == null && filter.hasCriteria() && filter.getCriteria().size() > 0) {
      throw new IllegalArgumentException("Entity class is null but filter is not empty.");
    }

    validateFilterCriteria(filter);
  }

  /**
   * Validate:
   * 1. if the entity type is null or empty, then the filter should also be emtpy.
   * 2. the entity filter only contains supported conditions.
   * If any of above is violated, throw an IllegalArgumentException.
   */
  private void validateEntityTypeAndFilter(@Nullable LocalRelationshipFilter filter, @Nullable String entityType) {
    if ((StringUtils.isBlank(entityType)) && filter != null && filter.hasCriteria() && !filter.getCriteria()
        .isEmpty()) {
      throw new IllegalArgumentException("Entity type string is null or empty but filter is not empty.");
    }

    if (filter != null) {
      validateFilterCriteria(filter);
    }
  }

  /**
   * Ensure that the source and destination entity filters abide by the following requirements:
   * 1) include no more than 1 criterion
   * 2) that 1 criterion must be on the urn field
   * 3) the passed in condition is supported by this DAO
   * This is useful for non-MG entities or when running in OLD_SCHEMA_ONLY mode.
   */
  private void validateEntityFilterOnlyOneUrn(@Nonnull LocalRelationshipFilter filter) {
    if (filter.hasCriteria() && !filter.getCriteria().isEmpty()) {
      if (filter.getCriteria().size() > 1) {
        throw new IllegalArgumentException("Only 1 filter is allowed for non-MG entities or when running in OLD_SCHEMA_ONLY mode.");
      }
      LocalRelationshipCriterion criterion = filter.getCriteria().get(0);

      if (!criterion.hasField() || !criterion.getField().isUrnField()) {
        throw new IllegalArgumentException("Only filters on the urn field are allowed for non-MG entities or when running in OLD_SCHEMA_ONLY mode.");
      }
      Condition condition = filter.getCriteria().get(0).getCondition();
      if (!SUPPORTED_CONDITIONS.containsKey(condition)) {
        throw new IllegalArgumentException(String.format("Condition %s is not supported by local relationship DAO.", condition));
      }
    }
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
      String colName =
          SQLSchemaUtils.getAspectColumnName(ModelUtils.getUrnTypeFromSnapshot(snapshotClass), aspectCanonicalName);
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
   * Constructs SQL similar to following.
   *
   * <p>SELECT rt.* FROM relationship_table rt
   * INNER JOIN destination_entity_table dt ON dt.urn = rt.destinationEntityUrn
   * INNER JOIN source_entity_table st ON st.urn = rt.sourceEntityUrn
   * WHERE destination entity filters AND source entity filters AND relationship filters</p>
   *
   * @param relationshipTableName   relationship table name
   * @param relationshipFilter      filter on relationship
   * @param sourceTableName         source entity table name
   * @param sourceEntityFilter      filter on source entity.
   * @param destTableName           destination entity table name. Always null if building relationship with non-mg
   *                                entity.
   * @param destinationEntityFilter filter on destination entity.
   * @param limit                   max number of records to return. If < 0, will return all records.
   * @param offset                  offset to start from. If < 0, will start from 0.
   */
  @Nonnull
  private String buildFindRelationshipSQL(
      @Nonnull final String relationshipTableName, @Nonnull final LocalRelationshipFilter relationshipFilter,
      @Nullable final String sourceTableName, @Nullable final LocalRelationshipFilter sourceEntityFilter,
      @Nullable final String destTableName, @Nullable final LocalRelationshipFilter destinationEntityFilter,
      int limit, int offset) {

    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder.append("SELECT rt.* FROM ").append(relationshipTableName).append(" rt ");

    List<Pair<LocalRelationshipFilter, String>> filters = new ArrayList<>();

    if (_schemaConfig == EbeanLocalDAO.SchemaConfig.NEW_SCHEMA_ONLY || _schemaConfig == EbeanLocalDAO.SchemaConfig.DUAL_SCHEMA) {
      if (destTableName != null) {
        sqlBuilder.append("INNER JOIN ").append(destTableName).append(" dt ON dt.urn=rt.destination ");

        if (destinationEntityFilter != null) {
          filters.add(new Pair<>(destinationEntityFilter, "dt"));
        }
      } else if (destinationEntityFilter != null) {
        validateEntityFilterOnlyOneUrn(destinationEntityFilter);
        // non-mg entity case, applying dest filter on relationship table
        filters.add(new Pair<>(destinationEntityFilter, "rt"));
      }

      if (sourceTableName != null) {
        sqlBuilder.append("INNER JOIN ").append(sourceTableName).append(" st ON st.urn=rt.source ");

        if (sourceEntityFilter != null) {
          filters.add(new Pair<>(sourceEntityFilter, "st"));
        }
      }

      sqlBuilder.append("WHERE deleted_ts is NULL");

      filters.add(new Pair<>(relationshipFilter, "rt"));

      String whereClause = SQLStatementUtils.whereClause(SUPPORTED_CONDITIONS,
          _eBeanDAOConfig.isNonDollarVirtualColumnsEnabled(),
          filters.toArray(new Pair[filters.size()]));

      if (whereClause != null) {
        sqlBuilder.append(" AND ").append(whereClause);
      }
    } else if (_schemaConfig == EbeanLocalDAO.SchemaConfig.OLD_SCHEMA_ONLY) {
      sqlBuilder.append("WHERE deleted_ts IS NULL");
      if (sourceEntityFilter != null) {
        validateEntityFilterOnlyOneUrn(sourceEntityFilter);
        if (sourceEntityFilter.hasCriteria() && sourceEntityFilter.getCriteria().size() > 0) {
          sqlBuilder.append(
              SQLStatementUtils.whereClauseOldSchema(SUPPORTED_CONDITIONS, sourceEntityFilter.getCriteria(), SQLStatementUtils.SOURCE));
        }
      }
      if (destinationEntityFilter != null) {
        validateEntityFilterOnlyOneUrn(destinationEntityFilter);
        if (destinationEntityFilter.hasCriteria() && destinationEntityFilter.getCriteria().size() > 0) {
          sqlBuilder.append(
              SQLStatementUtils.whereClauseOldSchema(SUPPORTED_CONDITIONS, destinationEntityFilter.getCriteria(), SQLStatementUtils.DESTINATION));
        }
      }
    } else {
      throw new RuntimeException("The schema config must be set to OLD_SCHEMA_ONLY, DUAL_SCHEMA, or NEW_SCHEMA_ONLY.");
    }

    if (limit > 0) {
      sqlBuilder.append(" LIMIT ").append(limit);

      if (offset > 0) {
        sqlBuilder.append(" OFFSET ").append(offset);
      }
    }

    return sqlBuilder.toString();
  }

  /**
   * Creates and return a set of MG entity type names by querying the database.
   */
  public Set<String> initMgEntityTypeNameSet() {
    // there is some race condition, the local relationship db might not be ready when EbeanLocalRelationshipQueryDAO inits.
    // so we can't init the _mgEntityTypeNameSet in constructor.
    if (_mgEntityTypeNameSet == null) {
      final String sql = "SELECT table_name FROM information_schema.tables"
          + " WHERE table_type = 'BASE TABLE' AND TABLE_SCHEMA=DATABASE() AND table_name LIKE 'metadata_entity_%'";
      _mgEntityTypeNameSet = _server.createSqlQuery(sql)
          .findList()
          .stream()
          .map(row -> row.getString("table_name").replace("metadata_entity_", ""))
          .collect(Collectors.toSet());
    }
    return _mgEntityTypeNameSet;
  }
}
