package com.linkedin.metadata.dao;

import com.linkedin.data.DataMap;
import com.google.common.annotations.VisibleForTesting;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.metadata.dao.utils.ClassUtils;
import com.linkedin.metadata.dao.utils.EBeanDAOUtils;
import com.linkedin.metadata.dao.utils.LogicalExpressionLocalRelationshipCriterionUtils;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.dao.utils.MultiHopsTraversalSqlGenerator;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.dao.utils.RelationshipLookUpContext;
import com.linkedin.metadata.dao.utils.SQLSchemaUtils;
import com.linkedin.metadata.dao.utils.SQLStatementUtils;
import com.linkedin.metadata.dao.utils.SchemaValidatorUtil;
import com.linkedin.metadata.query.Condition;
import com.linkedin.metadata.query.LocalRelationshipCriterion;
import com.linkedin.metadata.query.LocalRelationshipCriterionArray;
import com.linkedin.metadata.query.LocalRelationshipFilter;
import com.linkedin.metadata.query.RelationshipDirection;
import io.ebean.EbeanServer;
import io.ebean.SqlRow;
import java.sql.SQLException;
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
import javax.persistence.PersistenceException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.javatuples.Pair;
import pegasus.com.linkedin.metadata.query.LogicalExpressionLocalRelationshipCriterion;
import pegasus.com.linkedin.metadata.query.LogicalOperation;
import pegasus.com.linkedin.metadata.query.innerLogicalOperation.Operator;

import static com.linkedin.metadata.dao.utils.LogicalExpressionLocalRelationshipCriterionUtils.*;


/**
 * An Ebean implementation of {@link BaseQueryDAO} backed by local relationship tables.
 */
@Slf4j
public class EbeanLocalRelationshipQueryDAO {
  public static final String RELATED_TO = "relatedTo";
  public static final String SOURCE = "source";
  public static final String METADATA = "metadata";
  public static final String RELATIONSHIP_RETURN_TYPE = "relationship.return.type";
  public static final String MG_INTERNAL_ASSET_RELATIONSHIP_TYPE = "AssetRelationship.proto";
  private static final int FILTER_BATCH_SIZE = 200;
  private static final String IDX_DESTINATION_DELETED_TS = "idx_destination_deleted_ts";
  private static final String FORCE_IDX_ON_DESTINATION = " FORCE INDEX (idx_destination_deleted_ts) ";
  private static final String DESTINATION_FIELD =  "destination";
  private final EbeanServer _server;
  private final MultiHopsTraversalSqlGenerator _sqlGenerator;

  private final EBeanDAOConfig _eBeanDAOConfig;

  private Set<String> _mgEntityTypeNameSet;
  private EbeanLocalDAO.SchemaConfig _schemaConfig = EbeanLocalDAO.SchemaConfig.NEW_SCHEMA_ONLY;
  private SchemaValidatorUtil _schemaValidatorUtil;

  public EbeanLocalRelationshipQueryDAO(EbeanServer server, EBeanDAOConfig eBeanDAOConfig) {
    _server = server;
    _eBeanDAOConfig = eBeanDAOConfig;
    _sqlGenerator = new MultiHopsTraversalSqlGenerator(SUPPORTED_CONDITIONS);
    _schemaValidatorUtil = new SchemaValidatorUtil(server);
  }

  public EbeanLocalRelationshipQueryDAO(EbeanServer server) {
    _server = server;
    _eBeanDAOConfig = new EBeanDAOConfig();
    _sqlGenerator = new MultiHopsTraversalSqlGenerator(SUPPORTED_CONDITIONS);
    _schemaValidatorUtil = new SchemaValidatorUtil(server);
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
          put(Condition.START_WITH, "LIKE");
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
    return findEntitiesCore(snapshotClass, filter, offset, count, false);
  }

  private <SNAPSHOT extends RecordTemplate> List<SNAPSHOT> findEntitiesCore(@Nonnull Class<SNAPSHOT> snapshotClass,
      @Nonnull LocalRelationshipFilter filter, int offset, int count, boolean logicalExpressionFilterEnabled) throws OperationNotSupportedException {
    if (_schemaConfig == EbeanLocalDAO.SchemaConfig.OLD_SCHEMA_ONLY) {
      throw new OperationNotSupportedException("findEntities is not supported in OLD_SCHEMA_MODE");
    }
    validateEntityFilter(filter, snapshotClass, logicalExpressionFilterEnabled);

    final String tableName = SQLSchemaUtils.getTableName(ModelUtils.getUrnTypeFromSnapshot(snapshotClass));
    final StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder.append("SELECT * FROM ").append(tableName);
    if (filterHasNonEmptyCriteria(filter)) {
      sqlBuilder.append(" WHERE ").append(SQLStatementUtils.whereClause(filter, SUPPORTED_CONDITIONS, null,
          _eBeanDAOConfig.isNonDollarVirtualColumnsEnabled()));
    }
    sqlBuilder.append(" ORDER BY urn LIMIT ").append(Math.max(1, count)).append(" OFFSET ").append(Math.max(0, offset));

    return _server.createSqlQuery(sqlBuilder.toString()).findList().stream()
        .map(sqlRow -> constructSnapshot(sqlRow, snapshotClass))
        .collect(Collectors.toList());
  }

  /**
   * Finds a list of entities of a specific type based on the given filter on the entity.
   * Similar to {@link #findEntities(Class, LocalRelationshipFilter, int, int)},
   * but this method uses the LogicalExpressionLocalRelationshipCriterion in LocalRelationshipFilter.
   * The SNAPSHOT class must be defined within com.linkedin.metadata.snapshot package in metadata-models.
   * This method is not supported in OLD_SCHEMA_ONLY mode.
   * @param snapshotClass the snapshot class to query.
   * @param filter the filter to apply when querying. Uses `logicalExpressionCriteria` instead of `criteria`.
   * @param offset the offset the query should start at. Ignored if set to a negative value.
   * @param count the maximum number of entities to return. Ignored if set to a non-positive value.
   * @return A list of entity records of class SNAPSHOT.
   * @throws OperationNotSupportedException when called in OLD_SCHEMA_ONLY mode. This exception must be explicitly handled by the caller.
   */
  @Nonnull
  public <SNAPSHOT extends RecordTemplate> List<SNAPSHOT> findEntitiesV2(@Nonnull Class<SNAPSHOT> snapshotClass,
      @Nonnull LocalRelationshipFilter filter, int offset, int count) throws OperationNotSupportedException {
    return findEntitiesCore(snapshotClass, filter, offset, count, true);
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

    validateRelationshipFilter(relationshipFilter, false);
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

  public <SRC_SNAPSHOT extends RecordTemplate, DEST_SNAPSHOT extends RecordTemplate, RELATIONSHIP extends RecordTemplate> List<RELATIONSHIP> findRelationships(
      @Nullable Class<SRC_SNAPSHOT> sourceEntityClass, @Nonnull LocalRelationshipFilter sourceEntityFilter,
      @Nullable Class<DEST_SNAPSHOT> destinationEntityClass, @Nonnull LocalRelationshipFilter destinationEntityFilter,
      @Nonnull Class<RELATIONSHIP> relationshipType, @Nonnull LocalRelationshipFilter relationshipFilter, int offset, int count) {
    return findRelationships(sourceEntityClass, sourceEntityFilter, destinationEntityClass, destinationEntityFilter, relationshipType,
        relationshipFilter, offset, count, new RelationshipLookUpContext());
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
      @Nonnull Class<RELATIONSHIP> relationshipType, @Nonnull LocalRelationshipFilter relationshipFilter, int offset,
      int count, RelationshipLookUpContext relationshipLookUpContext) {
    validateEntityFilter(sourceEntityFilter, sourceEntityClass);
    validateEntityFilter(destinationEntityFilter, destinationEntityClass);
    validateRelationshipFilter(relationshipFilter, false);

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
        offset, relationshipLookUpContext);

    List<SqlRow> rows = executeSqlWithIndexCheck(sql, relationshipTableName);

    return rows.stream()
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
      int offset, int count, RelationshipLookUpContext relationshipLookUpContext) {
    List<SqlRow> sqlRows = findRelationshipsV2V3V4Core(
        sourceEntityType, sourceEntityFilter, destinationEntityType, destinationEntityFilter,
        relationshipType, relationshipFilter, offset, count, relationshipLookUpContext, false);

    return sqlRows.stream()
        .map(row -> RecordUtils.toRecordTemplate(relationshipType, row.getString(METADATA)))
        .collect(Collectors.toList());
  }

  /**
   * Fetches a list of SqlRow of relationships of a specific type (Urn) based on the given filters if applicable.
   *
   * @param sourceEntityType type of source entity to query (e.g. "dataset")
   * @param sourceEntityFilter the filter to apply to the source entity when querying (not applicable to non-MG entities)
   * @param destinationEntityType type of destination entity to query (e.g. "dataset")
   * @param destinationEntityFilter the filter to apply to the destination entity when querying (not applicable to non-MG entities)
   * @param relationshipType the type of relationship to query
   * @param relationshipFilter the filter to apply to relationship when querying
   * @param offset the offset query should start at. Ignored if set to a negative value.
   * @param count the maximum number of entities to return. Ignored if set to a non-positive value.
   * @param logicalExpressionFilterEnabled whether logical expression filter is enabled or not.
   * @return A list of relationship records in SqlRow (col: source, destination, metadata, etc).
   */
  @Nonnull
  private <RELATIONSHIP extends RecordTemplate> List<SqlRow> findRelationshipsV2V3V4Core(
      @Nullable String sourceEntityType, @Nullable LocalRelationshipFilter sourceEntityFilter,
      @Nullable String destinationEntityType, @Nullable LocalRelationshipFilter destinationEntityFilter,
      @Nonnull Class<RELATIONSHIP> relationshipType, @Nonnull LocalRelationshipFilter relationshipFilter,
      int offset, int count, RelationshipLookUpContext relationshipLookUpContext, boolean logicalExpressionFilterEnabled) {
    validateEntityTypeAndFilter(sourceEntityFilter, sourceEntityType, logicalExpressionFilterEnabled);
    validateEntityTypeAndFilter(destinationEntityFilter, destinationEntityType, logicalExpressionFilterEnabled);
    validateRelationshipFilter(relationshipFilter, logicalExpressionFilterEnabled);

    // the assumption is we have the table for every MG entity. For non-MG entities, sourceTableName will be null.
    final String sourceTableName = getMgEntityTableName(sourceEntityType);
    final String destTableName = getMgEntityTableName(destinationEntityType);
    final String relationshipTableName = SQLSchemaUtils.getRelationshipTableName(relationshipType);

    final String sql = buildFindRelationshipSQL(
        relationshipTableName, relationshipFilter,
        sourceTableName, sourceEntityFilter,
        destTableName, destinationEntityFilter, count, offset, relationshipLookUpContext);
    // Temporary log to help debug the slow SQL query
    log.info("Executing SQL for GQS: {}", sql);
    return executeSqlWithIndexCheck(sql, relationshipTableName);
  }

  /**
   * Finds a list of relationships of a specific type (Urn) based on the given filters if applicable.
   * Similar to findRelationshipsV2, but this method wraps the relationship in a specific class provided by user.
   * The intended use case is for MG internally with AssetRelationship, but since it is an open API, we are leaving room for extendability.
   *
   * @param sourceEntityType type of source entity to query (e.g. "dataset")
   * @param sourceEntityFilter the filter to apply to the source entity when querying (not applicable to non-MG entities)
   * @param destinationEntityType type of destination entity to query (e.g. "dataset")
   * @param destinationEntityFilter the filter to apply to the destination entity when querying (not applicable to non-MG entities)
   * @param relationshipType the type of relationship to query
   * @param relationshipFilter the filter to apply to relationship when querying
   * @param assetRelationshipClass the wrapper class for the relationship type
   * @param wrapOptions options to wrap the relationship. Currently unused. Leaving it open for the future.
   * @param offset the offset query should start at. Ignored if set to a negative value.
   * @param count the maximum number of entities to return. Ignored if set to a non-positive value.
   * @return A list of relationship records.
   */
  @Nonnull
  public <ASSET_RELATIONSHIP extends RecordTemplate, RELATIONSHIP extends RecordTemplate> List<ASSET_RELATIONSHIP> findRelationshipsV3(
      @Nullable String sourceEntityType, @Nullable LocalRelationshipFilter sourceEntityFilter,
      @Nullable String destinationEntityType, @Nullable LocalRelationshipFilter destinationEntityFilter,
      @Nonnull Class<RELATIONSHIP> relationshipType, @Nonnull LocalRelationshipFilter relationshipFilter,
      @Nonnull Class<ASSET_RELATIONSHIP> assetRelationshipClass, @Nullable Map<String, Object> wrapOptions,
      int offset, int count, RelationshipLookUpContext relationshipLookUpContext) {
    if (wrapOptions == null || !wrapOptions.containsKey(RELATIONSHIP_RETURN_TYPE)
        || !MG_INTERNAL_ASSET_RELATIONSHIP_TYPE.equals(wrapOptions.get(RELATIONSHIP_RETURN_TYPE))) {
      throw new IllegalArgumentException("Please check your use of the findRelationshipsV3 method.");
    }

    List<SqlRow> sqlRows = findRelationshipsV2V3V4Core(
        sourceEntityType, sourceEntityFilter, destinationEntityType, destinationEntityFilter,
        relationshipType, relationshipFilter, offset, count, relationshipLookUpContext, false);

    return sqlRows.stream()
        .map(row -> createAssetRelationshipWrapperForRelationship(
            relationshipType, assetRelationshipClass, row.getString(METADATA), row.getString(SOURCE), wrapOptions))
        .collect(Collectors.toList());
  }

  /**
   * Finds a list of relationships of a specific type (Urn) based on the given filters if applicable.
   * Similar to findRelationshipsV3, but this method takes filters with logical expressions (AND/OR).
   * The intended use case is for MG internally with AssetRelationship, but since it is an open API, we are leaving room for extendability.
   *
   * @param sourceEntityType type of source entity to query (e.g. "dataset")
   * @param sourceEntityFilter the filter to apply to the source entity when querying (not applicable to non-MG entities).
   *                           criteria must be null. Use logicalExpressionCriteria instead.
   * @param destinationEntityType type of destination entity to query (e.g. "dataset")
   * @param destinationEntityFilter the filter to apply to the destination entity when querying (not applicable to non-MG entities).
   *                                criteria must be null. Use logicalExpressionCriteria instead.
   * @param relationshipType the type of relationship to query
   * @param relationshipFilter the filter to apply to relationship when querying.
   *                           criteria must be null. Use logicalExpressionCriteria instead.
   * @param assetRelationshipClass the wrapper class for the relationship type
   * @param wrapOptions options to wrap the relationship. Currently unused. Leaving it open for the future.
   * @param offset the offset query should start at. Ignored if set to a negative value.
   * @param count the maximum number of entities to return. Ignored if set to a non-positive value.
   * @return A list of relationship records.
   */
  @Nonnull
  public <ASSET_RELATIONSHIP extends RecordTemplate, RELATIONSHIP extends RecordTemplate> List<ASSET_RELATIONSHIP> findRelationshipsV4(
      @Nullable String sourceEntityType, @Nullable LocalRelationshipFilter sourceEntityFilter,
      @Nullable String destinationEntityType, @Nullable LocalRelationshipFilter destinationEntityFilter,
      @Nonnull Class<RELATIONSHIP> relationshipType, @Nonnull LocalRelationshipFilter relationshipFilter,
      @Nonnull Class<ASSET_RELATIONSHIP> assetRelationshipClass, @Nullable Map<String, Object> wrapOptions,
      int offset, int count, RelationshipLookUpContext relationshipLookUpContext) {
    if (wrapOptions == null || !wrapOptions.containsKey(RELATIONSHIP_RETURN_TYPE)
        || !MG_INTERNAL_ASSET_RELATIONSHIP_TYPE.equals(wrapOptions.get(RELATIONSHIP_RETURN_TYPE))) {
      throw new IllegalArgumentException("Please check your use of the findRelationshipsV3 method.");
    }

    List<SqlRow> sqlRows = findRelationshipsV2V3V4Core(
        sourceEntityType, sourceEntityFilter, destinationEntityType, destinationEntityFilter,
        relationshipType, relationshipFilter, offset, count, relationshipLookUpContext, true);

    return sqlRows.stream()
        .map(row -> createAssetRelationshipWrapperForRelationship(
            relationshipType, assetRelationshipClass, row.getString(METADATA), row.getString(SOURCE), wrapOptions))
        .collect(Collectors.toList());
  }

  /**
   * Wraps the relationship in a specific class provided by user.
   * The intended use case is for MG internally with AssetRelationship, but since it is an open API, we are leaving room for extendability.
   *
   * @param relationshipType the type of relationship to query
   * @param assetRelationshipClass the wrapper class for the relationship type. By default, AssetRelationship.
   * @param metadata the metadata string which can be parsed into a relationship
   * @param sourceUrn the source urn
   * @param wrapOptions options to wrap the relationship. Currently unused. Leaving it open for the future.
   * @return A wrapped relationship record.
   */
  @Nonnull
  private <ASSET_RELATIONSHIP extends RecordTemplate, RELATIONSHIP extends RecordTemplate> ASSET_RELATIONSHIP createAssetRelationshipWrapperForRelationship(
      @Nonnull Class<RELATIONSHIP> relationshipType, @Nonnull Class<ASSET_RELATIONSHIP> assetRelationshipClass,
      @Nonnull String metadata, @Nonnull String sourceUrn, @Nullable Map<String, Object> wrapOptions) {
    // TODO: if other type of ASSET_RELATIONSHIP is needed, we need to distinguish it with wrapOptions and handles differently.

    // parse metadata json string into DataMap
    final DataMap relationshipDataMap = RecordUtils.toDataMap(metadata);

    final DataMap relatedToDataMap = new DataMap();
    // e.g. "BelongsToV2" -> "belongsToV2"
    final String relationshipName = decapitalize(relationshipType.getSimpleName());
    relatedToDataMap.put(relationshipName, relationshipDataMap);

    final DataMap dataMap = new DataMap();
    dataMap.put(RELATED_TO, relatedToDataMap);
    dataMap.put(SOURCE, sourceUrn);

    return RecordUtils.toRecordTemplate(assetRelationshipClass, dataMap);
  }

  private static String decapitalize(String str) {
    if (str == null || str.isEmpty()) {
      return str;
    }
    return str.substring(0, 1).toLowerCase() + str.substring(1);
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

  private <ENTITY extends RecordTemplate> void validateEntityFilter(@Nonnull LocalRelationshipFilter filter, @Nullable Class<ENTITY> entityClass) {
    validateEntityFilter(filter, entityClass, false);
  }

  private <ENTITY extends RecordTemplate> void validateEntityFilter(@Nonnull LocalRelationshipFilter filter, @Nullable Class<ENTITY> entityClass,
      boolean logicalExpressionFilterEnabled) {
    validateEntityTypeAndFilter(filter,
        entityClass != null ? ModelUtils.getUrnTypeFromSnapshot(entityClass) : null,
        logicalExpressionFilterEnabled);
  }

  /**
   * Validate:
   * 1. if the entity type is null or empty, then the filter should also be emtpy.
   * 2. the entity filter only contains supported conditions.
   * 3. if logicalExpressionFilterEnabled is true, ONLY logical expression filters are allowed. Vice versa.
   * If any of above is violated, throw an IllegalArgumentException.
   */
  private void validateEntityTypeAndFilter(@Nullable LocalRelationshipFilter filter, @Nullable String entityType,
      boolean logicalExpressionFilterEnabled) {
    if (filter == null) {
      return;
    }

    validateLogicalExpressionFilter(filter, logicalExpressionFilterEnabled);

    if ((StringUtils.isBlank(entityType)) && filterHasNonEmptyCriteria(filter)) {
      throw new IllegalArgumentException("Entity type string is null or empty but filter is not empty.");
    }

    validateFilterCriteria(filter, logicalExpressionFilterEnabled);
  }

  /**
   * Checks if the filter follows all logical expression related rules.
   * 1. if only one of criteria and logicalExpressionCriteria is used.
   * 2. if logicalExpressionFilterEnabled is true, only logicalExpressionCriteria is allowed. Else, only criteria is allowed.
   */
  private static void validateLogicalExpressionFilter(@Nonnull LocalRelationshipFilter filter, boolean logicalExpressionFilterEnabled) {
    if (filter.hasCriteria() && filter.hasLogicalExpressionCriteria()) {
      throw new IllegalArgumentException(
          "Please do not use both the 'criteria' field and the 'logicalExpressionCriteria' field.");
    }

    if (logicalExpressionFilterEnabled && filter.hasCriteria()) {
        throw new IllegalArgumentException(
            "Please do not use the 'criteria' field and use the 'logicalExpressionCriteria' field instead for this API.");
    } else if (!logicalExpressionFilterEnabled && filter.hasLogicalExpressionCriteria()) {
        throw new IllegalArgumentException(
            "Please do not use the 'logicalExpressionCriteria' field and use the 'criteria' field instead for this API.");
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
    LocalRelationshipCriterionArray criteria = null;

    if (filter.hasCriteria() && !filter.getCriteria().isEmpty()) {
      criteria = filter.getCriteria();
    } else if (filter.hasLogicalExpressionCriteria() && filter.getLogicalExpressionCriteria() != null) {
      criteria = LogicalExpressionLocalRelationshipCriterionUtils.flattenLogicalExpressionLocalRelationshipCriterion(filter.getLogicalExpressionCriteria());
    }

    if (criteria == null) {
      return;
    }

    if (criteria.size() > 1) {
      throw new IllegalArgumentException("Only 1 filter is allowed for non-MG entities or when running in OLD_SCHEMA_ONLY mode.");
    }
    LocalRelationshipCriterion criterion = criteria.get(0);

    if (!criterion.hasField() || !criterion.getField().isUrnField()) {
      throw new IllegalArgumentException("Only filters on the urn field are allowed for non-MG entities or when running in OLD_SCHEMA_ONLY mode.");
    }
    Condition condition = criterion.getCondition();
    if (!SUPPORTED_CONDITIONS.containsKey(condition)) {
      throw new IllegalArgumentException(String.format("Condition %s is not supported by local relationship DAO.", condition));
    }
  }

  /**
   * Validate:
   * 1. The relationship filter only contains supported condition.
   * 2. Relationship direction cannot be unknown.
   * 3. if logicalExpressionFilterEnabled is true, ONLY logical expression filters are allowed. Vice versa.
   * If any of above is violated, throw IllegalArgumentException.
   */
  private void validateRelationshipFilter(@Nonnull LocalRelationshipFilter filter,
      boolean logicalExpressionFilterEnabled) {

    validateLogicalExpressionFilter(filter, logicalExpressionFilterEnabled);

    if (filter.getDirection() == null || filter.getDirection() == RelationshipDirection.$UNKNOWN) {
      throw new IllegalArgumentException("Relationship direction cannot be null or UNKNOWN.");
    }

    if (filterHasNonEmptyCriteria(filter)) {
      validateFilterCriteria(filter, logicalExpressionFilterEnabled);
    }
  }

  /**
   * Validate whether filter criteria contains unsupported condition.
   *
   * @param filter                         the local relationship filter.
   * @param logicalExpressionFilterEnabled
   */
  private void validateFilterCriteria(@Nonnull LocalRelationshipFilter filter, boolean logicalExpressionFilterEnabled) {
    if (logicalExpressionFilterEnabled) {
      validateLogicalExpression(filter.getLogicalExpressionCriteria());
    } else {
      filter.getCriteria().forEach(EbeanLocalRelationshipQueryDAO::validateLocalRelationshipCriterion);
    }
  }

  private static void validateLocalRelationshipCriterion(LocalRelationshipCriterion criterion) {
      Condition condition = criterion.getCondition();
      if (!SUPPORTED_CONDITIONS.containsKey(condition)) {
        throw new IllegalArgumentException(
            String.format("Condition %s is not supported by local relationship DAO.", condition));
      }
  }

  /**
   * Recursively validate logical expression criteria. Checks Operator and uses validateLocalRelationshipCriterion to check each criterion.
   */
  private static void validateLogicalExpression(@Nullable LogicalExpressionLocalRelationshipCriterion logicalExpressionCriteria) {
    if (logicalExpressionCriteria == null) {
      return;
    }

    if (logicalExpressionCriteria.hasExpr()) {
      final LogicalExpressionLocalRelationshipCriterion.Expr expr = logicalExpressionCriteria.getExpr();
      if (expr == null) {
        throw new IllegalArgumentException("expr cannot be null in logical expression criteria.");
      }

      if (expr.isCriterion()) {
        validateLocalRelationshipCriterion(expr.getCriterion());
      }

      if (expr.isLogical()) {
        final LogicalOperation logical = expr.getLogical();

        if (!logical.hasOp() || logical.getOp() == Operator.UNKNOWN || logical.getOp() == Operator.$UNKNOWN) {
          throw new IllegalArgumentException("Logical operation must have an operation defined.");
        }

        if (!logical.hasExpressions()) {
          throw new IllegalArgumentException("Logical operation must have expressions.");
        }

        if (logical.getOp() == Operator.NOT) {
          if (!logical.hasExpressions() || logical.getExpressions() == null || logical.getExpressions().size() != 1) {
            throw new IllegalArgumentException("NOT operator must have exactly one expression.");
          }

          if (!logical.getExpressions().get(0).hasExpr()) {
            throw new IllegalArgumentException("NOT operator must have an expression.");
          }

          if (logical.getExpressions().get(0).getExpr() == null || !logical.getExpressions().get(0).getExpr().isCriterion()) {
            throw new IllegalArgumentException("NOT operator must have a criterion expression.");
          }

          validateLocalRelationshipCriterion(logical.getExpressions().get(0).getExpr().getCriterion());
          return;
        }

        if (logical.getExpressions() != null && logical.getExpressions().size() < 2) {
          throw new IllegalArgumentException("Logical operation must have at least two expressions.");
        }

        logical.getExpressions().forEach(EbeanLocalRelationshipQueryDAO::validateLogicalExpression);
      }
    }
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
        String extractedAspectStr = EBeanDAOUtils.extractAspectJsonString(auditedAspectStr);
        if (extractedAspectStr != null) {
          RecordTemplate aspect = RecordUtils.toRecordTemplate(ClassUtils.loadClass(aspectCanonicalName),
              extractedAspectStr);
          aspects.add(ModelUtils.newAspectUnion(ModelUtils.getUnionClassFromSnapshot(snapshotClass), aspect));
        }
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
   * <p> or if relationshipLookUpContext.isIncludeNonCurrentRelationships is true </p>
   *
   * <p>SELECT * FROM (
   * SELECT rt.*, ROW_NUMBER() OVER (PARTITION BY rt.source, rt.metadata$type, rt.destination ORDER BY rt.lastmodifiedon DESC) AS row_num
   * FROM relationship_table rt
   * INNER JOIN destination_entity_table dt ON dt.urn = rt.destinationEntityUrn
   * INNER JOIN source_entity_table st ON st.urn = rt.sourceEntityUrn
   * WHERE destination entity filters AND source entity filters AND relationship filters)
   * ranked_rows WHERE row_num = 1</p>
   *
   * @param relationshipTableName   relationship table name
   * @param relationshipFilter      filter on relationship
   * @param sourceTableName         source entity table name
   * @param sourceEntityFilter      filter on source entity.
   * @param destTableName           destination entity table name. Always null if building relationship with non-mg
   *                                entity.
   * @param destinationEntityFilter filter on destination entity.
   * @param limit                   max number of records to return. If less than 0, will return all records.
   * @param offset                  offset to start from. If less than 0, will start from 0.
   */
  @Nonnull
  @VisibleForTesting
  public String buildFindRelationshipSQL(@Nonnull final String relationshipTableName,
      @Nonnull LocalRelationshipFilter relationshipFilter, @Nullable final String sourceTableName,
      @Nullable LocalRelationshipFilter sourceEntityFilter, @Nullable final String destTableName,
      @Nullable LocalRelationshipFilter destinationEntityFilter, int limit, int offset,
      RelationshipLookUpContext relationshipLookUpContext) {

    relationshipFilter = LogicalExpressionLocalRelationshipCriterionUtils.normalizeLocalRelationshipFilter(relationshipFilter);
    sourceEntityFilter = LogicalExpressionLocalRelationshipCriterionUtils.normalizeLocalRelationshipFilter(sourceEntityFilter);
    destinationEntityFilter = LogicalExpressionLocalRelationshipCriterionUtils.normalizeLocalRelationshipFilter(destinationEntityFilter);

    boolean includeNonCurrentRelationships = relationshipLookUpContext.isIncludeNonCurrentRelationships();
    StringBuilder sqlBuilder = new StringBuilder();

    if (includeNonCurrentRelationships) {
      sqlBuilder.append("SELECT * FROM (");
    }

    sqlBuilder.append("SELECT rt.*");

    if (includeNonCurrentRelationships) {
      final boolean isNonDollarVirtualColumnsEnabled = _eBeanDAOConfig.isNonDollarVirtualColumnsEnabled();
      final String metadataTypeColName = isNonDollarVirtualColumnsEnabled ? "metadata0type" : "metadata$type";
      final boolean hasMetadataTypeCol = _schemaValidatorUtil.columnExists(relationshipTableName, metadataTypeColName);

      sqlBuilder.append(", ROW_NUMBER() OVER (PARTITION BY rt.source")
          .append(hasMetadataTypeCol ? ", rt." + metadataTypeColName : "")
          .append(", rt.destination ORDER BY rt.lastmodifiedon DESC) AS row_num");
    }

    sqlBuilder.append(" FROM ").append(relationshipTableName).append(" rt ");

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
      } else if (filterHasNonEmptyCriteria(relationshipFilter)) {
        // Apply FORCE INDEX if destination field is being filtered, and the index exists
        final LocalRelationshipCriterionArray relationshipCriteria =
            flattenLogicalExpressionLocalRelationshipCriterion(relationshipFilter.getLogicalExpressionCriteria());
        for (LocalRelationshipCriterion criterion : relationshipCriteria) {
          LocalRelationshipCriterion.Field field = criterion.getField();
          if (field.getUrnField() != null && DESTINATION_FIELD.equals(field.getUrnField().getName())) {
            // Check if index exists on 'destination' before applying FORCE INDEX
            if (_schemaValidatorUtil.indexExists(relationshipTableName, IDX_DESTINATION_DELETED_TS)) {
              sqlBuilder.append(FORCE_IDX_ON_DESTINATION);
            }
            break;
          }
        }
      }

      if (sourceTableName != null) {
        sqlBuilder.append("INNER JOIN ").append(sourceTableName).append(" st ON st.urn=rt.source ");

        if (sourceEntityFilter != null) {
          filters.add(new Pair<>(sourceEntityFilter, "st"));
        }
      }

      if (!includeNonCurrentRelationships) {
        sqlBuilder.append("WHERE rt.deleted_ts is NULL");
      }

      filters.add(new Pair<>(relationshipFilter, "rt"));

      String whereClause = SQLStatementUtils.whereClause(SUPPORTED_CONDITIONS,
          _eBeanDAOConfig.isNonDollarVirtualColumnsEnabled(),
          filters.toArray(new Pair[filters.size()]));

      if (whereClause != null) {
        sqlBuilder.append(includeNonCurrentRelationships ? " WHERE " : " AND ").append(whereClause);
      }
    } else if (_schemaConfig == EbeanLocalDAO.SchemaConfig.OLD_SCHEMA_ONLY) {
      StringBuilder whereClauseBuilder = new StringBuilder();
      if (!includeNonCurrentRelationships) {
        whereClauseBuilder.append("rt.deleted_ts IS NULL");
      }
      if (sourceEntityFilter != null) {
        validateEntityFilterOnlyOneUrn(sourceEntityFilter);
        if (filterHasNonEmptyCriteria(sourceEntityFilter)) {
          whereClauseBuilder.append(
              SQLStatementUtils.whereClauseOldSchema(SUPPORTED_CONDITIONS, sourceEntityFilter, SQLStatementUtils.SOURCE));
        }
      }
      if (destinationEntityFilter != null) {
        validateEntityFilterOnlyOneUrn(destinationEntityFilter);
        if (filterHasNonEmptyCriteria(destinationEntityFilter)) {
          whereClauseBuilder.append(
              SQLStatementUtils.whereClauseOldSchema(SUPPORTED_CONDITIONS, destinationEntityFilter, SQLStatementUtils.DESTINATION));
        }
      }
      if (whereClauseBuilder.length() != 0) {
        if (includeNonCurrentRelationships) {
          String where = whereClauseBuilder.toString().replaceFirst("\\s*AND\\s+", "");
          sqlBuilder.append("WHERE ").append(where);
        } else {
          sqlBuilder.append("WHERE ").append(whereClauseBuilder);
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

    if (includeNonCurrentRelationships) {
      sqlBuilder.append(") ranked_rows WHERE row_num = 1");
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

  private List<SqlRow> executeSqlWithIndexCheck(String sql, String relationshipTableName) {
    try {
      return _server.createSqlQuery(sql).findList();
    } catch (PersistenceException e) {
      Throwable cause = e.getCause();
      if (cause instanceof SQLException && cause.getMessage() != null
          && cause.getMessage().contains("doesn't exist in table")) {
        String errorMsg = String.format(
            "Missing index when querying table '%s'. "
                + "Make sure FORCE INDEX targets like idx_destination_deleted_ts or idx_source_deleted_ts are created.",
            relationshipTableName);
        log.error(errorMsg);
        throw new IllegalStateException(errorMsg, e);
      }

      throw new RuntimeException("Failed to execute SQL query for relationships", e);
    }
  }

}
