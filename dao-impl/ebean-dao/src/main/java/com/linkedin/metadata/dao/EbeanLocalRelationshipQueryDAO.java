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
import com.linkedin.metadata.dao.utils.SharedSchemaCache;
import com.linkedin.metadata.query.Condition;
import io.ebean.config.ServerConfig;
import com.linkedin.metadata.query.LocalRelationshipCriterion;
import com.linkedin.metadata.query.LocalRelationshipCriterionArray;
import com.linkedin.metadata.query.LocalRelationshipFilter;
import com.linkedin.metadata.query.RelationshipDirection;
import io.ebean.EbeanServer;
import io.ebean.SqlQuery;
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
import org.javatuples.Triplet;
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
  // Hard upper bound on keyset page size to keep per-page DB work bounded.
  private static final int MAX_KEYSET_PAGE_SIZE = 1000;
  private static final String IDX_DESTINATION_DELETED_TS = "idx_destination_deleted_ts";
  private static final String FORCE_IDX_ON_DESTINATION = " FORCE INDEX (idx_destination_deleted_ts) ";
  private static final String DESTINATION_FIELD =  "destination";
  private final EbeanServer _server;
  private final MultiHopsTraversalSqlGenerator _sqlGenerator;

  private final EBeanDAOConfig _eBeanDAOConfig;

  private Set<String> _mgEntityTypeNameSet;
  private EbeanLocalDAO.SchemaConfig _schemaConfig = EbeanLocalDAO.SchemaConfig.NEW_SCHEMA_ONLY;
  private SchemaValidatorUtil _schemaValidatorUtil;

  public EbeanLocalRelationshipQueryDAO(EbeanServer server, ServerConfig serverConfig,
      EBeanDAOConfig eBeanDAOConfig) {
    _server = server;
    _eBeanDAOConfig = eBeanDAOConfig;
    _schemaValidatorUtil = buildValidator(server, serverConfig);
    _sqlGenerator = new MultiHopsTraversalSqlGenerator(SUPPORTED_CONDITIONS, _schemaValidatorUtil);
  }

  public EbeanLocalRelationshipQueryDAO(EbeanServer server, EBeanDAOConfig eBeanDAOConfig) {
    _server = server;
    _eBeanDAOConfig = eBeanDAOConfig;
    _schemaValidatorUtil = new SchemaValidatorUtil(server);
    _sqlGenerator = new MultiHopsTraversalSqlGenerator(SUPPORTED_CONDITIONS, _schemaValidatorUtil);
  }

  public EbeanLocalRelationshipQueryDAO(EbeanServer server) {
    _server = server;
    _eBeanDAOConfig = new EBeanDAOConfig();
    _schemaValidatorUtil = new SchemaValidatorUtil(server);
    _sqlGenerator = new MultiHopsTraversalSqlGenerator(SUPPORTED_CONDITIONS, _schemaValidatorUtil);
  }

  private static SchemaValidatorUtil buildValidator(EbeanServer server, ServerConfig serverConfig) {
    String dbUrl = serverConfig.getDataSourceConfig() != null
        ? serverConfig.getDataSourceConfig().getUrl() : null;
    if (dbUrl == null || dbUrl.isEmpty()) {
      throw new IllegalStateException(
          "ServerConfig must have a DataSourceConfig with a non-empty URL to use SharedSchemaCache");
    }
    return new SchemaValidatorUtil(SharedSchemaCache.getInstance(server, dbUrl));
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
      sqlBuilder.append(" WHERE ").append(SQLStatementUtils.whereClause(filter, SUPPORTED_CONDITIONS, null, tableName,
          _schemaValidatorUtil, _eBeanDAOConfig.isNonDollarVirtualColumnsEnabled()));
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
   * Keyset (seek) paginated, scan-start membership variant of
   * {@link #findRelationships(Class, LocalRelationshipFilter, Class, LocalRelationshipFilter, Class,
   * LocalRelationshipFilter, int, int)} that walks the matching set in bounded pages. Ranked/
   * non-current pagination is unsupported because it could not bound the per-page DB work.
   *
   * <p>First page: pass {@code cursor = null}; the DAO captures {@code maxId}, the largest
   * relationship row id when paging starts ({@code COALESCE(MAX(id), 0)}), and returns rows with
   * {@code 0 < rt.id <= maxId} ordered by id, up to {@code pageSize}. Later inserts get larger ids
   * and are excluded, keeping the scan finite. Continuation: pass the next cursor from the previous
   * {@link RelationshipKeysetPage}. The cursor also carries a database scan-start timestamp. Later
   * pages include rows that were current at scan start even if they were soft-deleted before that
   * later page is read. Rows inserted after the scan started are still excluded by the fixed
   * {@code maxId} bound.</p>
   *
   * <p>Supported only when {@code SchemaConfig} is {@code NEW_SCHEMA_ONLY}; {@code OLD_SCHEMA_ONLY}
   * and {@code DUAL_SCHEMA} (deprecated) throw {@link UnsupportedOperationException}.</p>
   *
   * @param sourceEntityClass the source entity class to query
   * @param sourceEntityFilter the filter to apply to the source entity when querying
   * @param destinationEntityClass the destination entity class
   * @param destinationEntityFilter the filter to apply to the destination entity when querying
   * @param relationshipType the type of relationship to query
   * @param relationshipFilter the filter to apply to relationship when querying
   * @param pageSize the maximum number of relationships to return per page. Must be between 1 and
   *                 1000 inclusive.
   * @param cursor the cursor from the previous page, or {@code null} for the first page.
   * @return a page of relationship records plus a next cursor (null when the scan is exhausted).
   * @throws UnsupportedOperationException when the DAO is not in {@code NEW_SCHEMA_ONLY} mode.
   */
  @Nonnull
  public <SRC_SNAPSHOT extends RecordTemplate, DEST_SNAPSHOT extends RecordTemplate, RELATIONSHIP extends RecordTemplate>
      RelationshipKeysetPage<RELATIONSHIP> findRelationshipsByKeyset(
      @Nullable Class<SRC_SNAPSHOT> sourceEntityClass, @Nonnull LocalRelationshipFilter sourceEntityFilter,
      @Nullable Class<DEST_SNAPSHOT> destinationEntityClass, @Nonnull LocalRelationshipFilter destinationEntityFilter,
      @Nonnull Class<RELATIONSHIP> relationshipType, @Nonnull LocalRelationshipFilter relationshipFilter, int pageSize,
      @Nullable RelationshipKeysetCursor cursor) {
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

    final KeysetScanResult scan = findRelationshipsByKeysetCore(relationshipTableName, sourceTableName,
        sourceEntityFilter, destTableName, destinationEntityFilter, relationshipFilter, pageSize, cursor);

    final List<RELATIONSHIP> relationships = new ArrayList<>(scan.getRows().size());
    for (SqlRow row : scan.getRows()) {
      relationships.add(RecordUtils.toRecordTemplate(relationshipType, row.getString(METADATA)));
    }

    return new RelationshipKeysetPage<>(relationships, scan.getMaxId(), scan.getNextCursor());
  }

  /**
   * Keyset (seek) paginated, scan-start membership variant of
   * {@link #findRelationshipsV4(String, LocalRelationshipFilter, String, LocalRelationshipFilter,
   * Class, LocalRelationshipFilter, Class, Map, int, int, RelationshipLookUpContext)} that walks the
   * matching set in bounded pages and returns the same wrapped {@code ASSET_RELATIONSHIP} records.
   * Validates V4 logical-expression filters and the {@code wrapOptions} contract. Ranked/non-current
   * pagination is unsupported because it could not bound the per-page DB work.
   *
   * <p>First page: pass {@code cursor = null}; the DAO captures {@code maxId}, the largest
   * relationship row id when paging starts ({@code COALESCE(MAX(id), 0)}), and returns rows with
   * {@code 0 < rt.id <= maxId} ordered by id, up to {@code pageSize}. Continuation: pass the next
   * cursor from the previous {@link RelationshipKeysetPage}. The cursor also carries a database
   * scan-start timestamp. Later pages include rows that were current at scan start even if they were
   * soft-deleted before that later page is read. Rows inserted after the scan started are still
   * excluded by the fixed {@code maxId} bound.</p>
   *
   * <p>Supported only when {@code SchemaConfig} is {@code NEW_SCHEMA_ONLY}; {@code OLD_SCHEMA_ONLY}
   * and {@code DUAL_SCHEMA} (deprecated) throw {@link UnsupportedOperationException}.</p>
   *
   * @param sourceEntityType type of source entity to query (e.g. "dataset")
   * @param sourceEntityFilter filter on the source entity (not applicable to non-MG entities); criteria must be null, use logicalExpressionCriteria
   * @param destinationEntityType type of destination entity to query (e.g. "dataset")
   * @param destinationEntityFilter filter on the destination entity (not applicable to non-MG entities); criteria must be null, use logicalExpressionCriteria
   * @param relationshipType the type of relationship to query
   * @param relationshipFilter filter on the relationship; criteria must be null, use logicalExpressionCriteria
   * @param assetRelationshipClass the wrapper class for the relationship type
   * @param wrapOptions options to wrap the relationship. Must carry the AssetRelationship return type marker.
   * @param pageSize the maximum number of relationships to return per page. Must be between 1 and
   *                 1000 inclusive.
   * @param cursor the cursor from the previous page, or {@code null} for the first page.
   * @return a page of wrapped relationship records plus a next cursor (null when the scan is exhausted).
   * @throws IllegalArgumentException when {@code wrapOptions} does not carry {@code RELATIONSHIP_RETURN_TYPE}
   *         set to {@code MG_INTERNAL_ASSET_RELATIONSHIP_TYPE}.
   * @throws UnsupportedOperationException when the DAO is not in {@code NEW_SCHEMA_ONLY} mode.
   */
  @Nonnull
  public <ASSET_RELATIONSHIP extends RecordTemplate, RELATIONSHIP extends RecordTemplate>
      RelationshipKeysetPage<ASSET_RELATIONSHIP> findRelationshipsV4ByKeyset(
      @Nullable String sourceEntityType, @Nullable LocalRelationshipFilter sourceEntityFilter,
      @Nullable String destinationEntityType, @Nullable LocalRelationshipFilter destinationEntityFilter,
      @Nonnull Class<RELATIONSHIP> relationshipType, @Nonnull LocalRelationshipFilter relationshipFilter,
      @Nonnull Class<ASSET_RELATIONSHIP> assetRelationshipClass, @Nullable Map<String, Object> wrapOptions,
      int pageSize, @Nullable RelationshipKeysetCursor cursor) {
    validateAssetRelationshipWrapOptions(wrapOptions, "findRelationshipsV4ByKeyset");

    validateEntityTypeAndFilter(sourceEntityFilter, sourceEntityType, true);
    validateEntityTypeAndFilter(destinationEntityFilter, destinationEntityType, true);
    validateRelationshipFilter(relationshipFilter, true);

    final String sourceTableName = getMgEntityTableName(sourceEntityType);
    final String destTableName = getMgEntityTableName(destinationEntityType);
    final String relationshipTableName = SQLSchemaUtils.getRelationshipTableName(relationshipType);

    final KeysetScanResult scan = findRelationshipsByKeysetCore(relationshipTableName, sourceTableName,
        sourceEntityFilter, destTableName, destinationEntityFilter, relationshipFilter, pageSize, cursor);

    final List<ASSET_RELATIONSHIP> relationships = new ArrayList<>(scan.getRows().size());
    for (SqlRow row : scan.getRows()) {
      relationships.add(createAssetRelationshipWrapperForRelationship(
          relationshipType, assetRelationshipClass, row.getString(METADATA), row.getString(SOURCE), wrapOptions));
    }

    return new RelationshipKeysetPage<>(relationships, scan.getMaxId(), scan.getNextCursor());
  }

  /**
   * Shared row-level keyset core for {@link #findRelationshipsByKeyset} and
   * {@link #findRelationshipsV4ByKeyset}: captures the first-page {@code maxId} (largest row id when
   * paging starts) and database scan-start timestamp, runs the keyset SQL, validates strictly
   * increasing id progress and computes the next cursor. Callers resolve their own table
   * names/filters and map the returned rows. Supported only for {@code NEW_SCHEMA_ONLY};
   * {@code OLD_SCHEMA_ONLY} and {@code DUAL_SCHEMA} (deprecated) throw
   * {@link UnsupportedOperationException} before any SQL is built or executed.
   */
  @Nonnull
  private KeysetScanResult findRelationshipsByKeysetCore(@Nonnull final String relationshipTableName,
      @Nullable final String sourceTableName, @Nullable final LocalRelationshipFilter sourceEntityFilter,
      @Nullable final String destTableName, @Nullable final LocalRelationshipFilter destinationEntityFilter,
      @Nonnull final LocalRelationshipFilter relationshipFilter, final int pageSize,
      @Nullable final RelationshipKeysetCursor cursor) {
    if (pageSize < 1 || pageSize > MAX_KEYSET_PAGE_SIZE) {
      throw new IllegalArgumentException(
          "pageSize must be between 1 and " + MAX_KEYSET_PAGE_SIZE + " but was " + pageSize);
    }
    if (_schemaConfig != EbeanLocalDAO.SchemaConfig.NEW_SCHEMA_ONLY) {
      throw new UnsupportedOperationException(
          "Keyset pagination is only supported in NEW_SCHEMA_ONLY mode; OLD_SCHEMA_ONLY and DUAL_SCHEMA "
              + "(deprecated) are rejected.");
    }
    final long lastId;
    final long maxId;
    final String scanStartTime;
    if (cursor == null) {
      lastId = 0L;
      final KeysetScanStart scanStart = fetchScanStart(relationshipTableName);
      maxId = scanStart.getMaxId();
      scanStartTime = scanStart.getScanStartTime();
    } else {
      validateKeysetCursorTable(cursor, relationshipTableName);
      lastId = cursor.getLastId();
      maxId = cursor.getMaxId();
      scanStartTime = cursor.getScanStartTime();
    }

    // Nothing left to scan (empty table, or the previous page reached maxId).
    if (lastId >= maxId) {
      return new KeysetScanResult(Collections.emptyList(), maxId, null);
    }

    final String currentSql = buildFindRelationshipKeysetCurrentSQL(relationshipTableName, relationshipFilter,
        sourceTableName, sourceEntityFilter, destTableName, destinationEntityFilter, pageSize, lastId, maxId);

    // Query current rows first. If a row is soft-deleted between the two reads, Query B may also see
    // the same id; mergeKeysetRows dedups that id. Running B first could turn the race into a drop.
    final List<SqlRow> currentRows = executeSqlWithIndexCheck(currentSql, relationshipTableName);
    // No-op seam that tests override to inject a soft-delete between Query A and Query B.
    afterKeysetCurrentRowsFetched(relationshipTableName, currentRows);
    // Cap Query B at Query A's frontier when A filled the page. Query B is itself
    // ORDER BY id LIMIT pageSize, so this bounds per-page work rather than changing which rows the
    // merge selects: ids above A's last id would lose to lower ids in the merge regardless.
    final long deletedUpperId = currentRows.size() == pageSize ? currentRows.get(currentRows.size() - 1).getLong("id")
        : maxId;
    final String deletedSinceScanStartSql = buildFindRelationshipKeysetDeletedSinceScanStartSQL(
        relationshipTableName, relationshipFilter, sourceTableName, sourceEntityFilter, destTableName,
        destinationEntityFilter, pageSize, lastId, deletedUpperId, scanStartTime);
    final List<SqlRow> deletedSinceScanStartRows =
        executeSqlWithIndexCheck(deletedSinceScanStartSql, relationshipTableName, scanStartTime);
    final List<SqlRow> rows = mergeKeysetRows(currentRows, deletedSinceScanStartRows, pageSize);

    long previousId = lastId;
    long lastRowId = lastId;
    for (SqlRow row : rows) {
      final long id = row.getLong("id");
      if (id <= previousId) {
        throw new IllegalStateException(String.format(
            "Relationship ids must strictly increase during keyset pagination but saw id %d after %d in table '%s'",
            id, previousId, relationshipTableName));
      }
      previousId = id;
      lastRowId = id;
    }

    // A next cursor is only warranted when the page was full and did not reach maxId.
    RelationshipKeysetCursor nextCursor = null;
    if (rows.size() == pageSize && lastRowId < maxId) {
      nextCursor = new RelationshipKeysetCursor(lastRowId, maxId, scanStartTime, relationshipTableName);
    }

    return new KeysetScanResult(rows, maxId, nextCursor);
  }

  private static void validateKeysetCursorTable(@Nonnull RelationshipKeysetCursor cursor,
      @Nonnull String relationshipTableName) {
    final String cursorRelationshipTableName = cursor.getRelationshipTableName();
    if (!cursorRelationshipTableName.equals(relationshipTableName)) {
      throw new IllegalArgumentException("Relationship keyset cursor belongs to table '"
          + cursorRelationshipTableName + "' but this query targets table '" + relationshipTableName + "'.");
    }
  }

  @Nonnull
  private static List<SqlRow> mergeKeysetRows(@Nonnull List<SqlRow> currentRows,
      @Nonnull List<SqlRow> deletedSinceScanStartRows, int pageSize) {
    List<SqlRow> merged = new ArrayList<>(Math.min(pageSize, currentRows.size() + deletedSinceScanStartRows.size()));
    int currentIndex = 0;
    int deletedIndex = 0;
    while (merged.size() < pageSize
        && (currentIndex < currentRows.size() || deletedIndex < deletedSinceScanStartRows.size())) {
      if (currentIndex >= currentRows.size()) {
        merged.add(deletedSinceScanStartRows.get(deletedIndex++));
      } else if (deletedIndex >= deletedSinceScanStartRows.size()) {
        merged.add(currentRows.get(currentIndex++));
      } else {
        long currentId = currentRows.get(currentIndex).getLong("id");
        long deletedId = deletedSinceScanStartRows.get(deletedIndex).getLong("id");
        if (currentId == deletedId) {
          merged.add(currentRows.get(currentIndex++));
          deletedIndex++;
        } else if (currentId < deletedId) {
          merged.add(currentRows.get(currentIndex++));
        } else {
          merged.add(deletedSinceScanStartRows.get(deletedIndex++));
        }
      }
    }
    return merged;
  }

  @VisibleForTesting
  protected void afterKeysetCurrentRowsFetched(@Nonnull String relationshipTableName,
      @Nonnull List<SqlRow> currentRows) {
    // Test seam for deterministic simulation of a soft-delete between Query A and Query B.
  }

  /**
   * Immutable holder for one keyset scan's largest row id and database start time.
   */
  private static final class KeysetScanStart {
    private final long _maxId;
    private final String _scanStartTime;

    KeysetScanStart(long maxId, @Nonnull String scanStartTime) {
      _maxId = maxId;
      if (StringUtils.isBlank(scanStartTime)) {
        throw new IllegalArgumentException("scanStartTime must not be null or empty");
      }
      _scanStartTime = scanStartTime;
    }

    long getMaxId() {
      return _maxId;
    }

    @Nonnull
    String getScanStartTime() {
      return _scanStartTime;
    }
  }

  /**
   * Immutable holder for one keyset page's raw rows, {@code maxId} and next cursor.
   */
  private static final class KeysetScanResult {
    private final List<SqlRow> _rows;
    private final long _maxId;
    private final RelationshipKeysetCursor _nextCursor;

    KeysetScanResult(@Nonnull List<SqlRow> rows, long maxId,
        @Nullable RelationshipKeysetCursor nextCursor) {
      _rows = rows;
      _maxId = maxId;
      _nextCursor = nextCursor;
    }

    @Nonnull
    List<SqlRow> getRows() {
      return _rows;
    }

    long getMaxId() {
      return _maxId;
    }

    @Nullable
    RelationshipKeysetCursor getNextCursor() {
      return _nextCursor;
    }
  }

  /**
   * Returns the largest relationship row id ({@code maxId}) and the database timestamp for the
   * start of the scan. Both values come from one statement so the timestamp and max id are
   * captured against the same DB clock.
   */
  private KeysetScanStart fetchScanStart(@Nonnull final String relationshipTableName) {
    final String sql =
        "SELECT COALESCE(MAX(id), 0) AS max_id, DATE_FORMAT(NOW(6), '%Y-%m-%d %H:%i:%s.%f') AS scan_start_time FROM "
            + relationshipTableName;
    final SqlRow row = _server.createSqlQuery(sql).findOne();
    if (row == null) {
      throw new IllegalStateException("Scan-start query returned no rows for table " + relationshipTableName);
    }
    return new KeysetScanStart(row.getLong("max_id"), row.getString("scan_start_time"));
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
    validateAssetRelationshipWrapOptions(wrapOptions, "findRelationshipsV3");

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
    validateAssetRelationshipWrapOptions(wrapOptions, "findRelationshipsV4");

    List<SqlRow> sqlRows = findRelationshipsV2V3V4Core(
        sourceEntityType, sourceEntityFilter, destinationEntityType, destinationEntityFilter,
        relationshipType, relationshipFilter, offset, count, relationshipLookUpContext, true);

    return sqlRows.stream()
        .map(row -> createAssetRelationshipWrapperForRelationship(
            relationshipType, assetRelationshipClass, row.getString(METADATA), row.getString(SOURCE), wrapOptions))
        .collect(Collectors.toList());
  }

  /**
   * Validates that {@code wrapOptions} carries the AssetRelationship return-type marker required by
   * the V3/V4 asset-relationship APIs. Shared by {@link #findRelationshipsV3}, {@link #findRelationshipsV4}
   * and {@link #findRelationshipsV4ByKeyset} to avoid duplicating the contract check.
   *
   * @param wrapOptions the wrap options supplied by the caller
   * @param methodName the calling method name, surfaced in the error message
   * @throws IllegalArgumentException when {@code wrapOptions} is null or does not carry
   *         {@code RELATIONSHIP_RETURN_TYPE} set to {@code MG_INTERNAL_ASSET_RELATIONSHIP_TYPE}.
   */
  private static void validateAssetRelationshipWrapOptions(@Nullable Map<String, Object> wrapOptions,
      @Nonnull String methodName) {
    if (wrapOptions == null || !wrapOptions.containsKey(RELATIONSHIP_RETURN_TYPE)
        || !MG_INTERNAL_ASSET_RELATIONSHIP_TYPE.equals(wrapOptions.get(RELATIONSHIP_RETURN_TYPE))) {
      throw new IllegalArgumentException(methodName + " requires wrapOptions to carry key '"
          + RELATIONSHIP_RETURN_TYPE + "' set to '" + MG_INTERNAL_ASSET_RELATIONSHIP_TYPE + "'.");
    }
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

    List<Triplet<LocalRelationshipFilter, String, String>> filters = new ArrayList<>();

    if (_schemaConfig == EbeanLocalDAO.SchemaConfig.NEW_SCHEMA_ONLY || _schemaConfig == EbeanLocalDAO.SchemaConfig.DUAL_SCHEMA) {
      if (destTableName != null) {
        sqlBuilder.append("INNER JOIN ").append(destTableName).append(" dt ON dt.urn=rt.destination ");

        if (destinationEntityFilter != null) {
          filters.add(new Triplet<>(destinationEntityFilter, "dt", destTableName));
        }
      } else if (destinationEntityFilter != null) {
        validateEntityFilterOnlyOneUrn(destinationEntityFilter);
        // non-mg entity case, applying dest filter on relationship table
        filters.add(new Triplet<>(destinationEntityFilter, "rt", relationshipTableName));
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
          filters.add(new Triplet<>(sourceEntityFilter, "st", sourceTableName));
        }
      }

      if (!includeNonCurrentRelationships) {
        sqlBuilder.append("WHERE rt.deleted_ts is NULL");
      }

      filters.add(new Triplet<>(relationshipFilter, "rt", relationshipTableName));

      String whereClause = SQLStatementUtils.whereClause(SUPPORTED_CONDITIONS,
          _eBeanDAOConfig.isNonDollarVirtualColumnsEnabled(), _schemaValidatorUtil,
          filters.toArray(new Triplet[filters.size()]));

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
   * Keyset (seek) pagination counterpart of {@link #buildFindRelationshipSQL} for rows that are
   * still current: same join/filter construction plus {@code rt.deleted_ts IS NULL}, keyset bounds
   * ({@code rt.id > lastId AND rt.id <= maxId}, {@code maxId} being the largest relationship row id
   * when paging starts), ascending id order and {@code LIMIT pageSize}. Kept separate to leave
   * {@link #buildFindRelationshipSQL} untouched.
   *
   * <p>Ranked/non-current pagination is unsupported because it could not bound the per-page DB
   * work.</p>
   *
   * <p>Supported only for {@code NEW_SCHEMA_ONLY}; {@code OLD_SCHEMA_ONLY} and {@code DUAL_SCHEMA}
   * (deprecated) throw {@link UnsupportedOperationException} before any SQL is built.</p>
   */
  @Nonnull
  @VisibleForTesting
  public String buildFindRelationshipKeysetCurrentSQL(@Nonnull final String relationshipTableName,
      @Nonnull LocalRelationshipFilter relationshipFilter, @Nullable final String sourceTableName,
      @Nullable LocalRelationshipFilter sourceEntityFilter, @Nullable final String destTableName,
      @Nullable LocalRelationshipFilter destinationEntityFilter, int pageSize, long lastId, long maxId) {
    return buildFindRelationshipKeysetSQL(relationshipTableName, relationshipFilter, sourceTableName, sourceEntityFilter,
        destTableName, destinationEntityFilter, pageSize, lastId, maxId, "rt.deleted_ts is NULL");
  }

  /**
   * Supplementary keyset query for rows that were current when the scan started but were
   * soft-deleted before this page is read.
   */
  @Nonnull
  @VisibleForTesting
  public String buildFindRelationshipKeysetDeletedSinceScanStartSQL(@Nonnull final String relationshipTableName,
      @Nonnull LocalRelationshipFilter relationshipFilter, @Nullable final String sourceTableName,
      @Nullable LocalRelationshipFilter sourceEntityFilter, @Nullable final String destTableName,
      @Nullable LocalRelationshipFilter destinationEntityFilter, int pageSize, long lastId, long maxId,
      @Nonnull String scanStartTime) {
    if (StringUtils.isBlank(scanStartTime)) {
      throw new IllegalArgumentException("scanStartTime must not be null or empty");
    }
    return buildFindRelationshipKeysetSQL(relationshipTableName, relationshipFilter, sourceTableName, sourceEntityFilter,
        destTableName, destinationEntityFilter, pageSize, lastId, maxId,
        "rt.deleted_ts > STR_TO_DATE(:scanStartTime, '%Y-%m-%d %H:%i:%s.%f')");
  }

  @Nonnull
  private String buildFindRelationshipKeysetSQL(@Nonnull final String relationshipTableName,
      @Nonnull LocalRelationshipFilter relationshipFilter, @Nullable final String sourceTableName,
      @Nullable LocalRelationshipFilter sourceEntityFilter, @Nullable final String destTableName,
      @Nullable LocalRelationshipFilter destinationEntityFilter, int pageSize, long lastId, long maxId,
      @Nonnull String deletedTsPredicate) {
    if (pageSize < 1 || pageSize > MAX_KEYSET_PAGE_SIZE) {
      throw new IllegalArgumentException(
          "pageSize must be between 1 and " + MAX_KEYSET_PAGE_SIZE + " but was " + pageSize);
    }
    if (_schemaConfig != EbeanLocalDAO.SchemaConfig.NEW_SCHEMA_ONLY) {
      throw new UnsupportedOperationException(
          "Keyset pagination is only supported in NEW_SCHEMA_ONLY mode; OLD_SCHEMA_ONLY and DUAL_SCHEMA "
              + "(deprecated) are rejected.");
    }

    relationshipFilter = LogicalExpressionLocalRelationshipCriterionUtils.normalizeLocalRelationshipFilter(relationshipFilter);
    sourceEntityFilter = LogicalExpressionLocalRelationshipCriterionUtils.normalizeLocalRelationshipFilter(sourceEntityFilter);
    destinationEntityFilter = LogicalExpressionLocalRelationshipCriterionUtils.normalizeLocalRelationshipFilter(destinationEntityFilter);

    // Cursor bounds are validated non-negative longs, so inlining them is safe and matches the
    // existing flow that inlines filter values (including colon-bearing urns) without named params.
    final String maxIdPredicate = "rt.id <= " + maxId;
    final String lastIdPredicate = "rt.id > " + lastId;
    final StringBuilder sqlBuilder = new StringBuilder();

    sqlBuilder.append("SELECT rt.*");
    sqlBuilder.append(" FROM ").append(relationshipTableName).append(" rt ");

    final List<Triplet<LocalRelationshipFilter, String, String>> filters = new ArrayList<>();

    if (_schemaConfig == EbeanLocalDAO.SchemaConfig.NEW_SCHEMA_ONLY) {
      if (destTableName != null) {
        sqlBuilder.append("INNER JOIN ").append(destTableName).append(" dt ON dt.urn=rt.destination ");

        if (destinationEntityFilter != null) {
          filters.add(new Triplet<>(destinationEntityFilter, "dt", destTableName));
        }
      } else if (destinationEntityFilter != null) {
        validateEntityFilterOnlyOneUrn(destinationEntityFilter);
        filters.add(new Triplet<>(destinationEntityFilter, "rt", relationshipTableName));
      }

      if (sourceTableName != null) {
        sqlBuilder.append("INNER JOIN ").append(sourceTableName).append(" st ON st.urn=rt.source ");

        if (sourceEntityFilter != null) {
          filters.add(new Triplet<>(sourceEntityFilter, "st", sourceTableName));
        }
      }

      filters.add(new Triplet<>(relationshipFilter, "rt", relationshipTableName));

      final String whereClause = SQLStatementUtils.whereClause(SUPPORTED_CONDITIONS,
          _eBeanDAOConfig.isNonDollarVirtualColumnsEnabled(), _schemaValidatorUtil,
          filters.toArray(new Triplet[filters.size()]));

      sqlBuilder.append("WHERE ").append(deletedTsPredicate);
      if (whereClause != null) {
        sqlBuilder.append(" AND ").append(whereClause);
      }
      sqlBuilder.append(" AND ").append(lastIdPredicate).append(" AND ").append(maxIdPredicate);
    } else {
      // OLD_SCHEMA_ONLY and DUAL_SCHEMA are rejected above; only NEW_SCHEMA_ONLY is supported here.
      throw new RuntimeException("The schema config must be set to NEW_SCHEMA_ONLY.");
    }

    sqlBuilder.append(" ORDER BY rt.id ASC LIMIT ").append(pageSize);

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
      throwIfMissingIndex(e, relationshipTableName);
      throw new RuntimeException("Failed to execute SQL query for relationships", e);
    }
  }

  private List<SqlRow> executeSqlWithIndexCheck(String sql, String relationshipTableName,
      @Nonnull String scanStartTime) {
    try {
      SqlQuery query = _server.createSqlQuery(sql);
      query.setParameter("scanStartTime", scanStartTime);
      return query.findList();
    } catch (PersistenceException e) {
      throwIfMissingIndex(e, relationshipTableName);
      throw new RuntimeException("Failed to execute SQL query for relationships", e);
    }
  }

  private void throwIfMissingIndex(PersistenceException e, String relationshipTableName) {
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
  }

}
