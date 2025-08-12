package com.linkedin.metadata.dao.utils;

import com.google.common.escape.Escaper;
import com.google.common.escape.Escapers;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.query.AspectField;
import com.linkedin.metadata.query.Condition;
import com.linkedin.metadata.query.IndexFilter;
import com.linkedin.metadata.query.IndexGroupByCriterion;
import com.linkedin.metadata.query.LocalRelationshipCriterion;
import com.linkedin.metadata.query.LocalRelationshipCriterionArray;
import com.linkedin.metadata.query.LocalRelationshipFilter;
import com.linkedin.metadata.query.LocalRelationshipValue;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.javatuples.Pair;
import pegasus.com.linkedin.metadata.query.LogicalExpressionLocalRelationshipCriterion;
import pegasus.com.linkedin.metadata.query.LogicalExpressionLocalRelationshipCriterionArray;
import pegasus.com.linkedin.metadata.query.LogicalOperation;
import pegasus.com.linkedin.metadata.query.innerLogicalOperation.Operator;

import static com.linkedin.metadata.dao.utils.LogicalExpressionLocalRelationshipCriterionUtils.*;
import static com.linkedin.metadata.dao.utils.SQLIndexFilterUtils.*;
import static com.linkedin.metadata.dao.utils.SQLSchemaUtils.*;


/**
 * SQL statement util class to generate executable SQL query / execution statements.
 */
@Slf4j
public class SQLStatementUtils {
  private static final Escaper URN_ESCAPER = Escapers.builder()
      .addEscape('\'', "''")
      .addEscape('\\', "\\\\").build();

  public static final String SOFT_DELETED_CHECK = "JSON_EXTRACT(%s, '$.gma_deleted') IS NULL"; // true when not soft deleted

  public static final String DELETED_TS_IS_NULL_CHECK = "deleted_ts IS NULL"; // true when the deleted_ts is NULL, meaning the record is not soft deleted

  public static final String NONNULL_CHECK = "%s IS NOT NULL"; // true when the value of aspect_column is not NULL

  // Build manual SQL update query to enable optimistic locking on a given column
  // Optimistic locking is supported on ebean using @version, see https://ebean.io/docs/mapping/jpa/version
  // But we can't use @version annotation for optimistic locking for two reasons:
  //   1. That prevents flag guarding optimistic locking feature
  //   2. When using @version annotation, Ebean starts to override all updates to that column
  //      by disregarding any user change.
  // Ideally, another column for the sake of optimistic locking would be preferred but that means a change to
  // metadata_aspect schema and we don't take this route here to keep this change backward compatible.
  public static final String OPTIMISTIC_LOCKING_UPDATE_SQL = "UPDATE metadata_aspect "
      + "SET urn = :urn, aspect = :aspect, version = :version, metadata = :metadata, createdOn = :createdOn, createdBy = :createdBy "
      + "WHERE urn = :urn and aspect = :aspect and version = :version";

  private static final String SQL_UPSERT_ASPECT_TEMPLATE =
      "INSERT INTO %s (urn, %s, lastmodifiedon, lastmodifiedby) VALUE (:urn, :metadata, :lastmodifiedon, :lastmodifiedby) "
          + "ON DUPLICATE KEY UPDATE %s = :metadata, lastmodifiedon = :lastmodifiedon, deleted_ts = NULL;";

  // Note: include a_urn in the duplicate key update for backfilling the a_urn column in updateEntityTables() if the column was
  // added after data already exists in the entity table. Normally, a_urn never needs to get updated once it's set the first time.
  // Note: this SQL should only be called if the urnExtraction flag is true (i.e. a_urn column exists)
  private static final String SQL_UPSERT_ASPECT_WITH_URN_TEMPLATE =
      "INSERT INTO %s (urn, a_urn, %s, lastmodifiedon, lastmodifiedby) VALUE (:urn, :a_urn, :metadata, :lastmodifiedon, :lastmodifiedby) "
          + "ON DUPLICATE KEY UPDATE %s = :metadata, lastmodifiedon = :lastmodifiedon, a_urn = :a_urn, deleted_ts = NULL;";

  // INSERT prefix of the sql statement for inserting into metadata_aspect table with multiple aspects which will be combined with the VALUES suffix
  public static final String SQL_INSERT_INTO_ASSET_WITH_URN = "INSERT INTO %s (urn, a_urn, lastmodifiedon, lastmodifiedby,";
  // VALUES suffix of the sql statement for inserting into metadata_aspect table with multiple aspects which will be combined with the INSERT prefix
  public static final String SQL_INSERT_ASSET_VALUES_WITH_URN = "VALUES (:urn, :a_urn, :lastmodifiedon, :lastmodifiedby,";
  // INSERT prefix of the sql statement for inserting into metadata_aspect table with multiple aspects which will be combined with the VALUES suffix
  public static final String SQL_INSERT_INTO_ASSET = "INSERT INTO %s (urn, lastmodifiedon, lastmodifiedby,";
  // VALUES suffix of the sql statement for inserting into metadata_aspect table with multiple aspects which will be combined with the INSERT prefix
  public static final String SQL_INSERT_ASSET_VALUES = "VALUES (:urn, :lastmodifiedon, :lastmodifiedby,";
  // Delete prefix of the sql statement for deleting from metadata_aspect table
  public static final String SQL_SOFT_DELETE_ASSET_WITH_URN = "UPDATE %s SET deleted_ts = NOW() WHERE urn = '%s';";
  // closing bracket for the sql statement INSERT prefix
  // e.g. INSERT INTO metadata_aspect (urn, a_urn, lastmodifiedon, lastmodifiedby)
  public static final String CLOSING_BRACKET = ") ";
  // deleted_ts check on create Statement SQL. This is used to set the deleted_ts to a non-null value
  // If a record that is NOT marked for deletion is attempted to be created again, an exception will be thrown
  public static final String DELETED_TS_DUPLICATE_KEY_CHECK = "ON DUPLICATE KEY UPDATE ";
  public static final String DELETED_TS_SET_VALUE_CONDITIONALLY = ", deleted_ts = IF(deleted_ts IS NULL, CAST('DuplicateKeyException' AS UNSIGNED), NULL);";
  // "JSON_EXTRACT(%s, '$.gma_deleted') IS NOT NULL" is used to exclude soft-deleted entity which has no lastmodifiedon.
  // for details, see the known limitations on https://github.com/linkedin/datahub-gma/pull/311. Same reason for
  // SQL_UPDATE_ASPECT_WITH_URN_TEMPLATE
  private static final String SQL_UPDATE_ASPECT_TEMPLATE =
      "UPDATE %s SET %s = :metadata, lastmodifiedon = :lastmodifiedon, lastmodifiedby = :lastmodifiedby "
          + "WHERE urn = :urn and (JSON_EXTRACT(%s, '$.lastmodifiedon') = :oldTimestamp OR JSON_EXTRACT(%s, '$.gma_deleted') IS NOT NULL);";

  private static final String SQL_UPDATE_ASPECT_WITH_URN_TEMPLATE =
      "UPDATE %s SET %s = :metadata, a_urn = :a_urn, lastmodifiedon = :lastmodifiedon, lastmodifiedby = :lastmodifiedby "
          + "WHERE urn = :urn and (JSON_EXTRACT(%s, '$.lastmodifiedon') = :oldTimestamp OR JSON_EXTRACT(%s, '$.gma_deleted') IS NOT NULL);";

  private static final String SQL_UPDATE_ASPECT_TEMPLATE_WITH_SOFT_DELETE_OVERWRITE =
      "UPDATE %s SET %s = :metadata, lastmodifiedon = :lastmodifiedon, lastmodifiedby = :lastmodifiedby "
          + "WHERE urn = :urn ;";

  private static final String SQL_UPDATE_ASPECT_WITH_URN_TEMPLATE_WITH_SOFT_DELETE_OVERWRITE = "UPDATE %s SET %s = "
      + ":metadata, a_urn = :a_urn, lastmodifiedon = :lastmodifiedon, lastmodifiedby = :lastmodifiedby WHERE urn = :urn;";

  private static final String SQL_READ_ASPECT_TEMPLATE =
      String.format("SELECT urn, %%s, lastmodifiedon, lastmodifiedby FROM %%s WHERE %s AND urn IN (", SOFT_DELETED_CHECK);

  private static final String SQL_LIST_ASPECT_BY_URN_TEMPLATE =
      String.format("SELECT urn, %%s, lastmodifiedon, lastmodifiedby, createdfor FROM %%s WHERE urn = '%%s' AND %s AND %s", NONNULL_CHECK, SOFT_DELETED_CHECK);

  private static final String SQL_LIST_ASPECT_BY_URN_WITH_SOFT_DELETED_TEMPLATE =
      String.format("SELECT urn, %%s, lastmodifiedon, lastmodifiedby, createdfor FROM %%s WHERE urn = '%%s' AND %s", NONNULL_CHECK);

  private static final String SQL_LIST_ASPECT_WITH_PAGINATION_TEMPLATE =
      String.format("SELECT urn, %%s, lastmodifiedon, lastmodifiedby, createdfor, (SELECT COUNT(urn) FROM %%s WHERE %s AND %s) "
          + "as _total_count FROM %%s WHERE %s AND %s LIMIT %%s OFFSET %%s", NONNULL_CHECK, SOFT_DELETED_CHECK, NONNULL_CHECK, SOFT_DELETED_CHECK);

  private static final String SQL_LIST_ASPECT_WITH_PAGINATION_WITH_SOFT_DELETED_TEMPLATE =
      String.format("SELECT urn, %%s, lastmodifiedon, lastmodifiedby, createdfor, (SELECT COUNT(urn) FROM %%s WHERE %s) "
          + "as _total_count FROM %%s WHERE %s LIMIT %%s OFFSET %%s", NONNULL_CHECK,  NONNULL_CHECK);

  private static final String SQL_READ_ASPECT_WITH_SOFT_DELETED_TEMPLATE =
      "SELECT urn, %s, lastmodifiedon, lastmodifiedby FROM %s WHERE urn IN (";

  private static final String INDEX_GROUP_BY_CRITERION = "SELECT count(*) as COUNT, %s FROM %s";

  private static final String SQL_GET_ALL_COLUMNS =
      "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = database() AND TABLE_NAME = '%s'";

  private static final String SQL_URN_EXIST_TEMPLATE = "SELECT urn FROM %s WHERE urn = '%s' AND deleted_ts IS NULL";

  private static final String INSERT_LOCAL_RELATIONSHIPS = "INSERT INTO %s (metadata, source, destination, source_type, "
      + "destination_type, lastmodifiedon, lastmodifiedby) VALUES ";

  private static final String INSERT_LOCAL_RELATIONSHIPS_VALUES = "(:metadata%1$d, :source%1$d, :destination%1$d, :source_type%1$d,"
      + " :destination_type%1$d, :lastmodifiedon, :lastmodifiedby)";

  private static final String INSERT_LOCAL_RELATIONSHIPS_WITH_ASPECT = "INSERT INTO %s (metadata, source, destination, source_type, "
      + "destination_type, lastmodifiedon, lastmodifiedby, aspect) VALUES ";

  private static final String INSERT_LOCAL_RELATIONSHIPS_WITH_ASPECT_VALUES = "(:metadata%1$d, :source%1$d, :destination%1$d, :source_type%1$d,"
      + " :destination_type%1$d, :lastmodifiedon, :lastmodifiedby, :aspect)";

  private static final String DELETE_BY_SOURCE = "UPDATE %s SET deleted_ts=NOW() "
      + "WHERE source = :source AND deleted_ts IS NULL";

  private static final String DELETE_BY_SOURCE_AND_ASPECT = "UPDATE %s SET deleted_ts=NOW() "
      + "WHERE source = :source AND (aspect = :aspect OR aspect = :pegasus_aspect) AND deleted_ts IS NULL";

  /**
   *  Filter query has pagination params in the existing APIs. To accommodate this, we use subquery to include total result counts in the query response.
   *  For example, we will build the following filter query statement:
   *
   *  <p>SELECT *, (SELECT COUNT(urn) FROM metadata_entity_foo WHERE i_aspectfoo$value >= 25\n"
   *  AND i_aspectfoo$value < 50 AND JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL) as _total_count FROM metadata_entity_foo\n"
   *  WHERE i_aspectfoo$value >= 25 AND i_aspectfoo$value < 50 AND JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL;
   */
  private static final String SQL_FILTER_TEMPLATE = "SELECT *, (%s) as _total_count FROM %s";
  private static final String SQL_BROWSE_ASPECT_TEMPLATE =
      String.format("SELECT urn, %%s, lastmodifiedon, lastmodifiedby, (SELECT COUNT(urn) FROM %%s) as _total_count "
          + "FROM %%s WHERE %s LIMIT %%d OFFSET %%d", SOFT_DELETED_CHECK);

  private static final String GET_ESTIMATED_COUNT = "select table_rows from information_schema.tables where "
      + "table_schema = '%s' and table_name = '%s'";

  public static final String SOURCE = "source";
  public static final String DESTINATION = "destination";
  private static final String RIGHT_PARENTHESIS = ")";

  private SQLStatementUtils() {
    // Util class
  }

  /**
   * Get estimated row count for a table.
   */
  public static String getEstimatedRowCount(String db, String table) {
    return String.format(GET_ESTIMATED_COUNT, db, table);
  }

  /**
   * Create entity exist SQL statement.
   * @param urn entity urn
   * @return entity exist sql
   */
  public static String createExistSql(@Nonnull Urn urn) {
    final String tableName = getTableName(urn);
    return String.format(SQL_URN_EXIST_TEMPLATE, tableName, escapeReservedCharInUrn(urn.toString()));
  }

  /**
   * Create read aspect SQL statement for one aspect class (but could include many urns). Essentially, this will query for a
   * single aspect column in the metadata entity tables. The query includes a filter for filtering out soft-deleted aspects.
   *
   * <p>Example:
   * SELECT urn, aspect1, lastmodifiedon, lastmodifiedby FROM metadata_entity_foo WHERE aspect1 != '{"gma_deleted":true}' AND urn IN ('urn:1', 'urn:2')
   * </p>
   * @param aspectClass aspect class to query for
   * @param urns a Set of Urns to query for
   * @param includeSoftDeleted a flag to include soft deleted records
   * @param <ASPECT> aspect type
   * @return aspect read sql statement for a single aspect (across multiple tables and urns)
   */
  public static <ASPECT extends RecordTemplate> String createAspectReadSql(@Nonnull Class<ASPECT> aspectClass,
      @Nonnull Set<Urn> urns, boolean includeSoftDeleted, boolean isTestMode) {
    if (urns.size() == 0) {
      throw new IllegalArgumentException("Need at least 1 urn to query.");
    }

    StringBuilder stringBuilder = new StringBuilder();

    final Urn firstUrn = urns.iterator().next();
    final String columnName = getAspectColumnName(firstUrn.getEntityType(), aspectClass);
    final String tableName = isTestMode ? getTestTableName(firstUrn) : getTableName(firstUrn);
    // Generate URN list for IN clause
    String urnList = urns.stream()
        .map(urn -> "'" + escapeReservedCharInUrn(urn.toString()) + "'")
        .collect(Collectors.joining(", "));
    final String sqlTemplate =
        includeSoftDeleted ? SQL_READ_ASPECT_WITH_SOFT_DELETED_TEMPLATE : SQL_READ_ASPECT_TEMPLATE;
    stringBuilder.append(String.format(sqlTemplate, columnName, tableName, columnName));
    stringBuilder.append(urnList);
    stringBuilder.append(RIGHT_PARENTHESIS);
    stringBuilder.append(" AND ");
    stringBuilder.append(DELETED_TS_IS_NULL_CHECK);
    return stringBuilder.toString();
  }

  /**
   * List all the aspect record (0 or 1) for a given entity urn and aspect type.
   * @param aspectClass aspect type
   * @param urn entity urn
   * @param includeSoftDeleted whether to include soft deleted aspects
   * @param <ASPECT> aspect type
   * @return a SQL to run listing aspect query
   */
  public static <ASPECT extends RecordTemplate> String createListAspectByUrnSql(@Nonnull Class<ASPECT> aspectClass,
      @Nonnull Urn urn, boolean includeSoftDeleted) {
    final String columnName = getAspectColumnName(urn.getEntityType(), aspectClass);
    final String tableName = getTableName(urn);
    if (includeSoftDeleted) {
      return String.format(SQL_LIST_ASPECT_BY_URN_WITH_SOFT_DELETED_TEMPLATE, columnName, tableName,
          escapeReservedCharInUrn(urn.toString()), columnName);
    } else {
      return String.format(SQL_LIST_ASPECT_BY_URN_TEMPLATE, columnName, tableName,
          escapeReservedCharInUrn(urn.toString()), columnName, columnName);
    }
  }

  /**
   * List all the aspects for a given entity type and aspect type.
   * @param aspectClass aspect type
   * @param entityType entity name from Urn
   * @param includeSoftDeleted whether to include soft deleted aspects
   * @param start pagination offset
   * @param pageSize page size
   * @param <ASPECT> aspect type
   * @return a SQL to run listing aspect query with pagination.
   */
  public static <ASPECT extends RecordTemplate> String createListAspectWithPaginationSql(@Nonnull Class<ASPECT> aspectClass,
      String entityType, boolean includeSoftDeleted, int start, int pageSize) {
    final String tableName = getTableName(entityType);
    final String columnName = getAspectColumnName(entityType, aspectClass);
    if (includeSoftDeleted) {
      return String.format(SQL_LIST_ASPECT_WITH_PAGINATION_WITH_SOFT_DELETED_TEMPLATE, columnName, tableName,
          columnName, tableName, columnName, pageSize, start);
    } else {
      return String.format(SQL_LIST_ASPECT_WITH_PAGINATION_TEMPLATE, columnName, tableName, columnName, columnName,
          tableName, columnName, columnName, pageSize, start);
    }
  }

  /**
   * Create Upsert SQL statement.
   * @param urn  entity urn
   * @param <ASPECT> aspect type
   * @param aspectClass aspect class
   * @param isTestMode whether the test mode is enabled or not
   * @return aspect upsert sql
   */
  public static <ASPECT extends RecordTemplate> String createAspectUpsertSql(@Nonnull Urn urn,
      @Nonnull Class<ASPECT> aspectClass, boolean urnExtraction, boolean isTestMode) {
    final String tableName = isTestMode ? getTestTableName(urn) : getTableName(urn);
    final String columnName = getAspectColumnName(urn.getEntityType(), aspectClass);
    return String.format(urnExtraction ? SQL_UPSERT_ASPECT_WITH_URN_TEMPLATE : SQL_UPSERT_ASPECT_TEMPLATE, tableName, columnName, columnName);
  }

  /**
   * Create Delete SQL statement.
   * @param urn entity urn
   * @param isTestMode whether the test mode is enabled or not
   * @return delete sql
   */
  public static <ASPECT extends RecordTemplate> String createSoftDeleteAssetSql(@Nonnull Urn urn, boolean isTestMode) {
    final String tableName = isTestMode ? getTestTableName(urn) : getTableName(urn);
    return String.format(SQL_SOFT_DELETE_ASSET_WITH_URN, tableName, urn);
  }

  /**
   * Create Update with optimistic locking SQL statement. The SQL UPDATE use old_timestamp as a compareAndSet to check
   * if the current update is made on an unchange record. For example: UPDATE table WHERE modifiedon = :oldTimestamp.
   *
   * @param <ASPECT>            aspect type
   * @param urn                 entity urn
   * @param aspectClass         aspect class
   * @param isTestMode          whether the test mode is enabled or not
   * @param softDeleteOverwrite whether to overwrite soft deleted aspects marked with $gma_deleted when doing soft
   *                            delete by setting deleted_ts=NOW()
   * @return aspect upsert sql
   */
  public static <ASPECT extends RecordTemplate> String createAspectUpdateWithOptimisticLockSql(@Nonnull Urn urn,
      @Nonnull Class<ASPECT> aspectClass, boolean urnExtraction, boolean isTestMode, boolean softDeleteOverwrite) {
    final String tableName = isTestMode ? getTestTableName(urn) : getTableName(urn);
    final String columnName = getAspectColumnName(urn.getEntityType(), aspectClass);
    if (softDeleteOverwrite) {
      return String.format(urnExtraction ? SQL_UPDATE_ASPECT_WITH_URN_TEMPLATE_WITH_SOFT_DELETE_OVERWRITE
          : SQL_UPDATE_ASPECT_TEMPLATE_WITH_SOFT_DELETE_OVERWRITE, tableName, columnName, columnName, columnName);
    } else {
      return String.format(urnExtraction ? SQL_UPDATE_ASPECT_WITH_URN_TEMPLATE : SQL_UPDATE_ASPECT_TEMPLATE, tableName,
          columnName, columnName, columnName);
    }
  }

  /**
   * Create filter SQL statement.
   * @param entityType entity type from urn
   * @param indexFilter index filter
   * @param hasTotalCount whether to calculate total count in SQL.
   * @param nonDollarVirtualColumnsEnabled  true if virtual column does not contain $, false otherwise
   * @return translated SQL where statement
   */
  public static String createFilterSql(String entityType, @Nullable IndexFilter indexFilter, boolean hasTotalCount, boolean nonDollarVirtualColumnsEnabled,
      @Nonnull SchemaValidatorUtil schemaValidator) {
    final String tableName = getTableName(entityType);
    String whereClause = parseIndexFilter(entityType, indexFilter, nonDollarVirtualColumnsEnabled, schemaValidator);
    String totalCountSql = String.format("SELECT COUNT(urn) FROM %s %s", tableName, whereClause);
    StringBuilder sb = new StringBuilder();

    if (hasTotalCount) {
      sb.append(String.format(SQL_FILTER_TEMPLATE, totalCountSql, tableName));
    } else {
      sb.append("SELECT urn FROM ").append(tableName);
    }

    sb.append("\n");
    sb.append(whereClause);
    return sb.toString();
  }

  /**
   * Create index group by SQL statement.
   * @param entityType entity type
   * @param indexFilter index filter
   * @param indexGroupByCriterion group by
   * @param nonDollarVirtualColumnsEnabled  true if virtual column does not contain $, false otherwise
   * @return translated group by SQL
   */
  public static String createGroupBySql(String entityType, @Nullable IndexFilter indexFilter,
      @Nonnull IndexGroupByCriterion indexGroupByCriterion, boolean nonDollarVirtualColumnsEnabled, @Nonnull SchemaValidatorUtil schemaValidator) {
    final String tableName = getTableName(entityType);
    final String columnName =
        getGeneratedColumnName(entityType, indexGroupByCriterion.getAspect(), indexGroupByCriterion.getPath(),
            nonDollarVirtualColumnsEnabled);
    // Check if the column exists in the schema
    if (!schemaValidator.columnExists(tableName, columnName)) {
      log.warn("Skipping group-by: column '{}' not found in table '{}'", columnName, tableName);
      return ""; // skip query generation
    }
    StringBuilder sb = new StringBuilder();
    sb.append(String.format(INDEX_GROUP_BY_CRITERION, columnName, tableName));
    sb.append("\n");
    sb.append(parseIndexFilter(entityType, indexFilter, nonDollarVirtualColumnsEnabled, schemaValidator));
    sb.append("\nGROUP BY ");
    sb.append(columnName);
    return sb.toString();
  }


  /**
   * Create aspect browse SQL statement.
   * @param entityType entity type.
   * @param aspectClass aspect class
   * @param <ASPECT> {@link RecordTemplate}
   * @return aspect browse SQL.
   */
  public static <ASPECT extends RecordTemplate> String createAspectBrowseSql(String entityType,
      Class<ASPECT> aspectClass, int offset, int pageSize) {
    final String tableName = getTableName(entityType);
    final String columnName = getAspectColumnName(entityType, aspectClass);
    return String.format(SQL_BROWSE_ASPECT_TEMPLATE, columnName, tableName, tableName, columnName,
        Math.max(pageSize, 0), Math.max(offset, 0));
  }

  /**
   * Generate the create SQL statement for inserting local relationships. There can be multiple relationships added in
   * a single statement. The SQL generated should look like the following, where N is the number of relationships to insert:
   *
   * <p>
   * INSERT INTO tableName (metadata, source, destination, source_type, destination_type, lastmodifiedon, lastmodifiedby, {aspect}) VALUES
   * (:metadata0, :source0, :destination0, :source_type0, :destination_type0, :lastmodifiedon, :lastmodifiedby, {:aspect}),
   * (:metadata1, :source1, :destination1, :source_type1, :destination_type1, :lastmodifiedon, :lastmodifiedby, {:aspect}),
   * ...
   * (:metadataN-1, :sourceN-1, :destinationN-1, :source_typeN-1, :destination_typeN-1, :lastmodifiedon, :lastmodifiedby, {:aspect})
   * </p>
   * @param tableName Name of the table where the local relation metadata will be inserted.
   * @param numRelationships Number of relationships to insert
   * @param useAspectColumn Whether to populate the aspect column during relationship insertion
   * @return SQL statement for inserting local relation.
   */
  public static String insertLocalRelationshipSQL(String tableName, int numRelationships, boolean useAspectColumn) {
    StringBuilder builder = new StringBuilder();
    builder.append(useAspectColumn ? String.format(INSERT_LOCAL_RELATIONSHIPS_WITH_ASPECT, tableName) : String.format(INSERT_LOCAL_RELATIONSHIPS, tableName));
    for (int i = 0; i < numRelationships; i++) {
      builder.append(useAspectColumn ? String.format(INSERT_LOCAL_RELATIONSHIPS_WITH_ASPECT_VALUES, i) : String.format(INSERT_LOCAL_RELATIONSHIPS_VALUES, i));
      if (i != numRelationships - 1) {
        builder.append(',');
      }
    }

    return builder.toString();
  }

  /**
   * Some chars such as single quote (') are reserved chars. We need to escape them.
   * @param strInSql String in SQL which could contain reserved chars.
   * @return String which has reserved chars escaped.
   */
  public static String escapeReservedCharInUrn(String strInSql) {
    return URN_ESCAPER.escape(strInSql);
  }

  @Nonnull
  @ParametersAreNonnullByDefault
  public static String deleteLocalRelationshipSQL(final String tableName, boolean useAspectColumn) {
    return useAspectColumn ? String.format(DELETE_BY_SOURCE_AND_ASPECT, tableName) : String.format(DELETE_BY_SOURCE, tableName);
  }

  /**
   * Construct where clause SQL from multiple filters. Return null if all filters are empty.
   * @param supportedConditions contains supported conditions such as EQUAL.
   * @param nonDollarVirtualColumnsEnabled  true if virtual column does not contain $, false otherwise
   * @param filters An array of pairs which are filter and table prefix.
   * @return sql that can be appended after where clause.
   */
  @SafeVarargs
  @Nullable
  public static String whereClause(@Nonnull Map<Condition, String> supportedConditions, boolean nonDollarVirtualColumnsEnabled,
      @Nonnull Pair<LocalRelationshipFilter, String>... filters) {
    List<String> andClauses = new ArrayList<>();
    for (Pair<LocalRelationshipFilter, String> filter : filters) {
      if (LogicalExpressionLocalRelationshipCriterionUtils.filterHasNonEmptyCriteria(filter.getValue0())) {
        andClauses.add("(" + whereClause(filter.getValue0(), supportedConditions, filter.getValue1(), nonDollarVirtualColumnsEnabled) + ")");
      }
    }
    if (andClauses.isEmpty()) {
      return null;
    } else if (andClauses.size() == 1) {
      return andClauses.get(0).substring(1, andClauses.get(0).length() - 1);
    } else {
      return String.join(" AND ", andClauses);
    }
  }

  /**
   * Construct where clause SQL from a filter. Throw IllegalArgumentException if filter is empty.
   * @param filter contains field, condition and value
   * @param supportedConditions contains supported conditions such as EQUAL.
   * @param tablePrefix Table prefix append to the field name. Useful during SQL joining across multiple tables.
   * @param nonDollarVirtualColumnsEnabled whether to use dollar sign in virtual column names.
   * @return sql that can be appended after where clause.
   */
  @Nonnull
  public static String whereClause(@Nonnull LocalRelationshipFilter filter,
      @Nonnull Map<Condition, String> supportedConditions, @Nullable String tablePrefix,
      boolean nonDollarVirtualColumnsEnabled) {
    if (!LogicalExpressionLocalRelationshipCriterionUtils.filterHasNonEmptyCriteria(filter)) {
      throw new IllegalArgumentException("Empty filter cannot construct where clause.");
    }

    final LocalRelationshipFilter normalizedFilter = normalizeLocalRelationshipFilter(filter);

    return buildSQLQueryFromLogicalExpression(normalizedFilter.getLogicalExpressionCriteria(), supportedConditions, tablePrefix,
        nonDollarVirtualColumnsEnabled);
  }

  private static String buildSQLQueryFromLogicalExpression(@Nonnull LogicalExpressionLocalRelationshipCriterion criterion,
      @Nonnull Map<Condition, String> supportedConditions, @Nullable String tablePrefix,
      boolean nonDollarVirtualColumnsEnabled) {
    if (!criterion.hasExpr() || criterion.getExpr() == null) {
      throw new IllegalArgumentException("No logical expression found in criterion: " + criterion);
    }

    final LogicalExpressionLocalRelationshipCriterion.Expr expr = criterion.getExpr();

    if (expr.isCriterion()) {
      return buildSQLQueryFromLocalRelationshipCriterion(expr.getCriterion(), supportedConditions, tablePrefix, nonDollarVirtualColumnsEnabled);
    }

    // expr is logical
    final LogicalOperation logicalOperation = expr.getLogical();

    final Operator op = logicalOperation.getOp();

    if (op == Operator.NOT) {
      // NOT clause must only have 1 expreesion that is a criterion
      return "(NOT " + buildSQLQueryFromLocalRelationshipCriterion(expr.getLogical().getExpressions().get(0).getExpr().getCriterion(),
          supportedConditions, tablePrefix, nonDollarVirtualColumnsEnabled) + ")";
    }

    final String opString = op == Operator.AND ? " AND " : " OR ";

    final LogicalExpressionLocalRelationshipCriterionArray array = logicalOperation.getExpressions();

    final List<String> subClauses = array.stream().map(c -> {
      return buildSQLQueryFromLogicalExpression(c, supportedConditions, tablePrefix, nonDollarVirtualColumnsEnabled);
    }).collect(Collectors.toList());

    return "(" + String.join(opString, subClauses) + ")";
  }

  private static String buildSQLQueryFromLocalRelationshipCriterion(@Nonnull LocalRelationshipCriterion criterion,
      @Nonnull Map<Condition, String> supportedConditions, @Nullable String tablePrefix,
      boolean nonDollarVirtualColumnsEnabled) {

    final String field = parseLocalRelationshipField(criterion, tablePrefix, nonDollarVirtualColumnsEnabled);
    final Condition condition = criterion.getCondition();
    final LocalRelationshipValue value = criterion.getValue();

    if (condition == Condition.IN) {
      if (!value.isArray()) {
        throw new IllegalArgumentException("IN condition must be paired with array value");
      }
      return field + " IN " + parseLocalRelationshipValue(value);
    } else if (condition == Condition.START_WITH) {
      return field + " LIKE '" + parseLocalRelationshipValue(value) + "%'";
    } else {
      return field + supportedConditions.get(condition) + "'" + parseLocalRelationshipValue(value) + "'";
    }
  }

  /**
   * Construct the where clause SQL from a filter when running in old schema mode. Assumes that all filters are applied on
   * urn fields, thus only the relationship table needs to be queried. Urn fields refers to the source or destination urn
   * in a relationship query.
   * Ex.
   * AND rt.source = "urn:li:dataset:abc" AND rt.destination = "urn:li:corpuser:def"
   * @param supportedConditions map of supported conditions, such as EQUAL
   * @param filter singleton array of filter; old schema is limited to at most 1 filter, and it must be on the urn field
   * @param whichNode which node of the edge the filter is applied on, either "source" or "destination"
   * @return SQL string starting with AND to denote the conditions of a relationship query in old schema mode.
   */
  public static String whereClauseOldSchema(@Nonnull Map<Condition, String> supportedConditions,
      @Nonnull LocalRelationshipFilter filter, @Nonnull String whichNode) {
    if (!SOURCE.equals(whichNode) && !DESTINATION.equals(whichNode)) {
      throw new IllegalArgumentException("Must specify either 'source' or 'destination' node when parsing local relationship fields"
          + "in the old schema");
    }

    if (!filterHasNonEmptyCriteria(filter)) {
      return StringUtils.EMPTY;
    }

    StringBuilder sb = new StringBuilder();
    final LocalRelationshipCriterionArray criteria = filter.hasCriteria() ? filter.getCriteria()
        : flattenLogicalExpressionLocalRelationshipCriterion(filter.getLogicalExpressionCriteria());

    for (LocalRelationshipCriterion criterion : criteria) {
      String field = "rt." + whichNode;
      String condition = supportedConditions.get(criterion.getCondition());
      String value;

      if (criterion.getCondition() == Condition.IN) {
        value = parseLocalRelationshipValue(criterion.getValue());
      } else if (criterion.getCondition() == Condition.START_WITH) {
        value = "'" + parseLocalRelationshipValue(criterion.getValue()) + "%'";
      } else {
        value = "'" + parseLocalRelationshipValue(criterion.getValue()) + "'";
      }

      sb.append(String.format(" AND %s %s %s", field, condition, value));
    }
    return sb.toString();
  }

  private static String parseLocalRelationshipField(
      @Nonnull final LocalRelationshipCriterion localRelationshipCriterion, @Nullable String tablePrefix,
      boolean nonDollarVirtualColumnsEnabled) {
    tablePrefix = tablePrefix == null ? "" : tablePrefix + ".";
    LocalRelationshipCriterion.Field field = localRelationshipCriterion.getField();
    char delimiter = nonDollarVirtualColumnsEnabled ? '0' : '$';

    if (field.isUrnField()) {
      return tablePrefix + field.getUrnField().getName();
    }

    if (field.isRelationshipField()) {
      return tablePrefix + field.getRelationshipField().getName() + processPath(field.getRelationshipField().getPath(), delimiter);
    }

    if (field.isAspectField()) {
      // entity type from Urn definition.
      String assetType = getAssetType(field.getAspectField());
      return tablePrefix + getGeneratedColumnName(assetType, field.getAspectField().getAspect(),
          field.getAspectField().getPath(), nonDollarVirtualColumnsEnabled);
    }

    throw new IllegalArgumentException("Unrecognized field type");
  }

  /**
   * Get asset type from an aspectField.
   * @param aspectField {@link AspectField}
   * @return asset type, which is equivalent to Urn's entity type
   */
  protected static String getAssetType(AspectField aspectField) {

    String assetType = UNKNOWN_ASSET;
    if (aspectField.hasAsset()) {
      try {
        assetType =
            ModelUtils.getUrnTypeFromAsset(Class.forName(aspectField.getAsset()).asSubclass(RecordTemplate.class));
      } catch (ClassNotFoundException | ClassCastException e) {
        throw new IllegalArgumentException("Unrecognized asset type: " + aspectField.getAsset());
      }
    }
    return assetType;
  }

  /**
   * Parse the local relationship value to a string.
   * @param localRelationshipValue the local relationship value
   * @return the parsed string
   */
  private static String parseLocalRelationshipValue(@Nonnull final LocalRelationshipValue localRelationshipValue) {
    if (localRelationshipValue.isArray()) {
      return  "(" + localRelationshipValue.getArray().stream().map(s -> "'" + StringEscapeUtils.escapeSql(s) + "'")
          .collect(Collectors.joining(", ")) + ")";
    }

    if (localRelationshipValue.isString()) {
      return localRelationshipValue.getString();
    }

    throw new IllegalArgumentException("Unrecognized field value");
  }
}
