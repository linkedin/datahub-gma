package com.linkedin.metadata.dao.utils;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import io.ebean.EbeanServer;
import io.ebean.SqlRow;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;


/**
 * Utility class for validating presence of columns and indexes in MySQL tables
 * by querying information_schema. Uses Caffeine caches to reduce DB load and
 * support eventual consistency of schema evolution.
 */
@Slf4j
public class SchemaValidatorUtil {
  private final EbeanServer server;

  // Cache: tableName → Set of index names
  // Configuration:
  // - expireAfterWrite(10 minutes): Ensures that newly added indexes (e.g., via Pretzel) are picked up automatically
  //   without requiring a service restart. After 10 minutes, the next request will trigger a DB refresh.
  // - maximumSize(1000): Limits cache memory footprint by retaining entries for up to 1000 distinct tables.
  //   Least recently used entries are evicted when the size limit is reached.
  private final Cache<String, Set<String>> indexCache = Caffeine.newBuilder()
      .expireAfterWrite(10, TimeUnit.MINUTES)
      .maximumSize(1000)
      .build();

  // Cache: tableName → Set of column names
  private final Cache<String, Set<String>> columnCache = Caffeine.newBuilder()
      .expireAfterWrite(10, TimeUnit.MINUTES)
      .maximumSize(1000)
      .build();

  // Cache: tableName → Set of index names -> expression that defines the index, used as a replacement for creating an index on virtual columns
  // Configuration:
  // - expireAfterWrite(10 minutes): Ensures that newly added indexes (e.g., via Pretzel) are picked up automatically
  //   without requiring a service restart. After 10 minutes, the next request will trigger a DB refresh.
  // - maximumSize(1000): Limits cache memory footprint by retaining entries for up to 1000 distinct tables.
  //   Least recently used entries are evicted when the size limit is reached.
  // ** THIS IS NEEDED ** because of local testing limitations by MariaDB: expression-based indexes are not supported,
  //    so no existing logic should depend on anything introduced by the support of this. Otherwise, we'd need to mock
  //    all indexing code in the test DB, which I want to avoid if possible.
  // TODO: This can become the only cache needed for indexes once we are 100% migrated over to this logic.
  private final Cache<String, Map<String, String>> indexExpressionCache = Caffeine.newBuilder()
      .expireAfterWrite(10, TimeUnit.MINUTES)
      .maximumSize(1000)
      .build();

  private static final String SQL_GET_ALL_COLUMNS =
      "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = database() AND TABLE_NAME = '%s'";
  private static final String SQL_GET_ALL_INDEXES =
      "SELECT DISTINCT INDEX_NAME FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = database() AND TABLE_NAME = '%s'";
  private static final String SQL_GET_ALL_INDEXES_WITH_EXPRESSIONS =
      "SELECT DISTINCT INDEX_NAME, EXPRESSION FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = database() AND TABLE_NAME = '%s'";

  public SchemaValidatorUtil(EbeanServer server) {
    this.server = server;
  }

  /**
   * Clears all caches, including indexCache, columnCache, missingColumnCache, and missingIndexCache.
   * Useful for testing.
   */
  @VisibleForTesting
  void clearCaches() {
    indexCache.invalidateAll();
    columnCache.invalidateAll();
  }


  /**
   * Checks whether the given column exists in the specified table.
   *
   * @param tableName  Table name
   * @param columnName Column name
   * @return true if column exists, false otherwise
   */
  public boolean columnExists(@Nonnull String tableName, @Nonnull String columnName) {
    String lowerTable = tableName.toLowerCase();
    String lowerColumn = columnName.toLowerCase();

    Set<String> columns = columnCache.get(lowerTable, tbl -> {
      log.info("Refreshing column cache for table '{}'", tbl);
      return loadColumns(tbl);
    });

    return columns.contains(lowerColumn);
  }

  /**
   * Checks whether the given index exists in the specified table.
   *
   * @param tableName Table name
   * @param indexName Index name
   * @return true if index exists, false otherwise
   */
  public boolean indexExists(@Nonnull String tableName, @Nonnull String indexName) {
    String lowerTable = tableName.toLowerCase();
    String lowerIndex = indexName.toLowerCase();

    Set<String> indexes = indexCache.get(lowerTable, tbl -> {
      log.info("Refreshing index cache for table '{}'", tbl);
      return loadIndexes(tbl);
    });

    return indexes.contains(lowerIndex);
  }


  /**
   * Cleans SQL expression by removing MySQL-specific encoding artifacts.
   * Removes _utf8mb4 charset prefix, unescapes quotes, and removes newlines.
   * MySQL team is the POC for questions about this since there is preprocessing needed to transform the as-is
   * index expression from the index table to a (string) expression that is usable directly in an indexed query.
   *
   * @param expression Raw SQL expression from database
   * @return Cleaned expression string, with enclosing parentheses
   */
  @VisibleForTesting
  protected String cleanIndexExpression(@Nullable String expression) {
    if (expression == null) {
      return null;
    }

    return "(" + expression
        .replace("_utf8mb4\\'", "'")
        .replace("\\'", "'")
        .replace("\\\"", "\"")
        .replace("\n", "") + ")";
  }


  /**
   * Retrieves the expression associated with the given index.
   *
   * <p>NULL doesn't necessarily mean that an index doesn't exist, use {@link #indexExists(String, String)} to check for index existence.
   *
   * @param tableName Table name
   * @param indexName Index name
   * @return Expression string, or null if index does not exist OR is not created on an expression; will be enclosed in
   *         parentheses '()'
   */
  @Nullable
  public String getIndexExpression(@Nonnull String tableName, @Nonnull String indexName) {
    String lowerTable = tableName.toLowerCase();
    String lowerIndex = indexName.toLowerCase();

    Map<String, String> indexes = indexExpressionCache.get(lowerTable, tbl -> {
      log.info("Refreshing index cache for table '{}' from expression retrieval call", tbl);
      return loadIndexesAndExpressions(tbl);
    });

    // This will also return null if the Expression column is null itself
    return cleanIndexExpression(indexes.getOrDefault(lowerIndex, null));
  }

  /**
   * Loads all columns for the given table from information_schema.
   *
   * @param tableName Table to query
   * @return Set of lowercase column names
   */
  private Set<String> loadColumns(String tableName) {
    List<SqlRow> rows = server.createSqlQuery(String.format(SQL_GET_ALL_COLUMNS, tableName)).findList();
    Set<String> columns = new HashSet<>();
    for (SqlRow row : rows) {
      columns.add(row.getString("COLUMN_NAME").toLowerCase());
    }
    return columns;
  }

  /**
   * Loads all index names for the given table from information_schema.
   *
   * @param tableName Table to query
   * @return Set of lowercase index names
   */
  private Set<String> loadIndexes(String tableName) {
    List<SqlRow> rows = server.createSqlQuery(String.format(SQL_GET_ALL_INDEXES, tableName)).findList();
    Set<String> indexes = new HashSet<>();
    for (SqlRow row : rows) {
      indexes.add(row.getString("INDEX_NAME").toLowerCase());
    }
    return indexes;
  }

  /**
   * Loads all index names and  expressions for the given table from information_schema.
   * See the comment for indexExpressionCache for more details.
   *
   * @param tableName Table to query
   * @return Map of lowercase index names -> expressions
   */
  private Map<String, String> loadIndexesAndExpressions(String tableName) {
    List<SqlRow> rows = server.createSqlQuery(String.format(SQL_GET_ALL_INDEXES_WITH_EXPRESSIONS, tableName)).findList();
    Map<String, String> indexes = new HashMap<>();
    for (SqlRow row : rows) {
      // The Expression value will be null if the index is not created on an expression
      indexes.put(row.getString("INDEX_NAME").toLowerCase(), row.getString("EXPRESSION"));
    }
    return indexes;
  }

}
