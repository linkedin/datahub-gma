package com.linkedin.metadata.dao.utils;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import io.ebean.EbeanServer;
import io.ebean.SqlRow;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
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
  //   Least recently used entries are evicted when the size limit is reached.`
  private final Cache<String, Set<String>> indexCache = Caffeine.newBuilder()
      .expireAfterWrite(10, TimeUnit.MINUTES)
      .maximumSize(1000)
      .build();

  // Cache: tableName → Set of column names
  private final Cache<String, Set<String>> columnCache = Caffeine.newBuilder()
      .expireAfterWrite(10, TimeUnit.MINUTES)
      .maximumSize(1000)
      .build();

  // Negative cache: table:column → true (missing columns)
  private final Cache<String, Boolean> missingColumnCache = Caffeine.newBuilder()
      .expireAfterWrite(1, TimeUnit.MINUTES)
      .maximumSize(1000)
      .build();

  // Negative cache: table:index → true (missing indexes)
  private final Cache<String, Boolean> missingIndexCache = Caffeine.newBuilder()
      .expireAfterWrite(1, TimeUnit.MINUTES)
      .maximumSize(1000)
      .build();

  private static final String SQL_GET_ALL_COLUMNS =
      "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = database() AND TABLE_NAME = '%s'";
  private static final String SQL_GET_ALL_INDEXES =
      "SELECT DISTINCT INDEX_NAME FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = database() AND TABLE_NAME = '%s'";

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
    missingColumnCache.invalidateAll();
  }

  /**
   * Checks whether the given column exists in the specified table.
   * Uses positive (columnCache) and negative (missingColumnCache) caching to avoid
   * repeated queries to information_schema. Auto-refreshes after TTL expiry.
   *
   * @param tableName  Table name
   * @param columnName Column name
   * @return true if column exists, false otherwise
   */
  public boolean columnExists(@Nonnull String tableName, @Nonnull String columnName) {
    String lowerTable = tableName.toLowerCase();
    String lowerColumn = columnName.toLowerCase();
    String columnKey = lowerTable + ":" + lowerColumn;

    // Check negative cache
    if (Boolean.TRUE.equals(missingColumnCache.getIfPresent(columnKey))) {
      log.warn("Cache miss (negative hit): column '{}' not found in table '{}'", columnName, tableName);
      return false;
    }

    // Try to fetch from column cache or load if missing
    Set<String> columns = columnCache.get(lowerTable, tbl -> {
      log.info("Refreshing column cache for table '{}'", tbl);
      return loadColumns(tbl);
    });

    if (columns != null && columns.contains(lowerColumn)) {
      return true;
    }

    log.warn("Column '{}' not found in table '{}'. Adding to negative cache.", columnName, tableName);
    missingColumnCache.put(columnKey, true);
    return false;
  }


  /**
   * Checks whether the given index exists in the specified table.
   * Uses positive (indexCache) and negative (missingIndexCache) caching to avoid
   * repeated queries to information_schema. Auto-refreshes after TTL expiry.
   *
   * @param tableName Table name
   * @param indexName Index name
   * @return true if index exists, false otherwise
   */
  public boolean indexExists(@Nonnull String tableName, @Nonnull String indexName) {
    String lowerTable = tableName.toLowerCase();
    String lowerIndex = indexName.toLowerCase();
    String indexKey = lowerTable + ":" + lowerIndex;

    // Check negative cache
    if (Boolean.TRUE.equals(missingIndexCache.getIfPresent(indexKey))) {
      log.warn("Negative cache hit: index '{}' not found in table '{}'", indexName, tableName);
      return false;
    }

    // Try to fetch from index cache or load if missing
    Set<String> indexes = indexCache.get(lowerTable, tbl -> {
      log.info("Refreshing index cache for table '{}'", tbl);
      return loadIndexes(tbl);
    });

    if (indexes != null && indexes.contains(lowerIndex)) {
      return true;
    }

    log.warn("Index '{}' not found in table '{}'. Adding to negative cache.", indexName, tableName);
    missingIndexCache.put(indexKey, true);
    return false;
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

}
