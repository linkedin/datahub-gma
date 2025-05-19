package com.linkedin.metadata.dao.utils;

import io.ebean.EbeanServer;
import io.ebean.SqlRow;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;

/**
 * Utility class for checking the presence of columns and indexes in MySQL tables
 * at runtime by querying information_schema. Caches results per table for efficiency.
 */
public class SchemaValidatorUtil {

  private final EbeanServer server;
  // Cache: tableName -> Set of column names
  private final Map<String, Set<String>> tableColumns = new ConcurrentHashMap<>();
  // Cache: tableName -> Set of index names
  private final Map<String, Set<String>> tableIndexes = new ConcurrentHashMap<>();
  private static final String SQL_GET_ALL_COLUMNS =
      "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = database() AND TABLE_NAME = '%s'";
  private static final String SQL_GET_ALL_INDEXES =
      "SELECT DISTINCT INDEX_NAME FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = database() AND TABLE_NAME = '%s'";

  public SchemaValidatorUtil(EbeanServer server) {
    this.server = server;
  }

  /**
   * Checks whether the specified column exists in the given table.
   * If the column is not found on first check, the method will refresh the schema from information_schema.
   *
   * @param tableName  the table to check
   * @param columnName the column to check
   * @return true if the column exists, false otherwise
   */
  public  boolean columnExists(@Nonnull String tableName, @Nonnull String columnName) {
    String lowerTable = tableName.toLowerCase();
    String lowerColumn = columnName.toLowerCase();

    tableColumns.computeIfAbsent(lowerTable, this::loadColumns);
    if (!tableColumns.get(lowerTable).contains(lowerColumn)) {
      Set<String> refreshed = loadColumns(lowerTable);
      tableColumns.put(lowerTable, refreshed);
      return refreshed.contains(lowerColumn);
    }
    return true;
  }

  /**
   * Checks whether the specified index exists on the given table.
   * If the index is not found on first check, the method will refresh the schema from information_schema.
   *
   * @param tableName the table to check
   * @param indexName the index to check
   * @return true if the index exists, false otherwise
   */
  public boolean indexExists(@Nonnull String tableName, @Nonnull String indexName) {
    String lowerTable = tableName.toLowerCase();
    String lowerIndex = indexName.toLowerCase();

    tableIndexes.computeIfAbsent(lowerTable, this::loadIndexes);
    if (!tableIndexes.get(lowerTable).contains(lowerIndex)) {
      Set<String> refreshed = loadIndexes(lowerTable);
      tableIndexes.put(lowerTable, refreshed);
      return refreshed.contains(lowerIndex);
    }
    return true;
  }

  /**
   * Queries information_schema to fetch all column names for the given table.
   *
   * @param tableName the table to query
   * @return set of column names (lowercased)
   */
  private Set<String> loadColumns(String tableName) {
    List<SqlRow> rows = server.createSqlQuery(getAllColumnsQuery(tableName)).findList();
    Set<String> result = new HashSet<>();
    for (SqlRow row : rows) {
      result.add(row.getString("COLUMN_NAME").toLowerCase());
    }
    return result;
  }


  /**
   * Queries information_schema to fetch all index names for the given table.
   *
   * @param tableName the table to query
   * @return set of index names
   */
  private Set<String> loadIndexes(String tableName) {
    List<SqlRow> rows = server.createSqlQuery(getAllIndexesQuery(tableName)).findList();
    Set<String> result = new HashSet<>();
    for (SqlRow row : rows) {
      result.add(row.getString("INDEX_NAME").toLowerCase());
    }
    return result;
  }

  public static String getAllColumnsQuery(String tableName) {
    return String.format(SQL_GET_ALL_COLUMNS, tableName);
  }

  private String getAllIndexesQuery(String tableName) {
    return String.format(
        SQL_GET_ALL_INDEXES,
        tableName);
  }


}
