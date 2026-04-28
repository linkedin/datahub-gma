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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;


/**
 * Shared per-database schema metadata cache. One instance per database URL; obtained via {@link #getInstance}.
 *
 * <p>Compared to a per-entity-type {@link SchemaValidatorUtil}, this eliminates redundant
 * {@code information_schema} queries for entity types that share the same database, pre-warms
 * each table at startup so the first request is never slow, and refreshes in the background with
 * random per-host jitter to prevent thundering-herd cache expiry across a fleet.
 */
@Slf4j
public class SharedSchemaCache {

  private static final long CACHE_TTL_MINUTES = 10;
  // Refresh before TTL so the cache is always warm when Caffeine would evict
  private static final long REFRESH_INTERVAL_SECONDS = 9 * 60;
  private static final long JITTER_MAX_SECONDS = 60;
  private static final int CACHE_MAX_SIZE = 1000;

  private static final String SQL_GET_ALL_COLUMNS =
      "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = database() AND TABLE_NAME = '%s'";
  private static final String SQL_GET_ALL_INDEXES =
      "SELECT DISTINCT INDEX_NAME FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = database() AND TABLE_NAME = '%s'";
  private static final String SQL_GET_ALL_INDEXES_WITH_EXPRESSIONS =
      "SELECT DISTINCT INDEX_NAME, EXPRESSION FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = database() AND TABLE_NAME = '%s'";

  private static final Map<String, SharedSchemaCache> REGISTRY = new ConcurrentHashMap<>();

  private final EbeanServer server;
  private final Set<String> registeredTables = ConcurrentHashMap.newKeySet();

  private final Cache<String, Set<String>> columnCache = Caffeine.newBuilder()
      .expireAfterWrite(CACHE_TTL_MINUTES, TimeUnit.MINUTES)
      .maximumSize(CACHE_MAX_SIZE)
      .build();

  private final Cache<String, Set<String>> indexCache = Caffeine.newBuilder()
      .expireAfterWrite(CACHE_TTL_MINUTES, TimeUnit.MINUTES)
      .maximumSize(CACHE_MAX_SIZE)
      .build();

  private final Cache<String, Map<String, String>> indexExpressionCache = Caffeine.newBuilder()
      .expireAfterWrite(CACHE_TTL_MINUTES, TimeUnit.MINUTES)
      .maximumSize(CACHE_MAX_SIZE)
      .build();

  private SharedSchemaCache(@Nonnull EbeanServer server) {
    this.server = server;
    scheduleBackgroundRefresh();
  }

  /**
   * Returns the {@link SharedSchemaCache} for {@code dbUrl}, creating one if absent.
   * Multiple {@link com.linkedin.metadata.dao.EbeanLocalAccess} instances that share the same
   * database will receive the same cache instance, eliminating redundant information_schema queries.
   */
  @Nonnull
  public static SharedSchemaCache getInstance(@Nonnull EbeanServer server, @Nonnull String dbUrl) {
    return REGISTRY.computeIfAbsent(dbUrl, k -> new SharedSchemaCache(server));
  }

  /**
   * Registers {@code tableName} and immediately loads its column and index metadata.
   * Should be called at startup (e.g., from {@code ensureSchemaUpToDate}) so that the first
   * request to any entity never pays the cost of an inline information_schema query.
   */
  public void registerAndPreWarm(@Nonnull String tableName) {
    String lower = tableName.toLowerCase();
    registeredTables.add(lower);
    refreshTable(lower);
  }

  // ── cache access ─────────────────────────────────────────────────────────────

  public boolean columnExists(@Nonnull String tableName, @Nonnull String columnName) {
    Set<String> columns = columnCache.get(tableName.toLowerCase(), this::loadColumns);
    return columns != null && columns.contains(columnName.toLowerCase());
  }

  @Nonnull
  public Set<String> getColumns(@Nonnull String tableName) {
    Set<String> cols = columnCache.get(tableName.toLowerCase(), this::loadColumns);
    return cols != null ? cols : new HashSet<>();
  }

  public boolean indexExists(@Nonnull String tableName, @Nonnull String indexName) {
    Set<String> indexes = indexCache.get(tableName.toLowerCase(), this::loadIndexes);
    return indexes != null && indexes.contains(indexName.toLowerCase());
  }

  @Nullable
  public String getIndexExpression(@Nonnull String tableName, @Nonnull String indexName) {
    String lowerTable = tableName.toLowerCase();
    String lowerIndex = indexName.toLowerCase();
    try {
      Map<String, String> indexes = indexExpressionCache.get(lowerTable, this::loadIndexesAndExpressions);
      return SchemaValidatorUtil.cleanIndexExpression(
          indexes != null ? indexes.getOrDefault(lowerIndex, null) : null);
    } catch (Exception e) {
      // MariaDB for local testing doesn't support EXPRESSION column
      log.debug("Unable to load index expressions for table '{}': {}", lowerTable, e.getMessage());
      return null;
    }
  }

  // ── background refresh ────────────────────────────────────────────────────────

  private void scheduleBackgroundRefresh() {
    long jitterSeconds = ThreadLocalRandom.current().nextLong(0, JITTER_MAX_SECONDS + 1);
    long initialDelay = REFRESH_INTERVAL_SECONDS + jitterSeconds;
    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, "shared-schema-cache-refresh");
      t.setDaemon(true);
      return t;
    });
    executor.scheduleAtFixedRate(this::refreshAll, initialDelay, REFRESH_INTERVAL_SECONDS, TimeUnit.SECONDS);
    log.info("Scheduled background schema cache refresh with {}s initial delay", initialDelay);
  }

  private void refreshAll() {
    log.info("Background schema cache refresh: {} tables", registeredTables.size());
    for (String table : registeredTables) {
      refreshTable(table);
    }
  }

  private void refreshTable(String tableName) {
    try {
      columnCache.put(tableName, loadColumns(tableName));
      indexCache.put(tableName, loadIndexes(tableName));
      log.debug("Schema cache refreshed for table '{}'", tableName);
    } catch (Exception e) {
      log.warn("Schema cache refresh failed for table '{}': {}", tableName, e.getMessage());
    }
  }

  // ── DB loaders ────────────────────────────────────────────────────────────────

  private Set<String> loadColumns(String tableName) {
    List<SqlRow> rows = server.createSqlQuery(String.format(SQL_GET_ALL_COLUMNS, tableName)).findList();
    Set<String> columns = new HashSet<>();
    for (SqlRow row : rows) {
      columns.add(row.getString("COLUMN_NAME").toLowerCase());
    }
    return columns;
  }

  private Set<String> loadIndexes(String tableName) {
    List<SqlRow> rows = server.createSqlQuery(String.format(SQL_GET_ALL_INDEXES, tableName)).findList();
    Set<String> indexes = new HashSet<>();
    for (SqlRow row : rows) {
      indexes.add(row.getString("INDEX_NAME").toLowerCase());
    }
    return indexes;
  }

  private Map<String, String> loadIndexesAndExpressions(String tableName) {
    List<SqlRow> rows =
        server.createSqlQuery(String.format(SQL_GET_ALL_INDEXES_WITH_EXPRESSIONS, tableName)).findList();
    Map<String, String> indexes = new HashMap<>();
    for (SqlRow row : rows) {
      indexes.put(row.getString("INDEX_NAME").toLowerCase(), row.getString("EXPRESSION"));
    }
    return indexes;
  }

  // ── test helpers ──────────────────────────────────────────────────────────────

  @VisibleForTesting
  void clearCaches() {
    columnCache.invalidateAll();
    indexCache.invalidateAll();
    indexExpressionCache.invalidateAll();
    registeredTables.clear();
  }

  @VisibleForTesting
  public static void clearRegistry() {
    REGISTRY.clear();
  }
}
