package com.linkedin.metadata.dao.utils;

import com.google.common.io.Resources;
import com.linkedin.metadata.dao.EBeanDAOConfig;
import io.ebean.Ebean;
import io.ebean.EbeanServer;
import io.ebean.SqlQuery;
import io.ebean.SqlRow;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import static com.linkedin.common.AuditStamps.*;
import static com.linkedin.testing.TestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;


public class SharedSchemaCacheTest {

  private static final String FAKE_DB_URL = "jdbc:mysql://localhost:3306/testdb";

  private static EbeanServer server;
  private final EBeanDAOConfig ebeanConfig = new EBeanDAOConfig();
  private SharedSchemaCache cache;

  @Factory(dataProvider = "inputList")
  public SharedSchemaCacheTest(boolean nonDollarVirtualColumnsEnabled) {
    ebeanConfig.setNonDollarVirtualColumnsEnabled(nonDollarVirtualColumnsEnabled);
  }

  @DataProvider(name = "inputList")
  public static Object[][] inputList() {
    return new Object[][] {
        { true },
        { false }
    };
  }

  @BeforeClass
  public void init() {
    server = spy(EmbeddedMariaInstance.getServer(SharedSchemaCacheTest.class.getSimpleName()));
  }

  @BeforeMethod
  public void setupTest() throws IOException {
    // reset() removes any stubs added by testGetIndexExpression so real DB calls are used in other tests
    reset(server);

    String sqlFile = !ebeanConfig.isNonDollarVirtualColumnsEnabled()
        ? "ebean-local-access-create-all.sql"
        : "ebean-local-access-create-all-with-non-dollar-virtual-column-names.sql";

    server.execute(Ebean.createSqlUpdate(
        Resources.toString(Resources.getResource(sqlFile), StandardCharsets.UTF_8)));

    SharedSchemaCache.clearRegistry();
    cache = SharedSchemaCache.getInstance(server, FAKE_DB_URL);
  }

  @Test
  public void testColumnExists() {
    assertTrue(cache.columnExists("metadata_entity_foo", "a_aspectfoo"));
    assertFalse(cache.columnExists("metadata_entity_foo", "a_aspect_not_exist"));
    assertFalse(cache.columnExists("metadata_entity_notexist", "a_aspectfoo"));

    if (!ebeanConfig.isNonDollarVirtualColumnsEnabled()) {
      assertTrue(cache.columnExists("metadata_entity_foo", "i_aspectfoo$value"));
    } else {
      assertTrue(cache.columnExists("metadata_entity_foo", "i_aspectfoo0value"));
    }
  }

  @Test
  public void testGetColumns() {
    java.util.Set<String> columns = cache.getColumns("metadata_entity_foo");
    assertNotNull(columns);
    assertTrue(columns.contains("a_aspectfoo"));
    assertFalse(columns.contains("a_aspect_not_exist"));
    // result set for a nonexistent table should be empty, not throw
    assertTrue(cache.getColumns("metadata_entity_notexist").isEmpty());
  }

  @Test
  public void testIndexExists() {
    assertFalse(cache.indexExists("metadata_entity_foo", "i_aspect_not_exist"));

    if (!ebeanConfig.isNonDollarVirtualColumnsEnabled()) {
      assertTrue(cache.indexExists("metadata_entity_foo", "i_aspectfoo$value"));
    } else {
      assertTrue(cache.indexExists("metadata_entity_foo", "i_aspectfoo0value"));
    }
  }

  @Test
  public void testRegisterAndPreWarm() {
    cache.registerAndPreWarm("metadata_entity_foo");
    // After pre-warm the cache is hot; a subsequent columnExists call must not hit the DB again.
    clearInvocations(server);
    assertTrue(cache.columnExists("metadata_entity_foo", "a_aspectfoo"));
    verify(server, never()).createSqlQuery(anyString());
  }

  @Test
  public void testClearCaches() {
    // Populate caches
    assertTrue(cache.columnExists("metadata_entity_foo", "a_aspectfoo"));
    cache.clearCaches();
    // After clearing the cache should re-query — result should still be correct
    assertTrue(cache.columnExists("metadata_entity_foo", "a_aspectfoo"));
  }

  @Test
  public void testSingletonPerUrl() {
    SharedSchemaCache same = SharedSchemaCache.getInstance(server, FAKE_DB_URL);
    assertSame(cache, same);

    SharedSchemaCache different = SharedSchemaCache.getInstance(server, "jdbc:mysql://other-host:3306/testdb");
    assertNotSame(cache, different);
  }

  /**
   * Regression test for #618 (datahub-gma 0.6.185): the SharedSchemaCache pre-warm was moved into
   * the EbeanLocalAccess constructor so it always runs at startup. For consumers that run schema
   * migrations as a separate job (LinkedIn's prod pattern) or in integration tests, the
   * constructor — and therefore the pre-warm — can execute BEFORE the entity table is migrated
   * into existence. loadColumns() then returns an empty set, and caching that empty set pinned a
   * stale "table has no columns" view for the full TTL: every later columnExists() returned false,
   * so EbeanLocalAccess.batchGetUnion dropped the aspect from its query and reads silently returned
   * nothing (404 / wrong autofill defaults / lost writes / wrong counts).
   *
   * <p>An empty column load means "table not present yet" and must NOT be cached, so a later access
   * re-resolves once the schema is in place.</p>
   */
  @Test
  public void testPreWarmBeforeTableCreatedDoesNotPoisonColumnCache() {
    final String table = "metadata_entity_prewarm_regression";
    server.execute(Ebean.createSqlUpdate("DROP TABLE IF EXISTS " + table));

    // Pre-warm while the table does NOT yet exist (mimics constructor pre-warm before migration).
    cache.registerAndPreWarm(table);
    assertFalse(cache.columnExists(table, "a_aspectfoo"));

    // Schema migration runs AFTER construction: the table + aspect column now exist.
    server.execute(Ebean.createSqlUpdate(
        "CREATE TABLE " + table + " (urn VARCHAR(100) NOT NULL, a_aspectfoo JSON, "
            + "CONSTRAINT pk_" + table + " PRIMARY KEY (urn))"));

    // The column added after pre-warm must now be visible — the empty pre-warm result must not be
    // pinned. Before the fix this returned false because the empty set was cached for the TTL.
    assertTrue("column added after pre-warm must be visible; empty pre-warm load must not be cached (regression #618)",
        cache.columnExists(table, "a_aspectfoo"));

    server.execute(Ebean.createSqlUpdate("DROP TABLE IF EXISTS " + table));
  }

  /**
   * Same #618 regression as {@link #testPreWarmBeforeTableCreatedDoesNotPoisonColumnCache} but via
   * the lazy (cache-miss) read path rather than registerAndPreWarm: a columnExists() call issued
   * before the table exists must not pin an empty result.
   */
  @Test
  public void testLazyReadBeforeTableCreatedDoesNotPoisonColumnCache() {
    final String table = "metadata_entity_lazy_regression";
    server.execute(Ebean.createSqlUpdate("DROP TABLE IF EXISTS " + table));

    // Lazy read while the table does NOT yet exist.
    assertFalse(cache.columnExists(table, "a_aspectfoo"));

    server.execute(Ebean.createSqlUpdate(
        "CREATE TABLE " + table + " (urn VARCHAR(100) NOT NULL, a_aspectfoo JSON, "
            + "CONSTRAINT pk_" + table + " PRIMARY KEY (urn))"));

    assertTrue("column visible after table creation; prior empty read must not be cached (regression #618)",
        cache.columnExists(table, "a_aspectfoo"));

    server.execute(Ebean.createSqlUpdate("DROP TABLE IF EXISTS " + table));
  }

  /**
   * MariaDB does not support the EXPRESSION column in information_schema.STATISTICS, so we mock
   * the DB response to exercise the getIndexExpression happy path.
   */
  @Test
  public void testGetIndexExpression() {
    SqlQuery sqlQuery = mock(SqlQuery.class);
    List<SqlRow> indexTable = new ArrayList<>();

    when(sqlQuery.findList()).thenReturn(indexTable);
    when(server.createSqlQuery(anyString())).thenReturn(sqlQuery);

    SqlRow row1 = mock(SqlRow.class);
    indexTable.add(row1);
    when(row1.getString("EXPRESSION")).thenReturn(null);

    SqlRow row2 = mock(SqlRow.class);
    indexTable.add(row2);
    when(row2.getString("INDEX_NAME")).thenReturn("e_aspectfoo0value");
    when(row2.getString("EXPRESSION")).thenReturn(
        "cast(json_extract(`a_aspectfoo`, '$.aspect.value') as char(1024) charset utf8mb4)");

    if (!ebeanConfig.isNonDollarVirtualColumnsEnabled()) {
      when(row1.getString("INDEX_NAME")).thenReturn("i_aspectfoo$value");
    } else {
      when(row1.getString("INDEX_NAME")).thenReturn("i_aspectfoo0value");
    }

    // clear so the mock interception is used
    cache.clearCaches();

    assertNull(cache.getIndexExpression("metadata_entity_burger", "idx_fake"));

    assertNotNull(cache.getIndexExpression("metadata_entity_burger", "e_aspectfoo0value"));
    assertEquals("(cast(json_extract(`a_aspectfoo`, '$.aspect.value') as char(1024) charset utf8mb4))",
        cache.getIndexExpression("metadata_entity_burger", "e_aspectfoo0value"));

    if (!ebeanConfig.isNonDollarVirtualColumnsEnabled()) {
      assertNull(cache.getIndexExpression("metadata_entity_burger", "i_aspectfoo$value"));
    } else {
      assertNull(cache.getIndexExpression("metadata_entity_burger", "i_aspectfoo0value"));
    }
  }
}
