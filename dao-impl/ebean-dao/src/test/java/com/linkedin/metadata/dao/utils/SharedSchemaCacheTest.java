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
