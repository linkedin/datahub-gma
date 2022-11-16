package com.linkedin.metadata.dao;

import com.google.common.io.Resources;
import com.linkedin.metadata.dao.utils.EmbeddedMariaInstance;
import io.ebean.Ebean;
import io.ebean.EbeanServer;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class FlywaySchemaEvolutionManagerTest {
  private FlywaySchemaEvolutionManager _schemaEvolutionManager;
  private EbeanServer _server;

  @BeforeClass
  public void init() throws IOException {
    _server = EmbeddedMariaInstance.getServer();
    _server.execute(Ebean.createSqlUpdate(
        Resources.toString(Resources.getResource("schema-evolution-create-all.sql"), StandardCharsets.UTF_8)));
    SchemaEvolutionManager.Config config = new SchemaEvolutionManager.Config(
        EmbeddedMariaInstance.SERVER_CONFIG.getDataSourceConfig().getUrl(),
        EmbeddedMariaInstance.SERVER_CONFIG.getDataSourceConfig().getPassword(),
        EmbeddedMariaInstance.SERVER_CONFIG.getDataSourceConfig().getUsername()
    );

    _schemaEvolutionManager = new FlywaySchemaEvolutionManager(config);
  }

  @Test
  public void testSchemaUpToDate() {
    _schemaEvolutionManager.clean();

    // make sure table did not exists
    assertFalse(checkTableExists("metadata_entity_foo"));
    assertFalse(checkTableExists("metadata_entity_bar"));

    // Execute the evolution scripts to bring schema up-to-date.
    _schemaEvolutionManager.ensureSchemaUpToDate();

    // V1__create_foo_entity_table.sql create metadata_entity_foo table.
    assertTrue(checkTableExists("metadata_entity_foo"));

    // V2__create_bar_entity_table.sql create metadata_entity_bar table.
    assertTrue(checkTableExists("metadata_entity_bar"));

    // Make sure version table is created.
    assertTrue(checkTableExists("my_version_table"));
    _server.createSqlUpdate("DROP TABLE my_version_table").execute();
  }

  private boolean checkTableExists(String tableName) {
    String checkTableExistsSql = String.format("SELECT count(*) as count FROM information_schema.TABLES WHERE TABLE_SCHEMA = '%s' AND"
        + " TABLE_NAME = '%s'", EmbeddedMariaInstance.DB_SCHEMA, tableName);

    return _server.createSqlQuery(checkTableExistsSql).findOne().getInteger("count") == 1;
  }

  @Test
  public void testGetDatabaseName() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    Method method = FlywaySchemaEvolutionManager.class.getDeclaredMethod("getDatabaseName", SchemaEvolutionManager.Config.class);
    method.setAccessible(true);

    // Case 1: invalid database connection URL - whole string
    String databaseUrl = "asdfqwerty";
    SchemaEvolutionManager.Config config1 = new SchemaEvolutionManager.Config(databaseUrl, "pw", "user");
    assertThrows(IllegalArgumentException.class, () -> {
      try {
        method.invoke(_schemaEvolutionManager, config1);
      } catch (InvocationTargetException e) {
         throw e.getCause();
      }
    });

    // Case 2: invalid database connection URL - missing database name
    databaseUrl = "jdbc:mysql://example.linkedin.com:1234";
    SchemaEvolutionManager.Config config2 = new SchemaEvolutionManager.Config(databaseUrl, "pw", "user");
    assertThrows(IllegalArgumentException.class, () -> {
      try {
        method.invoke(_schemaEvolutionManager, config2);
      } catch (InvocationTargetException e) {
        throw e.getCause();
      }
    });

    // Case 3: valid database connection URL with no options
    databaseUrl = "jdbc:mysql://example.linkedin.com:1234/my_first_db";
    SchemaEvolutionManager.Config config3 = new SchemaEvolutionManager.Config(databaseUrl, "pw", "user");
    assertEquals(method.invoke(_schemaEvolutionManager, config3), "my_first_db");

    // Case 4: valid database connection URL with options
    databaseUrl = "jdbc:mysql://example.linkedin.com:1234/my_first_db?autoReconnect=true&useSSL=false";
    SchemaEvolutionManager.Config config4 = new SchemaEvolutionManager.Config(databaseUrl, "pw", "user");
    assertEquals(method.invoke(_schemaEvolutionManager, config4), "my_first_db");
  }
}
