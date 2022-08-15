package com.linkedin.metadata.dao;

import com.google.common.io.Resources;
import com.linkedin.metadata.dao.utils.MysqlDevInstance;
import io.ebean.Ebean;
import io.ebean.EbeanServer;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class FlywaySchemaEvolutionManagerTest {
  private FlywaySchemaEvolutionManager _schemaEvolutionManager;
  private EbeanServer _server;

  @BeforeClass
  public void init() throws IOException {
    _server = MysqlDevInstance.getServer();
    _server.execute(Ebean.createSqlUpdate(
        Resources.toString(Resources.getResource("schema-evolution-create-all.sql"), StandardCharsets.UTF_8)));
    SchemaEvolutionManager.Config config = new SchemaEvolutionManager.Config(
        MysqlDevInstance.SERVER_CONFIG.getDataSourceConfig().getUrl(),
        MysqlDevInstance.SERVER_CONFIG.getDataSourceConfig().getPassword(),
        MysqlDevInstance.SERVER_CONFIG.getDataSourceConfig().getUsername()
    );

    _schemaEvolutionManager = new FlywaySchemaEvolutionManager(config);
  }

  @Test
  public void testSchemaUpToDate() throws IOException {
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
  }

  private boolean checkTableExists(String tableName) {
    String checkTableExistsSql = String.format("SELECT count(*) as count FROM information_schema.TABLES WHERE TABLE_SCHEMA = '%s' AND"
        + " TABLE_NAME = '%s'", MysqlDevInstance.DB_SCHEMA, tableName);

    return _server.createSqlQuery(checkTableExistsSql).findOne().getInteger("count") == 1;
  }
}
