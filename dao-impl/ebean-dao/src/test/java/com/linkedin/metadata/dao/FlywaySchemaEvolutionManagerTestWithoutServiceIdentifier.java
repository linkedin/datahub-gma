package com.linkedin.metadata.dao;

import com.google.common.io.Resources;
import com.linkedin.metadata.dao.utils.EmbeddedMariaInstance;
import io.ebean.Ebean;
import io.ebean.EbeanServer;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * This test is to ensure without service identifier, the schema evolution manager can bring the schema up-to-date from
 * the default config file.
 */
public class FlywaySchemaEvolutionManagerTestWithoutServiceIdentifier {
  private FlywaySchemaEvolutionManager _schemaEvolutionManager;
  private EbeanServer _server;

  @BeforeClass
  public void init() throws IOException {
    //reuse the db instance
    //but with a different server config
    _server = EmbeddedMariaInstance.getServerWithoutServiceIdentifier(FlywaySchemaEvolutionManagerTest.class.getSimpleName());
    _server.execute(Ebean.createSqlUpdate(
        Resources.toString(Resources.getResource("schema-evolution-create-all.sql"), StandardCharsets.UTF_8)));
    SchemaEvolutionManager.Config config = new SchemaEvolutionManager.Config(
        EmbeddedMariaInstance.SERVER_CONFIG_MAP.get(_server.getName()).getDataSourceConfig().getUrl(),
        EmbeddedMariaInstance.SERVER_CONFIG_MAP.get(_server.getName()).getDataSourceConfig().getPassword(),
        EmbeddedMariaInstance.SERVER_CONFIG_MAP.get(_server.getName()).getDataSourceConfig().getUsername(),
        null
    );

    _schemaEvolutionManager = new FlywaySchemaEvolutionManager(config);
  }

  @Test
  public void testSchemaUpToDate() {
    _schemaEvolutionManager.clean();

    // make sure table did not exists
    assertFalse(checkTableExists("metadata_entity_foobaz"));
    assertFalse(checkTableExists("my_another_version_table"));

    // Execute the evolution scripts to bring schema up-to-date.
    _schemaEvolutionManager.ensureSchemaUpToDate();

    // V1__create_foobaz_entity_table.sql create metadata_entity_foobaz table.
    assertTrue(checkTableExists("metadata_entity_foobaz"));

    // Make sure version table is created.
    assertTrue(checkTableExists("my_another_version_table"));
    _server.createSqlUpdate("DROP TABLE my_another_version_table").execute();
  }

  private boolean checkTableExists(String tableName) {
    String checkTableExistsSql = String.format("SELECT count(*) as count FROM information_schema.TABLES WHERE TABLE_SCHEMA = '%s' AND"
        + " TABLE_NAME = '%s'", _server.getName(), tableName);
    return _server.createSqlQuery(checkTableExistsSql).findOne().getInteger("count") == 1;
  }
}
