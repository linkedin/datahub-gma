package com.linkedin.metadata.dao;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.flywaydb.core.Flyway;

public class FlywaySchemaEvolutionManager implements SchemaEvolutionManager {
  private static final String EVOLUTION_SCRIPTS_LOCATION = "script_directory";
  private static final String VERSION_TABLE = "version_table";
  private static final String CONFIG_FILE = "evolution.conf";
  private static final String DISABLE_CLEAN = "disable_clean";
  private final Flyway _flyway;

  public FlywaySchemaEvolutionManager(Config config) {
    InputStream configFile = getClass().getClassLoader().getResourceAsStream(CONFIG_FILE);
    Properties configProp = new Properties();

    try {
      configProp.load(configFile);

      if (!configProp.containsKey(EVOLUTION_SCRIPTS_LOCATION) || !configProp.containsKey(VERSION_TABLE)) {
        throw new RuntimeException("script_directory and version_table are required entries in evolution.conf");
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to load db evolution config");
    }

    //This is especially useful for production environments where running clean can be quite a career limiting move.
    boolean disableClean = !configProp.containsKey(DISABLE_CLEAN) || Boolean.parseBoolean(configProp.getProperty(DISABLE_CLEAN));

    _flyway = Flyway.configure(getClass().getClassLoader())
        .dataSource(config.getConnectionUrl(), config.getUsername(), config.getPassword())
        .locations(configProp.getProperty(EVOLUTION_SCRIPTS_LOCATION))
        .baselineOnMigrate(true)
        .baselineVersion("0")
        .cleanDisabled(disableClean)
        .table(configProp.getProperty(VERSION_TABLE))
        .load();
  }

  @Override
  public void ensureSchemaUpToDate() {
    _flyway.migrate();
  }

  @Override
  public void clean() {
    _flyway.clean();
  }
}
