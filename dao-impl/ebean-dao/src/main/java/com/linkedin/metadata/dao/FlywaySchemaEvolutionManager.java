package com.linkedin.metadata.dao;

import org.flywaydb.core.Flyway;

public class FlywaySchemaEvolutionManager implements SchemaEvolutionManager {
  private static final String EVOLUTION_SCRIPTS_LOCATION = "classpath:/db/evolution";
  private final Flyway _flyway;

  public FlywaySchemaEvolutionManager(Config config) {
    _flyway = Flyway.configure()
        .dataSource(config.getConnectionUrl(), config.getUsername(), config.getPassword())
        .locations(EVOLUTION_SCRIPTS_LOCATION)
        .baselineOnMigrate(true)
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
