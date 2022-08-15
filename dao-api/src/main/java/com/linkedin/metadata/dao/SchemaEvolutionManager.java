package com.linkedin.metadata.dao;

import lombok.Value;


/**
 * Managing DB evolution-related tasks.
 */
public interface SchemaEvolutionManager {

  /**
   * Ensure database schema is up-to-date based on the evolution scripts defined in /db/evolution.
   * It has no effect if schema is already up-to-date.
   */
  void ensureSchemaUpToDate();

  /**
   * Drop all the DB objects created for tracking schema evolution.
   */
  void clean();

  /**
   * Information needed for connecting schema manager to DB.
   */
  @Value
  class Config {
     String connectionUrl;
     String password;
     String username;
  }
}
