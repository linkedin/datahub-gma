package com.linkedin.metadata.dao.utils;

import ch.vorburger.exec.ManagedProcessException;
import ch.vorburger.mariadb4j.DB;
import ch.vorburger.mariadb4j.DBConfigurationBuilder;
import io.ebean.EbeanServer;
import io.ebean.EbeanServerFactory;
import io.ebean.config.ServerConfig;
import io.ebean.datasource.DataSourceConfig;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Create embedded MariaDB instance. Even though MariaDB is a fork of the MySQL, some SQL syntax are only
 * supported in MariaDB but not in MySQL and vice versa. We should use syntax that's both MySQL and MariaDB compatible.
 */
public class EmbeddedMariaInstance {
  public static final ConcurrentHashMap<String, ServerConfig> SERVER_CONFIG_MAP = new ConcurrentHashMap<>();
  private static volatile DB db;
  private EmbeddedMariaInstance() {
  }

  private static final String DB_USER = "user";
  private static final String DB_PASS = "password";
  private static final int PORT = 60273;

  public static synchronized EbeanServer getServer(String dbSchema) {
    initDB(); // initDB is idempotent

    try {
      db.createDB(dbSchema);
    } catch (ManagedProcessException e) {
      throw new RuntimeException(e);
    }

    DataSourceConfig dataSourceConfig = new DataSourceConfig();
    dataSourceConfig.setUsername(DB_USER);
    dataSourceConfig.setPassword(DB_PASS);
    dataSourceConfig.setUrl(String.format("jdbc:mysql://localhost:%s/%s?allowMultiQueries=true", PORT, dbSchema));
    dataSourceConfig.setDriver("com.mysql.cj.jdbc.Driver");
    Map<String, String> customProperties = new HashMap<>();
    customProperties.put("SERVICE_IDENTIFIER", "test");
    dataSourceConfig.setCustomProperties(customProperties);

    ServerConfig serverConfig = new ServerConfig();
    serverConfig.setName(dbSchema);
    serverConfig.setDataSourceConfig(dataSourceConfig);
    serverConfig.setDdlGenerate(false);
    serverConfig.setDdlRun(false);
    SERVER_CONFIG_MAP.put(serverConfig.getName(), serverConfig);
    return EbeanServerFactory.create(serverConfig);
  }

  public static synchronized EbeanServer getServerWithoutServiceIdentifier(String dbSchema) {
    initDB(); // initDB is idempotent

    try {
      db.createDB(dbSchema);
    } catch (ManagedProcessException e) {
      throw new RuntimeException(e);
    }

    DataSourceConfig dataSourceConfig = new DataSourceConfig();
    dataSourceConfig.setUsername(DB_USER);
    dataSourceConfig.setPassword(DB_PASS);
    dataSourceConfig.setUrl(String.format("jdbc:mysql://localhost:%s/%s?allowMultiQueries=true", PORT, dbSchema));
    dataSourceConfig.setDriver("com.mysql.cj.jdbc.Driver");

    ServerConfig serverConfig = new ServerConfig();
    serverConfig.setName(dbSchema);
    serverConfig.setDataSourceConfig(dataSourceConfig);
    serverConfig.setDdlGenerate(false);
    serverConfig.setDdlRun(false);
    SERVER_CONFIG_MAP.put(serverConfig.getName(), serverConfig);
    return EbeanServerFactory.create(serverConfig);
  }

  private static void initDB() {
    if (db == null) {
      synchronized (DB.class) {
        if (db == null) { // double check
          DBConfigurationBuilder configurationBuilder = DBConfigurationBuilder.newBuilder();
          configurationBuilder.setPort(PORT);
          String baseDbDir = getBaseDbDir();
          configurationBuilder.setDataDir(baseDbDir + File.separator + "data");
          configurationBuilder.setBaseDir(baseDbDir + File.separator + "base");

          /*
           * Add below 3 lines of code if building datahub-gma on a M1 / M2 chip Apple computer.
           *
           * configurationBuilder.setBaseDir("/opt/homebrew");
           * configurationBuilder.setUnpackingFromClasspath(false);
           * configurationBuilder.setLibDir(System.getProperty("java.io.tmpdir") + "/MariaDB4j/no-libs");
           */

          try {
            // ensure the DB directory is deleted before we start to have a clean start
            if (new File(baseDbDir).exists()) {
              Files.walk(Paths.get(baseDbDir)).map(Path::toFile)
                  .sorted(Comparator.reverseOrder())
                  .forEach(File::delete);
            }
            db = DB.newEmbeddedDB(configurationBuilder.build());
            db.start();
          } catch (ManagedProcessException | IOException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }
  }

  private static String getBaseDbDir() {
    return String.join(File.separator, System.getProperty("java.io.tmpdir"), "datahub-gma", "testDb");
  }
}
