package com.linkedin.metadata.dao.utils;

import io.ebean.EbeanServer;
import io.ebean.EbeanServerFactory;
import io.ebean.config.ServerConfig;
import io.ebean.datasource.DataSourceConfig;
import javax.annotation.Nonnull;


/**
 * Create a connection to mysql dev instance.
 */
public class MysqlDevInstance {
  private MysqlDevInstance() {
  }

  public static final String DB_SCHEMA = "gma_dev";
  private static final String DB_USER = "gma";
  private static final String DB_PASS = "Password_1";
  private static final String PORT = "23306";

  public static EbeanServer getServer() {
    return EbeanServerFactory.create(createLocalMySQLServerConfig());
  }

  @Nonnull
  private static ServerConfig createLocalMySQLServerConfig() {

    DataSourceConfig dataSourceConfig = new DataSourceConfig();
    dataSourceConfig.setUsername(DB_USER);
    dataSourceConfig.setPassword(DB_PASS);
    dataSourceConfig.setUrl(String.format("jdbc:mysql://localhost:%s/%s?allowMultiQueries=true", PORT, DB_SCHEMA));
    dataSourceConfig.setDriver("com.mysql.cj.jdbc.Driver");

    ServerConfig serverConfig = new ServerConfig();
    serverConfig.setName("gma");
    serverConfig.setDataSourceConfig(dataSourceConfig);
    serverConfig.setDdlGenerate(false);
    serverConfig.setDdlRun(false);
    return serverConfig;
  }
}
