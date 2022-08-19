package com.linkedin.metadata.dao.utils;

import com.linkedin.metadata.dao.EbeanMetadataAspect;
import com.linkedin.metadata.dao.EbeanMetadataIndex;
import io.ebean.EbeanServer;
import io.ebean.EbeanServerFactory;
import io.ebean.config.ServerConfig;
import io.ebean.datasource.DataSourceConfig;
import javax.annotation.Nonnull;


/**
 * Utils class for Ebean server and its configs.
 */
public class EbeanServerUtils {
  private static final String EBEAN_MODEL_PACKAGE = EbeanMetadataAspect.class.getPackage().getName();
  private static final String EBEAN_INDEX_PACKAGE = EbeanMetadataIndex.class.getPackage().getName();

  private EbeanServerUtils() {
    // Utils class
  }

  /**
   * Creates a private in-memory {@link EbeanServer} based on H2 for production.
   */
  @Nonnull
  public static ServerConfig createProductionH2ServerConfig(@Nonnull String dbName) {

    DataSourceConfig dataSourceConfig = new DataSourceConfig();
    dataSourceConfig.setUsername("tester");
    dataSourceConfig.setPassword("");
    String url = "jdbc:h2:mem:" + dbName + ";IGNORECASE=TRUE;DB_CLOSE_DELAY=-1;";
    dataSourceConfig.setUrl(url);
    dataSourceConfig.setDriver("org.h2.Driver");

    ServerConfig serverConfig = new ServerConfig();
    serverConfig.setName(dbName);
    serverConfig.setDataSourceConfig(dataSourceConfig);
    serverConfig.setDdlGenerate(false);
    serverConfig.setDdlRun(false);

    return serverConfig;
  }

  /**
   * Creates a private in-memory {@link EbeanServer} based on H2 for testing purpose.
   */
  @Nonnull
  public static ServerConfig createTestingH2ServerConfig() {
    DataSourceConfig dataSourceConfig = new DataSourceConfig();
    dataSourceConfig.setUsername("tester");
    dataSourceConfig.setPassword("");
    dataSourceConfig.setUrl("jdbc:h2:mem:testdb;IGNORECASE=TRUE;");
    dataSourceConfig.setDriver("org.h2.Driver");

    ServerConfig serverConfig = new ServerConfig();
    serverConfig.setName("gma");
    serverConfig.setDataSourceConfig(dataSourceConfig);
    serverConfig.setDdlGenerate(true);
    serverConfig.setDdlRun(true);

    return serverConfig;
  }

  /**
   * Create an Ebean server instance based on given server config.
   */
  @Nonnull
  public static EbeanServer createServer(@Nonnull ServerConfig serverConfig) {
    // Make sure that the serverConfig includes the package that contains DAO's Ebean model.
    if (!serverConfig.getPackages().contains(EBEAN_MODEL_PACKAGE)) {
      serverConfig.getPackages().add(EBEAN_MODEL_PACKAGE);
    }
    if (!serverConfig.getPackages().contains(EBEAN_INDEX_PACKAGE)) {
      serverConfig.getPackages().add(EBEAN_INDEX_PACKAGE);
    }
    return EbeanServerFactory.create(serverConfig);
  }
}
