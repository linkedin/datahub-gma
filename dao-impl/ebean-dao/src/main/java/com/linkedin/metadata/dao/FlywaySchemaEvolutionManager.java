package com.linkedin.metadata.dao;

import com.google.common.io.Resources;
import io.ebean.EbeanServer;
import io.ebean.SqlRow;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.JSQLParserException;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationInfo;

import static com.linkedin.metadata.dao.utils.SQLSchemaUtils.*;
import static com.linkedin.metadata.dao.utils.SQLStatementUtils.*;


@Slf4j
public class FlywaySchemaEvolutionManager implements SchemaEvolutionManager {
  private static final String EVOLUTION_SCRIPTS_LOCATION = "script_directory";
  private static final String VERSION_TABLE = "version_table";
  private static final String CONFIG_FILE_TEMPLATE = "%s.conf";
  private static final String CONFIG_FILE_TEMPLATE2 = "%s-%s.conf";
  private static final String DISABLE_CLEAN = "disable_clean";
  private static final long HIGH_RISK_THRESHOLD = 2_000_000L;
  private final Flyway _flyway;
  private final EbeanServer _server;
  private final String _databaseName;

  public FlywaySchemaEvolutionManager(Config config, EbeanServer server) {
    _server = server;
    _databaseName = getDatabaseName(config);
    String serviceIdentifier = config.getServiceIdentifier();
    String configFileName = serviceIdentifier == null
        ? String.format(CONFIG_FILE_TEMPLATE, _databaseName) : String.format(CONFIG_FILE_TEMPLATE2, serviceIdentifier, _databaseName);
    Properties configProp = new Properties();

    try (InputStream configFile = getClass().getClassLoader().getResourceAsStream(configFileName)) {
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
    //Retrieves the full set of infos about pending migrations, available locally, but not yet applied to the DB
    MigrationInfo[] pendingMigrations = _flyway.info().pending();


    for (MigrationInfo pendingMigration : pendingMigrations) {
      try {
        String location = _flyway.getConfiguration().getLocations()[0].getPath();
        String sql = Resources.toString(Resources.getResource(location + "/" + pendingMigration.getScript()), StandardCharsets.UTF_8);

        List<String> highRiskSQL = detectPotentialHighRiskSQL(sql);

        for (String statement : highRiskSQL) {
          String table = extractTableName(statement);
          SqlRow sqlRow = _server.createSqlQuery(getEstimatedRowCount(_databaseName, table)).findOne();

          if (sqlRow != null) {
            long rowCount = sqlRow.getLong("table_rows");

            if (rowCount > HIGH_RISK_THRESHOLD) {
              log.error("Estimated row count {}. Apply SQL {} is high risk. Please work with MySQL team to get it applied", rowCount, statement);
              return;
            }
          }
        }

      } catch (JSQLParserException e) {
        log.error("Unable to parse the SQL script for potential high risk SQL. Please work with MySQL team to get it applied", e);
        return;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    _flyway.migrate();
  }

  @Override
  public void clean() {
    _flyway.clean();
  }

  /**
   *   This function assumes that all database connection URLs are in the following format:
   *   jdbc:mysql://(host):(port)/(db_name)?(options)
   *   The ?(options) part may or may not be present.
   *   For example - jdbc:mysql://example.linkedin.com:1234/my_first_db?autoReconnect=true
   *   This function will return "my_first_db", using the example above.
    */
  private String getDatabaseName(Config config) throws IllegalArgumentException {
    String connectionUrl = config.getConnectionUrl();
    if (connectionUrl.lastIndexOf('/') == -1) {
      throw new IllegalArgumentException("Please double check that your database connection URL is in the following format:"
          + "jdbc:mysql://<host>:<port>/<db_name>?<options>");
    }
    // First, get the path, which includes the database name. In our example above, it would be "my_first_db?autoReconnect=true"
    String databaseNameWithOptions = connectionUrl.substring(connectionUrl.lastIndexOf('/') + 1);
    if (databaseNameWithOptions.contains(":")) {
      throw new IllegalArgumentException("Please double check that your database connection URL is in the following format:"
          + "jdbc:mysql://<host>:<port>/<db_name>?<options>");
    }
    // Next, strip off any characters after the '?' so that we are left with just the database name. Ex. "my_first_db"
    return databaseNameWithOptions.contains("?") ? databaseNameWithOptions.split("\\?")[0] : databaseNameWithOptions;
  }
}
