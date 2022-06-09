package com.linkedin.metadata.dao.utils;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.query.IndexFilter;
import com.linkedin.metadata.query.IndexSortCriterion;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.linkedin.metadata.dao.utils.SQLSchemaUtils.*;
import static com.linkedin.metadata.dao.utils.SQLIndexFilterUtils.*;


/**
 * SQL statement util class to generate executable SQL query / execution statements.
 */
public class SQLStatementUtils {


  private static final String SQL_UPSERT_ASPECT_TEMPLATE =
      "INSERT INTO %s (urn, %s, lastmodifiedon, lastmodifiedby) VALUE (:urn, :metadata, :lastmodifiedon, :lastmodifiedby) "
          + "ON DUPLICATE KEY UPDATE %s = :metadata;";

  private static final String SQL_READ_ASPECT_TEMPLATE =
      "SELECT urn, %s, lastmodifiedon, lastmodifiedby FROM %s WHERE urn = '%s';";

  /**
   *  Filter query has pagination params in the existing APIs. To accommodate this, we use WITH query to include total result counts in the query response.
   *  For example, we will build the following filter query statement:
   *
   *  <p>WITH _temp_results AS (SELECT * FROM metadata_entity_foo
   *      WHERE
   *        i_testing_aspectfoo$value >= 25 AND
   *        i_testing_aspectfoo$value < 50
   *      ORDER BY i_testing_aspectfoo$value ASC)
   *    SELECT *, (SELECT count(urn) FROM _temp_results) AS _total_count FROM _temp_results
   */
  private static final String SQL_FILTER_TEMPLATE_START = "WITH _temp_results AS (SELECT * FROM %s";
  private static final String SQL_FILTER_TEMPLATE_FINISH = ")\nSELECT *, (SELECT COUNT(urn) FROM _temp_results) AS _total_count FROM _temp_results";


  private SQLStatementUtils() {

  }

  /**
   * Create read aspect SQL statement.
   * @param urn entity urn
   * @param aspectClasses aspect urn class
   * @param <ASPECT> aspect type
   * @return aspect read sql
   */
  public static <ASPECT extends RecordTemplate> String createAspectReadSql(@Nonnull Urn urn,
      @Nonnull Class<ASPECT> aspectClasses) {
    final String tableName = getTableName(urn);
    final String columnName = getColumnName(aspectClasses);
    return String.format(SQL_READ_ASPECT_TEMPLATE, columnName, tableName, urn.toString());
  }

  /**
   * Create Upsert SQL statement.
   * @param urn  entity urn
   * @param newValue aspect value
   * @param <ASPECT> aspect type
   * @return aspect upsert sql
   */
  public static <ASPECT extends RecordTemplate> String createAspectUpsertSql(@Nonnull Urn urn,
      @Nonnull ASPECT newValue) {
    final String tableName = getTableName(urn);
    final String columnName = getColumnName(newValue);
    return String.format(SQL_UPSERT_ASPECT_TEMPLATE, tableName, columnName, columnName);
  }


  /**
   * Create filter SQL statement.
   * @param tableName table name
   * @param indexFilter index filter
   * @param indexSortCriterion sorting criterion
   * @return translated SQL where statement
   */
  public static String createFilterSql(String tableName, @Nonnull IndexFilter indexFilter,
      @Nullable IndexSortCriterion indexSortCriterion) {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format(SQL_FILTER_TEMPLATE_START, tableName));
    sb.append("\n");
    sb.append(parseIndexFilter(indexFilter));
    if (indexSortCriterion != null) {
      sb.append("\n");
      sb.append(parseSortCriteria(indexSortCriterion));
    }
    sb.append(SQL_FILTER_TEMPLATE_FINISH);
    return sb.toString();
  }
}
