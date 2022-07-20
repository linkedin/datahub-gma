package com.linkedin.metadata.dao.utils;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.internal.BaseGraphWriterDAO;
import com.linkedin.metadata.query.Condition;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.IndexFilter;
import com.linkedin.metadata.query.IndexGroupByCriterion;
import com.linkedin.metadata.query.IndexSortCriterion;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import org.javatuples.Pair;

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

  private static final String SQL_URN_EXIST_TEMPLATE = "SELECT urn FROM %s WHERE urn = '%s'";

  private static final String INSERT_LOCAL_RELATIONSHIP = "INSERT INTO %s (metadata, source, destination, source_type, "
      + "destination_type, lastmodifiedon, lastmodifiedby) VALUE (:metadata, :source, :destination, :source_type,"
      + " :destination_type, :lastmodifiedon, :lastmodifiedby)";

  private static final String DELETE_BY_SOURCE = "DELETE FROM %s WHERE source = :source";
  private static final String DELETE_BY_DESTINATION = "DELETE FROM %s WHERE destination = :destination";
  private static final String DELETE_BY_SOURCE_AND_DESTINATION = "DELETE FROM %s WHERE destination = :destination AND source = :source";

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
  private static final String SQL_FILTER_TEMPLATE_FINISH =
      ")\nSELECT *, (SELECT COUNT(urn) FROM _temp_results) AS _total_count FROM _temp_results";

  private static final String SQL_BROWSE_ASPECT_TEMPLATE =
      "SELECT urn, %s, lastmodifiedon, lastmodifiedby, (SELECT COUNT(urn) FROM %s) as _total_count FROM %s";

  private SQLStatementUtils() {
    // Util class
  }

  /**
   * Create entity exist SQL statement.
   * @param urn entity urn
   * @return entity exist sql
   */
  public static String createExistSql(@Nonnull Urn urn) {
    final String tableName = getTableName(urn);
    return String.format(SQL_URN_EXIST_TEMPLATE, tableName, urn.toString());
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

  private static final String INDEX_GROUP_BY_CRITERION = "SELECT count(*) as COUNT, %s FROM %s";

  /**
   * Create index group by SQL statement.
   * @param tableName table name
   * @param indexFilter index filter
   * @param indexGroupByCriterion group by
   * @return translated group by SQL
   */
  public static String createGroupBySql(String tableName, @Nonnull IndexFilter indexFilter,
      @Nonnull IndexGroupByCriterion indexGroupByCriterion) {
    final String columnName = SQLSchemaUtils.getIndexGroupByColumn(indexGroupByCriterion);
    StringBuilder sb = new StringBuilder();
    sb.append(String.format(INDEX_GROUP_BY_CRITERION, columnName, tableName));
    sb.append("\n");
    sb.append(parseIndexFilter(indexFilter));
    sb.append("\nGROUP BY ");
    sb.append(columnName);
    return sb.toString();
  }

  /**
   * Create aspect browse SQL statement.
   * @param entityType entity type.
   * @param aspectClass aspect class
   * @param <ASPECT> {@link RecordTemplate}
   * @return aspect browse SQL.
   */
  public static <ASPECT extends RecordTemplate> String createAspectBrowseSql(String entityType,
      Class<ASPECT> aspectClass) {
    final String tableName = getTableName(entityType);
    return String.format(SQL_BROWSE_ASPECT_TEMPLATE, getColumnName(aspectClass), tableName, tableName);
  }

  /**
   * Generate "Create Statement SQL" for local relation.
   * @param tableName Name of the table where the local relation metadata will be inserted.
   * @return SQL statement for inserting local relation.
   */
  public static String insertLocalRelationshipSQL(String tableName) {
    return String.format(INSERT_LOCAL_RELATIONSHIP, tableName);
  }

  @Nonnull
  @ParametersAreNonnullByDefault
  public static String deleteLocaRelationshipSQL(final String tableName, final BaseGraphWriterDAO.RemovalOption removalOption) {
    if (removalOption == BaseGraphWriterDAO.RemovalOption.REMOVE_ALL_EDGES_FROM_SOURCE) {
      return String.format(DELETE_BY_SOURCE, tableName);
    } else if (removalOption == BaseGraphWriterDAO.RemovalOption.REMOVE_ALL_EDGES_FROM_SOURCE_TO_DESTINATION) {
      return String.format(DELETE_BY_SOURCE_AND_DESTINATION, tableName);
    } else if (removalOption == BaseGraphWriterDAO.RemovalOption.REMOVE_ALL_EDGES_TO_DESTINATION) {
      return String.format(DELETE_BY_DESTINATION, tableName);
    }

    throw new IllegalArgumentException(String.format("Removal option %s is not valid.", removalOption));
  }

  /**
   * Construct where clause SQL from mulitple filters. Return null if all filters are empty.
   * @param supportedCondition contains supported conditions such as EQUAL.
   * @param filters contains field, condition and value
   * @return sql that can be appended after where clause.
   */
  @Nullable
  public static String whereClause(@Nonnull Map<Condition, String> supportedCondition, @Nonnull Pair<Filter, String>... filters) {
    List<String> andClauses = new ArrayList<>();
    for (Pair<Filter, String> filter : filters) {
      if (filter.getValue0().hasCriteria() && filter.getValue0().getCriteria().size() > 0) {
        andClauses.add("(" + whereClause(filter.getValue0(), supportedCondition, filter.getValue1()) + ")");
      }
    }

    if (andClauses.isEmpty()) {
      return null;
    } else if (andClauses.size() == 1) {
      return andClauses.get(0).substring(1, andClauses.get(0).length() - 1);
    } else {
      return String.join(" AND ", andClauses);
    }
  }

  /**
   * Construct where clause SQL from a filter. Throw IllegalArgumentException if filter is empty.
   * @param filter contains field, condition and value
   * @param supportedCondition contains supported conditions such as EQUAL.
   * @param tablePrefix Table prefix append to the field name. Useful during SQL joining across multiple tables.
   * @return sql that can be appended after where clause.
   */
  @Nonnull
  public static String whereClause(@Nonnull Filter filter, @Nonnull Map<Condition, String> supportedCondition, @Nullable String tablePrefix) {
    if (!filter.hasCriteria() || filter.getCriteria().size() == 0) {
      throw new IllegalArgumentException("Empty filter cannot construct where clause.");
    }

    if (tablePrefix != null) {
      filter.getCriteria().forEach(criterion -> {
        criterion.setField(tablePrefix + "." + criterion.getField());
      });
    }

    // Group the conditions by field.
    Map<String, List<Pair<Condition, String>>> groupByField = new HashMap<>();
    filter.getCriteria().forEach(criterion -> {
      List<Pair<Condition, String>> group = groupByField.getOrDefault(criterion.getField(), new ArrayList<>());
      group.add(new Pair<>(criterion.getCondition(), criterion.getValue()));
      groupByField.put(criterion.getField(), group);
    });

    List<String> andClauses = new ArrayList<>();
    for (Map.Entry<String, List<Pair<Condition, String>>> entry : groupByField.entrySet()) {
      List<String> orClauses = new ArrayList<>();
      for (Pair<Condition, String> pair : entry.getValue()) {
        orClauses.add(entry.getKey() + supportedCondition.get(pair.getValue0()) + pair.getValue1());
      }

      if (orClauses.size() == 1) {
        andClauses.add(orClauses.get(0));
      } else {
        andClauses.add("(" + String.join(" OR ", orClauses) + ")");
      }
    }

    if (andClauses.size() == 1) {
      String andClause = andClauses.get(0);
      if (andClauses.get(0).startsWith("(")) {
        return andClause.substring(1, andClause.length() - 1);
      }
      return andClause;
    }

    return String.join(" AND ", andClauses);
  }
}
