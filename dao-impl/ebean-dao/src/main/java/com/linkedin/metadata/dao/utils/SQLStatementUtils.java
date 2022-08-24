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
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import org.javatuples.Pair;

import static com.linkedin.metadata.dao.utils.EBeanDAOUtils.*;
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
      String.format("SELECT urn, %%s, lastmodifiedon, lastmodifiedby FROM %%s WHERE urn = '%%s' AND %%s != '%s'", DELETED_VALUE);

  private static final String INDEX_GROUP_BY_CRITERION = "SELECT count(*) as COUNT, %s FROM %s";
  private static final String SQL_GROUP_BY_COLUMN_EXISTS_TEMPLATE =
      "SELECT * FROM information_schema.COLUMNS WHERE TABLE_NAME = '%s' AND COLUMN_NAME = '%s'";

  private static final String SQL_URN_EXIST_TEMPLATE = "SELECT urn FROM %s WHERE urn = '%s'";

  private static final String INSERT_LOCAL_RELATIONSHIP = "INSERT INTO %s (metadata, source, destination, source_type, "
      + "destination_type, lastmodifiedon, lastmodifiedby) VALUE (:metadata, :source, :destination, :source_type,"
      + " :destination_type, :lastmodifiedon, :lastmodifiedby)";

  private static final String DELETE_BY_SOURCE = "DELETE FROM %s WHERE source = :source";

  private static final String DELETE_BY_DESTINATION = "DELETE FROM %s WHERE destination = :destination";

  private static final String DELETE_BY_SOURCE_AND_DESTINATION = "DELETE FROM %s WHERE destination = :destination AND source = :source";

  /**
   *  Filter query has pagination params in the existing APIs. To accommodate this, we use subquery to include total result counts in the query response.
   *  For example, we will build the following filter query statement:
   *
   *  <p>SELECT *, (SELECT COUNT(urn) FROM metadata_entity_foo WHERE i_aspectfoo$value >= 25\n"
   *  AND i_aspectfoo$value < 50 AND a_aspectfoo != '{\"gma_deleted\":true}') as _total_count FROM metadata_entity_foo\n"
   *  WHERE i_aspectfoo$value >= 25 AND i_aspectfoo$value < 50 AND a_aspectfoo != '{\"gma_deleted\":true}';
   */
  private static final String SQL_FILTER_TEMPLATE = "SELECT *, (%s) as _total_count FROM %s";
  private static final String SQL_BROWSE_ASPECT_TEMPLATE =
      String.format("SELECT urn, %%s, lastmodifiedon, lastmodifiedby, (SELECT COUNT(urn) FROM %%s) as _total_count "
          + "FROM %%s WHERE %%s != '%s' LIMIT %%d OFFSET %%d", DELETED_VALUE);

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
   * Create read aspect SQL statement for one aspect class (but could include many urns). Essentially, this will query for a
   * single aspect column in the metadata entity tables. The query includes a filter for filtering out soft-deleted aspects.
   *
   * <p>Example:
   * SELECT urn, aspect1, lastmodifiedon, lastmodifiedby FROM metadata_entity_foo WHERE urn = 'urn:1' AND aspect1 != '{"gma_deleted":true}'
   * UNION ALL
   * SELECT urn, aspect1, lastmodifiedon, lastmodifiedby FROM metadata_entity_foo WHERE urn = 'urn:2' AND aspect1 != '{"gma_deleted":true}'
   * UNION ALL
   * SELECT urn, aspect1, lastmodifiedon, lastmodifiedby FROM metadata_entity_bar WHERE urn = 'urn:1' AND aspect1 != '{"gma_deleted":true}'
   * </p>
   * @param aspectClass aspect class to query for
   * @param urns a Set of Urns to query for
   * @param <ASPECT> aspect type
   * @return aspect read sql statement for a single aspect (across multiple tables and urns)
   */
  public static <ASPECT extends RecordTemplate> String createAspectReadSql(@Nonnull Class<ASPECT> aspectClass,
      @Nonnull Set<Urn> urns) {
    if (urns.size() == 0) {
      throw new IllegalArgumentException("Need at least 1 urn to query.");
    }
    final String columnName = getAspectColumnName(aspectClass);
    StringBuilder stringBuilder = new StringBuilder();
    List<String> selectStatements = urns.stream().map(urn -> {
          final String tableName = getTableName(urn);
          return String.format(SQL_READ_ASPECT_TEMPLATE, columnName, tableName, urn.toString(), columnName);
        }).collect(Collectors.toList());
    stringBuilder.append(String.join(" UNION ALL ", selectStatements));
    return stringBuilder.toString();
  }

  /**
   * Create Upsert SQL statement.
   * @param urn  entity urn
   * @param <ASPECT> aspect type
   * @param aspectClass aspect class
   * @return aspect upsert sql
   */
  public static <ASPECT extends RecordTemplate> String createAspectUpsertSql(@Nonnull Urn urn,
      @Nonnull Class<ASPECT> aspectClass) {
    final String tableName = getTableName(urn);
    final String columnName = getAspectColumnName(aspectClass);
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
    String whereClause = parseIndexFilter(indexFilter);
    String totalCountSql = String.format("SELECT COUNT(urn) FROM %s %s", tableName, whereClause);
    StringBuilder sb = new StringBuilder();
    sb.append(String.format(SQL_FILTER_TEMPLATE, totalCountSql, tableName));
    sb.append("\n");
    sb.append(whereClause);
    return sb.toString();
  }

  /**
   * Create index group by SQL statement.
   * @param tableName table name
   * @param indexFilter index filter
   * @param indexGroupByCriterion group by
   * @return translated group by SQL
   */
  public static String createGroupBySql(String tableName, @Nonnull IndexFilter indexFilter,
      @Nonnull IndexGroupByCriterion indexGroupByCriterion) {
    final String columnName = getGeneratedColumnName(indexGroupByCriterion.getAspect(), indexGroupByCriterion.getPath());
    StringBuilder sb = new StringBuilder();
    sb.append(String.format(INDEX_GROUP_BY_CRITERION, columnName, tableName));
    sb.append("\n");
    sb.append(parseIndexFilter(indexFilter));
    sb.append("\nGROUP BY ");
    sb.append(columnName);
    return sb.toString();
  }

  public static String createGroupByColumnExistsSql(String tableName, @Nonnull IndexGroupByCriterion indexGroupByCriterion) {
    return String.format(SQL_GROUP_BY_COLUMN_EXISTS_TEMPLATE, tableName, getGeneratedColumnName(indexGroupByCriterion.getAspect(),
        indexGroupByCriterion.getPath()));
  }

  /**
   * Create aspect browse SQL statement.
   * @param entityType entity type.
   * @param aspectClass aspect class
   * @param <ASPECT> {@link RecordTemplate}
   * @return aspect browse SQL.
   */
  public static <ASPECT extends RecordTemplate> String createAspectBrowseSql(String entityType,
      Class<ASPECT> aspectClass, int offset, int pageSize) {
    final String tableName = getTableName(entityType);
    final String columnName = getAspectColumnName(aspectClass);
    return String.format(SQL_BROWSE_ASPECT_TEMPLATE, columnName, tableName, tableName, columnName,
        Math.max(pageSize, 0), Math.max(offset, 0));
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
   * Construct where clause SQL from multiple filters. Return null if all filters are empty.
   * @param supportedCondition contains supported conditions such as EQUAL.
   * @param filters An array of pairs which are filter and table prefix.
   * @return sql that can be appended after where clause.
   */
  @SafeVarargs
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
        orClauses.add(entry.getKey() + supportedCondition.get(pair.getValue0()) + "'" + pair.getValue1() + "'");
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
