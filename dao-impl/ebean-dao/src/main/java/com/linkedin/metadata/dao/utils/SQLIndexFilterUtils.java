package com.linkedin.metadata.dao.utils;

import com.linkedin.data.template.GetMode;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.query.Condition;
import com.linkedin.metadata.query.IndexCriterion;
import com.linkedin.metadata.query.IndexFilter;
import com.linkedin.metadata.query.IndexPathParams;
import com.linkedin.metadata.query.IndexSortCriterion;
import com.linkedin.metadata.query.IndexValue;
import com.linkedin.metadata.query.SortOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringEscapeUtils;

import static com.linkedin.metadata.dao.utils.SQLSchemaUtils.*;
import static com.linkedin.metadata.dao.utils.SQLStatementUtils.*;


/**
 * Condition SQL script utils based on {@link IndexFilter}.
 */
@Slf4j
public class SQLIndexFilterUtils {

  private SQLIndexFilterUtils() {

  }

  /**
   * Get value from {@link IndexValue} and convert into SQL syntax.
   * @param indexValue {@link IndexValue} to be parsed.
   * @return SQL syntax compatible representation of the Index Value
   */
  private static String parseIndexValue(@Nullable IndexValue indexValue) {
    if (indexValue == null) {
      return "NULL";
    }
    if (indexValue.isArray()) {
      StringArray stringArray = indexValue.getArray();
      return "(" + String.join(", ",
          stringArray.stream().map(s -> "'" + StringEscapeUtils.escapeSql(s) + "'").collect(Collectors.toList())) + ")";
    } else if (indexValue.isBoolean()) {
      return indexValue.getBoolean().toString();
    } else if (indexValue.isInt()) {
      return String.valueOf(Long.valueOf(indexValue.getInt()));
    } else if (indexValue.isDouble()) {
      return String.valueOf(indexValue.getDouble());
    } else if (indexValue.isFloat()) {
      return String.valueOf(indexValue.getFloat().doubleValue());
    } else if (indexValue.isLong()) {
      return String.valueOf(indexValue.getLong());
    } else if (indexValue.isString()) {
      return StringEscapeUtils.escapeSql(indexValue.getString());
    } else if (indexValue.isNull()) {
      return "NULL";
    } else {
      throw new UnsupportedOperationException("Invalid index value: " + indexValue);
    }
  }


  /**
   * Parse {@link IndexSortCriterion} into SQL syntax.
   * @param entityType entity type from the Urn
   * @param indexSortCriterion filter sorting criterion
   * @param nonDollarVirtualColumnsEnabled  true if virtual column does not contain $, false otherwise
   * @return SQL statement of sorting, e.g. ORDER BY ... DESC ..etc.
   */
  public static String parseSortCriteria(@Nonnull String entityType, @Nullable IndexSortCriterion indexSortCriterion,
      boolean nonDollarVirtualColumnsEnabled) {
    if (indexSortCriterion == null) {
      // Default to order by urn if user does not provide sort criterion.
      return "ORDER BY URN";
    }
    final String indexColumn =
        SQLSchemaUtils.getGeneratedColumnName(entityType, indexSortCriterion.getAspect(), indexSortCriterion.getPath(),
            nonDollarVirtualColumnsEnabled);

    if (!indexSortCriterion.hasOrder()) {
      return "ORDER BY " + indexColumn;
    } else {
      return "ORDER BY " + indexColumn + " " + (indexSortCriterion.getOrder() == SortOrder.ASCENDING ? "ASC" : "DESC");
    }
  }

  /**
   * Parse {@link IndexFilter} into MySQL syntax.
   * @param entityType entity type from the Urn
   * @param indexFilter index filter
   * @param nonDollarVirtualColumnsEnabled whether to enable non-dollar virtual columns
   * @return translated SQL condition expression, e.g. WHERE ...
   */
  public static String parseIndexFilter(@Nonnull String entityType, @Nullable IndexFilter indexFilter,
      boolean nonDollarVirtualColumnsEnabled, @Nonnull SchemaValidatorUtil schemaValidator) {
    List<String> sqlFilters = new ArrayList<>();

    // Process index filter criteria if present
    if (indexFilter != null && indexFilter.hasCriteria()) {
      for (IndexCriterion indexCriterion : indexFilter.getCriteria()) {
        final String aspect = indexCriterion.getAspect();
        if (!isUrn(aspect)) {
          // if aspect is not urn, then check aspect is not soft deleted and is not null
          final String aspectColumn = getAspectColumnName(entityType, indexCriterion.getAspect());
          sqlFilters.add(aspectColumn + " IS NOT NULL");
          sqlFilters.add(String.format(SOFT_DELETED_CHECK, aspectColumn));
        }

        final IndexPathParams pathParams = indexCriterion.getPathParams(GetMode.NULL);
        if (pathParams != null) {
          validateConditionAndValue(indexCriterion);
          final Condition condition = pathParams.getCondition();
          final String indexColumn = getGeneratedColumnName(entityType, aspect, pathParams.getPath(), nonDollarVirtualColumnsEnabled);
          final String tableName = SQLSchemaUtils.getTableName(entityType);
          // New: Skip filter if column doesn't exist
          if (!schemaValidator.columnExists(tableName, indexColumn)) {
            log.warn("Skipping filter: virtual column '{}' not found in table '{}'", indexColumn, tableName);
            continue;
          }
          sqlFilters.add(parseSqlFilter(indexColumn, condition, pathParams.getValue()));
        }
      }
    }

    // Add soft deleted check.
    sqlFilters.add(DELETED_TS_IS_NULL_CHECK);

    return "WHERE " + String.join("\nAND ", sqlFilters);

  }

  /**
   * Parse condition expression.
   * @param indexColumn the virtual generated column
   * @param condition {@link Condition} filter condition
   * @param indexValue {@link IndexValue} index value
   * @return SQL expression of the condition expression
   */
  private static String parseSqlFilter(String indexColumn, Condition condition, IndexValue indexValue) {
    switch (condition) {
      case CONTAIN:
        return String.format("JSON_SEARCH(%s, 'one', '%s') IS NOT NULL", indexColumn, parseIndexValue(indexValue));
      case IN:
        return indexColumn + " IN " + parseIndexValue(indexValue);
      case EQUAL:
        if (indexValue.isString() || indexValue.isBoolean()) {
          return indexColumn + " = '" + parseIndexValue(indexValue) + "'";
        }

        if (indexValue.isArray()) {
          return indexColumn + " = '" + convertToJsonArray(indexValue.getArray()) + "'";
        }

        return indexColumn + " = " + parseIndexValue(indexValue);
      case START_WITH:
        return indexColumn + " LIKE '" + parseIndexValue(indexValue) + "%'";
      case END_WITH:
        return indexColumn + " LIKE '%" + parseIndexValue(indexValue) + "'";
      case GREATER_THAN_OR_EQUAL_TO:
        return indexColumn + " >= " + parseIndexValue(indexValue);
      case GREATER_THAN:
        return indexColumn + " > " + parseIndexValue(indexValue);
      case LESS_THAN_OR_EQUAL_TO:
        return indexColumn + " <= " + parseIndexValue(indexValue);
      case LESS_THAN:
        return indexColumn + " < " + parseIndexValue(indexValue);
      default:
        throw new UnsupportedOperationException("Unsupported condition operation: " + condition);
    }
  }

  public static IndexCriterion createIndexCriterion(Class<? extends RecordTemplate> aspect, String path,
      Condition condition, IndexValue indexValue) {
    IndexCriterion indexCriterion = new IndexCriterion();
    indexCriterion.setAspect(aspect.getCanonicalName());
    IndexPathParams indexPathParams = new IndexPathParams();
    indexPathParams.setPath(path);
    indexPathParams.setCondition(condition);
    indexPathParams.setValue(indexValue);
    indexCriterion.setPathParams(indexPathParams);
    return indexCriterion;
  }

  public static IndexSortCriterion createIndexSortCriterion(Class<? extends RecordTemplate> aspect, String path,
      SortOrder sortOrder) {
    IndexSortCriterion indexSortCriterion = new IndexSortCriterion();
    indexSortCriterion.setAspect(aspect.getCanonicalName());
    indexSortCriterion.setPath(path);
    indexSortCriterion.setOrder(sortOrder);
    return indexSortCriterion;
  }

  /**
   * Validate IN condition to ensure the target is an array of at least 1 length.
   * @param criterion IndexCriterion
   * @throws IllegalArgumentException when IN targets a non-array value or empty array
   */
  public static void validateConditionAndValue(@Nonnull IndexCriterion criterion) {
    final Condition condition = criterion.getPathParams().getCondition();
    final IndexValue indexValue = criterion.getPathParams().getValue();

    if (condition == Condition.IN && (!indexValue.isArray() || indexValue.getArray().size() == 0)) {
      throw new IllegalArgumentException("Invalid condition " + condition + " for index value " + indexValue);
    }
  }

  /**
   * Convert a StringArray to json format.
   * @param stringArray an array of strings
   * @return a json representation of an array.
   */
  @Nonnull
  private static String convertToJsonArray(@Nonnull final StringArray stringArray) {
    if (stringArray.isEmpty()) {
      return "[]";
    }

    StringBuilder jsonArray = new StringBuilder();
    jsonArray.append("[").append("\"").append(stringArray.get(0)).append("\"");

    for (int idx = 1; idx < stringArray.size(); idx++) {
      jsonArray.append(", ").append("\"").append(stringArray.get(idx)).append("\"");
    }

    jsonArray.append("]");

    return jsonArray.toString();
  }
}
