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
import java.util.Arrays;
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
   * Get the expression index "identifier", if it exists, otherwise retrieve the generated column name.
   * The idea behind this is that whatever is returned from this method can be used verbatim to query the database;
   * it's either the expression index itself (new approach) or the virtual column (old approach).
   */
  @Nullable
  public static String getIndexedExpressionOrColumn(@Nonnull String assetType, @Nonnull String aspect, @Nonnull String path,
      boolean nonDollarVirtualColumnsEnabled, @Nonnull SchemaValidatorUtil schemaValidator) {
    final String indexColumn = getGeneratedColumnName(assetType, aspect, path, nonDollarVirtualColumnsEnabled);
    final String tableName = getTableName(assetType);

    // Check if an expression-based index exists... if it does, use that
    final String expressionIndexName = getExpressionIndexName(assetType, aspect, path);
    final String indexExpression = schemaValidator.getIndexExpression(tableName, expressionIndexName);
    if (indexExpression != null) {
      log.info("Using expression index '{}' in table '{}' with expression '{}'", expressionIndexName, tableName, indexExpression);
      return indexExpression;
    } else if (schemaValidator.columnExists(tableName, indexColumn)) {
      // (Pre-functional-index logic) Check for existence of (virtual) column
      return indexColumn;
    } else {
      return null;
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
      boolean nonDollarVirtualColumnsEnabled, @Nonnull SchemaValidatorUtil validator) {
    if (indexSortCriterion == null) {
      // Default to order by urn if user does not provide sort criterion.
      return "ORDER BY URN";
    }

    final String indexedExpressionOrColumn =
        getIndexedExpressionOrColumn(entityType, indexSortCriterion.getAspect(), indexSortCriterion.getPath(),
            nonDollarVirtualColumnsEnabled, validator);

    if (!indexSortCriterion.hasOrder()) {
      return "ORDER BY " + indexedExpressionOrColumn;
    } else {
      return "ORDER BY " + indexedExpressionOrColumn + " " + (indexSortCriterion.getOrder() == SortOrder.ASCENDING ? "ASC" : "DESC");
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

          final String indexedExpressionOrColumn =
              getIndexedExpressionOrColumn(entityType, aspect, pathParams.getPath(), nonDollarVirtualColumnsEnabled, schemaValidator);
          if (indexedExpressionOrColumn == null) {
            log.warn("Skipping filter: Neither expression index nor virtual column found for Aspect '{}' and Path '{}' for Asset '{}'",
                aspect, pathParams.getPath(), entityType);
            continue;
          }

          sqlFilters.add(parseSqlFilter(indexedExpressionOrColumn, condition, pathParams.getValue()));
        }
      }
    }

    // Add soft deleted check.
    sqlFilters.add(DELETED_TS_IS_NULL_CHECK);

    return "WHERE " + String.join("\nAND ", sqlFilters);

  }

  /**
   * Parse condition expression.
   * @param index the name of the virtual generated column OR the actual expression of a functional index
   *              (TODO: is this valid?) Note: the functional index is expected to be wrapped in parentheses already.
   * @param condition {@link Condition} filter condition
   * @param indexValue {@link IndexValue} index value
   * @return SQL expression of the condition expression
   */
  private static String parseSqlFilter(String index, Condition condition, IndexValue indexValue) {
    switch (condition) {
      // TODO: add validation to check that the index column value is an array type
      case ARRAY_CONTAINS:
        return String.format("'%s' MEMBER OF(%s)", parseIndexValue(indexValue), index);  // JSON Array
      case CONTAIN:
        return String.format("JSON_SEARCH(%s, 'one', '%s') IS NOT NULL", index, parseIndexValue(indexValue));  // JSON String, Array, Struct
      case IN:
        return index + " IN " + parseIndexValue(indexValue);  // note the usage here is "swapped": indexValue IN (input)
      case EQUAL:
        if (indexValue.isString() || indexValue.isBoolean()) {
          return index + " = '" + parseIndexValue(indexValue) + "'";
        }

        if (indexValue.isArray()) {
          return index + " = '" + convertToJsonArray(indexValue.getArray()) + "'";
        }

        return index + " = " + parseIndexValue(indexValue);
      case START_WITH:
        return index + " LIKE '" + parseIndexValue(indexValue) + "%'";
      case END_WITH:
        return index + " LIKE '%" + parseIndexValue(indexValue) + "'";
      case GREATER_THAN_OR_EQUAL_TO:
        return index + " >= " + parseIndexValue(indexValue);
      case GREATER_THAN:
        return index + " > " + parseIndexValue(indexValue);
      case LESS_THAN_OR_EQUAL_TO:
        return index + " <= " + parseIndexValue(indexValue);
      case LESS_THAN:
        return index + " < " + parseIndexValue(indexValue);
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

    if (condition == Condition.IN && (!indexValue.isArray() || indexValue.getArray().isEmpty())) {
      throw new IllegalArgumentException("Invalid condition IN for index value " + indexValue);
    }

    if (condition == Condition.ARRAY_CONTAINS && indexValue.isArray()) {
      throw new IllegalArgumentException(String.format("Array values are not allowed for the target of the condition ARRAY_CONTAINS."
          + " Please split the array of elements into different criteria. Index value: %s", Arrays.toString(indexValue.getArray().toArray())));
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
