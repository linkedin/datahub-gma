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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang.StringEscapeUtils;

import static com.linkedin.metadata.dao.utils.EBeanDAOUtils.*;
import static com.linkedin.metadata.dao.utils.SQLSchemaUtils.*;


/**
 * Condition SQL script utils based on {@link IndexFilter}.
 */
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
   * @param indexSortCriterion filter sorting criterion
   * @return SQL statement of sorting, e.g. ORDER BY ... DESC ..etc.
   */
  public static String parseSortCriteria(@Nullable IndexSortCriterion indexSortCriterion) {
    if (indexSortCriterion == null) {
      return "";
    }
    final String indexColumn = SQLSchemaUtils.getGeneratedColumnName(indexSortCriterion.getAspect(), indexSortCriterion.getPath());

    if (!indexSortCriterion.hasOrder()) {
      return "ORDER BY " + indexColumn;
    } else {
      return "ORDER BY " + indexColumn + " " + (indexSortCriterion.getOrder() == SortOrder.ASCENDING ? "ASC"
          : "DESC");
    }
  }

  /**
   * Parse {@link IndexFilter} into MySQL syntax.
   * @param indexFilter index filter
   * @return translated SQL condition expression, e.g. WHERE ...
   */
  public static String parseIndexFilter(@Nonnull IndexFilter indexFilter) {
    List<String> sqlFilters = new ArrayList<>();
    Set<String> aspectColumns = new HashSet<>(); // aspect columns (i.e. start with a_) to check if soft-deleted
    for (IndexCriterion indexCriterion : indexFilter.getCriteria()) {
      final String aspect = indexCriterion.getAspect();
      if (!(isUrn(aspect))) {
        // urns, which are entities, cannot be soft-deleted so no need to check them
        aspectColumns.add(getAspectColumnName(aspect));
      }
      final IndexPathParams pathParams = indexCriterion.getPathParams();
      if (pathParams != null) {
        validateConditionAndValue(indexCriterion);
        final String path = indexCriterion.getPathParams().getPath();
        final Condition condition = pathParams.getCondition();
        final String indexColumn = getGeneratedColumnName(indexCriterion.getAspect(), path);
        sqlFilters.add(
            indexColumn + parseConditionExpr(condition, indexCriterion.getPathParams().getValue(GetMode.NULL)));
      } else if (!isUrn(indexCriterion.getAspect())) {
        // if not given a path and condition, assume we are checking if the aspect exists.
        final String aspectColumn = getAspectColumnName(indexCriterion.getAspect());
        sqlFilters.add(aspectColumn + " IS NOT NULL");
      }
    }
    // add filters to check that each aspect being queried is not soft deleted
    // e.g. WHERE a_aspect1 != '{"gma_deleted":true}' AND a_aspect2 != '{"gma_deleted":true}'
    aspectColumns.forEach(aspect -> sqlFilters.add(String.format("%s != '%s'", aspect, DELETED_VALUE)));
    if (sqlFilters.isEmpty()) {
      return "";
    } else {
      return "WHERE " + String.join("\nAND ", sqlFilters);
    }
  }

  /**
   * Parse condition expression.
   * @param condition {@link Condition} filter condition
   * @param indexValue {@link IndexValue} index value
   * @return SQL expression of the condition expression
   */
  private static String parseConditionExpr(Condition condition, IndexValue indexValue) {
    switch (condition) {
      case IN:
      case CONTAIN:
        return " IN " + parseIndexValue(indexValue);
      case EQUAL:
        if (indexValue.isString() || indexValue.isBoolean()) {
          return " = '" + parseIndexValue(indexValue) + "'";
        }
        return " = " + parseIndexValue(indexValue);
      case START_WITH:
        return " LIKE '" + parseIndexValue(indexValue) + "%'";
      case END_WITH:
        return " LIKE '%" + parseIndexValue(indexValue) + "'";
      case GREATER_THAN_OR_EQUAL_TO:
        return " >= " + parseIndexValue(indexValue);
      case GREATER_THAN:
        return " > " + parseIndexValue(indexValue);
      case LESS_THAN_OR_EQUAL_TO:
        return " <= " + parseIndexValue(indexValue);
      case LESS_THAN:
        return " < " + parseIndexValue(indexValue);
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
  @Nonnull
  public static void validateConditionAndValue(@Nonnull IndexCriterion criterion) {
    final Condition condition = criterion.getPathParams().getCondition();
    final IndexValue indexValue = criterion.getPathParams().getValue();

    if (condition == Condition.IN && (!indexValue.isArray() || indexValue.getArray().size() == 0)) {
      throw new IllegalArgumentException("Invalid condition " + condition + " for index value " + indexValue);
    }
  }
}
