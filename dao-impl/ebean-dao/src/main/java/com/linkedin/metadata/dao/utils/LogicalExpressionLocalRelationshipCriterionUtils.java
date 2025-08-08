package com.linkedin.metadata.dao.utils;

import com.linkedin.data.template.SetMode;
import com.linkedin.metadata.query.LocalRelationshipCriterion;
import com.linkedin.metadata.query.LocalRelationshipCriterionArray;
import com.linkedin.metadata.query.LocalRelationshipFilter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import pegasus.com.linkedin.metadata.query.LogicalExpressionLocalRelationshipCriterion;
import pegasus.com.linkedin.metadata.query.LogicalExpressionLocalRelationshipCriterionArray;
import pegasus.com.linkedin.metadata.query.LogicalOperation;
import pegasus.com.linkedin.metadata.query.innerLogicalOperation.Operator;


/**
 * Utility class for handling logical expressions in LocalRelationshipCriterion and other related utility.
 */
public final class LogicalExpressionLocalRelationshipCriterionUtils {

  private LogicalExpressionLocalRelationshipCriterionUtils() {
  }

  /**
   * Converts all the LocalRelationshipCriterion into a LogicalExpressionLocalRelationshipCriterion. With the rules:
   * 1. first, for the criterion with the same field, they will be grouped with OR logical.
   * 2. then, all the OR clauses (with different fields) will be grouped with AND logical.
   * @param filter LocalRelationshipFilter should have either `criteria` or `logicalExpressionCriteria`, but not both.
   * @return a new hard-copy of the filter with only LogicalExpressionLocalRelationshipCriterion field
   */
  @Nullable
  public static LocalRelationshipFilter normalizeLocalRelationshipFilter(@Nullable LocalRelationshipFilter filter) {
    if (!filterHasNonEmptyCriteria(filter)) {
      return filter;
    }

    if (filter.hasCriteria() && filter.hasLogicalExpressionCriteria()) {
      throw new IllegalArgumentException("Cannot have both `criteria` and `logicalExpressionCriteria`.");
    } else if (filter.hasLogicalExpressionCriteria()) {
      return filter;
    }

    // group criteria by field name
    final Map<String, LogicalExpressionLocalRelationshipCriterionArray> groupedByField = new HashMap<>();
    for (LocalRelationshipCriterion criterion : filter.getCriteria()) {
      final String field = criterion.getField().toString();
      // if the field is not present, create a new LogicalExpressionLocalRelationshipCriterionArray
      groupedByField.computeIfAbsent(field, k -> new LogicalExpressionLocalRelationshipCriterionArray())
          // wraps criterion into LogicalExpressionLocalRelationshipCriterion
          .add(wrapCriterionAsLogicalExpression(criterion));
    }

    final List<LogicalExpressionLocalRelationshipCriterion> fieldGroups = groupedByField.values().stream()
        .map(array -> buildLogicalGroup(Operator.OR, array))
        .filter(Objects::nonNull)
        .collect(Collectors.toList());

    // combine all field groups with AND
    final LogicalExpressionLocalRelationshipCriterion root =
        buildLogicalGroup(Operator.AND, new LogicalExpressionLocalRelationshipCriterionArray(fieldGroups));

    if (root == null) {
      return null;
    }

    // return a hard copy of filter with new LogicalExpressionCriteria
    return new LocalRelationshipFilter()
        .setLogicalExpressionCriteria(root)
        .setDirection(filter.getDirection(), SetMode.IGNORE_NULL);
  }

  /**
   * Flattens a LogicalExpressionLocalRelationshipCriterion into a LocalRelationshipCriterionArray. Notice that this will
   * lose the logical structure and only return the leaf criteria.
   * @param criteria LogicalExpressionLocalRelationshipCriterion to flatten
   * @return LocalRelationshipCriterionArray containing all leaf criteria with no logical structure
   */
  @Nonnull
  public static LocalRelationshipCriterionArray flattenLogicalExpressionLocalRelationshipCriterion(
      @Nonnull LogicalExpressionLocalRelationshipCriterion criteria) {
    final List<LocalRelationshipCriterion> flattened = new ArrayList<>();
    final Deque<LogicalExpressionLocalRelationshipCriterion> stack = new ArrayDeque<>();

    stack.push(criteria);

    while (!stack.isEmpty()) {
      final LogicalExpressionLocalRelationshipCriterion current = stack.pop();

      if (!current.hasExpr()) {
        continue;
      }

      final LogicalExpressionLocalRelationshipCriterion.Expr expr = current.getExpr();

      if (expr.isCriterion()) {
        flattened.add(expr.getCriterion());
      } else if (expr.isLogical()) {
        final LogicalOperation operation = expr.getLogical();
        final List<LogicalExpressionLocalRelationshipCriterion> expressions = operation.getExpressions();
        // reverse to maintain original left-to-right order
        for (int i = expressions.size() - 1; i >= 0; i--) {
          stack.push(expressions.get(i));
        }
      }
    }

    return new LocalRelationshipCriterionArray(flattened);
  }

  /**
   * Wraps a single LocalRelationshipCriterion into a LogicalExpressionLocalRelationshipCriterion node.
   * @param criterion LocalRelationshipCriterion
   * @return LogicalExpressionLocalRelationshipCriterion
   */
  @Nonnull
  public static LogicalExpressionLocalRelationshipCriterion wrapCriterionAsLogicalExpression(@Nonnull LocalRelationshipCriterion criterion) {
    final LogicalExpressionLocalRelationshipCriterion.Expr expr = new LogicalExpressionLocalRelationshipCriterion.Expr();
    expr.setCriterion(criterion);

    return new LogicalExpressionLocalRelationshipCriterion()
        .setExpr(expr);
  }

  /**
   * Combines multiple LogicalExpressionLocalRelationshipCriterion into a single LogicalExpressionLocalRelationshipCriterion
   * with the given operator.
   * @param op one of the Operator, e.g. AND
   * @param children LogicalExpressionLocalRelationshipCriterionArray
   * @return LogicalExpressionLocalRelationshipCriterion
   */
  @Nullable
  public static LogicalExpressionLocalRelationshipCriterion buildLogicalGroup(@Nonnull Operator op,
      @Nonnull LogicalExpressionLocalRelationshipCriterionArray children) {
    if (children.size() == 0) {
      return null;
    }

    if (children.size() == 1 && op != Operator.NOT) {
      // avoid wrapping unnecessarily
      return children.get(0);
    }

    final LogicalOperation operation = new LogicalOperation();
    operation.setOp(op);
    operation.setExpressions(children);

    final LogicalExpressionLocalRelationshipCriterion.Expr expr = new LogicalExpressionLocalRelationshipCriterion.Expr();
    expr.setLogical(operation);

    return new LogicalExpressionLocalRelationshipCriterion()
        .setExpr(expr);
  }

  /**
   * Checks if the given LocalRelationshipFilter has non-empty criteria. Checks both `criteria` and `logicalExpressionCriteria`.
   * @param filter LocalRelationshipFilter to check
   * @return true if the filter has non-empty criteria, false otherwise
   */
  public static boolean filterHasNonEmptyCriteria(@Nullable LocalRelationshipFilter filter) {
    if (filter == null) {
      return false;
    }

    return (filter.hasCriteria() && !filter.getCriteria().isEmpty())
        || (filter.hasLogicalExpressionCriteria() && filter.getLogicalExpressionCriteria() != null
            && !flattenLogicalExpressionLocalRelationshipCriterion(filter.getLogicalExpressionCriteria()).isEmpty());
  }
}
