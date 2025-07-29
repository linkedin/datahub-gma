package com.linkedin.metadata.dao.utils;

import com.linkedin.metadata.query.AspectField;
import com.linkedin.metadata.query.Condition;
import com.linkedin.metadata.query.LocalRelationshipCriterion;
import com.linkedin.metadata.query.LocalRelationshipCriterionArray;
import com.linkedin.metadata.query.LocalRelationshipFilter;
import com.linkedin.metadata.query.LocalRelationshipValue;
import com.linkedin.metadata.query.UrnField;
import java.util.Arrays;
import org.testng.annotations.Test;
import pegasus.com.linkedin.metadata.query.LogicalExpressionLocalRelationshipCriterion;
import pegasus.com.linkedin.metadata.query.LogicalExpressionLocalRelationshipCriterionArray;
import pegasus.com.linkedin.metadata.query.LogicalOperation;
import pegasus.com.linkedin.metadata.query.innerLogicalOperation.Operator;

import static com.linkedin.metadata.dao.utils.LogicalExpressionLocalRelationshipCriterionUtils.*;
import static org.testng.Assert.*;


public class LogicalExpressionLocalRelationshipCriterionUtilsTest {

  @Test
  public void testNormalizeLocalRelationshipFilterWithNull() {
    assertNull(LogicalExpressionLocalRelationshipCriterionUtils.normalizeLocalRelationshipFilter(null));
  }

  @Test
  public void testNormalizeLocalRelationshipFilterWithEmptyCriteria() {
    LocalRelationshipFilter filter = new LocalRelationshipFilter();
    LocalRelationshipFilter result = LogicalExpressionLocalRelationshipCriterionUtils.normalizeLocalRelationshipFilter(filter);
    assertEquals(filter, result);
  }

  @Test
  public void testNormalizeLocalRelationshipFilterHasBothCriteriaAndLogicalExpression() {
    LocalRelationshipFilter filter = new LocalRelationshipFilter()
        .setLogicalExpressionCriteria(new LogicalExpressionLocalRelationshipCriterion())
        .setCriteria(new LocalRelationshipCriterionArray(new LocalRelationshipCriterion()));

    assertThrows(IllegalArgumentException.class, () -> {
      LogicalExpressionLocalRelationshipCriterionUtils.normalizeLocalRelationshipFilter(filter);
    });
  }

  @Test
  public void testNormalizeLocalRelationshipFilterHasLogicalExpressionAlready() {
    LocalRelationshipFilter filter = new LocalRelationshipFilter()
        .setLogicalExpressionCriteria(new LogicalExpressionLocalRelationshipCriterion());
    LocalRelationshipFilter result = LogicalExpressionLocalRelationshipCriterionUtils.normalizeLocalRelationshipFilter(filter);
    assertEquals(filter, result);
  }

  @Test
  public void testNormalizeLocalRelationshipFilterWithSingleCriterionNoGrouping() {
    LocalRelationshipCriterion criterion = createLocalRelationshipCriterionWithUrnField("foo");
    LocalRelationshipFilter filter = new LocalRelationshipFilter()
        .setCriteria(new LocalRelationshipCriterionArray(criterion));

    LocalRelationshipFilter normalized = LogicalExpressionLocalRelationshipCriterionUtils.normalizeLocalRelationshipFilter(filter);
    LogicalExpressionLocalRelationshipCriterion root = normalized.getLogicalExpressionCriteria();

    assertNotNull(root);
    assertTrue(root.hasExpr());
    assertTrue(root.getExpr().isCriterion());
    assertEquals(criterion, root.getExpr().getCriterion());
  }

  @Test
  public void testNormalizeLocalRelationshipFilterWithMultipleCriteriaSameFieldGroupedWithOr() {
    LocalRelationshipCriterion c1 = createLocalRelationshipCriterionWithUrnField("foo1");
    LocalRelationshipCriterion c2 = createLocalRelationshipCriterionWithUrnField("foo2");

    LocalRelationshipFilter filter = new LocalRelationshipFilter()
        .setCriteria(new LocalRelationshipCriterionArray(c1, c2));

    LocalRelationshipFilter normalized = LogicalExpressionLocalRelationshipCriterionUtils.normalizeLocalRelationshipFilter(filter);
    LogicalExpressionLocalRelationshipCriterion root = normalized.getLogicalExpressionCriteria();

    assertTrue(root.hasExpr());
    assertTrue(root.getExpr().isLogical());
    LogicalOperation logical = root.getExpr().getLogical();
    assertEquals(logical.getOp(), Operator.OR);
    assertEquals(logical.getExpressions().size(), 2);
  }

  @Test
  public void testNormalizeLocalRelationshipFilterWithDifferentFieldsGroupedWithAnd() {
    LocalRelationshipCriterion c1 = createLocalRelationshipCriterionWithUrnField("foo");
    LocalRelationshipCriterion c2 = createLocalRelationshipCriterionWithAspectField("bar");

    LocalRelationshipFilter filter = new LocalRelationshipFilter()
        .setCriteria(new LocalRelationshipCriterionArray(c1, c2));

    LocalRelationshipFilter normalized = LogicalExpressionLocalRelationshipCriterionUtils.normalizeLocalRelationshipFilter(filter);
    LogicalExpressionLocalRelationshipCriterion root = normalized.getLogicalExpressionCriteria();

    assertTrue(root.hasExpr());
    assertTrue(root.getExpr().isLogical());
    LogicalOperation andOp = root.getExpr().getLogical();
    assertEquals(andOp.getOp(), Operator.AND);
    assertEquals(andOp.getExpressions().size(), 2);
  }

  @Test
  public void testNormalizeLocalRelationshipFilterWithMixedFieldsNestedLogicalGroups() {
    LocalRelationshipCriterion c1 = createLocalRelationshipCriterionWithUrnField("foo1");
    LocalRelationshipCriterion c2 = createLocalRelationshipCriterionWithUrnField("foo2");
    LocalRelationshipCriterion c3 = createLocalRelationshipCriterionWithAspectField("bar");

    LocalRelationshipFilter filter = new LocalRelationshipFilter()
        .setCriteria(new LocalRelationshipCriterionArray(c1, c2, c3));

    LocalRelationshipFilter normalized = LogicalExpressionLocalRelationshipCriterionUtils.normalizeLocalRelationshipFilter(filter);
    LogicalExpressionLocalRelationshipCriterion root = normalized.getLogicalExpressionCriteria();

    assertTrue(root.hasExpr());
    assertTrue(root.getExpr().isLogical());

    LogicalOperation rootLogical = root.getExpr().getLogical();
    assertEquals(rootLogical.getOp(), Operator.AND);
    assertEquals(rootLogical.getExpressions().size(), 2);

    LogicalExpressionLocalRelationshipCriterion child1 = rootLogical.getExpressions().get(0);
    assertTrue(child1.getExpr().isCriterion());
    assertEquals(child1.getExpr().getCriterion(), c3);

    LogicalExpressionLocalRelationshipCriterion child2 = rootLogical.getExpressions().get(1);
    assertTrue(child2.getExpr().isLogical());
    LogicalOperation childLogical = child2.getExpr().getLogical();
    assertEquals(childLogical.getOp(), Operator.OR);
    assertEquals(childLogical.getExpressions().size(), 2);
    assertTrue(childLogical.getExpressions().get(0).getExpr().isCriterion());
    assertEquals(childLogical.getExpressions().get(0).getExpr().getCriterion(), c1);
    assertTrue(childLogical.getExpressions().get(1).getExpr().isCriterion());
    assertEquals(childLogical.getExpressions().get(1).getExpr().getCriterion(), c2);
  }

  @Test
  public void testFlattenLogicalExpressionLocalRelationshipCriterion() {
    LocalRelationshipCriterion c1 = createLocalRelationshipCriterionWithUrnField("foo1");
    LocalRelationshipCriterion c2 = createLocalRelationshipCriterionWithUrnField("foo2");
    LocalRelationshipCriterion c3 = createLocalRelationshipCriterionWithAspectField("bar");

    LogicalExpressionLocalRelationshipCriterion n1 = wrapCriterionAsLogicalExpression(c1);
    LogicalExpressionLocalRelationshipCriterion n2 = wrapCriterionAsLogicalExpression(c2);
    LogicalExpressionLocalRelationshipCriterion n3 = wrapCriterionAsLogicalExpression(c3);

    LogicalExpressionLocalRelationshipCriterion orNode =
        buildLogicalGroup(Operator.OR, new LogicalExpressionLocalRelationshipCriterionArray(n1, n2));

    LogicalExpressionLocalRelationshipCriterion root =
        buildLogicalGroup(Operator.AND, new LogicalExpressionLocalRelationshipCriterionArray(orNode, n3));

    LocalRelationshipCriterionArray result =
        LogicalExpressionLocalRelationshipCriterionUtils.flattenLogicalExpressionLocalRelationshipCriterion(root);

    assertEquals(result.size(), 3);
    assertTrue(result.containsAll(Arrays.asList(c1, c2, c3)));
  }

  @Test
  public void testWrapCriterionAsLogicalExpression() {
    LocalRelationshipCriterion inputCriterion = createLocalRelationshipCriterionWithUrnField("foo");

    LogicalExpressionLocalRelationshipCriterion wrapped =
        LogicalExpressionLocalRelationshipCriterionUtils.wrapCriterionAsLogicalExpression(inputCriterion);

    assertNotNull(wrapped);
    assertTrue(wrapped.hasExpr());
    assertTrue(wrapped.getExpr().isCriterion());
    assertEquals(wrapped.getExpr().getCriterion(), inputCriterion);
  }

  @Test
  public void testBuildLogicalGroupWithSingleCriterion() {
    LocalRelationshipCriterion criterion = createLocalRelationshipCriterionWithUrnField("foo");

    LogicalExpressionLocalRelationshipCriterion result =
        LogicalExpressionLocalRelationshipCriterionUtils.buildLogicalGroup(Operator.OR,
            new LogicalExpressionLocalRelationshipCriterionArray(wrapCriterionAsLogicalExpression(criterion)));

    assertNotNull(result);
    assertTrue(result.hasExpr());
    assertTrue(result.getExpr().isCriterion());
    assertEquals(result.getExpr().getCriterion(), criterion);
  }

  @Test
  public void testBuildLogicalGroupWithMultipleCriteria() {
    LocalRelationshipCriterion c1 = createLocalRelationshipCriterionWithUrnField("foo1");
    LocalRelationshipCriterion c2 = createLocalRelationshipCriterionWithUrnField("foo2");

    LogicalExpressionLocalRelationshipCriterion result =
        LogicalExpressionLocalRelationshipCriterionUtils.buildLogicalGroup(Operator.OR,
            new LogicalExpressionLocalRelationshipCriterionArray(
                wrapCriterionAsLogicalExpression(c1),
                wrapCriterionAsLogicalExpression(c2)));

    assertNotNull(result);
    assertTrue(result.hasExpr());
    assertTrue(result.getExpr().isLogical());

    LogicalOperation op = result.getExpr().getLogical();
    assertEquals(op.getOp(), Operator.OR);
    assertEquals(op.getExpressions().size(), 2);
    assertEquals(op.getExpressions().get(0).getExpr().getCriterion(), c1);
    assertEquals(op.getExpressions().get(1).getExpr().getCriterion(), c2);
  }

  @Test
  public void testFilterHasNonEmptyCriteriaWithNull() {
    assertFalse(filterHasNonEmptyCriteria(null));
  }

  @Test
  public void testFilterHasNonEmptyCriteriaWithNeitherCriteriaSet() {
    LocalRelationshipFilter filter = new LocalRelationshipFilter();
    assertFalse(filterHasNonEmptyCriteria(filter));
  }

  @Test
  public void testFilterHasNonEmptyCriteriaWithCriteriaSetButEmpty() {
    LocalRelationshipFilter filter = new LocalRelationshipFilter()
        .setCriteria(new LocalRelationshipCriterionArray());
    assertFalse(filterHasNonEmptyCriteria(filter));
  }

  @Test
  public void testFilterHasNonEmptyCriteriaWithCriteriaNonEmpty() {
    LocalRelationshipCriterion c1 = createLocalRelationshipCriterionWithUrnField("foo");
    LocalRelationshipFilter filter = new LocalRelationshipFilter()
        .setCriteria(new LocalRelationshipCriterionArray(c1));
    assertTrue(filterHasNonEmptyCriteria(filter));
  }

  @Test
  public void testFilterHasNonEmptyCriteriaWithLogicalExpressionSetButEmpty() {
    LocalRelationshipFilter filter = new LocalRelationshipFilter()
        .setLogicalExpressionCriteria(new LogicalExpressionLocalRelationshipCriterion());
    assertFalse(filterHasNonEmptyCriteria(filter));
  }

  @Test
  public void testFilterHasNonEmptyCriteriaWithLogicalExpressionSetExprSetButEmpty() {
    LocalRelationshipFilter filter = new LocalRelationshipFilter()
        .setLogicalExpressionCriteria(new LogicalExpressionLocalRelationshipCriterion().setExpr(
            new LogicalExpressionLocalRelationshipCriterion.Expr()));
    assertFalse(filterHasNonEmptyCriteria(filter));
  }

  @Test
  public void testFilterHasNonEmptyCriteriaWithLogicalExpressionNonEmpty() {
    LogicalOperation op = new LogicalOperation();
    op.setOp(Operator.AND);
    op.setExpressions(new LogicalExpressionLocalRelationshipCriterionArray(
        wrapCriterionAsLogicalExpression(createLocalRelationshipCriterionWithUrnField("foo1")),
        wrapCriterionAsLogicalExpression(createLocalRelationshipCriterionWithUrnField("foo2"))));

    LogicalExpressionLocalRelationshipCriterion.Expr expr = new LogicalExpressionLocalRelationshipCriterion.Expr();
    expr.setLogical(op);

    LogicalExpressionLocalRelationshipCriterion criteria = new LogicalExpressionLocalRelationshipCriterion();
    criteria.setExpr(expr);

    LocalRelationshipFilter filter = new LocalRelationshipFilter()
        .setLogicalExpressionCriteria(criteria);
    assertTrue(filterHasNonEmptyCriteria(filter));
  }

  private LocalRelationshipCriterion createLocalRelationshipCriterionWithUrnField(String value) {
    LocalRelationshipCriterion.Field field = new LocalRelationshipCriterion.Field();
    field.setUrnField(new UrnField());
    return new LocalRelationshipCriterion()
        .setField(field)
        .setCondition(Condition.EQUAL)
        .setValue(LocalRelationshipValue.create(value));
  }

  private LocalRelationshipCriterion createLocalRelationshipCriterionWithAspectField(String value) {
    LocalRelationshipCriterion.Field field = new LocalRelationshipCriterion.Field();
    field.setAspectField(new AspectField().setAspect("status"));
    return new LocalRelationshipCriterion()
        .setField(field)
        .setCondition(Condition.EQUAL)
        .setValue(LocalRelationshipValue.create(value));
  }

}
