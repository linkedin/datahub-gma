package com.linkedin.metadata.dao.utils;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.query.Condition;
import com.linkedin.metadata.query.Criterion;
import com.linkedin.metadata.query.CriterionArray;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.IndexCriterion;
import com.linkedin.metadata.query.IndexCriterionArray;
import com.linkedin.metadata.query.IndexFilter;
import com.linkedin.metadata.query.IndexGroupByCriterion;
import com.linkedin.metadata.query.IndexValue;
import com.linkedin.metadata.query.SortOrder;
import com.linkedin.testing.AspectFoo;
import com.linkedin.testing.urn.FooUrn;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.javatuples.Pair;
import org.testng.annotations.Test;

import static com.linkedin.testing.TestUtils.*;
import static org.testng.Assert.*;


public class SQLStatementUtilsTest {

  @Test
  public void testCreateUpsertAspectSql() {
    FooUrn fooUrn = makeFooUrn(1);
    AspectFoo aspectFoo = new AspectFoo();
    String expectedSql =
        "INSERT INTO metadata_entity_foo (urn, a_aspectfoo, lastmodifiedon, lastmodifiedby) VALUE (:urn, "
            + ":metadata, :lastmodifiedon, :lastmodifiedby) ON DUPLICATE KEY UPDATE a_aspectfoo = :metadata;";
    assertEquals(SQLStatementUtils.createAspectUpsertSql(fooUrn, AspectFoo.class), expectedSql);
  }

  @Test
  public void testCreateAspectReadSql() {
    FooUrn fooUrn1 = makeFooUrn(1);
    FooUrn fooUrn2 = makeFooUrn(2);
    Set<Urn> set = new HashSet<>();
    set.add(fooUrn1);
    set.add(fooUrn2);
    String expectedSql =
        "SELECT urn, a_aspectfoo, lastmodifiedon, lastmodifiedby FROM metadata_entity_foo WHERE urn = 'urn:li:foo:1' "
            + "AND a_aspectfoo != '{\"gma_deleted\":true}' UNION ALL SELECT urn, a_aspectfoo, lastmodifiedon, lastmodifiedby "
            + "FROM metadata_entity_foo WHERE urn = 'urn:li:foo:2' AND a_aspectfoo != '{\"gma_deleted\":true}'";
    assertEquals(SQLStatementUtils.createAspectReadSql(AspectFoo.class, set), expectedSql);
  }

  @Test
  public void testCreateFilterSql() {

    IndexFilter indexFilter = new IndexFilter();
    IndexCriterionArray indexCriterionArray = new IndexCriterionArray();

    IndexCriterion indexCriterion1 =
        SQLIndexFilterUtils.createIndexCriterion(AspectFoo.class, "value", Condition.GREATER_THAN_OR_EQUAL_TO,
            IndexValue.create(25));
    IndexCriterion indexCriterion2 =
        SQLIndexFilterUtils.createIndexCriterion(AspectFoo.class, "value", Condition.LESS_THAN, IndexValue.create(50));

    indexCriterionArray.add(indexCriterion1);
    indexCriterionArray.add(indexCriterion2);
    indexFilter.setCriteria(indexCriterionArray);

    String sql = SQLStatementUtils.createFilterSql("metadata_entity_foo", indexFilter,
        SQLIndexFilterUtils.createIndexSortCriterion(AspectFoo.class, "value", SortOrder.ASCENDING));
    String expectedSql = "SELECT *, (SELECT COUNT(urn) FROM metadata_entity_foo WHERE i_aspectfoo$value >= 25\n"
        + "AND i_aspectfoo$value < 50\n"
        + "AND a_aspectfoo != '{\"gma_deleted\":true}') as _total_count FROM metadata_entity_foo\n"
        + "WHERE i_aspectfoo$value >= 25\n" + "AND i_aspectfoo$value < 50\n"
        + "AND a_aspectfoo != '{\"gma_deleted\":true}'";

    assertEquals(sql, expectedSql);
  }

  @Test
  public void testCreateGroupBySql() {
    IndexFilter indexFilter = new IndexFilter();
    IndexCriterionArray indexCriterionArray = new IndexCriterionArray();

    IndexCriterion indexCriterion1 =
        SQLIndexFilterUtils.createIndexCriterion(AspectFoo.class, "value", Condition.GREATER_THAN_OR_EQUAL_TO,
            IndexValue.create(25));
    IndexCriterion indexCriterion2 =
        SQLIndexFilterUtils.createIndexCriterion(AspectFoo.class, "value", Condition.LESS_THAN, IndexValue.create(50));

    indexCriterionArray.add(indexCriterion1);
    indexCriterionArray.add(indexCriterion2);
    indexFilter.setCriteria(indexCriterionArray);

    IndexGroupByCriterion indexGroupByCriterion = new IndexGroupByCriterion();
    indexGroupByCriterion.setAspect(AspectFoo.class.getCanonicalName());
    indexGroupByCriterion.setPath("/value");

    String sql = SQLStatementUtils.createGroupBySql("metadata_entity_foo", indexFilter, indexGroupByCriterion);
    assertEquals(sql, "SELECT count(*) as COUNT, i_aspectfoo$value FROM metadata_entity_foo\n"
        + "WHERE i_aspectfoo$value >= 25\nAND i_aspectfoo$value < 50\n"
        + "AND a_aspectfoo != '{\"gma_deleted\":true}'\nGROUP BY i_aspectfoo$value");
  }

  @Test
  public void testWhereClauseSingleCondition() {
    Criterion criterion = new Criterion().setField("field1").setCondition(Condition.EQUAL).setValue("value1");
    CriterionArray criteria = new CriterionArray(criterion);
    Filter filter = new Filter().setCriteria(criteria);
    assertEquals(SQLStatementUtils.whereClause(filter, Collections.singletonMap(Condition.EQUAL, "="), null), "field1='value1'");
  }

  @Test
  public void testWhereClauseMultiConditionSameName() {
    Criterion criterion1 = new Criterion().setField("field1").setCondition(Condition.EQUAL).setValue("value1");
    Criterion criterion2 = new Criterion().setField("field1").setCondition(Condition.EQUAL).setValue("value2");
    CriterionArray criteria = new CriterionArray(criterion1, criterion2);
    Filter filter = new Filter().setCriteria(criteria);
    assertEquals(SQLStatementUtils.whereClause(filter, Collections.singletonMap(Condition.EQUAL, "="), null), "field1='value1' OR field1='value2'");
  }

  @Test
  public void testWhereClauseMultiConditionDifferentName() {
    Criterion criterion1 = new Criterion().setField("field1").setCondition(Condition.EQUAL).setValue("value1");
    Criterion criterion2 = new Criterion().setField("field2").setCondition(Condition.EQUAL).setValue("value2");
    CriterionArray criteria = new CriterionArray(criterion1, criterion2);
    Filter filter = new Filter().setCriteria(criteria);
    assertEquals(SQLStatementUtils.whereClause(filter, Collections.singletonMap(Condition.EQUAL, "="), null), "field1='value1' AND field2='value2'");
  }

  @Test
  public void testWhereClauseMultiConditionMixedName() {
    Criterion criterion1 = new Criterion().setField("field1").setCondition(Condition.EQUAL).setValue("value1");
    Criterion criterion2 = new Criterion().setField("field1").setCondition(Condition.EQUAL).setValue("value2");
    Criterion criterion3 = new Criterion().setField("field2").setCondition(Condition.EQUAL).setValue("value2");
    Criterion criterion4 = new Criterion().setField("field3").setCondition(Condition.EQUAL).setValue("value3");
    CriterionArray criteria = new CriterionArray(criterion1, criterion2, criterion3, criterion4);
    Filter filter = new Filter().setCriteria(criteria);
    assertEquals(SQLStatementUtils.whereClause(filter, Collections.singletonMap(Condition.EQUAL, "="), null),
        "(field1='value1' OR field1='value2') AND field3='value3' AND field2='value2'");
  }

  @Test
  public void testWhereClauseMultiFilters() {
    Criterion criterion1 = new Criterion().setField("field1").setCondition(Condition.EQUAL).setValue("value1");
    Criterion criterion2 = new Criterion().setField("field1").setCondition(Condition.EQUAL).setValue("value2");
    Criterion criterion3 = new Criterion().setField("field2").setCondition(Condition.EQUAL).setValue("value2");
    Criterion criterion4 = new Criterion().setField("field3").setCondition(Condition.EQUAL).setValue("value3");
    CriterionArray criteria1 = new CriterionArray(criterion1, criterion2, criterion3, criterion4);
    Filter filter1 = new Filter().setCriteria(criteria1);

    Criterion criterion5 = new Criterion().setField("field1").setCondition(Condition.EQUAL).setValue("value1");
    Criterion criterion6 = new Criterion().setField("field1").setCondition(Condition.EQUAL).setValue("value2");
    CriterionArray criteria2 = new CriterionArray(criterion5, criterion6);
    Filter filter2 = new Filter().setCriteria(criteria2);

    assertEquals(SQLStatementUtils.whereClause(Collections.singletonMap(Condition.EQUAL, "="), new Pair<>(filter1, "foo"), new Pair<>(filter2, "bar")),
        "(foo.field3='value3' AND foo.field2='value2' AND (foo.field1='value1' OR foo.field1='value2')) AND (bar.field1='value1' OR bar.field1='value2')");
  }
}