package com.linkedin.metadata.dao.utils;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.query.AspectField;
import com.linkedin.metadata.query.Condition;
import com.linkedin.metadata.query.IndexCriterion;
import com.linkedin.metadata.query.IndexCriterionArray;
import com.linkedin.metadata.query.IndexFilter;
import com.linkedin.metadata.query.IndexGroupByCriterion;
import com.linkedin.metadata.query.IndexValue;
import com.linkedin.metadata.query.LocalRelationshipCriterion;
import com.linkedin.metadata.query.LocalRelationshipCriterionArray;
import com.linkedin.metadata.query.LocalRelationshipFilter;
import com.linkedin.metadata.query.LocalRelationshipValue;
import com.linkedin.metadata.query.RelationshipField;
import com.linkedin.metadata.query.UrnField;
import com.linkedin.testing.AspectBar;
import com.linkedin.testing.AspectFoo;
import com.linkedin.testing.BarAsset;
import com.linkedin.testing.urn.BarUrn;
import com.linkedin.testing.urn.FooUrn;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.javatuples.Pair;
import org.testng.annotations.Test;

import static com.linkedin.metadata.dao.utils.SQLSchemaUtils.*;
import static com.linkedin.testing.TestUtils.*;
import static org.testng.Assert.*;


public class SQLStatementUtilsTest {

  @Test
  public void testCreateUpsertAspectSql() {
    FooUrn fooUrn = makeFooUrn(1);
    String expectedSql =
        "INSERT INTO metadata_entity_foo (urn, a_urn, a_aspectfoo, lastmodifiedon, lastmodifiedby) VALUE (:urn, "
            + ":a_urn, :metadata, :lastmodifiedon, :lastmodifiedby) ON DUPLICATE KEY UPDATE a_aspectfoo = :metadata,"
            + " lastmodifiedon = :lastmodifiedon;";
    assertEquals(SQLStatementUtils.createAspectUpsertSql(fooUrn, AspectFoo.class, true, false), expectedSql);

    expectedSql =
        "INSERT INTO metadata_entity_foo (urn, a_aspectfoo, lastmodifiedon, lastmodifiedby) VALUE (:urn, "
            + ":metadata, :lastmodifiedon, :lastmodifiedby) ON DUPLICATE KEY UPDATE a_aspectfoo = :metadata,"
            + " lastmodifiedon = :lastmodifiedon;";
    assertEquals(SQLStatementUtils.createAspectUpsertSql(fooUrn, AspectFoo.class, false, false), expectedSql);
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
            + "AND JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL UNION ALL SELECT urn, a_aspectfoo, lastmodifiedon, lastmodifiedby "
            + "FROM metadata_entity_foo WHERE urn = 'urn:li:foo:2' AND JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL";
    assertEquals(SQLStatementUtils.createAspectReadSql(AspectFoo.class, set, false, false), expectedSql);
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

    String sql1 = SQLStatementUtils.createFilterSql("foo", indexFilter, true, false);
    String expectedSql1 = "SELECT *, (SELECT COUNT(urn) FROM metadata_entity_foo WHERE a_aspectfoo IS NOT NULL\n"
        + "AND JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL\n" + "AND i_aspectfoo$value >= 25\n"
        + "AND a_aspectfoo IS NOT NULL\n" + "AND JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL\n"
        + "AND i_aspectfoo$value < 50) as _total_count FROM metadata_entity_foo\n" + "WHERE a_aspectfoo IS NOT NULL\n"
        + "AND JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL\n" + "AND i_aspectfoo$value >= 25\n"
        + "AND a_aspectfoo IS NOT NULL\n" + "AND JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL\n"
        + "AND i_aspectfoo$value < 50";

    assertEquals(sql1, expectedSql1);

    String sql2 = SQLStatementUtils.createFilterSql("foo", indexFilter, true, true);
    String expectedSql2 = "SELECT *, (SELECT COUNT(urn) FROM metadata_entity_foo WHERE a_aspectfoo IS NOT NULL\n"
        + "AND JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL\n" + "AND i_aspectfoo0value >= 25\n"
        + "AND a_aspectfoo IS NOT NULL\n" + "AND JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL\n"
        + "AND i_aspectfoo0value < 50) as _total_count FROM metadata_entity_foo\n" + "WHERE a_aspectfoo IS NOT NULL\n"
        + "AND JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL\n" + "AND i_aspectfoo0value >= 25\n"
        + "AND a_aspectfoo IS NOT NULL\n" + "AND JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL\n"
        + "AND i_aspectfoo0value < 50";

    assertEquals(sql2, expectedSql2);
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

    String sql1 = SQLStatementUtils.createGroupBySql("foo", indexFilter, indexGroupByCriterion, false);
    assertEquals(sql1, "SELECT count(*) as COUNT, i_aspectfoo$value FROM metadata_entity_foo\n"
        + "WHERE a_aspectfoo IS NOT NULL\n" + "AND JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL\n"
        + "AND i_aspectfoo$value >= 25\n" + "AND a_aspectfoo IS NOT NULL\n"
        + "AND JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL\n" + "AND i_aspectfoo$value < 50\n"
        + "GROUP BY i_aspectfoo$value");

    String sql2 = SQLStatementUtils.createGroupBySql("foo", indexFilter, indexGroupByCriterion, true);
    assertEquals(sql2, "SELECT count(*) as COUNT, i_aspectfoo0value FROM metadata_entity_foo\n"
        + "WHERE a_aspectfoo IS NOT NULL\n" + "AND JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL\n"
        + "AND i_aspectfoo0value >= 25\n" + "AND a_aspectfoo IS NOT NULL\n"
        + "AND JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL\n" + "AND i_aspectfoo0value < 50\n"
        + "GROUP BY i_aspectfoo0value");
  }

  @Test
  public void testWhereClauseSingleCondition() {
    LocalRelationshipCriterion.Field field = new LocalRelationshipCriterion.Field();
    field.setUrnField(new UrnField());
    LocalRelationshipCriterion criterion = new LocalRelationshipCriterion()
        .setField(field)
        .setCondition(Condition.EQUAL)
        .setValue(LocalRelationshipValue.create("value1"));
    LocalRelationshipCriterionArray criteria = new LocalRelationshipCriterionArray(criterion);
    LocalRelationshipFilter filter = new LocalRelationshipFilter().setCriteria(criteria);
    assertEquals(SQLStatementUtils.whereClause(filter, Collections.singletonMap(Condition.EQUAL, "="), null, false), "urn='value1'");
    assertEquals(SQLStatementUtils.whereClause(filter, Collections.singletonMap(Condition.EQUAL, "="), null, true), "urn='value1'");
  }

  @Test
  public void testWhereClauseMultiConditionSameName() {
    LocalRelationshipCriterion.Field field1 = new LocalRelationshipCriterion.Field();
    field1.setUrnField(new UrnField());
    LocalRelationshipCriterion criterion1 = new LocalRelationshipCriterion()
        .setField(field1)
        .setCondition(Condition.EQUAL)
        .setValue(LocalRelationshipValue.create("value1"));

    LocalRelationshipCriterion.Field field2 = new LocalRelationshipCriterion.Field();
    field2.setUrnField(new UrnField());
    LocalRelationshipCriterion criterion2 = new LocalRelationshipCriterion()
        .setField(field2)
        .setCondition(Condition.EQUAL)
        .setValue(LocalRelationshipValue.create("value2"));

    LocalRelationshipCriterionArray criteria = new LocalRelationshipCriterionArray(criterion1, criterion2);
    LocalRelationshipFilter filter = new LocalRelationshipFilter().setCriteria(criteria);
    assertEquals(SQLStatementUtils.whereClause(filter, Collections.singletonMap(Condition.EQUAL, "="), null, false), "urn='value1' OR urn='value2'");
    assertEquals(SQLStatementUtils.whereClause(filter, Collections.singletonMap(Condition.EQUAL, "="), null, true), "urn='value1' OR urn='value2'");
  }

  @Test
  public void testWhereClauseMultiConditionDifferentName() {
    LocalRelationshipCriterion.Field field1 = new LocalRelationshipCriterion.Field();
    field1.setUrnField(new UrnField());
    LocalRelationshipCriterion criterion1 = new LocalRelationshipCriterion()
        .setField(field1)
        .setCondition(Condition.EQUAL)
        .setValue(LocalRelationshipValue.create("value1"));

    LocalRelationshipCriterion.Field field2 = new LocalRelationshipCriterion.Field();
    field2.setAspectField((new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value")));
    LocalRelationshipCriterion criterion2 = new LocalRelationshipCriterion()
        .setField(field2)
        .setCondition(Condition.EQUAL)
        .setValue(LocalRelationshipValue.create("value2"));

    LocalRelationshipCriterionArray criteria = new LocalRelationshipCriterionArray(criterion1, criterion2);
    LocalRelationshipFilter filter = new LocalRelationshipFilter().setCriteria(criteria);
    assertEquals(SQLStatementUtils.whereClause(filter, Collections.singletonMap(Condition.EQUAL, "="), null, false),
        "urn='value1' AND i_aspectfoo$value='value2'");
    assertEquals(SQLStatementUtils.whereClause(filter, Collections.singletonMap(Condition.EQUAL, "="), null, true),
        "urn='value1' AND i_aspectfoo0value='value2'");
  }

  @Test
  public void testWhereClauseMultiConditionMixedName() {
    LocalRelationshipCriterion.Field field1 = new LocalRelationshipCriterion.Field();
    field1.setUrnField(new UrnField());
    LocalRelationshipCriterion criterion1 = new LocalRelationshipCriterion()
        .setField(field1)
        .setCondition(Condition.EQUAL)
        .setValue(LocalRelationshipValue.create("value1"));

    LocalRelationshipCriterion.Field field2 = new LocalRelationshipCriterion.Field();
    field2.setAspectField((new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value")));
    LocalRelationshipCriterion criterion2 = new LocalRelationshipCriterion()
        .setField(field2)
        .setCondition(Condition.EQUAL)
        .setValue(LocalRelationshipValue.create("value2"));

    LocalRelationshipCriterion.Field field3 = new LocalRelationshipCriterion.Field();
    field3.setUrnField(new UrnField());
    LocalRelationshipCriterion criterion3 = new LocalRelationshipCriterion()
        .setField(field3)
        .setCondition(Condition.EQUAL)
        .setValue(LocalRelationshipValue.create("value3"));

    LocalRelationshipCriterion.Field field4 = new LocalRelationshipCriterion.Field();
    field4.setRelationshipField(((new RelationshipField().setPath("/value"))));
    LocalRelationshipCriterion criterion4 = new LocalRelationshipCriterion()
        .setField(field4)
        .setCondition(Condition.EQUAL)
        .setValue(LocalRelationshipValue.create("value4"));

    LocalRelationshipCriterionArray criteria = new LocalRelationshipCriterionArray(criterion1, criterion2, criterion3, criterion4);
    LocalRelationshipFilter filter = new LocalRelationshipFilter().setCriteria(criteria);
    assertConditionsEqual(SQLStatementUtils.whereClause(filter, Collections.singletonMap(Condition.EQUAL, "="), null, false),
        "(urn='value1' OR urn='value3') AND metadata$value='value4' AND i_aspectfoo$value='value2'");
    assertConditionsEqual(SQLStatementUtils.whereClause(filter, Collections.singletonMap(Condition.EQUAL, "="), null, true),
        "(urn='value1' OR urn='value3') AND metadata0value='value4' AND i_aspectfoo0value='value2'");
  }

  @Test
  public void testWhereClauseMultiFilters() {
    LocalRelationshipCriterion.Field field1 = new LocalRelationshipCriterion.Field();
    field1.setUrnField(new UrnField());
    LocalRelationshipCriterion criterion1 = new LocalRelationshipCriterion()
        .setField(field1)
        .setCondition(Condition.EQUAL)
        .setValue(LocalRelationshipValue.create("value1"));

    LocalRelationshipCriterion.Field field2 = new LocalRelationshipCriterion.Field();
    field2.setAspectField((new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value")));
    LocalRelationshipCriterion criterion2 = new LocalRelationshipCriterion()
        .setField(field2)
        .setCondition(Condition.EQUAL)
        .setValue(LocalRelationshipValue.create("value2"));

    LocalRelationshipCriterion.Field field3 = new LocalRelationshipCriterion.Field();
    field3.setUrnField(new UrnField());
    LocalRelationshipCriterion criterion3 = new LocalRelationshipCriterion()
        .setField(field3)
        .setCondition(Condition.EQUAL)
        .setValue(LocalRelationshipValue.create("value3"));

    LocalRelationshipCriterion.Field field4 = new LocalRelationshipCriterion.Field();
    field4.setRelationshipField(((new RelationshipField().setPath("/value"))));
    LocalRelationshipCriterion criterion4 = new LocalRelationshipCriterion()
        .setField(field4)
        .setCondition(Condition.EQUAL)
        .setValue(LocalRelationshipValue.create("value4"));

    LocalRelationshipCriterionArray criteria1 = new LocalRelationshipCriterionArray(criterion1, criterion2, criterion3, criterion4);
    LocalRelationshipFilter filter1 = new LocalRelationshipFilter().setCriteria(criteria1);

    LocalRelationshipCriterion.Field field5 = new LocalRelationshipCriterion.Field();
    field5.setUrnField(new UrnField());
    LocalRelationshipCriterion criterion5 = new LocalRelationshipCriterion()
        .setField(field5)
        .setCondition(Condition.EQUAL)
        .setValue(LocalRelationshipValue.create("value1"));

    LocalRelationshipCriterion.Field field6 = new LocalRelationshipCriterion.Field();
    field6.setUrnField(new UrnField());
    LocalRelationshipCriterion criterion6 = new LocalRelationshipCriterion()
        .setField(field6)
        .setCondition(Condition.EQUAL)
        .setValue(LocalRelationshipValue.create("value2"));

    LocalRelationshipCriterionArray criteria2 = new LocalRelationshipCriterionArray(criterion5, criterion6);
    LocalRelationshipFilter filter2 = new LocalRelationshipFilter().setCriteria(criteria2);

    //test for multi filters with dollar virtual columns names
    assertConditionsEqual(SQLStatementUtils.whereClause(Collections.singletonMap(Condition.EQUAL, "="), false, new Pair<>(filter1, "foo"),
            new Pair<>(filter2, "bar")), "(foo.i_aspectfoo$value='value2' AND (foo.urn='value1' OR foo.urn='value3') "
            + "AND foo.metadata$value='value4') AND (bar.urn='value1' OR bar.urn='value2')"
        );

    //test for multi filters with non dollar virtual columns names
    assertConditionsEqual(SQLStatementUtils.whereClause(Collections.singletonMap(Condition.EQUAL, "="), true, new Pair<>(filter1, "foo"),
            new Pair<>(filter2, "bar")), "(foo.i_aspectfoo0value='value2' AND (foo.urn='value1' OR foo.urn='value3') "
            + "AND foo.metadata0value='value4') AND (bar.urn='value1' OR bar.urn='value2')"
        );
  }

  @Test
  public void testWhereClauseOldSchemaSimple() {
    LocalRelationshipCriterion.Field field = new LocalRelationshipCriterion.Field();
    field.setUrnField(new UrnField());
    LocalRelationshipCriterion criterion = new LocalRelationshipCriterion()
        .setField(field)
        .setCondition(Condition.EQUAL)
        .setValue(LocalRelationshipValue.create("value1"));
    LocalRelationshipCriterionArray sourceCriteria = new LocalRelationshipCriterionArray(criterion);
    String expected = " AND rt.source = 'value1'";
    assertEquals(SQLStatementUtils.whereClauseOldSchema(Collections.singletonMap(Condition.EQUAL, "="), sourceCriteria, "source"), expected);
  }

  @Test
  public void testWhereClauseOldSchemaConditionIn() {
    LocalRelationshipCriterion.Field field = new LocalRelationshipCriterion.Field();
    field.setUrnField(new UrnField());
    LocalRelationshipCriterion criterion = new LocalRelationshipCriterion()
        .setField(field)
        .setCondition(Condition.IN)
        .setValue(LocalRelationshipValue.create("(value1, value2)"));
    LocalRelationshipCriterionArray sourceCriteria = new LocalRelationshipCriterionArray(criterion);
    String expected = " AND rt.source IN (value1, value2)";
    assertEquals(SQLStatementUtils.whereClauseOldSchema(Collections.singletonMap(Condition.IN, "IN"), sourceCriteria, "source"), expected);
  }

  private void assertConditionsEqual(String actualWhereClause, String expectedWhereClause) {
    List<String> expectedConditions = splitAndSortConditions(expectedWhereClause);
    List<String> actualConditions = splitAndSortConditions(actualWhereClause);
    assertEquals(expectedConditions, actualConditions);
  }

  private List<String> splitAndSortConditions(String whereClause) {
    List<String> conditions = new ArrayList<>(Arrays.asList(whereClause.replace("(", "").replace(")", "").split(" AND ")));
    Collections.sort(conditions);
    return conditions;
  }

  @Test
  public void testCreateListAspectByUrnSql() throws URISyntaxException {
    FooUrn fooUrn = new FooUrn(1);
    assertEquals(SQLStatementUtils.createListAspectByUrnSql(AspectFoo.class, fooUrn, true),
        "SELECT urn, a_aspectfoo, lastmodifiedon, lastmodifiedby, createdfor FROM "
            + "metadata_entity_foo WHERE urn = 'urn:li:foo:1' AND a_aspectfoo IS NOT NULL");
    assertEquals(SQLStatementUtils.createListAspectByUrnSql(AspectFoo.class, fooUrn, false),
        "SELECT urn, a_aspectfoo, lastmodifiedon, lastmodifiedby, createdfor FROM "
            + "metadata_entity_foo WHERE urn = 'urn:li:foo:1' AND a_aspectfoo IS NOT NULL AND JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL");
  }

  @Test
  public void testCreateListAspectSql() throws URISyntaxException {
    FooUrn fooUrn = new FooUrn(1);
    assertEquals(
        SQLStatementUtils.createListAspectWithPaginationSql(AspectFoo.class, fooUrn.getEntityType(), true, 0, 5),
        "SELECT urn, a_aspectfoo, lastmodifiedon, lastmodifiedby, createdfor, (SELECT COUNT(urn) FROM "
            + "metadata_entity_foo WHERE a_aspectfoo IS NOT NULL) as _total_count FROM metadata_entity_foo "
            + "WHERE a_aspectfoo IS NOT NULL LIMIT 5 OFFSET 0");
    assertEquals(
        SQLStatementUtils.createListAspectWithPaginationSql(AspectFoo.class, fooUrn.getEntityType(), false, 0, 5),
        "SELECT urn, a_aspectfoo, lastmodifiedon, lastmodifiedby, createdfor, (SELECT COUNT(urn) FROM "
            + "metadata_entity_foo WHERE a_aspectfoo IS NOT NULL AND JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL) "
            + "as _total_count FROM metadata_entity_foo WHERE a_aspectfoo IS NOT NULL AND "
            + "JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL LIMIT 5 OFFSET 0");
  }

  @Test
  public void testUpdateAspectWithOptimisticLockSql() {
    FooUrn fooUrn = makeFooUrn(1);
    String expectedSql =
        "UPDATE metadata_entity_foo SET a_aspectfoo = :metadata, a_urn = :a_urn, lastmodifiedon = :lastmodifiedon, "
            + "lastmodifiedby = :lastmodifiedby WHERE urn = :urn and (JSON_EXTRACT(a_aspectfoo, '$.lastmodifiedon') = "
            + ":oldTimestamp OR JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NOT NULL);";
    assertEquals(SQLStatementUtils.createAspectUpdateWithOptimisticLockSql(fooUrn, AspectFoo.class, true, false),
        expectedSql);

    expectedSql =
        "UPDATE metadata_entity_foo SET a_aspectfoo = :metadata, lastmodifiedon = :lastmodifiedon, lastmodifiedby = "
            + ":lastmodifiedby WHERE urn = :urn and (JSON_EXTRACT(a_aspectfoo, '$.lastmodifiedon') = :oldTimestamp "
            + "OR JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NOT NULL);";
    assertEquals(
        SQLStatementUtils.createAspectUpdateWithOptimisticLockSql(fooUrn, AspectFoo.class, false, false),
        expectedSql);
  }

  @Test
  public void testAspectField() {
    AspectField aspectField = new AspectField();
    aspectField.setAspect(AspectBar.class.getCanonicalName());
    // unknown if asset field is not set
    assertEquals(SQLStatementUtils.getAssetType(aspectField), UNKNOWN_ASSET);

    try {
      aspectField.setAsset("invalid_class");
      SQLStatementUtils.getAssetType(aspectField);
      fail("should fail because invalid asset class");
    } catch (IllegalArgumentException e) {
    }

    try {
      aspectField.setAsset(String.class.getCanonicalName());
      SQLStatementUtils.getAssetType(aspectField);
      fail("should fail because not RecordTemplate");
    } catch (IllegalArgumentException e) {
    }

    try {
      aspectField.setAsset(RecordTemplate.class.getCanonicalName());
      SQLStatementUtils.getAssetType(aspectField);
      fail("should fail because not an valid Asset");
    } catch (IllegalArgumentException e) {
    }

    aspectField.setAsset(BarAsset.class.getCanonicalName());
    assertEquals(SQLStatementUtils.getAssetType(aspectField), BarUrn.ENTITY_TYPE);
  }
}