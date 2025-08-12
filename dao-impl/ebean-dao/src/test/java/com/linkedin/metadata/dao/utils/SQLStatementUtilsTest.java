package com.linkedin.metadata.dao.utils;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
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
import com.linkedin.testing.localrelationship.AspectFooBar;
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
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import pegasus.com.linkedin.metadata.query.LogicalExpressionLocalRelationshipCriterion;
import pegasus.com.linkedin.metadata.query.LogicalExpressionLocalRelationshipCriterionArray;
import pegasus.com.linkedin.metadata.query.innerLogicalOperation.Operator;

import static com.linkedin.metadata.dao.utils.LogicalExpressionLocalRelationshipCriterionUtils.*;
import static com.linkedin.metadata.dao.utils.SQLSchemaUtils.*;
import static com.linkedin.testing.TestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class SQLStatementUtilsTest {

  private static SchemaValidatorUtil mockValidator;

  @BeforeClass
  public void setupValidator() {
    mockValidator = mock(SchemaValidatorUtil.class);
    when(mockValidator.columnExists(anyString(), anyString())).thenReturn(true);
  }

  @Test
  public void testCreateUpsertAspectSql() {
    FooUrn fooUrn = makeFooUrn(1);
    String expectedSql =
        "INSERT INTO metadata_entity_foo (urn, a_urn, a_aspectfoo, lastmodifiedon, lastmodifiedby) VALUE (:urn, "
            + ":a_urn, :metadata, :lastmodifiedon, :lastmodifiedby) ON DUPLICATE KEY UPDATE a_aspectfoo = :metadata,"
            + " lastmodifiedon = :lastmodifiedon, a_urn = :a_urn, deleted_ts = NULL;";
    assertEquals(SQLStatementUtils.createAspectUpsertSql(fooUrn, AspectFoo.class, true, false), expectedSql);

    expectedSql =
        "INSERT INTO metadata_entity_foo (urn, a_aspectfoo, lastmodifiedon, lastmodifiedby) VALUE (:urn, "
            + ":metadata, :lastmodifiedon, :lastmodifiedby) ON DUPLICATE KEY UPDATE a_aspectfoo = :metadata,"
            + " lastmodifiedon = :lastmodifiedon, deleted_ts = NULL;";
    assertEquals(SQLStatementUtils.createAspectUpsertSql(fooUrn, AspectFoo.class, false, false), expectedSql);
  }

  @Test
  public void testCreateInsertAspectSql() {
    String expectedSql = "INSERT INTO %s (urn, a_urn, lastmodifiedon, lastmodifiedby,";
    assertEquals(expectedSql, SQLStatementUtils.SQL_INSERT_INTO_ASSET_WITH_URN);

    expectedSql = "VALUES (:urn, :a_urn, :lastmodifiedon, :lastmodifiedby,";
    assertEquals(expectedSql, SQLStatementUtils.SQL_INSERT_ASSET_VALUES_WITH_URN);

    expectedSql = "INSERT INTO %s (urn, lastmodifiedon, lastmodifiedby,";
    assertEquals(expectedSql, SQLStatementUtils.SQL_INSERT_INTO_ASSET);

    expectedSql = "VALUES (:urn, :lastmodifiedon, :lastmodifiedby,";
    assertEquals(expectedSql, SQLStatementUtils.SQL_INSERT_ASSET_VALUES);

    // expectedSql = "ON DUPLICATE KEY UPDATE deleted_ts = "
    //     + "IF(deleted_TS IS NULL, CAST('DuplicateKeyException' AS UNSIGNED), NULL);";
    // assertEquals(expectedSql, SQLStatementUtils.DELETED_TS_CHECK_FOR_CREATE);
  }

  @Test
  public void testDeleteAssetSql() {
    FooUrn fooUrn = makeFooUrn(1);
    // UPDATE %s SET deleted_ts = NOW() WHERE urn = '%s';
    // isTestMode=true
    String expectedSql = "UPDATE metadata_entity_foo_test SET deleted_ts = NOW() WHERE urn = '" + fooUrn + "';";
    assertEquals(SQLStatementUtils.createSoftDeleteAssetSql(fooUrn, true), expectedSql);
    // isTestMode=false
    expectedSql = "UPDATE metadata_entity_foo SET deleted_ts = NOW() WHERE urn = '" + fooUrn + "';";
    assertEquals(SQLStatementUtils.createSoftDeleteAssetSql(fooUrn, false), expectedSql);
  }

  @Test
  public void testCreateAspectReadSql() {
    FooUrn fooUrn1 = makeFooUrn(1);
    FooUrn fooUrn2 = makeFooUrn(2);
    Set<Urn> set = new HashSet<>();
    set.add(fooUrn1);
    set.add(fooUrn2);
    //test when includedSoftDeleted is false
    String expectedSql =
        "SELECT urn, a_aspectfoo, lastmodifiedon, lastmodifiedby "
            + "FROM metadata_entity_foo "
            + "WHERE JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL "
            + "AND urn IN ('urn:li:foo:1', 'urn:li:foo:2') "
            + "AND deleted_ts IS NULL";
    assertEquals(SQLStatementUtils.createAspectReadSql(AspectFoo.class, set, false, false), expectedSql);

    //test when includedSoftDeleted is true
    expectedSql =
        "SELECT urn, a_aspectfoo, lastmodifiedon, lastmodifiedby "
            + "FROM metadata_entity_foo "
            + "WHERE urn IN ('urn:li:foo:1', 'urn:li:foo:2') "
            + "AND deleted_ts IS NULL";
    assertEquals(SQLStatementUtils.createAspectReadSql(AspectFoo.class, set, true, false), expectedSql);
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

    String sql1 = SQLStatementUtils.createFilterSql("foo", indexFilter, true, false, mockValidator);
    String expectedSql1 = "SELECT *, (SELECT COUNT(urn) FROM metadata_entity_foo WHERE a_aspectfoo IS NOT NULL\n"
        + "AND JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL\n" + "AND i_aspectfoo$value >= 25\n"
        + "AND a_aspectfoo IS NOT NULL\n" + "AND JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL\n"
        + "AND i_aspectfoo$value < 50\n" + "AND deleted_ts IS NULL)" + " as _total_count FROM metadata_entity_foo\n"
        + "WHERE a_aspectfoo IS NOT NULL\n" + "AND JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL\n"
        + "AND i_aspectfoo$value >= 25\n" + "AND a_aspectfoo IS NOT NULL\n"
        + "AND JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL\n"
        + "AND i_aspectfoo$value < 50\n" + "AND deleted_ts IS NULL";

    assertEquals(sql1, expectedSql1);

    String sql2 = SQLStatementUtils.createFilterSql("foo", indexFilter, true, true, mockValidator);
    String expectedSql2 = "SELECT *, (SELECT COUNT(urn) FROM metadata_entity_foo WHERE a_aspectfoo IS NOT NULL\n"
        + "AND JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL\n" + "AND i_aspectfoo0value >= 25\n"
        + "AND a_aspectfoo IS NOT NULL\n" + "AND JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL\n"
        + "AND i_aspectfoo0value < 50\n" + "AND deleted_ts IS NULL)" + " as _total_count FROM metadata_entity_foo\n"
        + "WHERE a_aspectfoo IS NOT NULL\n" + "AND JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL\n"
        + "AND i_aspectfoo0value >= 25\n" + "AND a_aspectfoo IS NOT NULL\n"
        + "AND JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL\n"
        + "AND i_aspectfoo0value < 50\n" + "AND deleted_ts IS NULL";

    assertEquals(sql2, expectedSql2);
  }


  public void testCreateFilterSqlWithArrayContainsCondition() {
    IndexFilter indexFilter = new IndexFilter();
    IndexCriterionArray indexCriterionArray = new IndexCriterionArray();

    IndexCriterion indexCriterion1 =
        SQLIndexFilterUtils.createIndexCriterion(AspectFooBar.class, "bars", Condition.ARRAY_CONTAINS,
            IndexValue.create("bar1"));

    indexCriterionArray.add(indexCriterion1);
    indexFilter.setCriteria(indexCriterionArray);

    String sql1 = SQLStatementUtils.createFilterSql("foo", indexFilter, true, false, mockValidator);
    String expectedSql1 = "SELECT *, (SELECT COUNT(urn) FROM metadata_entity_foo WHERE a_aspectfoobar IS NOT NULL\n"
        + "AND JSON_EXTRACT(a_aspectfoobar, '$.gma_deleted') IS NULL\n" + "AND 'bar1' MEMBER OF(i_aspectfoobar$bars)\n"
        + "AND deleted_ts IS NULL)" + " as _total_count FROM metadata_entity_foo\n"
        + "WHERE a_aspectfoobar IS NOT NULL\n" + "AND JSON_EXTRACT(a_aspectfoobar, '$.gma_deleted') IS NULL\n"
        + "AND 'bar1' MEMBER OF(i_aspectfoobar$bars)\n" + "AND deleted_ts IS NULL";

    assertEquals(sql1, expectedSql1);
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

    String sql1 = SQLStatementUtils.createGroupBySql("foo", indexFilter, indexGroupByCriterion, false, mockValidator);
    assertEquals(sql1, "SELECT count(*) as COUNT, i_aspectfoo$value FROM metadata_entity_foo\n"
        + "WHERE a_aspectfoo IS NOT NULL\n" + "AND JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL\n"
        + "AND i_aspectfoo$value >= 25\n" + "AND a_aspectfoo IS NOT NULL\n"
        + "AND JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL\n" + "AND i_aspectfoo$value < 50\n"
        + "AND deleted_ts IS NULL\n" + "GROUP BY i_aspectfoo$value");

    String sql2 = SQLStatementUtils.createGroupBySql("foo", indexFilter, indexGroupByCriterion, true, mockValidator);
    assertEquals(sql2, "SELECT count(*) as COUNT, i_aspectfoo0value FROM metadata_entity_foo\n"
        + "WHERE a_aspectfoo IS NOT NULL\n" + "AND JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL\n"
        + "AND i_aspectfoo0value >= 25\n" + "AND a_aspectfoo IS NOT NULL\n"
        + "AND JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL\n" + "AND i_aspectfoo0value < 50\n"
        + "AND deleted_ts IS NULL\n" + "GROUP BY i_aspectfoo0value");
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
  public void testWhereClauseSingleINCondition() {
    LocalRelationshipCriterion.Field field = new LocalRelationshipCriterion.Field();
    field.setUrnField(new UrnField());
    StringArray values = new StringArray("value1");
    LocalRelationshipCriterion criterion = new LocalRelationshipCriterion()
        .setField(field)
        .setCondition(Condition.IN)
        .setValue(LocalRelationshipValue.create(values));
    LocalRelationshipCriterionArray criteria = new LocalRelationshipCriterionArray(criterion);
    LocalRelationshipFilter filter = new LocalRelationshipFilter().setCriteria(criteria);
    assertEquals(SQLStatementUtils.whereClause(filter, Collections.singletonMap(Condition.IN, "IN"), null, false), "urn IN ('value1')");
    assertEquals(SQLStatementUtils.whereClause(filter, Collections.singletonMap(Condition.IN, "IN"), null, true), "urn IN ('value1')");
  }

  @Test
  public void testWhereClauseSingleStartWithCondition() {
    LocalRelationshipCriterion.Field field = new LocalRelationshipCriterion.Field();
    field.setUrnField(new UrnField());
    LocalRelationshipCriterion criterion = new LocalRelationshipCriterion()
        .setField(field)
        .setCondition(Condition.START_WITH)
        .setValue(LocalRelationshipValue.create("value1"));
    LocalRelationshipCriterionArray criteria = new LocalRelationshipCriterionArray(criterion);
    LocalRelationshipFilter filter = new LocalRelationshipFilter().setCriteria(criteria);
    assertEquals(SQLStatementUtils.whereClause(filter, Collections.singletonMap(Condition.START_WITH, "LIKE"), null, false), "urn LIKE 'value1%'");
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
    assertEquals(SQLStatementUtils.whereClause(filter, Collections.singletonMap(Condition.EQUAL, "="), null, false), "(urn='value1' OR urn='value2')");
    assertEquals(SQLStatementUtils.whereClause(filter, Collections.singletonMap(Condition.EQUAL, "="), null, true), "(urn='value1' OR urn='value2')");
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
        "(i_aspectfoo$value='value2' AND urn='value1')");
    assertEquals(SQLStatementUtils.whereClause(filter, Collections.singletonMap(Condition.EQUAL, "="), null, true),
        "(i_aspectfoo0value='value2' AND urn='value1')");
  }

  @Test
  public void testWhereClauseMultiConditionMixedName() {
    // Create criteria for each field
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

    // Group all criteria into a LocalRelationshipCriterionArray
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
  public void testWhereClauseMultiFilters2() {
    LocalRelationshipCriterion.Field field1 = new LocalRelationshipCriterion.Field();
    field1.setUrnField(new UrnField());
    LocalRelationshipCriterion criterion1 = new LocalRelationshipCriterion()
        .setField(field1)
        .setCondition(Condition.START_WITH)
        .setValue(LocalRelationshipValue.create("value1"));

    LocalRelationshipCriterion.Field field2 = new LocalRelationshipCriterion.Field();
    field2.setAspectField((new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value")));
    LocalRelationshipCriterion criterion2 = new LocalRelationshipCriterion()
        .setField(field2)
        .setCondition(Condition.START_WITH)
        .setValue(LocalRelationshipValue.create("value2"));

    LocalRelationshipCriterion.Field field3 = new LocalRelationshipCriterion.Field();
    field3.setUrnField(new UrnField());
    LocalRelationshipCriterion criterion3 = new LocalRelationshipCriterion()
        .setField(field3)
        .setCondition(Condition.START_WITH)
        .setValue(LocalRelationshipValue.create("value3"));

    LocalRelationshipCriterion.Field field4 = new LocalRelationshipCriterion.Field();
    field4.setRelationshipField(((new RelationshipField().setPath("/value"))));
    LocalRelationshipCriterion criterion4 = new LocalRelationshipCriterion()
        .setField(field4)
        .setCondition(Condition.START_WITH)
        .setValue(LocalRelationshipValue.create("value4"));

    LocalRelationshipCriterionArray criteria1 = new LocalRelationshipCriterionArray(criterion1, criterion2, criterion3, criterion4);
    LocalRelationshipFilter filter1 = new LocalRelationshipFilter().setCriteria(criteria1);

    LocalRelationshipCriterion.Field field5 = new LocalRelationshipCriterion.Field();
    field5.setUrnField(new UrnField());
    LocalRelationshipCriterion criterion5 = new LocalRelationshipCriterion()
        .setField(field5)
        .setCondition(Condition.START_WITH)
        .setValue(LocalRelationshipValue.create("value1"));

    LocalRelationshipCriterion.Field field6 = new LocalRelationshipCriterion.Field();
    field6.setUrnField(new UrnField());
    LocalRelationshipCriterion criterion6 = new LocalRelationshipCriterion()
        .setField(field6)
        .setCondition(Condition.START_WITH)
        .setValue(LocalRelationshipValue.create("value2"));

    LocalRelationshipCriterionArray criteria2 = new LocalRelationshipCriterionArray(criterion5, criterion6);
    LocalRelationshipFilter filter2 = new LocalRelationshipFilter().setCriteria(criteria2);

    //test for multi filters with dollar virtual columns names
    assertConditionsEqual(SQLStatementUtils.whereClause(Collections.singletonMap(Condition.EQUAL, "="), false, new Pair<>(filter1, "foo"),
        new Pair<>(filter2, "bar")), "(foo.i_aspectfoo$value LIKE 'value2%' AND (foo.urn LIKE 'value1%' OR foo.urn LIKE 'value3%') "
        + "AND foo.metadata$value LIKE 'value4%') AND (bar.urn LIKE 'value1%' OR bar.urn LIKE 'value2%')"
    );

    //test for multi filters with non dollar virtual columns names
    assertConditionsEqual(SQLStatementUtils.whereClause(Collections.singletonMap(Condition.EQUAL, "="), true, new Pair<>(filter1, "foo"),
        new Pair<>(filter2, "bar")), "(foo.i_aspectfoo0value LIKE 'value2%' AND (foo.urn LIKE 'value1%' OR foo.urn LIKE 'value3%') "
        + "AND foo.metadata0value LIKE 'value4%') AND (bar.urn LIKE 'value1%' OR bar.urn LIKE 'value2%')"
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
    LocalRelationshipFilter filter = new LocalRelationshipFilter().setCriteria(sourceCriteria);
    String expected = " AND rt.source = 'value1'";
    assertEquals(SQLStatementUtils.whereClauseOldSchema(Collections.singletonMap(Condition.EQUAL, "="), filter, "source"), expected);
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
    LocalRelationshipFilter filter = new LocalRelationshipFilter().setCriteria(sourceCriteria);
    String expected = " AND rt.source IN (value1, value2)";
    assertEquals(SQLStatementUtils.whereClauseOldSchema(Collections.singletonMap(Condition.IN, "IN"), filter, "source"), expected);
  }

  @Test
  public void testWhereClauseOldSchemaConditionStartWith() {
    LocalRelationshipCriterion.Field field = new LocalRelationshipCriterion.Field();
    field.setUrnField(new UrnField());
    LocalRelationshipCriterion criterion = new LocalRelationshipCriterion()
        .setField(field)
        .setCondition(Condition.START_WITH)
        .setValue(LocalRelationshipValue.create("value1"));
    LocalRelationshipCriterionArray sourceCriteria = new LocalRelationshipCriterionArray(criterion);
    LocalRelationshipFilter filter = new LocalRelationshipFilter().setCriteria(sourceCriteria);
    String expected = " AND rt.source LIKE 'value1%'";
    assertEquals(SQLStatementUtils.whereClauseOldSchema(Collections.singletonMap(Condition.START_WITH, "LIKE"), filter, "source"), expected);
  }

  @Test
  public void testWhereClauseWithLogicalExpression() {
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

    LocalRelationshipFilter filter = new LocalRelationshipFilter().setLogicalExpressionCriteria(root);

    //test for multi filters with dollar virtual columns names
    assertConditionsEqual(SQLStatementUtils.whereClause(filter, Collections.singletonMap(Condition.EQUAL, "="), null, false),
        "((urn='foo1' OR urn='foo2') AND i_aspectfoo$value='bar')"
    );

    //test for multi filters with non dollar virtual columns names
    assertConditionsEqual(SQLStatementUtils.whereClause(filter, Collections.singletonMap(Condition.EQUAL, "="), null, true),
        "((urn='foo1' OR urn='foo2') AND i_aspectfoo0value='bar')"
    );
  }

  @Test
  public void testWhereClauseWithLogicalExpressionWithNot() {
    LocalRelationshipCriterion c1 = createLocalRelationshipCriterionWithAspectField("bar");
    LogicalExpressionLocalRelationshipCriterion n1 = wrapCriterionAsLogicalExpression(c1);
    LogicalExpressionLocalRelationshipCriterion notNode = buildLogicalGroup(Operator.NOT, new LogicalExpressionLocalRelationshipCriterionArray(n1));

    LocalRelationshipFilter filter = new LocalRelationshipFilter().setLogicalExpressionCriteria(notNode);

    //test for multi filters with dollar virtual columns names
    assertConditionsEqual(SQLStatementUtils.whereClause(filter, Collections.singletonMap(Condition.EQUAL, "="), null, false),
        "(NOT i_aspectfoo$value='bar')"
    );

    //test for multi filters with non dollar virtual columns names
    assertConditionsEqual(SQLStatementUtils.whereClause(filter, Collections.singletonMap(Condition.EQUAL, "="), null, true),
        "(NOT i_aspectfoo0value='bar')"
    );
  }

  @Test
  public void testWhereClauseWithLogicalExpressionWithNotNested() {
    LocalRelationshipCriterion c1 = createLocalRelationshipCriterionWithUrnField("foo1");
    LocalRelationshipCriterion c2 = createLocalRelationshipCriterionWithUrnField("foo2");
    LocalRelationshipCriterion c3 = createLocalRelationshipCriterionWithAspectField("bar");

    LogicalExpressionLocalRelationshipCriterion n1 = wrapCriterionAsLogicalExpression(c1);
    LogicalExpressionLocalRelationshipCriterion n2 = wrapCriterionAsLogicalExpression(c2);
    LogicalExpressionLocalRelationshipCriterion n3 = wrapCriterionAsLogicalExpression(c3);

    LogicalExpressionLocalRelationshipCriterion orNode =
        buildLogicalGroup(Operator.OR, new LogicalExpressionLocalRelationshipCriterionArray(n1, n2));

    LogicalExpressionLocalRelationshipCriterion notNode =
        buildLogicalGroup(Operator.NOT, new LogicalExpressionLocalRelationshipCriterionArray(n3));

    LogicalExpressionLocalRelationshipCriterion root =
        buildLogicalGroup(Operator.AND, new LogicalExpressionLocalRelationshipCriterionArray(orNode, notNode));

    LocalRelationshipFilter filter = new LocalRelationshipFilter().setLogicalExpressionCriteria(root);

    //test for multi filters with dollar virtual columns names
    assertConditionsEqual(SQLStatementUtils.whereClause(filter, Collections.singletonMap(Condition.EQUAL, "="), null, false),
        "((urn='foo1' OR urn='foo2') AND (NOT i_aspectfoo$value='bar'))"
    );

    //test for multi filters with non dollar virtual columns names
    assertConditionsEqual(SQLStatementUtils.whereClause(filter, Collections.singletonMap(Condition.EQUAL, "="), null, true),
        "((urn='foo1' OR urn='foo2') AND (NOT i_aspectfoo0value='bar'))"
    );
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
    field.setAspectField(new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
    return new LocalRelationshipCriterion()
        .setField(field)
        .setCondition(Condition.EQUAL)
        .setValue(LocalRelationshipValue.create(value));
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
    assertEquals(SQLStatementUtils.createAspectUpdateWithOptimisticLockSql(fooUrn, AspectFoo.class, true, false, false),
        expectedSql);

    expectedSql =
        "UPDATE metadata_entity_foo SET a_aspectfoo = :metadata, lastmodifiedon = :lastmodifiedon, lastmodifiedby = "
            + ":lastmodifiedby WHERE urn = :urn and (JSON_EXTRACT(a_aspectfoo, '$.lastmodifiedon') = :oldTimestamp "
            + "OR JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NOT NULL);";
    assertEquals(
        SQLStatementUtils.createAspectUpdateWithOptimisticLockSql(fooUrn, AspectFoo.class, false, false, false),
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

  @Test
  public void testExistsSql() {
    FooUrn fooUrn =  makeFooUrn(1);
    String expectedSql = "SELECT urn "
        + "FROM metadata_entity_foo "
        + "WHERE urn = 'urn:li:foo:1' "
        + "AND deleted_ts IS NULL";
    assertConditionsEqual(SQLStatementUtils.createExistSql(fooUrn), expectedSql);
  }

  @Test
  public void testParseIndexFilterSkipsMissingVirtualColumn() {
    SchemaValidatorUtil mockValidator1 = mock(SchemaValidatorUtil.class);
    when(mockValidator1.columnExists(anyString(), anyString())).thenReturn(false); // Simulate missing column

    IndexFilter indexFilter = new IndexFilter();
    IndexCriterion criterion = SQLIndexFilterUtils.createIndexCriterion(
        AspectFoo.class, "value", Condition.EQUAL, IndexValue.create("bar")
    );
    indexFilter.setCriteria(new IndexCriterionArray(criterion));

    String result = SQLIndexFilterUtils.parseIndexFilter("foo", indexFilter, false, mockValidator1);
    assertFalse(result.contains("i_aspectfoo$value"), "Should skip filter on missing index column");

  }

  @Test
  public void testCreateFilterSqlWithValidAndInvalidColumns() {
    SchemaValidatorUtil mockValidator1 = mock(SchemaValidatorUtil.class);
    when(mockValidator1.columnExists(anyString(), contains("value"))).thenReturn(true);
    when(mockValidator1.columnExists(anyString(), contains("invalid"))).thenReturn(false);

    IndexFilter indexFilter = new IndexFilter();
    indexFilter.setCriteria(new IndexCriterionArray(
        SQLIndexFilterUtils.createIndexCriterion(AspectFoo.class, "value", Condition.EQUAL, IndexValue.create("val")),
        SQLIndexFilterUtils.createIndexCriterion(AspectFoo.class, "invalid", Condition.EQUAL, IndexValue.create("val2"))
    ));

    String sql = SQLStatementUtils.createFilterSql("foo", indexFilter, true, false, mockValidator1);
    assertTrue(sql.contains("i_aspectfoo$value = 'val'"), "Should contain valid column condition");
    assertFalse(sql.contains("invalid"), "Should skip invalid column");
  }

  @Test
  public void testCreateGroupBySqlFilterColumnPresentGroupByColumnPresent() {
    IndexFilter indexFilter = new IndexFilter();
    IndexCriterionArray indexCriterionArray = new IndexCriterionArray();

    IndexCriterion indexCriterion1 =
        SQLIndexFilterUtils.createIndexCriterion(AspectFoo.class, "age", Condition.GREATER_THAN_OR_EQUAL_TO,
            IndexValue.create(25));

    IndexCriterion indexCriterion2 =
        SQLIndexFilterUtils.createIndexCriterion(AspectBar.class, "name", Condition.EQUAL,
            IndexValue.create("PizzaMan"));

    indexCriterionArray.add(indexCriterion1);
    indexCriterionArray.add(indexCriterion2);
    indexFilter.setCriteria(indexCriterionArray);

    IndexGroupByCriterion indexGroupByCriterion = new IndexGroupByCriterion();
    indexGroupByCriterion.setAspect(AspectBar.class.getCanonicalName());
    indexGroupByCriterion.setPath("/name");

    //Case 1: both columns a_aspectfoo and a_aspectbar are present in the schema
    when(mockValidator.columnExists(anyString(), contains("i_aspectfoo$age"))).thenReturn(true);
    when(mockValidator.columnExists(anyString(), contains("i_aspectbar$name"))).thenReturn(true);

    String sql1 = SQLStatementUtils.createGroupBySql("foo", indexFilter, indexGroupByCriterion, false, mockValidator);
    assertEquals(sql1, "SELECT count(*) as COUNT, i_aspectbar$name FROM metadata_entity_foo\n"
        + "WHERE a_aspectfoo IS NOT NULL\n" + "AND JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL\n"
        + "AND i_aspectfoo$age >= 25\n" + "AND a_aspectbar IS NOT NULL\n"
        + "AND JSON_EXTRACT(a_aspectbar, '$.gma_deleted') IS NULL\n" + "AND i_aspectbar$name = 'PizzaMan'\n"
        + "AND deleted_ts IS NULL\n" + "GROUP BY i_aspectbar$name");
  }

  @Test
  public void testCreateGroupBySqlFilterColumnMissingGroupByPresent() {
    IndexFilter indexFilter = new IndexFilter();
    IndexCriterionArray indexCriterionArray = new IndexCriterionArray();

    // Missing filter column
    IndexCriterion indexCriterion1 =
        SQLIndexFilterUtils.createIndexCriterion(AspectFoo.class, "age", Condition.GREATER_THAN_OR_EQUAL_TO,
            IndexValue.create(25));
    indexCriterionArray.add(indexCriterion1);

    // Present filter and group-by column
    IndexCriterion indexCriterion2 =
        SQLIndexFilterUtils.createIndexCriterion(AspectBar.class, "name", Condition.EQUAL,
            IndexValue.create("PizzaMan"));
    indexCriterionArray.add(indexCriterion2);

    indexFilter.setCriteria(indexCriterionArray);

    IndexGroupByCriterion groupBy = new IndexGroupByCriterion();
    groupBy.setAspect(AspectBar.class.getCanonicalName());
    groupBy.setPath("/name");

    // Mock missing filter column and present group-by column
    when(mockValidator.columnExists(anyString(), contains("i_aspectfoo$age"))).thenReturn(false);
    when(mockValidator.columnExists(anyString(), contains("i_aspectbar$name"))).thenReturn(true);

    String sql = SQLStatementUtils.createGroupBySql("foo", indexFilter, groupBy, false, mockValidator);
    assertEquals(sql, "SELECT count(*) as COUNT, i_aspectbar$name FROM metadata_entity_foo\n"
        + "WHERE a_aspectfoo IS NOT NULL\n"
        + "AND JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL\n"
        + "AND a_aspectbar IS NOT NULL\n"
        + "AND JSON_EXTRACT(a_aspectbar, '$.gma_deleted') IS NULL\n"
        + "AND i_aspectbar$name = 'PizzaMan'\n"
        + "AND deleted_ts IS NULL\n"
        + "GROUP BY i_aspectbar$name");
  }

  @Test
  public void testCreateGroupBySqlGroupByColumnMissing() {
    IndexFilter indexFilter = new IndexFilter();
    IndexCriterionArray indexCriterionArray = new IndexCriterionArray();

    // Missing filter column
    IndexCriterion indexCriterion1 =
        SQLIndexFilterUtils.createIndexCriterion(AspectFoo.class, "age", Condition.GREATER_THAN_OR_EQUAL_TO,
            IndexValue.create(25));
    indexCriterionArray.add(indexCriterion1);

    // Present filter column
    IndexCriterion indexCriterion2 =
        SQLIndexFilterUtils.createIndexCriterion(AspectBar.class, "name", Condition.EQUAL,
            IndexValue.create("PizzaMan"));
    indexCriterionArray.add(indexCriterion2);

    indexFilter.setCriteria(indexCriterionArray);

    IndexGroupByCriterion groupBy = new IndexGroupByCriterion();
    groupBy.setAspect(AspectFoo.class.getCanonicalName());
    groupBy.setPath("/age");

    // Mock column existence
    when(mockValidator.columnExists(anyString(), contains("i_aspectfoo$age"))).thenReturn(false);
    when(mockValidator.columnExists(anyString(), contains("i_aspectbar$name"))).thenReturn(true);

    String sql = SQLStatementUtils.createGroupBySql("foo", indexFilter, groupBy, false, mockValidator);
    // Expect no GROUP BY clause when group-by column is missing
    assertFalse(sql.contains("GROUP BY"), "Should not contain GROUP BY if group-by column is missing");
  }


}