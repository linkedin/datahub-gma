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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.javatuples.Triplet;
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

  // This is (dummy) table name placeholder for testing, introduced because we need to propagate the table name
  // used in a "where filter" through the "where()" call.
  // HOWEVER, since the table name ends up being directed into mocked calls in SchemaValidator, it doesn't matter
  // what the table name is, so we'll just create and use a placeholder here.
  private static final String PLACEHOLDER_TABLE_NAME = "placeholder";

  @BeforeClass
  public void setupValidator() {
    mockValidator = mock(SchemaValidatorUtil.class);
    when(mockValidator.columnExists(anyString(), anyString())).thenReturn(true);

    /////// NEW MOCKS for functional index testing
    // NOTE that " charset utf8mb4" is appended after "char(1024)" for some of the (real) use cases in our DB's
    //    but does NOT pass Calcite's syntax checker. However, in actual queries, it is appended as a part of the index
    //    and works just fine: we will omit it here so that we can check the syntax otherwise.

    // "AspectBar" as the aspect (any asset) with a functional index and "value" as the field (path) to be indexed
    when(mockValidator.getIndexExpression(anyString(), matches("e_aspectbar0value")))
        .thenReturn("(cast(json_extract(`a_aspectbar`, '$.aspect.value') as char(1024)))");
    //    This is an existing new way of Array extraction (AssetLabels.derived_labels)
    when(mockValidator.getIndexExpression(anyString(), matches("e_aspectbar0value_array")))
        .thenReturn("(cast(json_extract(`a_aspectbar`, '$.aspect.value_array') as char(128) array))");
    //    This is an existing legacy way of array extraction, casting to a string (DataPolicyInfo.annotation.ontologyIris)
    when(mockValidator.getIndexExpression(anyString(), matches("e_aspectbar0annotation0ontologyIris")))
        .thenReturn("(cast(replace(json_unquote(json_extract(`a_aspectbar`,'$.aspect.annotation.ontologyIris[*]')),'\"','') as char(255)))");

    // New mocks for relationship field validation
    when(mockValidator.getIndexExpression(anyString(), matches("e_metadata0field")))
        .thenReturn("(cast(json_extract(`metadata`, '$.field') as char(64)))");
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
    String expectedSql1 = "SELECT urn, (SELECT COUNT(urn) FROM metadata_entity_foo WHERE a_aspectfoo IS NOT NULL\n"
        + "AND JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL\n" + "AND i_aspectfoo$value >= 25\n"
        + "AND a_aspectfoo IS NOT NULL\n" + "AND JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL\n"
        + "AND i_aspectfoo$value < 50\n" + "AND deleted_ts IS NULL)" + " as _total_count FROM metadata_entity_foo\n"
        + "WHERE a_aspectfoo IS NOT NULL\n" + "AND JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL\n"
        + "AND i_aspectfoo$value >= 25\n" + "AND a_aspectfoo IS NOT NULL\n"
        + "AND JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL\n"
        + "AND i_aspectfoo$value < 50\n" + "AND deleted_ts IS NULL";

    assertEquals(sql1, expectedSql1);

    String sql2 = SQLStatementUtils.createFilterSql("foo", indexFilter, true, true, mockValidator);
    String expectedSql2 = "SELECT urn, (SELECT COUNT(urn) FROM metadata_entity_foo WHERE a_aspectfoo IS NOT NULL\n"
        + "AND JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL\n" + "AND i_aspectfoo0value >= 25\n"
        + "AND a_aspectfoo IS NOT NULL\n" + "AND JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL\n"
        + "AND i_aspectfoo0value < 50\n" + "AND deleted_ts IS NULL)" + " as _total_count FROM metadata_entity_foo\n"
        + "WHERE a_aspectfoo IS NOT NULL\n" + "AND JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL\n"
        + "AND i_aspectfoo0value >= 25\n" + "AND a_aspectfoo IS NOT NULL\n"
        + "AND JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL\n"
        + "AND i_aspectfoo0value < 50\n" + "AND deleted_ts IS NULL";

    assertEquals(sql2, expectedSql2);
  }

  @Test
  public void testCreateFilterSqlWithArrayContainsCondition() {
    IndexFilter indexFilter = new IndexFilter();
    IndexCriterionArray indexCriterionArray = new IndexCriterionArray();

    IndexCriterion indexCriterion1 =
        SQLIndexFilterUtils.createIndexCriterion(AspectFooBar.class, "bars", Condition.ARRAY_CONTAINS,
            IndexValue.create("bar1"));

    indexCriterionArray.add(indexCriterion1);
    indexFilter.setCriteria(indexCriterionArray);

    String sql1 = SQLStatementUtils.createFilterSql("foo", indexFilter, true, false, mockValidator);
    String expectedSql1 = "SELECT urn, (SELECT COUNT(urn) FROM metadata_entity_foo WHERE a_aspectfoobar IS NOT NULL\n"
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
    assertEquals(SQLStatementUtils.whereClause(filter, Collections.singletonMap(Condition.EQUAL, "="), null,
        PLACEHOLDER_TABLE_NAME, mockValidator, false), "urn='value1'");
    assertEquals(SQLStatementUtils.whereClause(filter, Collections.singletonMap(Condition.EQUAL, "="), null,
        PLACEHOLDER_TABLE_NAME, mockValidator, true), "urn='value1'");
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
    assertEquals(SQLStatementUtils.whereClause(filter, Collections.singletonMap(Condition.IN, "IN"), null,
        PLACEHOLDER_TABLE_NAME, mockValidator, false), "urn IN ('value1')");
    assertEquals(SQLStatementUtils.whereClause(filter, Collections.singletonMap(Condition.IN, "IN"), null,
        PLACEHOLDER_TABLE_NAME, mockValidator, true), "urn IN ('value1')");
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
    assertEquals(SQLStatementUtils.whereClause(
        filter, Collections.singletonMap(Condition.START_WITH, "LIKE"), null, null,
        mockValidator, false), "urn LIKE 'value1%'");
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
    assertEquals(SQLStatementUtils.whereClause(
        filter, Collections.singletonMap(Condition.EQUAL, "="), null, null,
        mockValidator, false), "(urn='value1' OR urn='value2')");
    assertEquals(SQLStatementUtils.whereClause(
        filter, Collections.singletonMap(Condition.EQUAL, "="), null, null,
        mockValidator, true), "(urn='value1' OR urn='value2')");
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
    assertEquals(SQLStatementUtils.whereClause(filter, Collections.singletonMap(Condition.EQUAL, "="), null, PLACEHOLDER_TABLE_NAME, mockValidator, false),
        "(i_aspectfoo$value='value2' AND urn='value1')");
    assertEquals(SQLStatementUtils.whereClause(filter, Collections.singletonMap(Condition.EQUAL, "="), null, PLACEHOLDER_TABLE_NAME, mockValidator, true),
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

    assertConditionsEqual(SQLStatementUtils.whereClause(filter, Collections.singletonMap(Condition.EQUAL, "="),
        null, PLACEHOLDER_TABLE_NAME, mockValidator, false),
        "(urn='value1' OR urn='value3') AND metadata$value='value4' AND i_aspectfoo$value='value2'");

    assertConditionsEqual(SQLStatementUtils.whereClause(filter, Collections.singletonMap(Condition.EQUAL, "="),
        null, PLACEHOLDER_TABLE_NAME, mockValidator, true),
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
    assertConditionsEqual(SQLStatementUtils.whereClause(Collections.singletonMap(Condition.EQUAL, "="), false,
        mockValidator, new Triplet<>(filter1, "foo", "faketable1"), new Triplet<>(filter2, "bar", "faketable2")),
        "(foo.i_aspectfoo$value='value2' AND (foo.urn='value1' OR foo.urn='value3') "
        + "AND foo.metadata$value='value4') AND (bar.urn='value1' OR bar.urn='value2')"
    );

    //test for multi filters with non dollar virtual columns names
    assertConditionsEqual(SQLStatementUtils.whereClause(Collections.singletonMap(Condition.EQUAL, "="), true,
        mockValidator, new Triplet<>(filter1, "foo", "faketable1"), new Triplet<>(filter2, "bar", "faketable2")),
        "(foo.i_aspectfoo0value='value2' AND (foo.urn='value1' OR foo.urn='value3') "
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
    assertConditionsEqual(SQLStatementUtils.whereClause(Collections.singletonMap(Condition.EQUAL, "="), false,
        mockValidator, new Triplet<>(filter1, "foo", "faketable1"), new Triplet<>(filter2, "bar", "faketable2")),
        "(foo.i_aspectfoo$value LIKE 'value2%' AND (foo.urn LIKE 'value1%' OR foo.urn LIKE 'value3%') "
        + "AND foo.metadata$value LIKE 'value4%') AND (bar.urn LIKE 'value1%' OR bar.urn LIKE 'value2%')"
    );

    //test for multi filters with non dollar virtual columns names
    assertConditionsEqual(SQLStatementUtils.whereClause(Collections.singletonMap(Condition.EQUAL, "="), true,
        mockValidator, new Triplet<>(filter1, "foo", "faketable1"), new Triplet<>(filter2, "bar", "faketable2")),
        "(foo.i_aspectfoo0value LIKE 'value2%' AND (foo.urn LIKE 'value1%' OR foo.urn LIKE 'value3%') "
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
    assertConditionsEqual(SQLStatementUtils.whereClause(filter, Collections.singletonMap(Condition.EQUAL, "="),
        null, PLACEHOLDER_TABLE_NAME, mockValidator, false),
        "((urn='foo1' OR urn='foo2') AND i_aspectfoo$value='bar')"
    );

    //test for multi filters with non dollar virtual columns names
    assertConditionsEqual(SQLStatementUtils.whereClause(filter, Collections.singletonMap(Condition.EQUAL, "="),
        null, PLACEHOLDER_TABLE_NAME, mockValidator, true),
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
    assertConditionsEqual(SQLStatementUtils.whereClause(filter, Collections.singletonMap(Condition.EQUAL, "="),
        null, PLACEHOLDER_TABLE_NAME, mockValidator, false),
        "(NOT i_aspectfoo$value='bar')"
    );

    //test for multi filters with non dollar virtual columns names
    assertConditionsEqual(SQLStatementUtils.whereClause(filter, Collections.singletonMap(Condition.EQUAL, "="),
        null, PLACEHOLDER_TABLE_NAME, mockValidator, true),
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
    assertConditionsEqual(SQLStatementUtils.whereClause(filter, Collections.singletonMap(Condition.EQUAL, "="),
        null, PLACEHOLDER_TABLE_NAME, mockValidator, false),
        "((urn='foo1' OR urn='foo2') AND (NOT i_aspectfoo$value='bar'))"
    );

    //test for multi filters with non dollar virtual columns names
    assertConditionsEqual(SQLStatementUtils.whereClause(filter, Collections.singletonMap(Condition.EQUAL, "="),
        null, PLACEHOLDER_TABLE_NAME, mockValidator, true),
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
  public void testCreateGroupBySqlFilterColumnPresentGroupByPresentFunctionalIndexes() {
    // Note that functional indexes are already mocked!
    when(mockValidator.columnExists(anyString(), contains("i_aspectfoo$age"))).thenReturn(true);
    when(mockValidator.columnExists(anyString(), contains("i_aspectbar$name"))).thenReturn(true);

    IndexCriterion indexCriterion1 =
        SQLIndexFilterUtils.createIndexCriterion(AspectFoo.class, "age", Condition.GREATER_THAN_OR_EQUAL_TO,
            IndexValue.create(25));

    IndexCriterion indexCriterionFunctionalIndex =
        SQLIndexFilterUtils.createIndexCriterion(AspectBar.class, "value", Condition.EQUAL,
            IndexValue.create("jhui"));

    IndexGroupByCriterion indexGroupByCriterion = new IndexGroupByCriterion();
    indexGroupByCriterion.setAspect(AspectBar.class.getCanonicalName());
    indexGroupByCriterion.setPath("/name");

    IndexGroupByCriterion indexGroupByFunctionalIndex = new IndexGroupByCriterion();
    indexGroupByFunctionalIndex.setAspect(AspectBar.class.getCanonicalName());
    indexGroupByFunctionalIndex.setPath("/value");

    // Case 1.1: (Stuff is present) both filter and group by are on functional indexes
    IndexFilter indexFilter = new IndexFilter();
    IndexCriterionArray indexCriterionArray = new IndexCriterionArray();
    indexCriterionArray.add(indexCriterionFunctionalIndex);
    indexFilter.setCriteria(indexCriterionArray);

    String sql = SQLStatementUtils.createGroupBySql("foo", indexFilter, indexGroupByFunctionalIndex, false, mockValidator);
    assertEquals(sql, "SELECT count(*) as COUNT, (cast(json_extract(`a_aspectbar`, '$.aspect.value') as char(1024))) FROM metadata_entity_foo\n"
        + "WHERE a_aspectbar IS NOT NULL\n"
        + "AND JSON_EXTRACT(a_aspectbar, '$.gma_deleted') IS NULL\n"
        + "AND (cast(json_extract(`a_aspectbar`, '$.aspect.value') as char(1024))) = 'jhui'\n"
        + "AND deleted_ts IS NULL\n"
        + "GROUP BY (cast(json_extract(`a_aspectbar`, '$.aspect.value') as char(1024)))");

    // Case 1.2: (Stuff is present) filter is on a functional index, group by is on a column
    String sql2 = SQLStatementUtils.createGroupBySql("foo", indexFilter, indexGroupByCriterion, false, mockValidator);
    assertEquals(sql2, "SELECT count(*) as COUNT, i_aspectbar$name FROM metadata_entity_foo\n"
        + "WHERE a_aspectbar IS NOT NULL\n"
        + "AND JSON_EXTRACT(a_aspectbar, '$.gma_deleted') IS NULL\n"
        + "AND (cast(json_extract(`a_aspectbar`, '$.aspect.value') as char(1024))) = 'jhui'\n"
        + "AND deleted_ts IS NULL\n"
        + "GROUP BY i_aspectbar$name");

    // Case 1.3: (Stuff is present) filter is on a column, group by is on a functional index
    indexFilter = new IndexFilter();
    indexCriterionArray = new IndexCriterionArray();
    indexCriterionArray.add(indexCriterion1);
    indexFilter.setCriteria(indexCriterionArray);

    String sql3 = SQLStatementUtils.createGroupBySql("foo", indexFilter, indexGroupByFunctionalIndex, false, mockValidator);
    assertEquals(sql3, "SELECT count(*) as COUNT, (cast(json_extract(`a_aspectbar`, '$.aspect.value') as char(1024))) FROM metadata_entity_foo\n"
        + "WHERE a_aspectfoo IS NOT NULL\n"
        + "AND JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL\n"
        + "AND i_aspectfoo$age >= 25\n"
        + "AND deleted_ts IS NULL\n"
        + "GROUP BY (cast(json_extract(`a_aspectbar`, '$.aspect.value') as char(1024)))");
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
  public void testCreateGroupBySqlFilterColumnMissingGroupByPresentFunctionalIndexes() {
    // Note that functional indexes are already mocked!

    // This mocks "fakefield" as NEITHER having an associated column nor a functional index
    when(mockValidator.getIndexExpression(anyString(), matches("e_aspectbar0fakefield")))
        .thenReturn(null);
    when(mockValidator.columnExists(anyString(), contains("i_aspectbar$fakefield"))).thenReturn(false);

    // Filter: does NOT exist
    IndexCriterion indexCriterionNonexistent =
        SQLIndexFilterUtils.createIndexCriterion(AspectBar.class, "fakefield", Condition.EQUAL,
            IndexValue.create("nothing"));
    IndexFilter indexFilter = new IndexFilter();
    IndexCriterionArray indexCriterionArray = new IndexCriterionArray();
    indexCriterionArray.add(indexCriterionNonexistent);
    indexFilter.setCriteria(indexCriterionArray);

    // GroupBy: functional
    IndexGroupByCriterion indexGroupByFunctionalIndex = new IndexGroupByCriterion();
    indexGroupByFunctionalIndex.setAspect(AspectBar.class.getCanonicalName());
    indexGroupByFunctionalIndex.setPath("/value");

    // Case 2.1: (FC missing, GB present) group by is functional
    String sql21 = SQLStatementUtils.createGroupBySql("foo", indexFilter, indexGroupByFunctionalIndex, false, mockValidator);
    assertEquals(sql21, "SELECT count(*) as COUNT, (cast(json_extract(`a_aspectbar`, '$.aspect.value') as char(1024))) FROM metadata_entity_foo\n"
        + "WHERE a_aspectbar IS NOT NULL\n"
        + "AND JSON_EXTRACT(a_aspectbar, '$.gma_deleted') IS NULL\n"
        + "AND deleted_ts IS NULL\n"
        + "GROUP BY (cast(json_extract(`a_aspectbar`, '$.aspect.value') as char(1024)))");
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

  @Test
  public void testCreateGroupBySqlFilterColumnPresentGroupByMissingFunctionalIndexes() {
    // Note that functional indexes are already mocked!

    // This mocks "fakefield" as NEITHER having an associated column nor a functional index
    when(mockValidator.getIndexExpression(anyString(), matches("e_aspectbar0fakefield")))
        .thenReturn(null);
    when(mockValidator.columnExists(anyString(), contains("i_aspectbar$fakefield"))).thenReturn(false);

    // Filter: functional
    IndexCriterion indexCriterionFunctionalIndex =
        SQLIndexFilterUtils.createIndexCriterion(AspectBar.class, "value", Condition.EQUAL,
            IndexValue.create("jhui"));
    IndexFilter indexFilter = new IndexFilter();
    IndexCriterionArray indexCriterionArray = new IndexCriterionArray();
    indexCriterionArray.add(indexCriterionFunctionalIndex);
    indexFilter.setCriteria(indexCriterionArray);

    // GroupBy: does NOT exist
    IndexGroupByCriterion indexGroupByNonexistent = new IndexGroupByCriterion();
    indexGroupByNonexistent.setAspect(AspectBar.class.getCanonicalName());
    indexGroupByNonexistent.setPath("/fakefield");

    // Case 3.1: (GB missing, FC present) filter is functional ==> always results in empty query because no group by
    String sql31 = SQLStatementUtils.createGroupBySql("foo", indexFilter, indexGroupByNonexistent, false, mockValidator);
    assertEquals(sql31, "");
  }

  @Test
  public void testParseLocalRelationshipValueSingleQuote() {
    // Test case: Single quote in URN (the original issue raised in META-22917)
    LocalRelationshipValue value = new LocalRelationshipValue();
    value.setString("urn:li:dataset:(urn:li:dataPlatform:hdfs,/jobs/dsotnt/lix_evaluations/premium_custom_button_acq_convoad_trex_eval_results',PROD)");

    String result = SQLStatementUtils.parseLocalRelationshipValue(value);

    // Single quote should be escaped to two single quotes
    assertEquals(result, "urn:li:dataset:(urn:li:dataPlatform:hdfs,/jobs/dsotnt/lix_evaluations/premium_custom_button_acq_convoad_trex_eval_results'',PROD)");
  }

  @Test
  public void testParseLocalRelationshipValueMultipleQuotes() {
    // Test case: Multiple single quotes
    LocalRelationshipValue value = new LocalRelationshipValue();
    value.setString("urn:li:dataset:(urn:li:dataPlatform:hdfs,/O'Reilly's_\"books\"_data,PROD)");

    String result = SQLStatementUtils.parseLocalRelationshipValue(value);

    // All single quotes should be escaped
    assertEquals(result, "urn:li:dataset:(urn:li:dataPlatform:hdfs,/O''Reilly''s_\"books\"_data,PROD)");
  }

  @Test
  public void testParseLocalRelationshipValueSQLInjectionAttempt() {
    // Test case: SQL injection attempt with OR 1=1
    LocalRelationshipValue value = new LocalRelationshipValue();
    value.setString("urn:li:dataset:(urn:li:dataPlatform:hdfs,/path',PROD) OR '1'='1");

    String result = SQLStatementUtils.parseLocalRelationshipValue(value);

    // The quotes should be escaped, preventing injection
    assertEquals(result, "urn:li:dataset:(urn:li:dataPlatform:hdfs,/path'',PROD) OR ''1''=''1");
  }

  @Test
  public void testParseLocalRelationshipValueBackslashAndQuote() {
    // Test case: Backslash followed by quote
    LocalRelationshipValue value = new LocalRelationshipValue();
    value.setString("urn:li:dataset:(urn:li:dataPlatform:hdfs,/path\\'data,PROD)");

    String result = SQLStatementUtils.parseLocalRelationshipValue(value);

    // Quote escaped, backslash preserved
    assertEquals(result, "urn:li:dataset:(urn:li:dataPlatform:hdfs,/path\\''data,PROD)");
  }

  @Test
  public void testParseLocalRelationshipValueDoubleQuotes() {
    // Test case: Double quotes (should be preserved as they're not special in single-quoted SQL strings)
    LocalRelationshipValue value = new LocalRelationshipValue();
    value.setString("urn:li:dataset:(urn:li:dataPlatform:hdfs,/path/with\"double\"quotes,PROD)");

    String result = SQLStatementUtils.parseLocalRelationshipValue(value);

    // Double quotes should be preserved
    assertEquals(result, "urn:li:dataset:(urn:li:dataPlatform:hdfs,/path/with\"double\"quotes,PROD)");
  }

  @Test
  public void testParseLocalRelationshipValueCommentSequence() {
    // Test case: SQL comment sequences
    LocalRelationshipValue value = new LocalRelationshipValue();
    value.setString("urn:li:dataset:(urn:li:dataPlatform:hdfs,/path--comment/**/data,PROD)");

    String result = SQLStatementUtils.parseLocalRelationshipValue(value);

    // Comment sequences should be preserved as data
    assertEquals(result, "urn:li:dataset:(urn:li:dataPlatform:hdfs,/path--comment/**/data,PROD)");
  }

  @Test
  public void testParseLocalRelationshipValueSemicolon() {
    // Test case: Semicolon (statement terminator)
    LocalRelationshipValue value = new LocalRelationshipValue();
    value.setString("urn:li:dataset:(urn:li:dataPlatform:hdfs,/path;DROP TABLE users;--,PROD)");

    String result = SQLStatementUtils.parseLocalRelationshipValue(value);

    // Semicolons should be preserved as data
    assertEquals(result, "urn:li:dataset:(urn:li:dataPlatform:hdfs,/path;DROP TABLE users;--,PROD)");
  }

  @Test
  public void testParseLocalRelationshipValueNewlinesAndTabs() {
    // Test case: Newlines and tabs
    LocalRelationshipValue value = new LocalRelationshipValue();
    value.setString("urn:li:dataset:(urn:li:dataPlatform:hdfs,/path\nwith\nnewlines\tand\ttabs,PROD)");

    String result = SQLStatementUtils.parseLocalRelationshipValue(value);

    // Control characters should be preserved
    assertEquals(result, "urn:li:dataset:(urn:li:dataPlatform:hdfs,/path\nwith\nnewlines\tand\ttabs,PROD)");
  }

  @Test
  public void testParseLocalRelationshipValueNullByte() {
    // Test case: Null byte (extremely unlikely in urn's though)
    LocalRelationshipValue value = new LocalRelationshipValue();
    value.setString("urn:li:dataset:(urn:li:dataPlatform:hdfs,/path\u0000with_null,PROD)");

    String result = SQLStatementUtils.parseLocalRelationshipValue(value);

    assertNotNull(result);
    assertEquals(result, "urn:li:dataset:(urn:li:dataPlatform:hdfs,/path\u0000with_null,PROD)");
  }

  @Test
  public void testParseLocalRelationshipValueArrayWithSpecialChars() {
    // Test case: Array values with special characters
    LocalRelationshipValue value = new LocalRelationshipValue();
    value.setArray(new StringArray(Arrays.asList(
        "urn:li:dataset:value1'with'quotes",
        "urn:li:dataset:value2\\with\\backslash",
        "urn:li:dataset:value3;with;semicolon"
    )));

    String result = SQLStatementUtils.parseLocalRelationshipValue(value);

    // Should be formatted as SQL IN clause with all values escaped
    assertTrue(result.startsWith("("));
    assertTrue(result.endsWith(")"));
    assertTrue(result.contains("'urn:li:dataset:value1''with''quotes'"));
    assertTrue(result.contains(", "));  // Proper separation between values
  }

  @Test
  public void testWhereClauseCompleteInjectionScenario() {
    // Test case: Complete SQL injection scenario through whereClause
    LocalRelationshipFilter filter = new LocalRelationshipFilter();
    LocalRelationshipCriterion criterion = new LocalRelationshipCriterion();

    LocalRelationshipCriterion.Field field = new LocalRelationshipCriterion.Field();
    field.setUrnField(new UrnField().setName("destination"));
    criterion.setField(field);
    criterion.setCondition(Condition.EQUAL);

    // Malicious URN attempting SQL injection
    String maliciousUrn = "urn:li:dataset:(urn:li:dataPlatform:hdfs,/data') OR 1=1 OR destination LIKE '%";
    LocalRelationshipValue value = new LocalRelationshipValue();
    value.setString(maliciousUrn);
    criterion.setValue(value);

    filter.setCriteria(new LocalRelationshipCriterionArray(criterion));

    Map<Condition, String> supportedConditions = new HashMap<>();
    supportedConditions.put(Condition.EQUAL, "=");

    String whereClause = SQLStatementUtils.whereClause(filter, supportedConditions, "rt", PLACEHOLDER_TABLE_NAME, mockValidator, false);

    // Expect all quotes escaped
    assertEquals(whereClause, "rt.destination='urn:li:dataset:(urn:li:dataPlatform:hdfs,/data'') OR 1=1 OR destination LIKE ''%'");
  }

  @Test
  public void testWhereClauseOldSchemaWithSpecialCharacters() {
    // Test case: Old schema mode with special characters
    // OLD_SCHEMA builds simpler sql, affects WHERE directly, no complex query with JOINs
    LocalRelationshipCriterion.Field field = new LocalRelationshipCriterion.Field();
    field.setUrnField(new UrnField());
    LocalRelationshipCriterion criterion = new LocalRelationshipCriterion()
        .setField(field)
        .setCondition(Condition.EQUAL)
        .setValue(LocalRelationshipValue.create("urn:li:dataset:test'data"));
    LocalRelationshipCriterionArray criteria = new LocalRelationshipCriterionArray(criterion);
    LocalRelationshipFilter filter = new LocalRelationshipFilter().setCriteria(criteria);

    String result = SQLStatementUtils.whereClauseOldSchema(
        Collections.singletonMap(Condition.EQUAL, "="), filter, "destination");

    // Should escape the quote
    assertEquals(result, " AND rt.destination = 'urn:li:dataset:test''data'");
  }

  @Test
  public void testBuildSQLQueryFromLocalRelationshipCriterionWithInjection() {
    // Test the complete flow with START_WITH condition
    LocalRelationshipCriterion.Field field = new LocalRelationshipCriterion.Field();
    field.setUrnField(new UrnField().setName("destination"));

    LocalRelationshipCriterion criterion = new LocalRelationshipCriterion()
        .setField(field)
        .setCondition(Condition.START_WITH)
        .setValue(LocalRelationshipValue.create("urn:li:dataset:prefix'%"));

    LocalRelationshipFilter filter = new LocalRelationshipFilter()
        .setCriteria(new LocalRelationshipCriterionArray(criterion));

    Map<Condition, String> supportedConditions = new HashMap<>();
    supportedConditions.put(Condition.START_WITH, "LIKE");

    String whereClause = SQLStatementUtils.whereClause(filter, supportedConditions, null, PLACEHOLDER_TABLE_NAME, mockValidator, false);

    // Should properly escape and add the wildcard
    assertEquals(whereClause, "destination LIKE 'urn:li:dataset:prefix''%%'");
  }

  @Test
  public void testAddTablePrefixToExpression() {
    // Test case 1: Empty table prefix should return expression unchanged
    String expression1 = "(cast(json_extract(`a_aspectbar`, '$.aspect.value') as char(1024)))";
    assertEquals(SQLStatementUtils.addTablePrefixToExpression("", expression1, "a_aspectbar"), expression1);

    // Test case 2: Simple column (no parentheses) should just prepend prefix
    assertEquals(SQLStatementUtils.addTablePrefixToExpression("rt", "i_aspectfoo$value", "a_aspectfoo"),
        "rt.i_aspectfoo$value");

    // Test case 3 (from comments): Expression with backticks around column name
    // (cast(json_extract(`a_aspectbar`, '$.aspect.value') as char(1024)))
    // --> (cast(json_extract(`PREFIX`.`a_aspectbar`, '$.aspect.value') as char(1024)))
    assertEquals(
        SQLStatementUtils.addTablePrefixToExpression("PREFIX",
            "(cast(json_extract(`a_aspectbar`, '$.aspect.value') as char(1024)))", "a_aspectbar"),
        "(cast(json_extract(`PREFIX`.`a_aspectbar`, '$.aspect.value') as char(1024)))");

    // Test case 4 (from comments): Expression without backticks around column name
    // (cast(json_extract(a_aspectbar, '$.aspect.value') as char(1024)))
    // --> (cast(json_extract(`PREFIX`.`a_aspectbar`, '$.aspect.value') as char(1024)))
    assertEquals(
        SQLStatementUtils.addTablePrefixToExpression("PREFIX",
            "(cast(json_extract(a_aspectbar, '$.aspect.value') as char(1024)))", "a_aspectbar"),
        "(cast(json_extract(`PREFIX`.a_aspectbar, '$.aspect.value') as char(1024)))");

    // Test case 5: Metadata column in relationship table (common use case) with backticks
    assertEquals(
        SQLStatementUtils.addTablePrefixToExpression("rt",
            "(cast(json_extract(`metadata`, '$.field') as char(64)))", "metadata"),
        "(cast(json_extract(`rt`.`metadata`, '$.field') as char(64)))");

    // Test case 6: Metadata column without backticks
    assertEquals(
        SQLStatementUtils.addTablePrefixToExpression("rt",
            "(cast(json_extract(metadata, '$.field') as char(64)))", "metadata"),
        "(cast(json_extract(`rt`.metadata, '$.field') as char(64)))");

    // Test case 7: Array expression index
    assertEquals(
        SQLStatementUtils.addTablePrefixToExpression("dt",
            "(cast(json_extract(`a_aspectbar`, '$.aspect.value_array') as char(128) array))", "a_aspectbar"),
        "(cast(json_extract(`dt`.`a_aspectbar`, '$.aspect.value_array') as char(128) array))");

    // Test case 8: Complex nested JSON path (legacy array extraction)
    assertEquals(
        SQLStatementUtils.addTablePrefixToExpression("st",
            "(cast(replace(json_unquote(json_extract(`a_aspectbar`,'$.aspect.annotation.ontologyIris[*]')),'\"','') as char(255)))",
            "a_aspectbar"),
        "(cast(replace(json_unquote(json_extract(`st`.`a_aspectbar`,'$.aspect.annotation.ontologyIris[*]')),'\"','') as char(255)))");

    // Test case 9: Column name appears in JSON path - should only replace column reference
    assertEquals(
        SQLStatementUtils.addTablePrefixToExpression("foo",
            "(cast(json_extract(`a_aspectfoo`, '$.a_aspectfoo.value') as char(1024)))", "a_aspectfoo"),
        "(cast(json_extract(`foo`.`a_aspectfoo`, '$.a_aspectfoo.value') as char(1024)))");

    // Test case 13: Column name substring appears in JSON path - should only replace actual column reference
    assertEquals(
        SQLStatementUtils.addTablePrefixToExpression("rt",
            "(cast(json_extract(`metadata`, '$.metadata.field') as char(64)))", "metadata"),
        "(cast(json_extract(`rt`.`metadata`, '$.metadata.field') as char(64)))");

    // Test case 14: Multiple occurrences of column name - should replace all
    assertEquals(
        SQLStatementUtils.addTablePrefixToExpression("t1", "CONCAT(`a_col`, `a_col`)", "a_col"),
        "CONCAT(`t1`.`a_col`, `t1`.`a_col`)");
  }

}