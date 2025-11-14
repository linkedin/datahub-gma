package com.linkedin.metadata.dao.utils;

import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.query.Condition;
import com.linkedin.metadata.query.IndexCriterion;
import com.linkedin.metadata.query.IndexCriterionArray;
import com.linkedin.metadata.query.IndexFilter;
import com.linkedin.metadata.query.IndexSortCriterion;
import com.linkedin.metadata.query.IndexValue;
import com.linkedin.metadata.query.SortOrder;
import com.linkedin.testing.AspectBar;
import com.linkedin.testing.AspectFoo;
import com.linkedin.testing.urn.FooUrn;
import java.net.URISyntaxException;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.parser.SqlParser;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class SQLIndexFilterUtilsTest {

  private static SchemaValidatorUtil mockValidator;

  @BeforeClass
  public void setupValidator() {
    mockValidator = mock(SchemaValidatorUtil.class);
    when(mockValidator.columnExists(anyString(), anyString())).thenReturn(true);

    /////// NEW MOCKS for functional index testing
    // NOTE that " charset utf8mb4" is appended after "char(1024)" for some of the (real) use cases in our DB's
    //    but does NOT pass Calcite's syntax checker. However, in actual queries, it is appended as a part of the index
    //    and works just fine: we will omit it here so that we can check the syntax otherwise.

    // "AspectBar" as the aspect with a functional index and "value" as the field (path) to be indexed
    when(mockValidator.getIndexExpression(anyString(), matches("e_aspectbar0value")))
        .thenReturn("(cast(json_extract(`a_aspectbar`, '$.aspect.value') as char(1024)))");
    //    This is an existing new way of Array extraction (AssetLabels.derived_labels)
    when(mockValidator.getIndexExpression(anyString(), matches("e_aspectbar0value_array")))
        .thenReturn("(cast(json_extract(`a_aspectbar`, '$.aspect.value_array') as char(128) array))");
    //    This is an existing legacy way of array extraction, casting to a string (DataPolicyInfo.annotation.ontologyIris)
    when(mockValidator.getIndexExpression(anyString(), matches("e_aspectbar0annotation0ontologyIris")))
        .thenReturn("(cast(replace(json_unquote(json_extract(`a_aspectbar`,'$.aspect.annotation.ontologyIris[*]')),'\"','') as char(255)))");
  }

  @Test
  public void testParseSortCriteria() throws URISyntaxException {
    FooUrn fooUrn = new FooUrn(1);
    IndexSortCriterion indexSortCriterion =
        SQLIndexFilterUtils.createIndexSortCriterion(AspectFoo.class, "id", SortOrder.ASCENDING);
    assertEquals(indexSortCriterion.getOrder(), SortOrder.ASCENDING);
    assertEquals(indexSortCriterion.getAspect(), AspectFoo.class.getCanonicalName());

    String sql1 = SQLIndexFilterUtils.parseSortCriteria(fooUrn.getEntityType(), indexSortCriterion, false);
    assertEquals(sql1, "ORDER BY i_aspectfoo$id ASC");

    String sql2 = SQLIndexFilterUtils.parseSortCriteria(fooUrn.getEntityType(), indexSortCriterion, true);
    assertEquals(sql2, "ORDER BY i_aspectfoo0id ASC");

    indexSortCriterion.setOrder(SortOrder.DESCENDING);
    sql1 = SQLIndexFilterUtils.parseSortCriteria(fooUrn.getEntityType(), indexSortCriterion, false);
    assertEquals(sql1, "ORDER BY i_aspectfoo$id DESC");

    sql2 = SQLIndexFilterUtils.parseSortCriteria(fooUrn.getEntityType(), indexSortCriterion, true);
    assertEquals(sql2, "ORDER BY i_aspectfoo0id DESC");
  }

  @Test
  public void testParseIndexFilter() {
    IndexCriterion
        indexCriterion = SQLIndexFilterUtils.createIndexCriterion(AspectFoo.class, "id", Condition.LESS_THAN, IndexValue.create(12L));
    IndexFilter indexFilter = new IndexFilter();
    IndexCriterionArray indexCriterionArray = new IndexCriterionArray();
    indexCriterionArray.add(indexCriterion);
    indexFilter.setCriteria(indexCriterionArray);

    String sql = SQLIndexFilterUtils.parseIndexFilter(FooUrn.ENTITY_TYPE, indexFilter, false, mockValidator);
    assertEquals(sql,
        "WHERE a_aspectfoo IS NOT NULL\nAND JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL\nAND i_aspectfoo$id < 12\nAND deleted_ts IS NULL");

    sql = SQLIndexFilterUtils.parseIndexFilter(FooUrn.ENTITY_TYPE, indexFilter, true, mockValidator);
    assertEquals(sql,
        "WHERE a_aspectfoo IS NOT NULL\nAND JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL\nAND i_aspectfoo0id < 12\nAND deleted_ts IS NULL");
  }

  private static void assertValidSql(String sql) {
    final String sqlPrefix = "SELECT * FROM metadata_entity_fakeplaceholder\n";
    try {
      SqlParser.create(sqlPrefix + sql, SqlParser.config().withLex(Lex.MYSQL)).parseQuery();
    } catch (Exception e) {
      System.err.println("\nINPUT: " + sqlPrefix + sql);
      throw new AssertionError("Expected valid SQL but got exception: " + e.getMessage());
    }
  }

  // Note that the other tests passing with the existing mocks inherently test the non-functional index case
  @SuppressWarnings("checkstyle:LineLength")
  @Test
  public void testParseIndexFilterWithFunctionalIndex() {
    // set up reusable index filter code
    IndexFilter indexFilter = new IndexFilter();
    IndexCriterionArray indexCriterionArray = new IndexCriterionArray();
    indexFilter.setCriteria(indexCriterionArray);

    // Test 1: ARRAY_CONTAINS
    indexCriterionArray.add(0,
        SQLIndexFilterUtils.createIndexCriterion(AspectBar.class, "value_array", Condition.ARRAY_CONTAINS,
            IndexValue.create(12L)));
    final String expectedSql1 =
        "WHERE a_aspectbar IS NOT NULL\nAND JSON_EXTRACT(a_aspectbar, '$.gma_deleted') IS NULL\nAND '12' MEMBER OF((cast(json_extract(`a_aspectbar`, '$.aspect.value_array') as char(128) array)))\nAND deleted_ts IS NULL";
    assertValidSql(expectedSql1);  // assert that the expected SQL is valid to begin with
    assertEquals(SQLIndexFilterUtils.parseIndexFilter(FooUrn.ENTITY_TYPE, indexFilter, false, mockValidator),
        expectedSql1);
    assertEquals(SQLIndexFilterUtils.parseIndexFilter(FooUrn.ENTITY_TYPE, indexFilter, true, mockValidator),
        expectedSql1);

    // Test 2.0: CONTAIN with simple string value
    indexCriterionArray.set(0,
        SQLIndexFilterUtils.createIndexCriterion(AspectBar.class, "value", Condition.CONTAIN, IndexValue.create(12L)));
    final String expectedSql20 =
        "WHERE a_aspectbar IS NOT NULL\nAND JSON_EXTRACT(a_aspectbar, '$.gma_deleted') IS NULL\nAND JSON_SEARCH((cast(json_extract(`a_aspectbar`, '$.aspect.value') as char(1024))), 'one', '12') IS NOT NULL\nAND deleted_ts IS NULL";
    assertValidSql(expectedSql20);  // assert that the expected SQL is valid to begin with
    assertEquals(SQLIndexFilterUtils.parseIndexFilter(FooUrn.ENTITY_TYPE, indexFilter, false, mockValidator),
        expectedSql20);
    assertEquals(SQLIndexFilterUtils.parseIndexFilter(FooUrn.ENTITY_TYPE, indexFilter, true, mockValidator),
        expectedSql20);

    // Test 2.1: CONTAIN with array value
    indexCriterionArray.set(0,
        SQLIndexFilterUtils.createIndexCriterion(AspectBar.class, "value_array", Condition.CONTAIN,
            IndexValue.create(12L)));
    final String expectedSql21 =
        "WHERE a_aspectbar IS NOT NULL\nAND JSON_EXTRACT(a_aspectbar, '$.gma_deleted') IS NULL\nAND JSON_SEARCH((cast(json_extract(`a_aspectbar`, '$.aspect.value_array') as char(128) array)), 'one', '12') IS NOT NULL\nAND deleted_ts IS NULL";
    assertValidSql(expectedSql21);  // assert that the expected SQL is valid to begin with
    assertEquals(SQLIndexFilterUtils.parseIndexFilter(FooUrn.ENTITY_TYPE, indexFilter, false, mockValidator),
        expectedSql21);
    assertEquals(SQLIndexFilterUtils.parseIndexFilter(FooUrn.ENTITY_TYPE, indexFilter, true, mockValidator),
        expectedSql21);

    // Test 2.2: CONTAIN with string value (note that this is a legacy way of array extraction, which results in a string)
    indexCriterionArray.set(0,
        SQLIndexFilterUtils.createIndexCriterion(AspectBar.class, "annotation/ontologyIris", Condition.CONTAIN,
            IndexValue.create(12L)));
    final String expectedSql22 =
        "WHERE a_aspectbar IS NOT NULL\nAND JSON_EXTRACT(a_aspectbar, '$.gma_deleted') IS NULL\nAND JSON_SEARCH((cast(replace(json_unquote(json_extract(`a_aspectbar`,'$.aspect.annotation.ontologyIris[*]')),'\"','') as char(255))), 'one', '12') IS NOT NULL\nAND deleted_ts IS NULL";
    assertValidSql(expectedSql22);  // assert that the expected SQL is valid to begin with
    assertEquals(SQLIndexFilterUtils.parseIndexFilter(FooUrn.ENTITY_TYPE, indexFilter, false, mockValidator),
        expectedSql22);
    assertEquals(SQLIndexFilterUtils.parseIndexFilter(FooUrn.ENTITY_TYPE, indexFilter, true, mockValidator),
        expectedSql22);

    // Test 3: IN
    indexCriterionArray.set(0, SQLIndexFilterUtils.createIndexCriterion(AspectBar.class, "value", Condition.IN,
        IndexValue.create(new StringArray("six", "seven"))));
    final String expectedSql3 =
        "WHERE a_aspectbar IS NOT NULL\nAND JSON_EXTRACT(a_aspectbar, '$.gma_deleted') IS NULL\nAND (cast(json_extract(`a_aspectbar`, '$.aspect.value') as char(1024))) IN ('six', 'seven')\nAND deleted_ts IS NULL";
    assertValidSql(expectedSql3);  // assert that the expected SQL is valid to begin with
    assertEquals(SQLIndexFilterUtils.parseIndexFilter(FooUrn.ENTITY_TYPE, indexFilter, false, mockValidator),
        expectedSql3);
    assertEquals(SQLIndexFilterUtils.parseIndexFilter(FooUrn.ENTITY_TYPE, indexFilter, true, mockValidator),
        expectedSql3);

    // Test 4.1: EQUAL -- with string
    indexCriterionArray.set(0,
        SQLIndexFilterUtils.createIndexCriterion(AspectBar.class, "value", Condition.EQUAL, IndexValue.create("six")));
    final String expectedSql41 =
        "WHERE a_aspectbar IS NOT NULL\nAND JSON_EXTRACT(a_aspectbar, '$.gma_deleted') IS NULL\nAND (cast(json_extract(`a_aspectbar`, '$.aspect.value') as char(1024))) = 'six'\nAND deleted_ts IS NULL";
    assertValidSql(expectedSql41);  // assert that the expected SQL is valid to begin with
    assertEquals(SQLIndexFilterUtils.parseIndexFilter(FooUrn.ENTITY_TYPE, indexFilter, false, mockValidator),
        expectedSql41);
    assertEquals(SQLIndexFilterUtils.parseIndexFilter(FooUrn.ENTITY_TYPE, indexFilter, true, mockValidator),
        expectedSql41);

    // Test 4.2: EQUAL -- with JSON array
    indexCriterionArray.set(0,
        SQLIndexFilterUtils.createIndexCriterion(AspectBar.class, "value_array", Condition.EQUAL,
            IndexValue.create(new StringArray("six", "seven"))));
    final String expectedSql42 =
        "WHERE a_aspectbar IS NOT NULL\nAND JSON_EXTRACT(a_aspectbar, '$.gma_deleted') IS NULL\nAND (cast(json_extract(`a_aspectbar`, '$.aspect.value_array') as char(128) array)) = '[\"six\", \"seven\"]'\nAND deleted_ts IS NULL";
    assertValidSql(expectedSql42);  // assert that the expected SQL is valid to begin with
    assertEquals(SQLIndexFilterUtils.parseIndexFilter(FooUrn.ENTITY_TYPE, indexFilter, false, mockValidator),
        expectedSql42);
    assertEquals(SQLIndexFilterUtils.parseIndexFilter(FooUrn.ENTITY_TYPE, indexFilter, true, mockValidator),
        expectedSql42);

    // Test 5: START_WITH -- will arbitrarily use "annotation/ontologyIris", the more complex string option
    indexCriterionArray.set(0,
        SQLIndexFilterUtils.createIndexCriterion(AspectBar.class, "annotation/ontologyIris", Condition.START_WITH,
            IndexValue.create("six")));
    final String expectedSql5 =
        "WHERE a_aspectbar IS NOT NULL\nAND JSON_EXTRACT(a_aspectbar, '$.gma_deleted') IS NULL\nAND (cast(replace(json_unquote(json_extract(`a_aspectbar`,'$.aspect.annotation.ontologyIris[*]')),'\"','') as char(255))) LIKE 'six%'\nAND deleted_ts IS NULL";
    assertValidSql(expectedSql5);  // assert that the expected SQL is valid to begin with
    assertEquals(SQLIndexFilterUtils.parseIndexFilter(FooUrn.ENTITY_TYPE, indexFilter, false, mockValidator),
        expectedSql5);
    assertEquals(SQLIndexFilterUtils.parseIndexFilter(FooUrn.ENTITY_TYPE, indexFilter, true, mockValidator),
        expectedSql5);

    // Test 10: LESS_THAN
    indexCriterionArray.set(0, SQLIndexFilterUtils.createIndexCriterion(AspectBar.class, "value", Condition.LESS_THAN,
        IndexValue.create(12L)));
    final String expectedSql10 =
        "WHERE a_aspectbar IS NOT NULL\nAND JSON_EXTRACT(a_aspectbar, '$.gma_deleted') IS NULL\nAND (cast(json_extract(`a_aspectbar`, '$.aspect.value') as char(1024))) < 12\nAND deleted_ts IS NULL";
    assertValidSql(expectedSql10);  // assert that the expected SQL is valid to begin with
    assertEquals(SQLIndexFilterUtils.parseIndexFilter(FooUrn.ENTITY_TYPE, indexFilter, false, mockValidator),
        expectedSql10);
    assertEquals(SQLIndexFilterUtils.parseIndexFilter(FooUrn.ENTITY_TYPE, indexFilter, true, mockValidator),
        expectedSql10);
  }
}