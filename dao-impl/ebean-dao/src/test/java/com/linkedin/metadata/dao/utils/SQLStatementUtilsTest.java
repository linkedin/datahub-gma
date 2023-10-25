package com.linkedin.metadata.dao.utils;

import com.linkedin.common.urn.Urn;
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
import com.linkedin.metadata.query.SortOrder;
import com.linkedin.metadata.query.UrnField;
import com.linkedin.testing.AspectFoo;
import com.linkedin.testing.urn.FooUrn;
import java.net.URISyntaxException;
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
    String expectedSql =
        "INSERT INTO metadata_entity_foo (urn, a_urn, a_aspectfoo, lastmodifiedon, lastmodifiedby) VALUE (:urn, "
            + ":a_urn, :metadata, :lastmodifiedon, :lastmodifiedby) ON DUPLICATE KEY UPDATE a_aspectfoo = :metadata,"
            + " lastmodifiedon = :lastmodifiedon;";
    assertEquals(SQLStatementUtils.createAspectUpsertSql(fooUrn, AspectFoo.class, true), expectedSql);

    expectedSql =
        "INSERT INTO metadata_entity_foo (urn, a_aspectfoo, lastmodifiedon, lastmodifiedby) VALUE (:urn, "
            + ":metadata, :lastmodifiedon, :lastmodifiedby) ON DUPLICATE KEY UPDATE a_aspectfoo = :metadata,"
            + " lastmodifiedon = :lastmodifiedon;";
    assertEquals(SQLStatementUtils.createAspectUpsertSql(fooUrn, AspectFoo.class, false), expectedSql);
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
    assertEquals(SQLStatementUtils.createAspectReadSql(AspectFoo.class, set, false), expectedSql);
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
    String expectedSql = "SELECT *, (SELECT COUNT(urn) FROM metadata_entity_foo WHERE a_aspectfoo IS NOT NULL\n"
        + "AND JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL\n" + "AND i_aspectfoo$value >= 25\n"
        + "AND a_aspectfoo IS NOT NULL\n" + "AND JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL\n"
        + "AND i_aspectfoo$value < 50) as _total_count FROM metadata_entity_foo\n" + "WHERE a_aspectfoo IS NOT NULL\n"
        + "AND JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL\n" + "AND i_aspectfoo$value >= 25\n"
        + "AND a_aspectfoo IS NOT NULL\n" + "AND JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL\n"
        + "AND i_aspectfoo$value < 50";

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
        + "WHERE a_aspectfoo IS NOT NULL\n" + "AND JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL\n"
        + "AND i_aspectfoo$value >= 25\n" + "AND a_aspectfoo IS NOT NULL\n"
        + "AND JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL\n" + "AND i_aspectfoo$value < 50\n"
        + "GROUP BY i_aspectfoo$value");
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
    assertEquals(SQLStatementUtils.whereClause(filter, Collections.singletonMap(Condition.EQUAL, "="), null), "urn='value1'");
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
    assertEquals(SQLStatementUtils.whereClause(filter, Collections.singletonMap(Condition.EQUAL, "="), null), "urn='value1' OR urn='value2'");
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
    assertEquals(SQLStatementUtils.whereClause(filter, Collections.singletonMap(Condition.EQUAL, "="), null), "urn='value1' AND i_aspectfoo$value='value2'");
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
    assertEquals(SQLStatementUtils.whereClause(filter, Collections.singletonMap(Condition.EQUAL, "="), null),
        "(urn='value1' OR urn='value3') AND metadata$value='value4' AND i_aspectfoo$value='value2'");
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

    assertEquals(SQLStatementUtils.whereClause(Collections.singletonMap(Condition.EQUAL, "="), new Pair<>(filter1, "foo"),
        new Pair<>(filter2, "bar")), "(foo.i_aspectfoo$value='value2' AND (foo.urn='value1' OR foo.urn='value3')"
        + " AND foo.metadata$value='value4') AND (bar.urn='value1' OR bar.urn='value2')");
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
    String tableName = SQLSchemaUtils.getTableName(fooUrn.getEntityType());
    assertEquals(
        SQLStatementUtils.createListAspectWithPaginationSql(AspectFoo.class, tableName, true, 0, 5),
        "SELECT urn, a_aspectfoo, lastmodifiedon, lastmodifiedby, createdfor, (SELECT COUNT(urn) FROM "
            + "metadata_entity_foo WHERE a_aspectfoo IS NOT NULL) as _total_count FROM metadata_entity_foo "
            + "WHERE a_aspectfoo IS NOT NULL LIMIT 5 OFFSET 0");
    assertEquals(
        SQLStatementUtils.createListAspectWithPaginationSql(AspectFoo.class, tableName, false, 0, 5),
        "SELECT urn, a_aspectfoo, lastmodifiedon, lastmodifiedby, createdfor, (SELECT COUNT(urn) FROM "
            + "metadata_entity_foo WHERE a_aspectfoo IS NOT NULL AND JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL) "
            + "as _total_count FROM metadata_entity_foo WHERE a_aspectfoo IS NOT NULL AND "
            + "JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL LIMIT 5 OFFSET 0");
  }
}