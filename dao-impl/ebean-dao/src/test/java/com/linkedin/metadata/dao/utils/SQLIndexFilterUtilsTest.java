package com.linkedin.metadata.dao.utils;

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

    // new mocks for functional index testing: we will designate "AspectBar" as the aspect with a functional index and
    //    "value" as the field (path) to be indexed
    when(mockValidator.getIndexExpression(anyString(), matches("i_aspectbar[$0]value")))
        .thenReturn("(cast(json_extract(`a_aspectbar`, '$.aspect.value') as char(1024) charset utf8mb4))");
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

  // Note that the other tests passing with the existing mocks inherently test the non-functional index case
  @Test
  public void testParseIndexFilterWithFunctionalIndex() {
    IndexCriterion
        indexCriterion = SQLIndexFilterUtils.createIndexCriterion(AspectBar.class, "value", Condition.LESS_THAN, IndexValue.create(12L));
    IndexFilter indexFilter = new IndexFilter();
    IndexCriterionArray indexCriterionArray = new IndexCriterionArray();
    indexCriterionArray.add(indexCriterion);
    indexFilter.setCriteria(indexCriterionArray);

    String sql = SQLIndexFilterUtils.parseIndexFilter(FooUrn.ENTITY_TYPE, indexFilter, false, mockValidator);
    assertEquals(sql,
        "WHERE a_aspectbar IS NOT NULL\nAND JSON_EXTRACT(a_aspectbar, '$.gma_deleted') IS NULL\nAND (cast(json_extract(`a_aspectbar`, '$.aspect.value') as char(1024) charset utf8mb4)) < 12\nAND deleted_ts IS NULL");

    sql = SQLIndexFilterUtils.parseIndexFilter(FooUrn.ENTITY_TYPE, indexFilter, true, mockValidator);
    assertEquals(sql,
        "WHERE a_aspectbar IS NOT NULL\nAND JSON_EXTRACT(a_aspectbar, '$.gma_deleted') IS NULL\nAND (cast(json_extract(`a_aspectbar`, '$.aspect.value') as char(1024) charset utf8mb4)) < 12\nAND deleted_ts IS NULL");
  }
}