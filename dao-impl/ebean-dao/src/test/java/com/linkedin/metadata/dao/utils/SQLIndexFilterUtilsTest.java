package com.linkedin.metadata.dao.utils;

import com.linkedin.metadata.query.Condition;
import com.linkedin.metadata.query.IndexCriterion;
import com.linkedin.metadata.query.IndexCriterionArray;
import com.linkedin.metadata.query.IndexFilter;
import com.linkedin.metadata.query.IndexSortCriterion;
import com.linkedin.metadata.query.IndexValue;
import com.linkedin.metadata.query.SortOrder;
import com.linkedin.testing.AspectFoo;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class SQLIndexFilterUtilsTest {

  @Factory(dataProvider = "inputList")
  public SQLIndexFilterUtilsTest(boolean nonDollarVirtualColumnsEnabled) {
    SQLSchemaUtils.setNonDollarVirtualColumnsEnabled(nonDollarVirtualColumnsEnabled);
  }

  @DataProvider(name = "inputList")
  public static Object[][] inputList() {
    return new Object[][] {
        { true },
        { false }
    };
  }

  @Test
  public void testParseSortCriteria() {
    IndexSortCriterion indexSortCriterion =
        SQLIndexFilterUtils.createIndexSortCriterion(AspectFoo.class, "id", SortOrder.ASCENDING);
    assertEquals(indexSortCriterion.getOrder(), SortOrder.ASCENDING);
    assertEquals(indexSortCriterion.getAspect(), AspectFoo.class.getCanonicalName());

    if (!SQLSchemaUtils.isNonDollarVirtualColumnsEnabled()) {
      String sql1 = SQLIndexFilterUtils.parseSortCriteria(indexSortCriterion);
      assertEquals(sql1, "ORDER BY i_aspectfoo$id ASC");

      indexSortCriterion.setOrder(SortOrder.DESCENDING);
      sql1 = SQLIndexFilterUtils.parseSortCriteria(indexSortCriterion);
      assertEquals(sql1, "ORDER BY i_aspectfoo$id DESC");
    } else {
      String sql2 = SQLIndexFilterUtils.parseSortCriteria(indexSortCriterion);
      assertEquals(sql2, "ORDER BY i_aspectfoo0id ASC");

      sql2 = SQLIndexFilterUtils.parseSortCriteria(indexSortCriterion);
      assertEquals(sql2, "ORDER BY i_aspectfoo0id DESC");
    }
  }

  @Test
  public void testParseIndexFilter() {
    IndexCriterion
        indexCriterion = SQLIndexFilterUtils.createIndexCriterion(AspectFoo.class, "id", Condition.LESS_THAN, IndexValue.create(12L));
    IndexFilter indexFilter = new IndexFilter();
    IndexCriterionArray indexCriterionArray = new IndexCriterionArray();
    indexCriterionArray.add(indexCriterion);
    indexFilter.setCriteria(indexCriterionArray);

    if (!SQLSchemaUtils.isNonDollarVirtualColumnsEnabled()) {
      String sql1 = SQLIndexFilterUtils.parseIndexFilter(indexFilter);
      assertEquals(sql1,
          "WHERE a_aspectfoo IS NOT NULL\nAND JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL\nAND i_aspectfoo$id < 12");
    } else {
      String sql2 = SQLIndexFilterUtils.parseIndexFilter(indexFilter);
      assertEquals(sql2,
          "WHERE a_aspectfoo IS NOT NULL\nAND JSON_EXTRACT(a_aspectfoo, '$.gma_deleted') IS NULL\nAND i_aspectfoo0id < 12");
    }
  }
}