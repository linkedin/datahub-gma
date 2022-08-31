package com.linkedin.metadata.dao.utils;

import com.linkedin.metadata.query.Condition;
import com.linkedin.metadata.query.IndexCriterion;
import com.linkedin.metadata.query.IndexCriterionArray;
import com.linkedin.metadata.query.IndexFilter;
import com.linkedin.metadata.query.IndexSortCriterion;
import com.linkedin.metadata.query.IndexValue;
import com.linkedin.metadata.query.SortOrder;
import com.linkedin.testing.AspectFoo;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class SQLIndexFilterUtilsTest {

  @Test
  public void testParseSortCriteria() {
    IndexSortCriterion indexSortCriterion =
        SQLIndexFilterUtils.createIndexSortCriterion(AspectFoo.class, "id", SortOrder.ASCENDING);
    assertEquals(indexSortCriterion.getOrder(), SortOrder.ASCENDING);
    assertEquals(indexSortCriterion.getAspect(), AspectFoo.class.getCanonicalName());

    String sql = SQLIndexFilterUtils.parseSortCriteria(indexSortCriterion);
    assertEquals(sql, "ORDER BY i_aspectfoo$id ASC");

    indexSortCriterion.setOrder(SortOrder.DESCENDING);
    sql = SQLIndexFilterUtils.parseSortCriteria(indexSortCriterion);
    assertEquals(sql, "ORDER BY i_aspectfoo$id DESC");
  }

  @Test
  public void testParseIndexFilter() {
    IndexCriterion
        indexCriterion = SQLIndexFilterUtils.createIndexCriterion(AspectFoo.class, "id", Condition.LESS_THAN, IndexValue.create(12L));
    IndexFilter indexFilter = new IndexFilter();
    IndexCriterionArray indexCriterionArray = new IndexCriterionArray();
    indexCriterionArray.add(indexCriterion);
    indexFilter.setCriteria(indexCriterionArray);

    String sql = SQLIndexFilterUtils.parseIndexFilter(indexFilter);
    assertEquals(sql, "WHERE i_aspectfoo$id < 12\nAND a_aspectfoo != CAST('{\"gma_deleted\":true}' AS JSON)");
  }
}