package com.linkedin.metadata.dao.utils;

import com.linkedin.metadata.query.Condition;
import com.linkedin.metadata.query.Criterion;
import com.linkedin.metadata.query.CriterionArray;
import com.linkedin.metadata.query.Filter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.testng.annotations.Test;

import static com.linkedin.metadata.dao.utils.SearchUtils.*;
import static org.testng.Assert.*;

public class SearchUtilsTest {
  @Test
  public void testGetRequestMap() {
    // Empty filter
    final Filter filter1 = QueryUtils.newFilter(null);
    final Map<String, String> actual1 = getRequestMap(filter1);
    assertTrue(actual1.isEmpty());

    // Filter with criteria with default condition
    final Map requestParams = Collections.unmodifiableMap(new HashMap() {
      {
        put("key1", "value1");
        put("key2", "value2");
      }
    });
    final Filter filter2 = QueryUtils.newFilter(requestParams);
    final Map<String, String> actual2 = getRequestMap(filter2);
    assertEquals(actual2, requestParams);

    // Filter with unsupported condition
    final Filter filter3 = new Filter().setCriteria(new CriterionArray(
        new Criterion().setField("key").setValue("value").setCondition(Condition.CONTAIN)
    ));
    assertThrows(UnsupportedOperationException.class, () -> getRequestMap(filter3));
  }

  @Test
  public void testGetQueryBuilderFromContainCriterion() {

    // Given: a 'contain' criterion
    Criterion containCriterion = new Criterion();
    containCriterion.setValue("match * text");
    containCriterion.setCondition(Condition.CONTAIN);
    containCriterion.setField("text");

    // Expect 'contain' criterion creates a MatchQueryBuilder
    QueryBuilder queryBuilder = SearchUtils.getQueryBuilderFromCriterion(containCriterion);
    assertNotNull(queryBuilder);
    assertTrue(queryBuilder instanceof WildcardQueryBuilder);

    // Expect 'field name' and search terms
    assertEquals(((WildcardQueryBuilder) queryBuilder).fieldName(), "text");
    assertEquals(((WildcardQueryBuilder) queryBuilder).value(), "*match \\* text*");
  }

  @Test
  public void testGetQueryBuilderFromStartWithCriterion() {

    // Given: a 'start_with' criterion
    Criterion containCriterion = new Criterion();
    containCriterion.setValue("match * text");
    containCriterion.setCondition(Condition.START_WITH);
    containCriterion.setField("text");

    // Expect 'start_with' criterion creates a WildcardQueryBuilder
    QueryBuilder queryBuilder = SearchUtils.getQueryBuilderFromCriterion(containCriterion);
    assertNotNull(queryBuilder);
    assertTrue(queryBuilder instanceof WildcardQueryBuilder);

    // Expect 'field name' and search terms
    assertEquals(((WildcardQueryBuilder) queryBuilder).fieldName(), "text");
    assertEquals(((WildcardQueryBuilder) queryBuilder).value(), "match \\* text*");
  }

  @Test
  public void testGetQueryBuilderFromEndWithCriterion() {

    // Given: a 'end_with' criterion
    Criterion containCriterion = new Criterion();
    containCriterion.setValue("match * text");
    containCriterion.setCondition(Condition.END_WITH);
    containCriterion.setField("text");

    // Expect 'end_with' criterion creates a MatchQueryBuilder
    QueryBuilder queryBuilder = SearchUtils.getQueryBuilderFromCriterion(containCriterion);
    assertNotNull(queryBuilder);
    assertTrue(queryBuilder instanceof WildcardQueryBuilder);

    // Expect 'field name' and search terms
    assertEquals(((WildcardQueryBuilder) queryBuilder).fieldName(), "text");
    assertEquals(((WildcardQueryBuilder) queryBuilder).value(), "*match \\* text");
  }
}
