package com.linkedin.metadata.dao.utils;

import com.linkedin.metadata.query.Condition;
import com.linkedin.metadata.query.IndexCriterion;
import com.linkedin.metadata.query.IndexCriterionArray;
import com.linkedin.metadata.query.IndexFilter;
import com.linkedin.metadata.query.IndexValue;
import com.linkedin.metadata.query.SortOrder;
import com.linkedin.testing.AspectFoo;
import com.linkedin.testing.urn.FooUrn;
import org.testng.annotations.Test;

import static com.linkedin.testing.TestUtils.*;
import static org.testng.Assert.*;


public class SQLStatementUtilsTest {

  @Test
  public void testCreateUpsertAspectSql() {
    FooUrn fooUrn = makeFooUrn(1);
    AspectFoo aspectFoo = new AspectFoo();
    String expectedSql =
        "INSERT INTO metadata_entity_foo (urn, a_testing_aspectfoo, lastmodifiedon, lastmodifiedby) VALUE (:urn, "
            + ":metadata, :lastmodifiedon, :lastmodifiedby) ON DUPLICATE KEY UPDATE a_testing_aspectfoo = :metadata;";
    assertEquals(SQLStatementUtils.createAspectUpsertSql(fooUrn, aspectFoo), expectedSql);
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
    String expectedSql = "WITH _temp_results AS (SELECT * FROM metadata_entity_foo\n" + "WHERE "
        + "i_testing_aspectfoo$value >= 25\nAND " + "i_testing_aspectfoo$value < 50\n"
        + "ORDER BY i_testing_aspectfoo$value ASC)\nSELECT *, (SELECT COUNT(urn) FROM _temp_results) AS _total_count FROM _temp_results";
    assertEquals(sql, expectedSql);
  }
}