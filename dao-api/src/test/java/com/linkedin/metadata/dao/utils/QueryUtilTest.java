package com.linkedin.metadata.dao.utils;

import com.google.common.collect.ImmutableSet;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.AspectVersion;
import com.linkedin.metadata.query.Condition;
import com.linkedin.metadata.query.Criterion;
import com.linkedin.metadata.query.CriterionArray;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.RelationshipDirection;
import com.linkedin.metadata.query.RelationshipFilter;
import com.linkedin.testing.AspectBar;
import com.linkedin.testing.AspectFoo;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;

import static com.linkedin.metadata.dao.utils.QueryUtils.newRelationshipFilter;
import static org.testng.Assert.*;


public class QueryUtilTest {

  @Test
  public void testNewCriterion() {
    Criterion criterion = QueryUtils.newCriterion("foo", "bar");
    assertEquals(criterion, new Criterion().setField("foo").setValue("bar").setCondition(Condition.EQUAL));

    criterion = QueryUtils.newCriterion("f", "v", Condition.CONTAIN);
    assertEquals(criterion, new Criterion().setField("f").setValue("v").setCondition(Condition.CONTAIN));
  }

  @Test
  public void testNewFilter() {
    Filter filter = QueryUtils.newFilter("foo", "bar");
    assertEquals(filter.getCriteria().size(), 1);
    assertEquals(filter.getCriteria().get(0), QueryUtils.newCriterion("foo", "bar"));

    // test null values
    Map<String, String> paramsWithNulls = Collections.singletonMap("foo", null);
    filter = QueryUtils.newFilter(paramsWithNulls);
    assertEquals(filter.getCriteria().size(), 0);
  }

  @Test
  public void testCreateRelationshipFilter() {
    String field = "field";
    String value = "value";
    RelationshipDirection direction = RelationshipDirection.OUTGOING;

    RelationshipFilter relationshipFilter = new RelationshipFilter().setCriteria(new CriterionArray(
                    Collections.singletonList(new Criterion().setField(field).setValue(value).setCondition(Condition.EQUAL))))
            .setDirection(direction);

    assertEquals(newRelationshipFilter(field, value, direction), relationshipFilter);
  }

  @Test
  public void testLatestAspectVersions() {
    Set<Class<? extends RecordTemplate>> aspects = ImmutableSet.of(AspectFoo.class, AspectBar.class);

    Set<AspectVersion> aspectVersions = QueryUtils.latestAspectVersions(aspects);

    assertEquals(aspectVersions.size(), 2);

    assertTrue(hasAspectVersion(aspectVersions, AspectFoo.class.getCanonicalName(), 0));
    assertTrue(hasAspectVersion(aspectVersions, AspectBar.class.getCanonicalName(), 0));
  }

  private boolean hasAspectVersion(Set<AspectVersion> aspectVersions, String aspectName, long version) {
    return aspectVersions.stream()
        .filter(av -> av.getAspect().equals(aspectName) && av.getVersion().equals(version))
        .count() == 1;
  }

  @Test
  public void testGetTotalPageCount() {

    int totalPageCount = QueryUtils.getTotalPageCount(23, 10);
    assertEquals(totalPageCount, 3);

    totalPageCount = QueryUtils.getTotalPageCount(20, 10);
    assertEquals(totalPageCount, 2);

    totalPageCount = QueryUtils.getTotalPageCount(19, 10);
    assertEquals(totalPageCount, 2);

    totalPageCount = QueryUtils.getTotalPageCount(9, 0);
    assertEquals(totalPageCount, 0);
  }

  @Test
  public void testHasMore() {

    int totalCount = 23;
    int totalPageCount = QueryUtils.getTotalPageCount(totalCount, 10); // 3

    boolean havingMore = QueryUtils.hasMore(0, 10, totalPageCount);
    assertTrue(havingMore);

    havingMore = QueryUtils.hasMore(10, 10, totalPageCount);
    assertTrue(havingMore);

    havingMore = QueryUtils.hasMore(20, 10, totalPageCount);
    assertFalse(havingMore);

    havingMore = QueryUtils.hasMore(30, 10, totalPageCount);
    assertFalse(havingMore);

    havingMore = QueryUtils.hasMore(30, 0, totalPageCount);
    assertFalse(havingMore);
  }

  private AspectVersion makeAspectVersion(String aspectName, long version) {
    return new AspectVersion().setAspect(aspectName).setVersion(version);
  }
}
