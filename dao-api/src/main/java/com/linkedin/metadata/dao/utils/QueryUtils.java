package com.linkedin.metadata.dao.utils;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.BaseReadDAO;
import com.linkedin.metadata.aspect.AspectVersion;
import com.linkedin.metadata.query.Condition;
import com.linkedin.metadata.query.Criterion;
import com.linkedin.metadata.query.CriterionArray;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.RelationshipDirection;
import com.linkedin.metadata.query.RelationshipFilter;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


public class QueryUtils {

  public static final Filter EMPTY_FILTER = new Filter().setCriteria(new CriterionArray());

  private QueryUtils() {
  }

  // Creates new Criterion with field and value, using EQUAL condition.
  @Nonnull
  public static Criterion newCriterion(@Nonnull String field, @Nonnull String value) {
    return newCriterion(field, value, Condition.EQUAL);
  }

  // Creates new Criterion with field, value and condition.
  @Nonnull
  public static Criterion newCriterion(@Nonnull String field, @Nonnull String value, @Nonnull Condition condition) {
    return new Criterion().setField(field).setValue(value).setCondition(condition);
  }

  // Creates new Filter from a map of Criteria by removing null-valued Criteria and using EQUAL condition (default).
  @Nonnull
  public static Filter newFilter(@Nullable Map<String, String> params) {
    if (params == null) {
      return EMPTY_FILTER;
    }

    CriterionArray criteria = params.entrySet()
        .stream()
        .filter(e -> Objects.nonNull(e.getValue()))
        .map(e -> newCriterion(e.getKey(), e.getValue()))
        .collect(Collectors.toCollection(CriterionArray::new));
    return new Filter().setCriteria(criteria);
  }

  // Creates new Filter from a single Criterion with EQUAL condition (default).
  @Nonnull
  public static Filter newFilter(@Nonnull String field, @Nonnull String value) {
    return newFilter(Collections.singletonMap(field, value));
  }


  /**
   * Create {@link RelationshipFilter} using filter and relationship direction.
   *
   * @param filter {@link Filter} filter
   * @param relationshipDirection {@link RelationshipDirection} relationship direction
   * @return RelationshipFilter
   */
  @Nonnull
  public static RelationshipFilter newRelationshipFilter(@Nonnull Filter filter,
                                                         @Nonnull RelationshipDirection relationshipDirection) {
    return new RelationshipFilter().setCriteria(filter.getCriteria()).setDirection(relationshipDirection);
  }

  /**
   * Create {@link RelationshipFilter} using filter conditions and relationship direction.
   *
   * @param field field to create a filter on
   * @param value field value to be filtered
   * @param relationshipDirection {@link RelationshipDirection} relationship direction
   * @return RelationshipFilter
   */
  @Nonnull
  public static RelationshipFilter newRelationshipFilter(@Nonnull String field, @Nonnull String value,
                                                         @Nonnull RelationshipDirection relationshipDirection) {
    return newRelationshipFilter(newFilter(field, value), relationshipDirection);
  }

  /**
   * Converts a set of aspect classes to a set of {@link AspectVersion} with the version all set to latest.
   */
  @Nonnull
  public static Set<AspectVersion> latestAspectVersions(@Nonnull Set<Class<? extends RecordTemplate>> aspectClasses) {
    return aspectClasses.stream()
        .map(aspectClass -> new AspectVersion().setAspect(ModelUtils.getAspectName(aspectClass))
            .setVersion(BaseReadDAO.LATEST_VERSION))
        .collect(Collectors.toSet());
  }

  /**
   * Calculates the total page count.
   *
   * @param totalCount total count
   * @param size page size
   * @return total page count
   */
  public static int getTotalPageCount(int totalCount, int size) {
    if (size <= 0) {
      return 0;
    }
    return (int) Math.ceil((double) totalCount / size);
  }

  /**
   * Calculates whether there is more results.
   *
   * @param from offset from the first result you want to fetch
   * @param size page size
   * @param totalPageCount total page count
   * @return whether there's more results
   */
  public static boolean hasMore(int from, int size, int totalPageCount) {
    if (size <= 0) {
      return false;
    }
    return (from + size) / size < totalPageCount;
  }
}
