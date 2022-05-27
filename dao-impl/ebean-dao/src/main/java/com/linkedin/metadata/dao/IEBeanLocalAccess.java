package com.linkedin.metadata.dao;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.query.IndexFilter;
import com.linkedin.metadata.query.IndexSortCriterion;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * EBeanLocalAccess provides model-agnostic data access (read / write) to MySQL database.
 */
public interface IEBeanLocalAccess<URN extends Urn> {

  /**
   * Upsert aspect into entity table
   * @param urn entity urn
   * @param newValue aspect value in {@link RecordTemplate}
   * @param auditStamp audit timestamp
   * @param <ASPECT>
   * @return
   */
  @Nonnull
  <ASPECT extends RecordTemplate> int add(@Nonnull URN urn, @Nonnull ASPECT newValue,
      @Nonnull AuditStamp auditStamp);


  /**
   * Get read aspects from entity table. This a new schema implementation for batchGetUnion() in {@link EbeanLocalDAO}
   * @param keys {@link AspectKey} to retrieve aspect metadata
   * @param keysCount pagination key count limit
   * @param position starting position of pagination
   * @param <ASPECT>
   * @return
   */
  @Nonnull
  <ASPECT extends RecordTemplate> List<EbeanMetadataAspect> batchGetUnion(@Nonnull List<AspectKey<URN, ? extends RecordTemplate>> keys,
      int keysCount, int position);

  /**
   * Get read aspects from entity table. This a new schema implementation for batchGetOr() in {@link EbeanLocalDAO}
   * @param keys {@link AspectKey} to retrieve aspect metadata
   * @param keysCount pagination key count limit
   * @param position starting position of pagination
   * @return
   */
  @Nonnull
  <ASPECT extends RecordTemplate> List<EbeanMetadataAspect> batchGetOr(@Nonnull List<AspectKey<URN, ? extends RecordTemplate>> keys,
      int keysCount, int position);


  /**
   * Returns list of urns that satisfy the given filter conditions.
   *
   * <p>Results are ordered by the order criterion but defaults to sorting lexicographically by the string
   * representation of the URN.
   *
   * @param indexFilter {@link IndexFilter} containing filter conditions to be applied
   * @param indexSortCriterion {@link IndexSortCriterion} sorting criterion to be applied
   * @param lastUrn last urn of the previous fetched page. For the first page, this should be set as NULL
   * @param pageSize maximum number of distinct urns to return
   * @return List of urns from local secondary index that satisfy the given filter conditions
   */
  List<URN> listUrns(@Nonnull IndexFilter indexFilter, @Nullable IndexSortCriterion indexSortCriterion,
      @Nullable URN lastUrn, int pageSize);


  /**
   * Similar to {@link #listUrns(IndexFilter, IndexSortCriterion, Urn, int)} but returns a list result with pagination
   * information.
   *
   * @param start the starting offset of the page
   * @return a {@link ListResult} containing a list of urns and other pagination information
   */
  ListResult<URN> listUrns(@Nonnull IndexFilter indexFilter, @Nullable IndexSortCriterion indexSortCriterion,
      int start, int pageSize);
}
