package com.linkedin.metadata.dao;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.urnpath.UrnPathExtractor;
import com.linkedin.metadata.events.IngestionTrackingContext;
import com.linkedin.metadata.query.IndexFilter;
import com.linkedin.metadata.query.IndexGroupByCriterion;
import com.linkedin.metadata.query.IndexSortCriterion;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * EBeanLocalAccess provides model-agnostic data access (read / write) to MySQL database.
 */
public interface IEbeanLocalAccess<URN extends Urn> {

  void setUrnPathExtractor(@Nonnull UrnPathExtractor<URN> urnPathExtractor);

  /**
   * Upsert aspect into entity table.
   *
   * @param <ASPECT>                 metadata aspect value
   * @param urn                      entity urn
   * @param newValue                 aspect value in {@link RecordTemplate}
   * @param aspectClass              class of the aspect
   * @param auditStamp               audit timestamp
   * @param ingestionTrackingContext the ingestionTrackingContext of the MCE responsible for this update
   * @param isTestMode               whether the test mode is enabled or not
   * @return number of rows inserted or updated
   */
  <ASPECT extends RecordTemplate> int add(@Nonnull URN urn, @Nullable ASPECT newValue, @Nonnull Class<ASPECT> aspectClass,
      @Nonnull AuditStamp auditStamp, @Nullable IngestionTrackingContext ingestionTrackingContext, boolean isTestMode);

  /**
   * Update aspect on entity table with optimistic locking. (compare-and-update on oldTimestamp).
   *
   * @param <ASPECT>                 metadata aspect value
   * @param urn                      entity urn
   * @param newValue                 aspect value in {@link RecordTemplate}
   * @param aspectClass              class of the aspect
   * @param auditStamp               audit timestamp
   * @param oldTimestamp             old time stamp for optimistic lock checking
   * @param ingestionTrackingContext the ingestionTrackingContext of the MCE responsible for calling this update
   * @param isTestMode               whether the test mode is enabled or not
   * @return number of rows inserted or updated
   */
  <ASPECT extends RecordTemplate> int addWithOptimisticLocking(@Nonnull URN urn, @Nullable ASPECT newValue,
      @Nonnull Class<ASPECT> aspectClass, @Nonnull AuditStamp auditStamp, @Nullable Timestamp oldTimestamp,
      @Nullable IngestionTrackingContext ingestionTrackingContext, boolean isTestMode);

  /**
   * Create aspect from entity table.
   *
   * @param <ASPECT_UNION>           metadata aspect value
   * @param urn                      entity urn
   * @param aspectValues             aspect value in {@link RecordTemplate}
   * @param aspectCreateLambdas      class of the aspect
   * @param auditStamp               audit timestamp
   * @param ingestionTrackingContext the ingestionTrackingContext of the MCE responsible for this update
   * @param isTestMode               whether the test mode is enabled or not
   * @return number of rows inserted or updated
   */
  <ASPECT_UNION extends RecordTemplate> int create(@Nonnull URN urn,
      @Nonnull List<? extends RecordTemplate> aspectValues,
      @Nonnull List<BaseLocalDAO.AspectCreateLambda<? extends RecordTemplate>> aspectCreateLambdas,
      @Nonnull AuditStamp auditStamp,
      @Nullable IngestionTrackingContext ingestionTrackingContext, boolean isTestMode);

  /**
   * Get read aspects from entity table. This a new schema implementation for batchGetUnion() in {@link EbeanLocalDAO}
   * @param keys {@link AspectKey} to retrieve aspect metadata
   * @param keysCount pagination key count limit
   * @param position starting position of pagination
   * @param includeSoftDeleted include soft deleted aspects, default false
   * @param isTestMode whether the operation is in test mode or not
   * @param <ASPECT> metadata aspect value
   * @return a list of {@link EbeanMetadataAspect} as get response
   */
  @Nonnull
  <ASPECT extends RecordTemplate> List<EbeanMetadataAspect> batchGetUnion(@Nonnull List<AspectKey<URN, ? extends RecordTemplate>> keys,
      int keysCount, int position, boolean includeSoftDeleted, boolean isTestMode);

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
  List<URN> listUrns(@Nullable IndexFilter indexFilter, @Nullable IndexSortCriterion indexSortCriterion,
      @Nullable URN lastUrn, int pageSize);

  /**
   * Similar to {@link #listUrns(IndexFilter, IndexSortCriterion, Urn, int)} but returns a list result with pagination
   * information.
   *
   * @param start the starting offset of the page
   * @return a {@link ListResult} containing a list of urns and other pagination information
   */
  ListResult<URN> listUrns(@Nullable IndexFilter indexFilter, @Nullable IndexSortCriterion indexSortCriterion,
      int start, int pageSize);

  /**
   * Returns a boolean representing if an Urn has any Aspects associated with it (i.e. if it exists in the DB).
   * @param urn {@link Urn} for the entity
   * @return boolean representing if entity associated with Urn exists
   */
  boolean exists(@Nonnull URN urn);

  /**
   * Gets the count of an aggregation specified by the aspect and field to group on.
   * @param indexFilter {@link IndexFilter} that defines the filter conditions
   * @param indexGroupByCriterion {@link IndexGroupByCriterion} that defines the aspect to group by
   * @return map of the field to the count
   */
  @Nonnull
  Map<String, Long> countAggregate(@Nullable IndexFilter indexFilter,
      @Nonnull IndexGroupByCriterion indexGroupByCriterion);

  /**
   * Paginates over all URNs for entities that have a specific aspect. This does not include the urn(s) for which the
   * aspect is soft deleted in the latest version.
   *
   * @param aspectClass the type of the aspect to query
   * @param start the starting offset of the page
   * @param pageSize the size of the page
   * @param <ASPECT> must be a supported aspect type in {@code ASPECT_UNION}.
   * @return a {@link ListResult} containing a list of URN and other pagination information
   */
  @Nonnull
  <ASPECT extends RecordTemplate> ListResult<URN> listUrns(@Nonnull Class<ASPECT> aspectClass, int start, int pageSize);


  /**
   * Paginates over all versions of an aspect for a specific Urn. It does not return metadata corresponding to versions
   * indicating soft deleted aspect(s).
   *
   * @param aspectClass the type of the aspect to query
   * @param urn {@link Urn} for the entity
   * @param start the starting offset of the page
   * @param pageSize the size of the page
   * @param <ASPECT> must be a supported aspect type in {@code ASPECT_UNION}.
   * @return a {@link ListResult} containing a list of aspects and other pagination information
   */
  @Nonnull
  <ASPECT extends RecordTemplate> ListResult<ASPECT> list(@Nonnull Class<ASPECT> aspectClass,
      @Nonnull URN urn, int start, int pageSize);

  /**
   * Paginates over a specific version of a specific aspect for all Urns. The result does not include soft deleted
   * aspect if the specific version of a specific aspect was soft deleted.
   *
   * @param aspectClass the type of the aspect to query
   * @param start the starting offset of the page
   * @param pageSize the size of the page
   * @param <ASPECT> must be a supported aspect type in {@code ASPECT_UNION}.
   * @return a {@link ListResult} containing a list of aspects and other pagination information
   */
  @Nonnull
  <ASPECT extends RecordTemplate> ListResult<ASPECT> list(@Nonnull Class<ASPECT> aspectClass,
     int start, int pageSize);

  /**
   * Ensure table schemas are up-to-date according to db evolution scripts.
   */
  void ensureSchemaUpToDate();
}
