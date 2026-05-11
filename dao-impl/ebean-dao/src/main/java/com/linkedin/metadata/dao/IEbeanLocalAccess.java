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
   * Configures a conditional MySQL FORCE INDEX hint for the offset-pagination listUrns filter
   * query. The force index is only emitted when the {@link IndexFilter}'s path-bearing criteria
   * (those with {@code pathParams}) exactly match the configured (aspect, path) pairs -- same
   * count, all match. Criteria without {@code pathParams} (aspect-existence checks) are excluded.
   * This is a surgical fix for one known-bad query shape: if the filter shape changes, the hint
   * deactivates and MySQL picks its own plan. Pass {@code null} for both to disable.
   *
   * @param indexName MySQL index name, or null to disable
   * @param requiredCriteria map of aspect or URN class to path (e.g. {@code "/status"}, {@code "/model_urn"})
   *                         that must exactly match the filter's path-bearing criteria for the
   *                         force index to activate, or null to disable. Accepts both
   *                         {@code RecordTemplate} subclasses (aspects) and {@code Urn} subclasses
   *                         (URN-derived index columns). Leading '/' in paths is optional --
   *                         comparison is normalized.
   */
  void configureOptionalForceIndex(@Nullable String indexName,
      @Nullable Map<Class<?>, String> requiredCriteria);

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
   * @param softDeleteOverwrite      whether to overwrite soft deleted aspects marked with $gma_deleted.
   *                                 Will always be false for old schema code path since those tables do not contain
   *                                 deleted_ts column.
   * @return number of rows inserted or updated
   */
  <ASPECT extends RecordTemplate> int addWithOptimisticLocking(@Nonnull URN urn, @Nullable ASPECT newValue,
      @Nonnull Class<ASPECT> aspectClass, @Nonnull AuditStamp auditStamp, @Nullable Timestamp oldTimestamp,
      @Nullable IngestionTrackingContext ingestionTrackingContext, boolean isTestMode, boolean softDeleteOverwrite);

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
   * Batch upsert multiple aspects for a single URN using multi-column UPDATE.
   * This method generates a single SQL statement that updates all aspect columns at once.
   *
   * @param urn                      entity URN
   * @param updateContexts           list of aspect update contexts containing values and lambdas
   * @param auditStamp               audit stamp for tracking
   * @param ingestionTrackingContext tracking context for ingestion
   * @param isTestMode               whether the test mode is enabled or not
   * @return number of rows inserted or updated
   */
  <ASPECT_UNION extends RecordTemplate> int batchUpsert(@Nonnull URN urn,
      @Nonnull List<BaseLocalDAO.AspectUpdateContext<RecordTemplate>> updateContexts,
      @Nonnull AuditStamp auditStamp,
      @Nullable IngestionTrackingContext ingestionTrackingContext, boolean isTestMode);

  /**
   * Fetch aspects from the entity table using a single multi-column SELECT per URN chunk.
   * This is the new-schema implementation for batchGetUnion() in {@link EbeanLocalDAO}.
   *
   * <p>Aspect-level soft-deletes (gma_deleted) are always returned as marker rows — callers must
   * filter them (e.g., via {@code EbeanLocalDAO.toRecordTemplate} which checks {@code isSoftDeletedAspect}).
   * The {@code includeSoftDeleted} flag controls only asset-level deletion (deleted_ts column).
   *
   * <p>URNs are chunked internally (max {@link EbeanLocalAccess#MAX_URNS_PER_QUERY} per SQL IN clause).
   *
   * @param keys {@link AspectKey} to retrieve aspect metadata
   * @param keysCount controls how many keys from the list are processed (caller passes keys.size() to process all)
   * @param position starting index into the keys list (caller passes 0 to start from the beginning)
   * @param includeSoftDeleted whether to include asset-level soft deleted entities (deleted_ts)
   * @param isTestMode whether the operation is in test mode or not
   * @param <ASPECT> metadata aspect value
   * @return a list of {@link EbeanMetadataAspect} as get response
   */
  @Nonnull
  <ASPECT extends RecordTemplate> List<EbeanMetadataAspect> batchGetUnion(@Nonnull List<AspectKey<URN, ? extends RecordTemplate>> keys,
      int keysCount, int position, boolean includeSoftDeleted, boolean isTestMode);

  /**
   * Soft delete all aspects + urn for the given urn by setting deleted_ts=NOW().
   *
   * @param urn        {@link Urn} for the entity
   * @param isTestMode whether the operation is in test mode or not
   * @return number of rows deleted
   */
  int softDeleteAsset(@Nonnull URN urn, boolean isTestMode);

  /**
   * Read deletion-relevant fields for a batch of URNs in a single SELECT.
   * Returns deletion-relevant fields for validation and all aspect columns for Kafka archival.
   * URNs not found in the database will not have entries in the returned map.
   *
   * @param urns list of URNs to check
   * @param isTestMode whether to use test schema
   * @return map of URN to {@link EntityDeletionInfo}
   */
  Map<URN, EntityDeletionInfo> readDeletionInfoBatch(@Nonnull List<URN> urns, boolean isTestMode);

  /**
   * Batch soft-delete entities by setting deleted_ts = NOW() for URNs that meet all deletion criteria.
   * The UPDATE includes guard clauses (deleted_ts IS NULL, Status.removed = true, lastmodifiedon &lt; cutoff)
   * as defense-in-depth against race conditions. The Status aspect column name is resolved internally via
   * {@link com.linkedin.metadata.dao.utils.SQLSchemaUtils#getAspectColumnName}.
   *
   * @param urns list of URNs to soft-delete
   * @param cutoffTimestamp only delete if Status.lastmodifiedon is before this timestamp
   * @param isTestMode whether to use test schema
   * @return number of rows actually soft-deleted
   */
  int batchSoftDeleteAssets(@Nonnull List<URN> urns, @Nonnull String cutoffTimestamp, boolean isTestMode);

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
