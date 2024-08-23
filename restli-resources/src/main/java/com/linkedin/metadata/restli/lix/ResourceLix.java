package com.linkedin.metadata.restli.lix;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * Experimental controls on resources APIs.
 * LI internal: go/gma/experimentalLiX
 */
public interface ResourceLix {

  /**
   * Experiment on the GET.
   * @param urn urnString of the entity
   * @param entityType type of the entity
   * @return enabling/not
   */
  boolean testGet(@Nonnull String urn, @Nonnull String entityType);

  /**
   * Experiment on the BatchGet.
   * @param urn urnString of the entity
   * @param entityType type of the entity
   * @return enabling/not
   */
  boolean testBatchGet(@Nullable String urn, @Nullable String entityType);

  /**
   * Experiment on the BatchGetWithErrors.
   * @param urn urnString of the entity
   * @param type type of the entity
   * @return enabling/not
   */
  boolean testBatchGetWithErrors(@Nullable String urn, @Nullable String type);

  /**
   * Experiment on the Ingest.
   * @param urn urnString of the entity
   * @param entityType type of the entity
   * @param aspectName aspect FQCN of the urn
   * @return enabling/not
   */
  boolean testIngest(@Nonnull String urn, @Nonnull String entityType, @Nullable String aspectName);

  /**
   * Experiment on the IngestWithTracking.
   * @param urn urnString of the entity
   * @param entityType type of the entity
   * @param aspectName aspect FQCN of the urn
   * @return enabling/not enabling/not
   */
  boolean testIngestWithTracking(@Nonnull String urn, @Nonnull String entityType, @Nullable String aspectName);

  /**
   * Experiment on the IngestAsset.
   * @param urn urnString of the entity
   * @param entityType type of the entity
   * @param aspectName aspect FQCN of the urn
   * @return enabling/not
   */
  boolean testIngestAsset(@Nonnull String urn, @Nonnull String entityType, @Nullable String aspectName);

  /**
   * Experiment on the GetSnapshot.
   * @param urn urnString of the entity
   * @param entityType type of the entity
   * @return enabling/not
   */
  boolean testGetSnapshot(@Nullable String urn, @Nullable String entityType);

  /**
   * Experiment on the BackfillLegacy.
   * @param urn urnString of the entity
   * @param entityType type of the entity
   * @return enabling/not
   */
  boolean testBackfillLegacy(@Nullable String urn, @Nullable String entityType);

  /**
   * Experiment on the BackfillWithUrns.
   * @param urn urnString of the entity
   * @param entityType type of the entity
   * @return enabling/not
   */
  boolean testBackfillWithUrns(@Nullable String urn, @Nullable String entityType);

  /**
   * Experiment on the EmitNoChangeMetadataAuditEvent.
   * @param urn urnString of the entity
   * @param entityType type of the entity
   * @return enabling/not
   */
  boolean testEmitNoChangeMetadataAuditEvent(@Nullable String urn, @Nullable String entityType);

  /**
   * Experiment on the BackfillWithNewValue.
   * @param urn urnString of the entity
   * @param entityType type of the entity
   * @return enabling/not
   */
  boolean testBackfillWithNewValue(@Nullable String urn, @Nullable String entityType);

  /**
   * Experiment on the BackfillEntityTables.
   * @param urn urnString of the entity
   * @param entityType type of the entity
   * @return enabling/not
   */
  boolean testBackfillEntityTables(@Nullable String urn, @Nullable String entityType);

  /**
   * Experiment on the BackfillRelationshipTables.
   * @param urn urnString of the entity
   * @param entityType type of the entity
   * @return enabling/not
   */
  boolean testBackfillRelationshipTables(@Nullable String urn, @Nullable String entityType);

  /**
   * Experiment on the Backfill.
   * @param assetType the type of the asset
   * @param mode backfill mode
   * @return enabling/not
   */
  boolean testBackfill(@Nonnull String assetType, @Nonnull String mode);

  /**
   * Experiment on the Filter.
   * @param assetType the type of the asset
   * @return enabling/not
   */
  boolean testFilter(@Nonnull String assetType);

  /**
   * Experiment on the GetAll.
   * @param urnType the type of the urn
   * @return enabling/not
   */
  boolean testGetAll(@Nullable String urnType);

  /**
   * Experiment on the Search.
   * @param urnType the type of the urn
   * @return enabling/not
   */
  boolean testSearch(@Nullable String urnType);

  /**
   * Experiment on the SearchV2.
   * @param urnType the type of the urn
   * @return enabling/not
   */
  boolean testSearchV2(@Nullable String urnType);
}