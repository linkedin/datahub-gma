package com.linkedin.metadata.restli.lix;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * Experimental controls on resources APIs.
 */
public interface ResourceLix {

  /**
   * Experiment on the GET.
   * @param urn urnString of the entity
   * @param type type of the entity
   * @return enabling/not
   */
  boolean testGet(@Nonnull String urn, @Nonnull String type);

  /**
   * Experiment on the BatchGet.
   * @param urn urnString of the entity
   * @param type type of the entity
   * @return enabling/not
   */
  boolean testBatchGet(@Nullable String urn, @Nullable String type);

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
   * @param type type of the entity
   * @param aspectName aspect FQCN of the urn
   * @return enabling/not
   */
  boolean testIngest(@Nonnull String urn, @Nonnull String type, @Nullable String aspectName);

  /**
   * Experiment on the IngestWithTracking.
   * @param urn urnString of the entity
   * @param type type of the entity
   * @param aspectName aspect FQCN of the urn
   * @return enabling/not enabling/not
   */
  boolean testIngestWithTracking(@Nonnull String urn, @Nonnull String type, @Nullable String aspectName);

  /**
   * Experiment on the GetSnapshot.
   * @param urn urnString of the entity
   * @param type type of the entity
   * @return enabling/not
   */
  boolean testGetSnapshot(@Nullable String urn, @Nullable String type);

  /**
   * Experiment on the BackfillLegacy.
   * @param urn urnString of the entity
   * @param type type of the entity
   * @return enabling/not
   */
  boolean testBackfillLegacy(@Nullable String urn, @Nullable String type);

  /**
   * Experiment on the BackfillWithUrns.
   * @param urn urnString of the entity
   * @param type type of the entity
   * @return enabling/not
   */
  boolean testBackfillWithUrns(@Nullable String urn, @Nullable String type);

  /**
   * Experiment on the EmitNoChangeMetadataAuditEvent.
   * @param urn urnString of the entity
   * @param type type of the entity
   * @return enabling/not
   */
  boolean testEmitNoChangeMetadataAuditEvent(String urn, String type);

  /**
   * Experiment on the BackfillWithNewValue.
   * @param urn urnString of the entity
   * @param type type of the entity
   * @return enabling/not
   */
  boolean testBackfillWithNewValue(@Nullable String urn, @Nullable String type);

  /**
   * Experiment on the BackfillEntityTables.
   * @param urn urnString of the entity
   * @param type type of the entity
   * @return enabling/not
   */
  boolean testBackfillEntityTables(@Nullable String urn, @Nullable String type);

  /**
   * Experiment on the BackfillRelationshipTables.
   * @param urn urnString of the entity
   * @param type type of the entity
   * @return enabling/not
   */
  boolean testBackfillRelationshipTables(@Nullable String urn, @Nullable String type);

  /**
   * Experiment on the Backfill.
   * @param assetName the FQCN of the asset
   * @param mode backfill mode
   * @return enabling/not
   */
  boolean testBackfill(@Nonnull String assetName, @Nonnull String mode);

  /**
   * Experiment on the Filter.
   * @param assetName the FQCN of the asset
   * @return enabling/not
   */
  boolean testFilter(@Nonnull String assetName);

  /**
   * Experiment on the GetAll.
   * @param urnClassName the FQCN of the urn
   * @return enabling/not
   */
  boolean testGetAll(@Nullable String urnClassName);

  /**
   * Experiment on the Search.
   * @param urnClassName the FQCN of the urn
   * @return enabling/not
   */
  boolean testSearch(@Nullable String urnClassName);

  /**
   * Experiment on the SearchV2.
   * @param urnClassName the FQCN of the urn
   * @return enabling/not
   */
  boolean testSearchV2(@Nullable String urnClassName);
}