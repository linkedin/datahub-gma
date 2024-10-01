package com.linkedin.metadata.restli.lix;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * Ramped Resource will always choose using the new MG Kernel logic, equivalent to Lix is always 'treatment'.
 * Usage: assign to 'resourceLix' fields at each GMS entity resource, after all validation is complete.
 */
public class RampedResourceImpl implements ResourceLix {
  @Override
  public boolean testGet(@Nonnull String urn, @Nonnull String entityType) {
    return true;
  }

  @Override
  public boolean testBatchGet(@Nullable String urn, @Nullable String entityType) {
    return true;
  }

  @Override
  public boolean testBatchGetWithErrors(@Nullable String urn, @Nullable String type) {
    return true;
  }

  @Override
  public boolean testGetSnapshot(@Nullable String urn, @Nullable String entityType) {
    return true;
  }

  @Override
  public boolean testBackfillLegacy(@Nullable String urn, @Nullable String entityType) {
    return true;
  }

  @Override
  public boolean testBackfillWithUrns(@Nullable String urn, @Nullable String entityType) {
    return true;
  }

  @Override
  public boolean testEmitNoChangeMetadataAuditEvent(@Nullable String urn, @Nullable String entityType) {
    return true;
  }

  @Override
  public boolean testBackfillWithNewValue(@Nullable String urn, @Nullable String entityType) {
    return true;
  }

  @Override
  public boolean testBackfillEntityTables(@Nullable String urn, @Nullable String entityType) {
    return true;
  }

  @Override
  public boolean testBackfillRelationshipTables(@Nullable String urn, @Nullable String entityType) {
    return true;
  }

  @Override
  public boolean testBackfill(@Nonnull String assetType, @Nonnull String mode) {
    return true;
  }

  @Override
  public boolean testFilter(@Nonnull String assetType) {
    return true;
  }

  @Override
  public boolean testGetAll(@Nullable String urnType) {
    return true;
  }

  @Override
  public boolean testSearch(@Nullable String urnType) {
    return true;
  }

  @Override
  public boolean testSearchV2(@Nullable String urnType) {
    return true;
  }
}