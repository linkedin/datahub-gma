package com.linkedin.metadata.restli.lix;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;


public class DummyResourceLix implements ResourceLix {

  @Override
  public boolean testGet(@Nonnull String urn, @Nonnull String type) {
    return false;
  }

  @Override
  public boolean testBatchGet(@Nullable String urn, @Nullable String type) {
    return false;
  }

  @Override
  public boolean testBatchGetWithErrors(@Nullable String urn, @Nullable String type) {
    return false;
  }

  @Override
  public boolean testIngest(@Nonnull String urn, @Nonnull String type, @Nullable String aspectName) {
    return false;
  }

  @Override
  public boolean testIngestWithTracking(@Nonnull String urn, @Nonnull String type, @Nullable String aspectName) {
    return false;
  }

  @Override
  public boolean testGetSnapshot(@Nullable String urn, @Nullable String type) {
    return false;
  }

  @Override
  public boolean testBackfillLegacy(@Nullable String urn, @Nullable String type) {
    return false;
  }

  @Override
  public boolean testBackfillWithUrns(@Nullable String urn, @Nullable String type) {
    return false;
  }

  @Override
  public boolean testEmitNoChangeMetadataAuditEvent(String urn, String type) {
    return false;
  }

  @Override
  public boolean testBackfillWithNewValue(@Nullable String urn, @Nullable String type) {
    return false;
  }

  @Override
  public boolean testBackfillEntityTables(@Nullable String urn, @Nullable String type) {
    return false;
  }

  @Override
  public boolean testBackfillRelationshipTables(@Nullable String urn, @Nullable String type) {
    return false;
  }

  @Override
  public boolean testBackfill(@Nonnull String assetName, @Nonnull String mode) {
    return false;
  }

  @Override
  public boolean testFilter(@Nonnull String assetName) {
    return false;
  }

  @Override
  public boolean testGetAll(@Nullable String urnClassName) {
    return false;
  }

  @Override
  public boolean testSearch(@Nullable String urnClassName) {
    return false;
  }

  @Override
  public boolean testSearchV2(@Nullable String urnClassName) {
    return false;
  }
}