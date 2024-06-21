package com.linkedin.metadata.restli;

public final class RestliConstants {
  private RestliConstants() { }

  public static final String FINDER_SEARCH = "search";
  public static final String FINDER_SEARCH_V2 = "searchV2";
  public static final String FINDER_FILTER = "filter";
  public static final String FINDER_COUNT_AGGREGATE = "countAggregate";

  public static final String ACTION_AUTOCOMPLETE = "autocomplete";
  public static final String ACTION_BACKFILL = "backfill";
  public static final String ACTION_BACKFILL_MAE = "backfillMAE";
  public static final String ACTION_BACKFILL_ENTITY_TABLES = "backfillEntityTables";
  public static final String ACTION_BACKFILL_RELATIONSHIP_TABLES = "backfillRelationshipTables";
  public static final String ACTION_BACKFILL_WITH_URNS = "backfillWithUrns";
  public static final String ACTION_BACKFILL_WITH_URN = "backfillWithUrn";
  public static final String ACTION_BACKFILL_WITH_NEW_VALUE = "backfillWithNewValue";
  public static final String ACTION_BACKFILL_LEGACY = "backfillLegacy";
  public static final String ACTION_BROWSE = "browse";
  public static final String ACTION_COUNT_AGGREGATE = "countAggregate";
  public static final String ACTION_EMIT_NO_CHANGE_METADATA_AUDIT_EVENT = "emitNoChangeMetadataAuditEvent";
  public static final String ACTION_GET_BROWSE_PATHS = "getBrowsePaths";
  public static final String ACTION_GET_SNAPSHOT = "getSnapshot";
  public static final String ACTION_INGEST = "ingest";
  public static final String ACTION_QUERY = "query";
  public static final String ACTION_INGEST_WITH_TRACKING = "ingestWithTracking";
  public static final String ACTION_LIST_URNS_FROM_INDEX = "listUrnsFromIndex";
  public static final String ACTION_LIST_URNS = "listUrns";
  public static final String PARAM_INPUT = "input";
  public static final String PARAM_ASPECTS = "aspects";
  public static final String PARAM_ASPECT = "aspect";
  public static final String PARAM_ASPECT_CLASS = "aspectClass";
  public static final String PARAM_FILTER = "filter";
  public static final String PARAM_GROUP = "group";
  public static final String PARAM_SORT = "sort";
  public static final String PARAM_QUERY = "query";
  public static final String PARAM_FIELD = "field";
  public static final String PARAM_PATH = "path";
  public static final String PARAM_START = "start";
  public static final String PARAM_COUNT = "count";
  public static final String PARAM_LIMIT = "limit";
  public static final String PARAM_SNAPSHOT = "snapshot";
  public static final String PARAM_URN = "urn";
  public static final String PARAM_URNS = "urns";
  public static final String PARAM_MODE = "mode";
  public static final String PARAM_INGESTION_MODE = "ingestionMode";
  public static final String PARAM_INGESTION_PARAMS = "ingestionParams";
  public static final String PARAM_TRACKING_CONTEXT = "trackingContext";
  public static final String PARAM_METADATA = "metadata";
  public static final String PARAM_ENTITY_TYPE = "entityType";
  public static final String PARAM_PREFERENCE = "preference";
}
