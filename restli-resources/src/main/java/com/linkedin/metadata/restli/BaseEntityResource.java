package com.linkedin.metadata.restli;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.LongMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.metadata.backfill.BackfillMode;
import com.linkedin.metadata.dao.AspectKey;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.ListResult;
import com.linkedin.metadata.dao.UrnAspectEntry;
import com.linkedin.metadata.dao.exception.ModelValidationException;
import com.linkedin.metadata.dao.tracking.BaseTrackingManager;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.events.IngestionMode;
import com.linkedin.metadata.events.IngestionTrackingContext;
import com.linkedin.metadata.internal.IngestionParams;
import com.linkedin.metadata.query.IndexFilter;
import com.linkedin.metadata.query.IndexGroupByCriterion;
import com.linkedin.metadata.query.IndexSortCriterion;
import com.linkedin.metadata.query.ListResultMetadata;
import com.linkedin.metadata.query.MapMetadata;
import com.linkedin.metadata.restli.lix.RampedResourceImpl;
import com.linkedin.metadata.restli.lix.ResourceLix;
import com.linkedin.parseq.Task;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.BatchResult;
import com.linkedin.restli.server.CollectionResult;
import com.linkedin.restli.server.PagingContext;
import com.linkedin.restli.server.RestLiServiceException;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.annotations.Finder;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.PagingContextParam;
import com.linkedin.restli.server.annotations.QueryParam;
import com.linkedin.restli.server.annotations.RestMethod;
import com.linkedin.restli.server.resources.CollectionResourceTaskTemplate;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.dao.BaseReadDAO.*;
import static com.linkedin.metadata.dao.utils.IngestionUtils.*;
import static com.linkedin.metadata.restli.RestliConstants.*;


/**
 * A base class for the entity rest.li resource, that supports CRUD methods.
 *
 * <p>See http://go/gma for more details
 *
 * @param <KEY> the resource's key type
 * @param <VALUE> the resource's value type
 * @param <URN> must be a valid {@link Urn} type for the snapshot
 * @param <SNAPSHOT> must be a valid snapshot type defined in com.linkedin.metadata.snapshot
 * @param <ASPECT_UNION> must be a valid aspect union type supported by the snapshot
 * @param <INTERNAL_SNAPSHOT> must be a valid internal snapshot type defined in com.linkedin.metadata.snapshot
 * @param <INTERNAL_ASPECT_UNION> must be a valid internal aspect union type supported by the internal snapshot
 * @param <ASSET> must be a valid asset type defined in com.linkedin.metadata.asset
 */
@Slf4j
public abstract class BaseEntityResource<
    // @formatter:off
    KEY,
    VALUE extends RecordTemplate,
    URN extends Urn,
    SNAPSHOT extends RecordTemplate,
    ASPECT_UNION extends UnionTemplate,
    INTERNAL_SNAPSHOT extends RecordTemplate,
    INTERNAL_ASPECT_UNION extends UnionTemplate,
    ASSET extends RecordTemplate>
    // @formatter:on
    extends CollectionResourceTaskTemplate<KEY, VALUE> {

  private static final BaseRestliAuditor DUMMY_AUDITOR = new DummyRestliAuditor(Clock.systemUTC());

  private final Class<SNAPSHOT> _snapshotClass;
  private final Class<ASPECT_UNION> _aspectUnionClass;
  private final Set<Class<? extends RecordTemplate>> _supportedAspectClasses;
  private final Set<Class<? extends RecordTemplate>> _supportedInternalAspectClasses;
  private final Class<INTERNAL_SNAPSHOT> _internalSnapshotClass;
  private final Class<INTERNAL_ASPECT_UNION> _internalAspectUnionClass;
  private final Class<ASSET> _assetClass;
  protected final Class<URN> _urnClass;
  protected BaseTrackingManager _trackingManager = null;
  private ResourceLix _defaultResourceLix = new RampedResourceImpl();

  /**
   * This method is to be overriden by specific resource endpoint implementation with real lix impl.
   * @return {@link ResourceLix}
   */
  protected ResourceLix getResourceLix() {
    return _defaultResourceLix;
  }


  public BaseEntityResource(@Nullable Class<SNAPSHOT> snapshotClass, @Nullable Class<ASPECT_UNION> aspectUnionClass,
      @Nonnull Class<INTERNAL_SNAPSHOT> internalSnapshotClass,
      @Nonnull Class<INTERNAL_ASPECT_UNION> internalAspectUnionClass, @Nonnull Class<ASSET> assetClass) {
    this(snapshotClass, aspectUnionClass, null, internalSnapshotClass, internalAspectUnionClass, assetClass);
  }

  public BaseEntityResource(@Nullable Class<SNAPSHOT> snapshotClass, @Nullable Class<ASPECT_UNION> aspectUnionClass,
      @Nonnull Class<INTERNAL_SNAPSHOT> internalSnapshotClass,
      @Nonnull Class<INTERNAL_ASPECT_UNION> internalAspectUnionClass, @Nonnull Class<ASSET> assetClass,
      @Nonnull ResourceLix resourceLix) {
    this(snapshotClass, aspectUnionClass, null, internalSnapshotClass, internalAspectUnionClass, assetClass);
  }

  public BaseEntityResource(@Nullable Class<SNAPSHOT> snapshotClass, @Nullable Class<ASPECT_UNION> aspectUnionClass,
      @Nullable Class<URN> urnClass, @Nonnull Class<INTERNAL_SNAPSHOT> internalSnapshotClass,
      @Nonnull Class<INTERNAL_ASPECT_UNION> internalAspectUnionClass, @Nonnull Class<ASSET> assetClass) {
    super();
    ModelUtils.validateSnapshotAspect(internalSnapshotClass, internalAspectUnionClass);
    _snapshotClass = snapshotClass;
    _aspectUnionClass = aspectUnionClass;
    _urnClass = urnClass;
    _internalSnapshotClass = internalSnapshotClass;
    _internalAspectUnionClass = internalAspectUnionClass;
    _supportedAspectClasses = ModelUtils.getValidAspectTypes(_aspectUnionClass);
    _supportedInternalAspectClasses = ModelUtils.getValidAspectTypes(_internalAspectUnionClass);
    _assetClass = assetClass;
  }

  public BaseEntityResource(@Nullable Class<SNAPSHOT> snapshotClass, @Nullable Class<ASPECT_UNION> aspectUnionClass,
      @Nullable Class<URN> urnClass, @Nullable BaseTrackingManager trackingManager,
      @Nonnull Class<INTERNAL_SNAPSHOT> internalSnapshotClass,
      @Nonnull Class<INTERNAL_ASPECT_UNION> internalAspectUnionClass, @Nonnull Class<ASSET> assetClass,
      @Nonnull ResourceLix resourceLix) {
    this(snapshotClass, aspectUnionClass, urnClass, internalSnapshotClass, internalAspectUnionClass, assetClass);
    _trackingManager = trackingManager;
  }

  public BaseEntityResource(@Nullable Class<SNAPSHOT> snapshotClass, @Nullable Class<ASPECT_UNION> aspectUnionClass,
      @Nullable Class<URN> urnClass, @Nullable BaseTrackingManager trackingManager,
      @Nonnull Class<INTERNAL_SNAPSHOT> internalSnapshotClass,
      @Nonnull Class<INTERNAL_ASPECT_UNION> internalAspectUnionClass, @Nonnull Class<ASSET> assetClass) {
    this(snapshotClass, aspectUnionClass, urnClass, internalSnapshotClass, internalAspectUnionClass, assetClass);
    _trackingManager = trackingManager;
  }

  /**
   * Returns a {@link BaseRestliAuditor} for this resource.
   */
  @Nonnull
  protected BaseRestliAuditor getAuditor() {
    return DUMMY_AUDITOR;
  }

  /**
   * Returns an aspect-specific {@link BaseLocalDAO}.
   */
  @Nonnull
  protected abstract BaseLocalDAO<INTERNAL_ASPECT_UNION, URN> getLocalDAO();

  @Nullable
  protected BaseLocalDAO<INTERNAL_ASPECT_UNION, URN> getShadowLocalDAO() {
    return null; // override in resource class only if needed
  }

  @Nullable
  protected BaseLocalDAO<INTERNAL_ASPECT_UNION, URN> getShadowReadLocalDAO() {
    return null; // override in resource class only if needed
  }

  /**
   * Creates an URN from its string representation.
   */
  @Nonnull
  protected abstract URN createUrnFromString(@Nonnull String urnString) throws Exception;

  /**
   * Converts a resource key to URN.
   */
  @Nonnull
  protected abstract URN toUrn(@Nonnull KEY key);

  /**
   * Converts a URN to a resource's key.
   */
  @Nonnull
  protected abstract KEY toKey(@Nonnull URN urn);

  /**
   * Converts a snapshot to resource's value.
   */
  @Nonnull
  protected abstract VALUE toValue(@Nonnull SNAPSHOT snapshot);

  /**
   * Converts an internal snapshot to resource's value.
   */
  @Nonnull
  protected VALUE toInternalValue(@Nonnull INTERNAL_SNAPSHOT internalSnapshot) {
    final SNAPSHOT snapshot = ModelUtils.convertSnapshots(_snapshotClass, internalSnapshot);
    return ModelUtils.decorateValue(internalSnapshot, toValue(snapshot));
  }

  /**
   * Converts a resource's value to a snapshot.
   */
  @Nonnull
  protected abstract SNAPSHOT toSnapshot(@Nonnull VALUE value, @Nonnull URN urn);

  /**
   * Retrieves the value for an entity that is made up of latest versions of specified aspects.
   */
  @RestMethod.Get
  @Nonnull
  public Task<VALUE> get(@Nonnull KEY id, @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    final URN urn = toUrn(id);
    return get(id, aspectNames, getResourceLix().testGet(String.valueOf(urn), urn.getEntityType()));
  }

  protected Task<VALUE> get(@Nonnull KEY id, @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames,
      boolean isInternalModelsEnabled) {

    return RestliUtils.toTask(() -> {
      final URN urn = toUrn(id);
      if (!getLocalDAO().exists(urn)) {
        throw RestliUtils.resourceNotFoundException();
      }
      final VALUE value =
          getInternal(Collections.singleton(urn), parseAspectsParam(aspectNames, isInternalModelsEnabled),
              isInternalModelsEnabled).get(urn);
      if (value == null) {
        throw RestliUtils.resourceNotFoundException();
      }
      return value;
    });
  }

  /**
   * Similar to {@link #get(Object, String[])} but for multiple entities. This method is deprecated in favor of
   * {@link #batchGetWithErrors}. This method has incorrect behavior when dealing with keys which don't exist
   * in the database (<a href="https://github.com/linkedin/datahub-gma/issues/136">Issue #136</a>). The latter method
   * properly returns a BatchResult which includes a map of errors in addition to the successful batch results.
   */
  @RestMethod.BatchGet
  @Deprecated
  @Nonnull
  public Task<Map<KEY, VALUE>> batchGet(@Nonnull Set<KEY> ids,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    final URN urn = ids.stream().findFirst().isPresent() ? toUrn(ids.stream().findFirst().get()) : null;
    return batchGet(ids, aspectNames, getResourceLix().testBatchGet(String.valueOf(urn), ModelUtils.getEntityType(urn)));
  }

  @Deprecated
  @Nonnull
  private Task<Map<KEY, VALUE>> batchGet(@Nonnull Set<KEY> ids,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames, boolean isInternalModelsEnabled) {
    return RestliUtils.toTask(() -> {
      final Map<URN, KEY> urnMap = ids.stream().collect(Collectors.toMap(this::toUrn, Function.identity()));
      return getInternal(urnMap.keySet(), parseAspectsParam(aspectNames, isInternalModelsEnabled),
          isInternalModelsEnabled).entrySet()
          .stream()
          .collect(Collectors.toMap(e -> urnMap.get(e.getKey()), Map.Entry::getValue));
    });
  }

  /**
   * Similar to {@link #get(Object, String[])} but for multiple entities. Compared to the deprecated {@link #batchGet(Set, String[])}
   * method, this method properly returns a BatchResult which includes a map of errors in addition to the successful
   * batch results.
   */
  @RestMethod.BatchGet
  @Nonnull
  public Task<BatchResult<KEY, VALUE>> batchGetWithErrors(@Nonnull Set<KEY> ids,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    final URN urn = ids.stream().findFirst().isPresent() ? toUrn(ids.stream().findFirst().get()) : null;
    return batchGetWithErrors(ids, aspectNames,
        getResourceLix().testBatchGetWithErrors(String.valueOf(urn), ModelUtils.getEntityType(urn)));
  }

  @Nonnull
  private Task<BatchResult<KEY, VALUE>> batchGetWithErrors(@Nonnull Set<KEY> ids,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames, boolean isInternalModelsEnabled) {
    return RestliUtils.toTask(() -> {
      final Map<KEY, RestLiServiceException> errors = new HashMap<>();
      final Map<KEY, HttpStatus> statuses = new HashMap<>();
      final Map<URN, KEY> urnMap = ids.stream().collect(Collectors.toMap(this::toUrn, Function.identity()));
      final Map<URN, VALUE> batchResult =
          getInternal(urnMap.keySet(), parseAspectsParam(aspectNames, isInternalModelsEnabled),
              isInternalModelsEnabled);
      batchResult.entrySet().removeIf(entry -> {
        if (!entry.getValue().data().isEmpty()) {
          // don't remove if there is a non-empty value associated with the key
          statuses.put(urnMap.get(entry.getKey()), HttpStatus.S_200_OK);
          return false;
        }
        // if this key's value is empty, then this key doesn't exist in the db.
        // mark this key with 404 and remove the entry from the map
        errors.put(urnMap.get(entry.getKey()), new RestLiServiceException(HttpStatus.S_404_NOT_FOUND));
        statuses.put(urnMap.get(entry.getKey()), HttpStatus.S_404_NOT_FOUND);
        return true;
      });
      return new BatchResult<>(
          batchResult.entrySet().stream().collect(Collectors.toMap(e -> urnMap.get(e.getKey()), Map.Entry::getValue)),
          statuses, errors);
    });
  }

  /**
   * Deprecated to use {@link #ingestAsset(RecordTemplate, IngestionParams)} instead.
   * An action method for automated ingestion pipeline.
   */
  @Deprecated
  @Action(name = ACTION_INGEST)
  @Nonnull
  public Task<Void> ingest(@ActionParam(PARAM_SNAPSHOT) @Nonnull SNAPSHOT snapshot) {
    return ingestInternal(snapshot, Collections.emptySet(), null, null);
  }

  /**
   * Deprecated to use {@link #ingestAsset(RecordTemplate, IngestionParams)} instead.
   * Same as {@link #ingest(RecordTemplate)} but with tracking context attached.
   * @param snapshot Snapshot of the metadata change to be ingested
   * @param trackingContext {@link IngestionTrackingContext} to 1) track DAO-level metrics and 2) to pass on to MAE emission
   * @return ingest task
   */
  @Deprecated
  @Action(name = ACTION_INGEST_WITH_TRACKING)
  @Nonnull
  public Task<Void> ingestWithTracking(@ActionParam(PARAM_SNAPSHOT) @Nonnull SNAPSHOT snapshot,
      @ActionParam(PARAM_TRACKING_CONTEXT) @Nonnull IngestionTrackingContext trackingContext,
      @Optional @ActionParam(PARAM_INGESTION_PARAMS) IngestionParams ingestionParams) {
    return ingestInternal(snapshot, Collections.emptySet(), trackingContext, ingestionParams);
  }

  /**
   * Deprecated to use {@link #rawIngestAsset(RecordTemplate, IngestionParams)} instead.
   * Same as {@link #ingestWithTracking(RecordTemplate, IngestionTrackingContext, IngestionParams)} but skips any pre-ingestion updates.
   * @param snapshot Snapshot of the metadata change to be ingested
   * @param trackingContext {@link IngestionTrackingContext} to 1) track DAO-level metrics and 2) to pass on to MAE emission
   * @return ingest task
   */
  @Deprecated
  @Action(name = ACTION_RAW_INGEST)
  @Nonnull
  public Task<Void> rawIngest(@ActionParam(PARAM_SNAPSHOT) @Nonnull SNAPSHOT snapshot,
      @ActionParam(PARAM_TRACKING_CONTEXT) @Nonnull IngestionTrackingContext trackingContext,
      @Optional @ActionParam(PARAM_INGESTION_PARAMS) IngestionParams ingestionParams) {
    return rawIngestInternal(snapshot, Collections.emptySet(), trackingContext, ingestionParams);
  }

  /**
   * An action method for automated ingestion pipeline, also called high-level write.
   * @param asset Asset of the metadata change to be ingested
   * @return ingest task
   */
  @Action(name = ACTION_INGEST_ASSET)
  @Nonnull
  public Task<Void> ingestAsset(@ActionParam(PARAM_ASSET) @Nonnull ASSET asset,
      @Optional @ActionParam(PARAM_INGESTION_PARAMS) IngestionParams ingestionParams) {
    return ingestInternalAsset(asset, Collections.emptySet(), ingestionParams);
  }

  /**
   * An action method for automated ingestion pipeline which skips any pre-ingestion updates, also called low-level write.
   * @param asset Asset of the metadata change to be ingested
   * @return ingest task
   */
  @Action(name = ACTION_RAW_INGEST_ASSET)
  @Nonnull
  public Task<Void> rawIngestAsset(@ActionParam(PARAM_ASSET) @Nonnull ASSET asset,
      @Optional @ActionParam(PARAM_INGESTION_PARAMS) IngestionParams ingestionParams) {
    return rawIngestAssetInternal(asset, Collections.emptySet(), ingestionParams);
  }

  /**
   * Internal ingest method for snapshots. First execute any pre-ingestion updates. Then, save the aspect locally.
   * @param snapshot snapshot to process
   * @param aspectsToIgnore aspects to ignore
   * @param trackingContext context for tracking ingestion health
   * @param ingestionParams optional ingestion parameters
   * @return Restli Task for metadata ingestion
   */
  @Nonnull
  protected Task<Void> ingestInternal(@Nonnull SNAPSHOT snapshot,
      @Nonnull Set<Class<? extends RecordTemplate>> aspectsToIgnore, @Nullable IngestionTrackingContext trackingContext,
      @Nullable IngestionParams ingestionParams)  {
    return RestliUtils.toTask(() -> {
      final URN urn = (URN) ModelUtils.getUrnFromSnapshot(snapshot);
      final AuditStamp auditStamp = getAuditor().requestAuditStamp(getContext().getRawRequestContext());
      ModelUtils.getAspectsFromSnapshot(snapshot).stream().forEach(aspect -> {
        if (!aspectsToIgnore.contains(aspect.getClass())) {
          // Write to primary
          getLocalDAO().add(urn, aspect, auditStamp, trackingContext, ingestionParams);
          final BaseLocalDAO<INTERNAL_ASPECT_UNION, URN> shadowDao = getShadowLocalDAO();
          // dual-write to shadow
          if (shadowDao != null) {
            shadowDao.add(urn, aspect, auditStamp, trackingContext, ingestionParams);
          }
        }
      });
      return null;
    });
  }

  /**
   * Raw internal ingest method for snapshots which skips any pre-, intra-, or post-processing. Save the aspect locally.
   * @param snapshot snapshot to process
   * @param aspectsToIgnore aspects to ignore
   * @param trackingContext context for tracking ingestion health
   * @param ingestionParams optional ingestion parameters
   * @return Restli Task for metadata ingestion
   */
  @Nonnull
  protected Task<Void> rawIngestInternal(@Nonnull SNAPSHOT snapshot,
      @Nonnull Set<Class<? extends RecordTemplate>> aspectsToIgnore, @Nullable IngestionTrackingContext trackingContext,
      @Nullable IngestionParams ingestionParams) {
    return RestliUtils.toTask(() -> {
      final URN urn = (URN) ModelUtils.getUrnFromSnapshot(snapshot);
      final AuditStamp auditStamp = getAuditor().requestAuditStamp(getContext().getRawRequestContext());
      ModelUtils.getAspectsFromSnapshot(snapshot).stream().forEach(aspect -> {
        if (!aspectsToIgnore.contains(aspect.getClass())) {
          getLocalDAO().rawAdd(urn, aspect, auditStamp, trackingContext, ingestionParams);
          final BaseLocalDAO<INTERNAL_ASPECT_UNION, URN> shadowDao = getShadowLocalDAO();
          // dual-write to shadow
          if (shadowDao != null) {
            shadowDao.add(urn, aspect, auditStamp, trackingContext, ingestionParams);
          }
        }
      });
      return null;
    });
  }

  /**
   * Internal ingest method for assets. First execute any pre-ingestion updates. Then, save the aspect locally.
   * @param asset asset to process
   * @param aspectsToIgnore aspects to ignore
   * @param ingestionParams optional ingestion parameters
   * @return Restli Task for metadata ingestion
   */
  @Nonnull
  protected Task<Void> ingestInternalAsset(@Nonnull ASSET asset,
      @Nonnull Set<Class<? extends RecordTemplate>> aspectsToIgnore,
      @Nullable IngestionParams ingestionParams) {
    return RestliUtils.toTask(() -> {
      final URN urn = (URN) ModelUtils.getUrnFromAsset(asset);
      final AuditStamp auditStamp = getAuditor().requestAuditStamp(getContext().getRawRequestContext());
      IngestionTrackingContext ingestionTrackingContext =
          ingestionParams != null ? ingestionParams.getIngestionTrackingContext() : null;
      ModelUtils.getAspectsFromAsset(asset).stream().forEach(aspect -> {
        if (!aspectsToIgnore.contains(aspect.getClass())) {
          getLocalDAO().add(urn, aspect, auditStamp, ingestionTrackingContext, ingestionParams);
          final BaseLocalDAO<INTERNAL_ASPECT_UNION, URN> shadowDao = getShadowLocalDAO();
          // dual-write to shadow
          if (shadowDao != null) {
            shadowDao.add(urn, aspect, auditStamp, ingestionTrackingContext, ingestionParams);
          }
        }
      });
      return null;
    });
  }

  /**
   * Raw internal ingest method for assets which skips any pre-, intra-, or post-processing. Save the aspect locally.
   * @param asset asset to process
   * @param aspectsToIgnore aspects to ignore
   * @param ingestionParams optional ingestion parameters
   * @return Restli Task for metadata ingestion
   */
  @Nonnull
  protected Task<Void> rawIngestAssetInternal(@Nonnull ASSET asset,
      @Nonnull Set<Class<? extends RecordTemplate>> aspectsToIgnore,
      @Nullable IngestionParams ingestionParams) {
    return RestliUtils.toTask(() -> {
      final URN urn = (URN) ModelUtils.getUrnFromAsset(asset);
      final AuditStamp auditStamp = getAuditor().requestAuditStamp(getContext().getRawRequestContext());
      IngestionTrackingContext ingestionTrackingContext =
          ingestionParams != null ? ingestionParams.getIngestionTrackingContext() : null;
      ModelUtils.getAspectsFromAsset(asset).stream().forEach(aspect -> {
        if (!aspectsToIgnore.contains(aspect.getClass())) {
          getLocalDAO().rawAdd(urn, aspect, auditStamp, ingestionTrackingContext, ingestionParams);
          final BaseLocalDAO<INTERNAL_ASPECT_UNION, URN> shadowDao = getShadowLocalDAO();
          // dual-write to shadow
          if (shadowDao != null) {
            shadowDao.add(urn, aspect, auditStamp, ingestionTrackingContext, ingestionParams);
          }
        }
      });
      return null;
    });
  }

  /**
   * Deprecated to use {@link #getAsset(String, String[])} instead.
   * An action method for getting a snapshot of aspects for an entity.
   */
  @Deprecated
  @Action(name = ACTION_GET_SNAPSHOT)
  @Nonnull
  public Task<SNAPSHOT> getSnapshot(@ActionParam(PARAM_URN) @Nonnull String urnString,
      @ActionParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    try {
      final URN urn = parseUrnParam(urnString);
      return getSnapshot(urnString, aspectNames,
          getResourceLix().testGetSnapshot(String.valueOf(urn), ModelUtils.getEntityType(urn)));
    } catch (ModelValidationException e) {
      throw RestliUtils.invalidArgumentsException(e.getMessage());
    }
  }

  @Deprecated
  @Nonnull
  protected Task<SNAPSHOT> getSnapshot(@ActionParam(PARAM_URN) @Nonnull String urnString,
      @ActionParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames, boolean isInternalModelsEnabled) {
    final URN urn = parseUrnParam(urnString);
    final Set<AspectKey<URN, ? extends RecordTemplate>> keys =
        parseAspectsParam(aspectNames, isInternalModelsEnabled).stream()
            .map(aspectClass -> new AspectKey<>(aspectClass, urn, LATEST_VERSION))
            .collect(Collectors.toSet());
    if (isInternalModelsEnabled) {
      return RestliUtils.toTask(() -> {
        final List<UnionTemplate> aspects = getLocalDAO().get(keys)
            .values()
            .stream()
            .filter(java.util.Optional::isPresent)
            .map(aspect -> ModelUtils.newAspectUnion(_internalAspectUnionClass, aspect.get()))
            .collect(Collectors.toList());

        return ModelUtils.newSnapshot(_snapshotClass, urn,
            ModelUtils.convertInternalAspectUnionToAspectUnion(_aspectUnionClass, aspects));
      });
    } else {
      return RestliUtils.toTask(() -> {
        final List<UnionTemplate> aspects = getLocalDAO().get(keys)
            .values()
            .stream()
            .filter(java.util.Optional::isPresent)
            .filter(aspect -> _supportedAspectClasses.contains(aspect.get().getClass()))
            .map(aspect -> ModelUtils.newAspectUnion(_aspectUnionClass, aspect.get()))
            .collect(Collectors.toList());

        return ModelUtils.newSnapshot(_snapshotClass, urn, aspects);
      });
    }
  }

  /**
   * An action method for getting an asset of aspects for an entity.
   */
  @Action(name = ACTION_GET_ASSET)
  @Nonnull
  public Task<ASSET> getAsset(@ActionParam(PARAM_URN) @Nonnull String urnString,
      @ActionParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    try {
      return RestliUtils.toTask(() -> {
        final URN urn = parseUrnParam(urnString);

        if (!getLocalDAO().exists(urn)) {
          throw RestliUtils.resourceNotFoundException();
        }

        final Set<AspectKey<URN, ? extends RecordTemplate>> keys = parseAspectsParam(aspectNames, true).stream()
            .map(aspectClass -> new AspectKey<>(aspectClass, urn, LATEST_VERSION))
            .collect(Collectors.toSet());

        final List<UnionTemplate> aspects = getLocalDAO().get(keys)
            .values()
            .stream()
            .filter(java.util.Optional::isPresent)
            .map(aspect -> ModelUtils.newAspectUnion(_internalAspectUnionClass, aspect.get()))
            .collect(Collectors.toList());

        return ModelUtils.newAsset(_assetClass, urn, aspects);
      });
    } catch (ModelValidationException e) {
      throw RestliUtils.invalidArgumentsException(e.getMessage());
    }
  }

  /**
   * An action method for emitting MAE backfill messages for an entity.
   *
   * @deprecated Use {@link #backfill(String[], String[])} instead
   */
  @Action(name = ACTION_BACKFILL_LEGACY)
  @Nonnull
  public Task<BackfillResult> backfill(@ActionParam(PARAM_URN) @Nonnull String urnString,
      @ActionParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    final URN urn = parseUrnParam(urnString);
    return backfill(urnString, aspectNames,
        getResourceLix().testBackfillLegacy(String.valueOf(urn), ModelUtils.getEntityType(urn)));
  }

  @Nonnull
  private Task<BackfillResult> backfill(@ActionParam(PARAM_URN) @Nonnull String urnString,
      @ActionParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames, boolean isInternalModelsEnabled) {

    return RestliUtils.toTask(() -> {
      final URN urn = parseUrnParam(urnString);
      final List<String> backfilledAspects = parseAspectsParam(aspectNames, isInternalModelsEnabled).stream()
          .map(aspectClass -> getLocalDAO().backfill(aspectClass, urn))
          .filter(optionalAspect -> optionalAspect.isPresent())
          .map(optionalAspect -> ModelUtils.getAspectName(optionalAspect.get().getClass()))
          .collect(Collectors.toList());
      return new BackfillResult().setEntities(new BackfillResultEntityArray(Collections.singleton(
          new BackfillResultEntity().setUrn(urn).setAspects(new StringArray(backfilledAspects))
      )));
    });
  }

  /**
   * An action method for emitting MAE backfill messages for a set of entities.
   */
  @Action(name = ACTION_BACKFILL_WITH_URNS)
  @Nonnull
  public Task<BackfillResult> backfill(@ActionParam(PARAM_URNS) @Nonnull String[] urns,
      @ActionParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    final String urnString = urns[0];
    final URN urn = parseUrnParam(urnString);
    return backfill(urns, aspectNames, getResourceLix().testBackfillWithUrns(urnString, ModelUtils.getEntityType(urn)));
  }

  private Task<BackfillResult> backfill(@ActionParam(PARAM_URNS) @Nonnull String[] urns,
      @ActionParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames, boolean isInternalModelsEnabled) {

    return RestliUtils.toTask(() -> {
      final Set<URN> urnSet =
          Arrays.stream(urns).map(urnString -> parseUrnParam(urnString)).collect(Collectors.toSet());
      return RestliUtils.buildBackfillResult(
          getLocalDAO().backfill(parseAspectsParam(aspectNames, isInternalModelsEnabled), urnSet));
    });
  }

  /**
   * An action method for emitting no change MAE messages (oldValue == newValue). This action will add ingestionMode
   * in the MAE payload to allow downstream consumers to decide processing strategy. Only BOOTSTRAP and BACKFILL are
   * supported ingestion mode, other mode will result in no-op.
   */
  @Deprecated
  @Action(name = ACTION_EMIT_NO_CHANGE_METADATA_AUDIT_EVENT)
  @Nonnull
  public Task<BackfillResult> emitNoChangeMetadataAuditEvent(@ActionParam(PARAM_URNS) @Nonnull String[] urns,
      @ActionParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames,
      @ActionParam(PARAM_INGESTION_MODE) @Nonnull IngestionMode ingestionMode) {
    final String urnString = urns[0];
    final URN urn = parseUrnParam(urnString);
    return emitNoChangeMetadataAuditEvent(urns, aspectNames, ingestionMode,
        getResourceLix().testEmitNoChangeMetadataAuditEvent(urnString, ModelUtils.getEntityType(urn)));
  }

  @Nonnull
  private Task<BackfillResult> emitNoChangeMetadataAuditEvent(@ActionParam(PARAM_URNS) @Nonnull String[] urns,
      @ActionParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames,
      @ActionParam(PARAM_INGESTION_MODE) @Nonnull IngestionMode ingestionMode, boolean isInternalModelsEnabled) {
    BackfillMode backfillMode = ALLOWED_INGESTION_BACKFILL_BIMAP.get(ingestionMode);
    if (backfillMode == null) {
      return RestliUtils.toTask(BackfillResult::new);
    }
    return RestliUtils.toTask(() -> {
      final Set<URN> urnSet = Arrays.stream(urns).map(urnString -> parseUrnParam(urnString)).collect(Collectors.toSet());
      return RestliUtils.buildBackfillResult(
          getLocalDAO().backfill(backfillMode, parseAspectsParam(aspectNames, isInternalModelsEnabled), urnSet));
    });
  }

  /**
   * An action method for emitting MAE backfill messages with new value (old value will be set as null). This action
   * should be deprecated once the secondary store is moving away from elastic search, or the standard backfill
   * method starts to safely backfill against live index.
   */
  @Deprecated
  @Action(name = ACTION_BACKFILL_WITH_NEW_VALUE)
  @Nonnull
  public Task<BackfillResult> backfillWithNewValue(@ActionParam(PARAM_URNS) @Nonnull String[] urns,
      @ActionParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    final String urnString = urns[0];
    final URN urn = parseUrnParam(urnString);
    return backfillWithNewValue(urns, aspectNames,
        getResourceLix().testBackfillWithNewValue(urnString, ModelUtils.getEntityType(urn)));
  }

  private Task<BackfillResult> backfillWithNewValue(@ActionParam(PARAM_URNS) @Nonnull String[] urns,
      @ActionParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames, boolean isInternalModelsEnabled) {

      return RestliUtils.toTask(() -> {
      final Set<URN> urnSet = Arrays.stream(urns).map(urnString -> parseUrnParam(urnString)).collect(Collectors.toSet());
      return RestliUtils.buildBackfillResult(
          getLocalDAO().backfillWithNewValue(parseAspectsParam(aspectNames, isInternalModelsEnabled), urnSet));
    });
  }

  /**
   * An action method for backfilling the new schema's entity tables with metadata from the old schema.
   */
  @Action(name = ACTION_BACKFILL_ENTITY_TABLES)
  @Nonnull
  public Task<BackfillResult> backfillEntityTables(@ActionParam(PARAM_URNS) @Nonnull String[] urns,
      @ActionParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    final String urnString = urns[0];
    final URN urn = parseUrnParam(urnString);
    return backfillEntityTables(urns, aspectNames,
        getResourceLix().testBackfillEntityTables(urnString, ModelUtils.getEntityType(urn)));
  }

  private Task<BackfillResult> backfillEntityTables(@ActionParam(PARAM_URNS) @Nonnull String[] urns,
      @ActionParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames, boolean isInternalModelsEnabled) {

    return RestliUtils.toTask(() -> {
      final Set<URN> urnSet = Arrays.stream(urns).map(urnString -> parseUrnParam(urnString)).collect(Collectors.toSet());
      return RestliUtils.buildBackfillResult(
          getLocalDAO().backfillEntityTables(parseAspectsParam(aspectNames, isInternalModelsEnabled), urnSet));
    });
  }

  /**
   * Backfill the relationship tables from entity table.
   */
  @Action(name = ACTION_BACKFILL_RELATIONSHIP_TABLES)
  @Nonnull
  public Task<BackfillResult> backfillRelationshipTables(@ActionParam(PARAM_URNS) @Nonnull String[] urns,
      @ActionParam(PARAM_ASPECTS) @Nonnull String[] aspectNames) {
    final String urnString = urns[0];
    final URN urn = parseUrnParam(urnString);
    return backfillRelationshipTables(urns, aspectNames,
        getResourceLix().testBackfillRelationshipTables(urnString, ModelUtils.getEntityType(urn)));
  }

  private Task<BackfillResult> backfillRelationshipTables(@ActionParam(PARAM_URNS) @Nonnull String[] urns,
      @ActionParam(PARAM_ASPECTS) @Nonnull String[] aspectNames, boolean isInternalModelsEnabled) {

    // Use the shadow DAO if it exists, otherwise use the local DAO. It's a temporary solution for EGG migration.
    BaseLocalDAO<INTERNAL_ASPECT_UNION, URN> dao = getShadowLocalDAO() != null ? getShadowLocalDAO() : getLocalDAO();

    final BackfillResult backfillResult = new BackfillResult()
        .setEntities(new BackfillResultEntityArray())
        .setRelationships(new BackfillResultRelationshipArray());

    for (String urn : urns) {
      for (Class<? extends RecordTemplate> aspect : parseAspectsParam(aspectNames, isInternalModelsEnabled)) {
        dao.backfillLocalRelationships(parseUrnParam(urn), aspect).forEach(relationshipUpdates -> {
          relationshipUpdates.getRelationships().forEach(relationship -> {
            try {
              Urn source = (Urn) relationship.getClass().getMethod("getSource").invoke(relationship);
              Urn dest = (Urn) relationship.getClass().getMethod("getDestination").invoke(relationship);
              BackfillResultRelationship backfillResultRelationship = new BackfillResultRelationship()
                  .setSource(source)
                  .setDestination(dest)
                  .setRemovalOption(relationshipUpdates.getRemovalOption().name())
                  .setRelationship(relationship.getClass().getSimpleName());

              backfillResult.getRelationships().add(backfillResultRelationship);
            } catch (ReflectiveOperationException e) {
              throw new RuntimeException(e);
            }
          });
        });
      }
    }

    return RestliUtils.toTask(() -> backfillResult);
  }

  /**
   * An action method for emitting MAE backfill messages for a set of entities using SCSI.
   */
  @Action(name = ACTION_BACKFILL)
  @Nonnull
  public Task<BackfillResult> backfill(@ActionParam(PARAM_MODE) @Nonnull BackfillMode mode,
      @ActionParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames,
      @ActionParam(PARAM_URN) @Optional @Nullable String lastUrn,
      @ActionParam(PARAM_LIMIT) int limit) {
    return backfill(mode, aspectNames, lastUrn, limit,
        getResourceLix().testBackfill(_assetClass.getSimpleName(), mode.name()));
  }

  @Nonnull
  private Task<BackfillResult> backfill(@ActionParam(PARAM_MODE) @Nonnull BackfillMode mode,
      @ActionParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames,
      @ActionParam(PARAM_URN) @Optional @Nullable String lastUrn,
      @ActionParam(PARAM_LIMIT) int limit, boolean isInternalModelsEnabled) {

    return RestliUtils.toTask(() -> RestliUtils.buildBackfillResult(
        getLocalDAO().backfill(mode, parseAspectsParam(aspectNames, isInternalModelsEnabled),
            _urnClass,
            parseUrnParam(lastUrn),
            limit)));
  }

  /**
   * An action method for getting filtered urns.
   * If no filter conditions are provided, then it returns urns of given entity type.
   *
   * @param indexFilter {@link IndexFilter} that defines the filter conditions
   * @param lastUrn last urn of the previous fetched page. For the first page, this should be set as NULL
   * @param limit maximum number of distinct urns to return
   * @return Array of urns represented as string
   *
   * @deprecated Use {@link #filter(IndexFilter, String[], String, PagingContext)} instead
   */
  @Action(name = ACTION_LIST_URNS_FROM_INDEX)
  @Nonnull
  public Task<String[]> listUrnsFromIndex(@ActionParam(PARAM_FILTER) @Optional @Nullable IndexFilter indexFilter,
      @ActionParam(PARAM_URN) @Optional @Nullable String lastUrn, @ActionParam(PARAM_LIMIT) int limit) {

    return RestliUtils.toTask(() ->
        getLocalDAO()
            .listUrns(indexFilter, parseUrnParam(lastUrn), limit)
            .stream()
            .map(Urn::toString)
            .collect(Collectors.toList())
            .toArray(new String[0]));
  }

  /**
   * Returns a list of values of multiple entities from urn aspect entries.
   *
   * @param urnAspectEntries entries used to make values
   * @return ordered list of values of multiple entities
   */
  @Nonnull
  private List<VALUE> getUrnAspectValues(List<UrnAspectEntry<URN>> urnAspectEntries, boolean isInternalModelsEnabled) {
    final Map<URN, List<UnionTemplate>> urnAspectsMap = new LinkedHashMap<>();
    for (UrnAspectEntry<URN> entry : urnAspectEntries) {
      urnAspectsMap.compute(entry.getUrn(), (k, v) -> {
        if (v == null) {
          v = new ArrayList<>();
        }
        v.addAll(entry.getAspects()
            .stream()
            .map(recordTemplate -> isInternalModelsEnabled ? ModelUtils.newAspectUnion(_internalAspectUnionClass,
                recordTemplate) : ModelUtils.newAspectUnion(_aspectUnionClass, recordTemplate))
            .collect(Collectors.toList()));
        return v;
      });
    }

    return urnAspectsMap.entrySet()
        .stream()
        .map(e -> isInternalModelsEnabled ? toInternalValue(newInternalSnapshot(e.getKey(), e.getValue()))
            : toValue(newSnapshot(e.getKey(), e.getValue())))
        .collect(Collectors.toList());
  }

  /**
   * Returns ordered list of values of multiple entities obtained after filtering urns
   * from local secondary index. The returned list is ordered by the sort criterion but defaults to sorting
   * lexicographically by the string representation of the URN.
   * The list of values is in the same order as the list of urns contained in {@link ListResultMetadata}.
   *
   * @param aspectClasses set of aspect classes that needs to be populated in the values
   * @param filter {@link IndexFilter} that defines the filter conditions
   * @param indexSortCriterion {@link IndexSortCriterion} that defines the sort conditions
   * @param lastUrn last urn of the previous fetched page. For the first page, this should be set as NULL
   * @param count defining the maximum number of values returned
   * @return ordered list of values of multiple entities
   */
  @Nonnull
  private List<VALUE> filterAspects(@Nonnull Set<Class<? extends RecordTemplate>> aspectClasses,
      @Nullable IndexFilter filter, @Nullable IndexSortCriterion indexSortCriterion, @Nullable String lastUrn,
      int count, boolean isInternalModelsEnabled) {

    final List<UrnAspectEntry<URN>> urnAspectEntries =
        getLocalDAO().getAspects(aspectClasses, filter, indexSortCriterion, parseUrnParam(lastUrn), count);

    return getUrnAspectValues(urnAspectEntries, isInternalModelsEnabled);
  }

  /**
   * Similar to {@link #filterAspects(Set, IndexFilter, IndexSortCriterion, String, int, boolean)} but
   * takes in a start offset and returns a list result with pagination information.
   *
   * @param start defining the paging start
   * @param count defining the maximum number of values returned
   * @return a {@link ListResult} containing a list of version numbers and other pagination information
   */
  @Nonnull
  private ListResult<VALUE> filterAspects(@Nonnull Set<Class<? extends RecordTemplate>> aspectClasses,
      @Nullable IndexFilter filter, @Nullable IndexSortCriterion indexSortCriterion, int start, int count,
      boolean isInternalModelsEnabled) {

    final ListResult<UrnAspectEntry<URN>> listResult =
        getLocalDAO().getAspects(aspectClasses, filter, indexSortCriterion, start, count);
    final List<UrnAspectEntry<URN>> urnAspectEntries = listResult.getValues();
    final List<VALUE> values = getUrnAspectValues(urnAspectEntries, isInternalModelsEnabled);

    return ListResult.<VALUE>builder()
        .values(values)
        .metadata(listResult.getMetadata())
        .nextStart(listResult.getNextStart())
        .havingMore(listResult.isHavingMore())
        .totalCount(listResult.getTotalCount())
        .totalPageCount(listResult.getTotalPageCount())
        .pageSize(listResult.getPageSize())
        .build();
  }

  /**
   * Returns ordered list of values of multiple entities obtained after filtering urns
   * from local secondary index. The returned list is ordered by the sort criterion but defaults to
   * being ordered lexicographically by the string representation of the URN.
   * The values returned do not contain any metadata aspect, only parts of the urn (if applicable).
   * The list of values is in the same order as the list of urns contained in {@link ListResultMetadata}.
   *
   * @param filter {@link IndexFilter} that defines the filter conditions
   * @param indexSortCriterion {@link IndexSortCriterion} that defines the sorting conditions
   * @param lastUrn last urn of the previous fetched page
   * @param count defining the maximum number of values returned
   * @return ordered list of values of multiple entities
   */
  @Nonnull
  private List<VALUE> filterUrns(@Nullable IndexFilter filter, @Nullable IndexSortCriterion indexSortCriterion,
      @Nullable String lastUrn, int count, boolean isInternalModelsEnabled) {

    final List<URN> urns = getLocalDAO().listUrns(filter, indexSortCriterion, parseUrnParam(lastUrn), count);
    return urns.stream()
        .map(urn -> isInternalModelsEnabled ? toInternalValue(newInternalSnapshot(urn)) : toValue(newSnapshot(urn)))
        .collect(Collectors.toList());
  }

  /**
   * Similar to {@link #filterUrns(IndexFilter, IndexSortCriterion, String, int, boolean)} but
   * takes in a start offset and returns a list result with pagination information.
   *
   * @param start defining the paging start
   * @param count defining the maximum number of values returned
   * @return a {@link ListResult} containing an ordered list of values of multiple entities and other pagination information
   */
  @Nonnull
  private ListResult<VALUE> filterUrns(@Nullable IndexFilter filter, @Nullable IndexSortCriterion indexSortCriterion,
      int start, int count, boolean isInternalModelsEnabled) {

    final ListResult<URN> listResult = getLocalDAO().listUrns(filter, indexSortCriterion, start, count);
    final List<URN> urns = listResult.getValues();
    final List<VALUE> urnValues = urns.stream()
        .map(urn -> isInternalModelsEnabled ? toInternalValue(newInternalSnapshot(urn)) : toValue(newSnapshot(urn)))
        .collect(Collectors.toList());

    return ListResult.<VALUE>builder()
        .values(urnValues)
        .metadata(listResult.getMetadata())
        .nextStart(listResult.getNextStart())
        .havingMore(listResult.isHavingMore())
        .totalCount(listResult.getTotalCount())
        .totalPageCount(listResult.getTotalPageCount())
        .pageSize(listResult.getPageSize())
        .build();
  }

  /**
   * Retrieves the values for multiple entities obtained after filtering urns from local secondary index. Here the value is
   * made up of latest versions of specified aspects. If no aspects are provided, value model will not contain any metadata aspect.
   * {@link ListResultMetadata} contains relevant list of urns.
   *
   * <p>If no filter conditions are provided, then it returns values of given entity type.
   *
   * <p>Note: Only one of the filter finders should be implemented in your resource implementation.
   *
   * @param indexFilter {@link IndexFilter} that defines the filter conditions
   * @param indexSortCriterion {@link IndexSortCriterion} that defines the sorting conditions
   * @param aspectNames list of aspects to be returned in the VALUE model
   * @param lastUrn last urn of the previous fetched page. For the first page, this should be set as NULL
   * @param count defining the maximum number of urns to return
   * @return {@link CollectionResult} containing values along with the associated urns in {@link ListResultMetadata}
   */
  @Finder(FINDER_FILTER)
  @Nonnull
  public Task<List<VALUE>> filter(@QueryParam(PARAM_FILTER) @Optional @Nullable IndexFilter indexFilter,
      @QueryParam(PARAM_SORT) @Optional @Nullable IndexSortCriterion indexSortCriterion,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames,
      @QueryParam(PARAM_URN) @Optional @Nullable String lastUrn, @QueryParam(PARAM_COUNT) @Optional("10") int count) {

    return filter(indexFilter, indexSortCriterion, aspectNames, lastUrn, count,
        getResourceLix().testFilter(_assetClass.getSimpleName()));
  }

  @Nonnull
  private Task<List<VALUE>> filter(@QueryParam(PARAM_FILTER) @Optional @Nullable IndexFilter indexFilter,
      @QueryParam(PARAM_SORT) @Optional @Nullable IndexSortCriterion indexSortCriterion,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames,
      @QueryParam(PARAM_URN) @Optional @Nullable String lastUrn, @QueryParam(PARAM_COUNT) @Optional("10") int count,
      boolean isInternalModelsEnabled) {

    return RestliUtils.toTask(() -> {
      final Set<Class<? extends RecordTemplate>> aspectClasses = parseAspectsParam(aspectNames, isInternalModelsEnabled);
      if (aspectClasses.isEmpty()) {
        return filterUrns(indexFilter, indexSortCriterion, lastUrn, count, isInternalModelsEnabled);
      } else {
        return filterAspects(aspectClasses, indexFilter, indexSortCriterion, lastUrn, count, isInternalModelsEnabled);
      }
    });
  }

  /**
   * Similar to {@link #filter(IndexFilter, IndexSortCriterion, String[], String, int)} but
   * uses null sorting criterion.
   *
   * <p>Note: Only one of the filter finders should be implemented in your resource implementation.
   *
   * @deprecated Use {@link #filter(IndexFilter, IndexSortCriterion, String[], String, int)} instead
   */
  @Finder(FINDER_FILTER)
  @Nonnull
  public Task<List<VALUE>> filter(
      @QueryParam(PARAM_FILTER) @Optional @Nullable IndexFilter indexFilter,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames,
      @QueryParam(PARAM_URN) @Optional @Nullable String lastUrn,
      @PagingContextParam @Nonnull PagingContext pagingContext) {
    return filter(indexFilter, null, aspectNames, lastUrn, pagingContext.getCount());
  }

  /**
   * Similar to {@link #filter(IndexFilter, IndexSortCriterion, String[], String, int)} but
   * returns a list result with pagination information.
   *
   * <p>Note: Only one of the filter finders should be implemented in your resource implementation.
   *
   * @param pagingContext {@link PagingContext} defines the paging start and count
   * @return {@link ListResult} containing values along with the associated urns in {@link ListResultMetadata} and
   *        pagination information
   */
  @Finder(FINDER_FILTER)
  @Nonnull
  public Task<ListResult<VALUE>> filter(@QueryParam(PARAM_FILTER) @Optional @Nullable IndexFilter indexFilter,
      @QueryParam(PARAM_SORT) @Optional @Nullable IndexSortCriterion indexSortCriterion,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames,
      @PagingContextParam @Nonnull PagingContext pagingContext) {
    return filter(indexFilter, indexSortCriterion, aspectNames, pagingContext,
        getResourceLix().testFilter(_assetClass.getSimpleName()));
  }

  @Nonnull
  private Task<ListResult<VALUE>> filter(@QueryParam(PARAM_FILTER) @Optional @Nullable IndexFilter indexFilter,
      @QueryParam(PARAM_SORT) @Optional @Nullable IndexSortCriterion indexSortCriterion,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames,
      @PagingContextParam @Nonnull PagingContext pagingContext, boolean isInternalModelsEnabled) {

    return RestliUtils.toTask(() -> {
      final Set<Class<? extends RecordTemplate>> aspectClasses =
          parseAspectsParam(aspectNames, isInternalModelsEnabled);
      if (aspectClasses.isEmpty()) {
        return filterUrns(indexFilter, indexSortCriterion, pagingContext.getStart(), pagingContext.getCount(),
            isInternalModelsEnabled);
      } else {
        return filterAspects(aspectClasses, indexFilter, indexSortCriterion, pagingContext.getStart(),
            pagingContext.getCount(), isInternalModelsEnabled);
      }
    });
  }

  /**
   * Gets a collection result with count aggregate metadata, which has the count of an aggregation
   * specified by the aspect and field to group by.
   *
   * @param indexFilter {@link IndexFilter} that defines the filter conditions
   * @param indexGroupByCriterion {@link IndexGroupByCriterion} that defines the aspect to group by
   * @return {@link CollectionResult} containing metadata that has a map of the field values to their count
   */
  @Finder(FINDER_COUNT_AGGREGATE)
  @Nonnull
  public Task<CollectionResult<EmptyRecord, MapMetadata>> countAggregateFilter(
      @QueryParam(PARAM_FILTER) @Optional @Nullable IndexFilter indexFilter,
      @QueryParam(PARAM_GROUP) IndexGroupByCriterion indexGroupByCriterion
  ) {

    return RestliUtils.toTask(() -> {
      Map<String, Long> countAggregateMap = getLocalDAO().countAggregate(indexFilter, indexGroupByCriterion);
      MapMetadata mapMetadata = new MapMetadata().setLongMap(new LongMap(countAggregateMap));
      return new CollectionResult<EmptyRecord, MapMetadata>(new ArrayList<>(), mapMetadata);
    });
  }

  /**
   * Gets the count of an aggregation specified by the aspect and field to group by.
   * @param indexFilter {@link IndexFilter} that defines the filter conditions
   * @param indexGroupByCriterion {@link IndexGroupByCriterion} that defines the aspect to group by
   * @return map of the field values to their count
   *
   * @deprecated Use {@link #countAggregateFilter(IndexFilter, IndexGroupByCriterion)} instead
   */
  @Action(name = ACTION_COUNT_AGGREGATE)
  @Nonnull
  public Task<Map<String, Long>> countAggregate(
      @ActionParam(PARAM_FILTER) @Optional @Nullable IndexFilter indexFilter,
      @ActionParam(PARAM_GROUP) IndexGroupByCriterion indexGroupByCriterion
  ) {
    return RestliUtils.toTask(() -> getLocalDAO().countAggregate(indexFilter, indexGroupByCriterion));
  }

  @Nonnull
  protected Set<Class<? extends RecordTemplate>> parseAspectsParam(@Nullable String[] aspectNames,
      boolean isInternalModelsEnabled) {
    if (aspectNames == null) {
      return isInternalModelsEnabled ? _supportedInternalAspectClasses : _supportedAspectClasses;
    }
    return Arrays.asList(aspectNames).stream().map(ModelUtils::getAspectClass).collect(Collectors.toSet());
  }

  /**
   * Returns a map of {@link VALUE} models given the collection of {@link URN}s and set of aspect classes.
   *
   * @param urns collection of urns
   * @param aspectClasses set of aspect classes
   * @param isInternalModelsEnabled flag to switch the internal models
   * @return All {@link VALUE} objects keyed by {@link URN} obtained from DB
   */
  @Nonnull
  protected Map<URN, VALUE> getInternal(@Nonnull Collection<URN> urns,
      @Nonnull Set<Class<? extends RecordTemplate>> aspectClasses, boolean isInternalModelsEnabled) {
    return getUrnAspectMap(urns, aspectClasses, isInternalModelsEnabled).entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey,
            e -> isInternalModelsEnabled ? toInternalValue(newInternalSnapshot(e.getKey(), e.getValue()))
                : toValue(newSnapshot(e.getKey(), e.getValue()))));
  }

  /**
   * Similar to {@link #getInternal(Collection, Set, boolean)}  but filter out {@link URN}s which are not in the DB.
   */
  @Nonnull
  protected Map<URN, VALUE> getInternalNonEmpty(@Nonnull Collection<URN> urns,
      @Nonnull Set<Class<? extends RecordTemplate>> aspectClasses, boolean isInternalModelsEnabled) {
    return getUrnAspectMap(urns, aspectClasses, isInternalModelsEnabled).entrySet()
        .stream()
        .filter(e -> !e.getValue().isEmpty())
        .collect(Collectors.toMap(Map.Entry::getKey,
            e -> isInternalModelsEnabled ? toInternalValue(newInternalSnapshot(e.getKey(), e.getValue()))
                : toValue(newSnapshot(e.getKey(), e.getValue()))));
  }

  @Nonnull
  private Map<URN, List<UnionTemplate>> getUrnAspectMap(@Nonnull Collection<URN> urns,
      @Nonnull Set<Class<? extends RecordTemplate>> aspectClasses, boolean isInternalModelsEnabled) {
    // Construct the keys to retrieve latest version of all supported aspects for all URNs.
    final Set<AspectKey<URN, ? extends RecordTemplate>> keys = urns.stream()
        .map(urn -> aspectClasses.stream()
            .map(clazz -> new AspectKey<>(clazz, urn, LATEST_VERSION))
            .collect(Collectors.toList()))
        .flatMap(List::stream)
        .collect(Collectors.toSet());

    final Map<URN, List<UnionTemplate>> urnAspectsMap =
        urns.stream().collect(Collectors.toMap(Function.identity(), urn -> new ArrayList<>()));

    if (getShadowReadLocalDAO() == null) {
      if (isInternalModelsEnabled) {
        getLocalDAO().get(keys)
            .forEach((key, aspect) -> aspect.ifPresent(metadata -> urnAspectsMap.get(key.getUrn())
                .add(ModelUtils.newAspectUnion(_internalAspectUnionClass, metadata))));
      } else {
        getLocalDAO().get(keys)
            .forEach((key, aspect) -> aspect.ifPresent(metadata -> urnAspectsMap.get(key.getUrn())
                .add(ModelUtils.newAspectUnion(_aspectUnionClass, metadata))));
      }
      return urnAspectsMap;
    } else {
      return getUrnAspectMapFromShadowDao(urns, keys, isInternalModelsEnabled);
    }
  }

  @Nonnull
  private Map<URN, List<UnionTemplate>> getUrnAspectMapFromShadowDao(
      @Nonnull Collection<URN> urns,
      @Nonnull Set<AspectKey<URN, ? extends RecordTemplate>> keys,
      boolean isInternalModelsEnabled) {

    Map<AspectKey<URN, ? extends RecordTemplate>, java.util.Optional<? extends RecordTemplate>> localResults =
        getLocalDAO().get(keys);

    BaseLocalDAO<INTERNAL_ASPECT_UNION, URN> shadowDao = getShadowReadLocalDAO();
    Map<AspectKey<URN, ? extends RecordTemplate>, java.util.Optional<? extends RecordTemplate>> shadowResults =
        shadowDao.get(keys);

    final Map<URN, List<UnionTemplate>> urnAspectsMap =
        urns.stream().collect(Collectors.toMap(Function.identity(), urn -> new ArrayList<>()));

    keys.forEach(key -> {
      java.util.Optional<? extends RecordTemplate> localValue = localResults.getOrDefault(key, java.util.Optional.empty());
      java.util.Optional<? extends RecordTemplate> shadowValue = shadowResults.getOrDefault(key, java.util.Optional.empty());

      RecordTemplate valueToUse = null;

      if (localValue.isPresent() && shadowValue.isPresent()) {
        if (!Objects.equals(localValue.get(), shadowValue.get())) {
          log.warn("Aspect mismatch for URN {} and aspect {}: local = {}, shadow = {}",
              key.getUrn(), key.getAspectClass().getSimpleName(),
              localValue.get(), shadowValue.get());
          valueToUse = localValue.get(); // fallback to local if there's mismatch
        } else {
          valueToUse = shadowValue.get(); // match → use shadow
        }
      } else if (shadowValue.isPresent()) {
        valueToUse = shadowValue.get();
      } else if (localValue.isPresent()) {
        valueToUse = localValue.get();
      }

      if (valueToUse != null) {
        urnAspectsMap.get(key.getUrn()).add(ModelUtils.newAspectUnion(
            (Class<? extends ASPECT_UNION>) (isInternalModelsEnabled ? _internalAspectUnionClass : _aspectUnionClass),
            valueToUse));
      }
    });

    return urnAspectsMap;
  }


  @Nonnull
  private SNAPSHOT newSnapshot(@Nonnull URN urn, @Nonnull List<UnionTemplate> aspects) {
    return ModelUtils.newSnapshot(_snapshotClass, urn, aspects);
  }

  /**
   * Creates a snapshot of the entity with no aspects set, just the URN.
   */
  @Nonnull
  protected SNAPSHOT newSnapshot(@Nonnull URN urn) {
    return ModelUtils.newSnapshot(_snapshotClass, urn, Collections.emptyList());
  }

  @Nonnull
  private INTERNAL_SNAPSHOT newInternalSnapshot(@Nonnull URN urn, @Nonnull List<UnionTemplate> aspects) {
    return ModelUtils.newSnapshot(_internalSnapshotClass, urn, aspects);
  }

  /**
   * Creates an Internal snapshot of the entity with no aspects set, just the URN.
   */
  @Nonnull
  protected INTERNAL_SNAPSHOT newInternalSnapshot(@Nonnull URN urn) {
    return ModelUtils.newSnapshot(_internalSnapshotClass, urn, Collections.emptyList());
  }

  @Nullable
  protected URN parseUrnParam(@Nullable String urnString) {
    if (urnString == null) {
      return null;
    }

    try {
      return createUrnFromString(urnString);
    } catch (Exception e) {
      throw RestliUtils.badRequestException("Invalid URN: " + urnString);
    }
  }
}
