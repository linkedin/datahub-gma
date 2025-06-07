package com.linkedin.metadata.restli;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.metadata.dao.AspectKey;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.exception.ModelValidationException;
import com.linkedin.metadata.dao.ingestion.AspectCallbackRegistry;
import com.linkedin.metadata.dao.ingestion.AspectCallbackResponse;
import com.linkedin.metadata.dao.ingestion.AspectCallbackRoutingClient;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.events.IngestionTrackingContext;
import com.linkedin.metadata.internal.IngestionParams;
import com.linkedin.parseq.Task;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.RestLiServiceException;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.QueryParam;
import com.linkedin.restli.server.annotations.RestMethod;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.dao.BaseReadDAO.*;
import static com.linkedin.metadata.dao.utils.ModelUtils.*;
import static com.linkedin.metadata.restli.RestliConstants.*;
import static com.linkedin.restli.common.HttpStatus.*;


/**
 * Extends {@link BaseBrowsableEntityResource} with aspect routing capability.
 * For certain aspects of an entity, incoming requests will be routed to a different GMS in addition to being
 * written locally (i.e. dual write).
 * See http://go/aspect-routing for more details
 */
@Slf4j
public abstract class BaseAspectRoutingResource<
    // @formatter:off
    KEY,
    VALUE extends RecordTemplate,
    URN extends Urn,
    SNAPSHOT extends RecordTemplate,
    ASPECT_UNION extends UnionTemplate,
    DOCUMENT extends RecordTemplate,
    INTERNAL_SNAPSHOT extends RecordTemplate,
    INTERNAL_ASPECT_UNION extends UnionTemplate,
    ASSET extends RecordTemplate>
    // @formatter:on
    extends
    BaseBrowsableEntityResource<KEY, VALUE, URN, SNAPSHOT, ASPECT_UNION, DOCUMENT, INTERNAL_SNAPSHOT, INTERNAL_ASPECT_UNION, ASSET> {

  private final Class<VALUE> _valueClass;
  private final Class<ASPECT_UNION> _aspectUnionClass;
  private final Class<SNAPSHOT> _snapshotClass;
  private final Class<INTERNAL_SNAPSHOT> _internalSnapshotClass;
  private final Class<INTERNAL_ASPECT_UNION> _internalAspectUnionClass;
  private final Class<ASSET> _assetClass;
  private static final List<String> SKIP_INGESTION_FOR_ASPECTS = Collections.singletonList("com.linkedin.dataset.DatasetAccountableOwnership");

  public BaseAspectRoutingResource(@Nullable Class<SNAPSHOT> snapshotClass,
      @Nullable Class<ASPECT_UNION> aspectUnionClass, @Nonnull Class<URN> urnClass, @Nonnull Class<VALUE> valueClass,
      @Nonnull Class<INTERNAL_SNAPSHOT> internalSnapshotClass,
      @Nonnull Class<INTERNAL_ASPECT_UNION> internalAspectUnionClass, @Nonnull Class<ASSET> assetClass) {
    super(snapshotClass, aspectUnionClass, urnClass, internalSnapshotClass, internalAspectUnionClass, assetClass);
    _valueClass = valueClass;
    _aspectUnionClass = aspectUnionClass;
    _snapshotClass = snapshotClass;
    _internalSnapshotClass = internalSnapshotClass;
    _internalAspectUnionClass = internalAspectUnionClass;
    _assetClass = assetClass;
  }

  /**
   * Get the aspect routing client manager which provides multiple aspect routing support.
   * @return {@link AspectRoutingGmsClientManager}
   */
  public abstract AspectRoutingGmsClientManager getAspectRoutingGmsClientManager();

  /**
   * Retrieves the value for an entity that is made up of latest versions of specified aspects.
   */
  @RestMethod.Get
  @Nonnull
  @Override
  public Task<VALUE> get(@Nonnull KEY id, @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    final URN urn = toUrn(id);
    return get(id, aspectNames, getResourceLix().testGet(String.valueOf(urn), urn.getEntityType()));
  }

  @Nonnull
  @Override
  protected Task<VALUE> get(@Nonnull KEY id, @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames,
      boolean isInternalModelsEnabled) {

    return RestliUtils.toTask(() -> {
      final Set<Class<? extends RecordTemplate>> aspectClasses = parseAspectsParam(aspectNames, isInternalModelsEnabled);

      // The assumption is main GMS must have this entity.
      // If entity only has routing aspect, resourceNotFoundException will be thrown.
      final URN urn = toUrn(id);
      if (!getLocalDAO().exists(urn)) {
        throw RestliUtils.resourceNotFoundException(String.format("Cannot find entity {%s} from Master GMS.", urn));
      }
      final Set<Class<? extends RecordTemplate>> nonRoutingAspects = getNonRoutingAspects(aspectClasses);
      final VALUE valueFromLocalDao;
      if (nonRoutingAspects.isEmpty()) {
        valueFromLocalDao =
            isInternalModelsEnabled ? toInternalValue(newInternalSnapshot(urn)) : toValue(newSnapshot(urn));
      } else {
        valueFromLocalDao = getValueFromLocalDao(id, nonRoutingAspects, isInternalModelsEnabled);
      }
      return merge(valueFromLocalDao, getValueFromRoutingGms(toUrn(id), getRoutingAspects(aspectClasses)));
    });
  }

  /**
   * Deprecated to use {@link #getAsset(String, String[])} .
   * An action method for getting a snapshot of aspects for an entity.
   */
  @Deprecated
  @Action(name = ACTION_GET_SNAPSHOT)
  @Nonnull
  @Override
  public Task<SNAPSHOT> getSnapshot(@ActionParam(PARAM_URN) @Nonnull String urnString,
      @ActionParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    final URN urn = parseUrnParam(urnString);
    try {
      return getSnapshot(urnString, aspectNames,
          getResourceLix().testGetSnapshot(String.valueOf(urn), ModelUtils.getEntityType(urn)));
    } catch (ModelValidationException e) {
      throw RestliUtils.invalidArgumentsException(e.getMessage());
    }
  }

  @Nonnull
  @Override
  protected Task<SNAPSHOT> getSnapshot(@ActionParam(PARAM_URN) @Nonnull String urnString,
      @ActionParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames, boolean isInternalModelsEnabled) {
    final URN urn = parseUrnParam(urnString);
    final Set<Class<? extends RecordTemplate>> aspectClasses = parseAspectsParam(aspectNames, isInternalModelsEnabled);
    if (isInternalModelsEnabled) {
      return RestliUtils.toTask(() -> {
        if (!containsRoutingAspect(aspectClasses)) {
          // Get snapshot from Local DAO.
          final List<INTERNAL_ASPECT_UNION> aspectUnions = getInternalAspectsFromLocalDao(urn, aspectClasses);
          return ModelUtils.newSnapshot(_snapshotClass, urn,
              convertInternalAspectUnionToAspectUnion(_aspectUnionClass, aspectUnions));
        } else {
          final Set<Class<? extends RecordTemplate>> nonRoutingAspects = getNonRoutingAspects(aspectClasses);
          final List<INTERNAL_ASPECT_UNION> aspectsFromLocalDao =
              getInternalAspectsFromLocalDao(urn, nonRoutingAspects);
          final Set<Class<? extends RecordTemplate>> routingAspects = getRoutingAspects(aspectClasses);
          final List<INTERNAL_ASPECT_UNION> aspectsFromGms = routingAspects.stream()
              .map(routingAspect -> getInternalAspectsFromGms(urn, routingAspect))
              .flatMap(List::stream)
              .collect(Collectors.toList());
          return ModelUtils.newSnapshot(_snapshotClass, urn, convertInternalAspectUnionToAspectUnion(_aspectUnionClass,
              Stream.concat(aspectsFromGms.stream(), aspectsFromLocalDao.stream()).collect(Collectors.toList())));
        }
      });
    } else {
      return RestliUtils.toTask(() -> {
        if (!containsRoutingAspect(aspectClasses)) {
          // Get snapshot from Local DAO.
          final List<ASPECT_UNION> aspectUnions = getAspectsFromLocalDao(urn, aspectClasses);
          return ModelUtils.newSnapshot(_snapshotClass, urn, aspectUnions);
        } else {
          final Set<Class<? extends RecordTemplate>> nonRoutingAspects = getNonRoutingAspects(aspectClasses);
          final List<ASPECT_UNION> aspectsFromLocalDao = getAspectsFromLocalDao(urn, nonRoutingAspects);
          final Set<Class<? extends RecordTemplate>> routingAspects = getRoutingAspects(aspectClasses);
          final List<ASPECT_UNION> aspectsFromGms = routingAspects.stream()
              .map(routingAspect -> getAspectsFromGms(urn, routingAspect))
              .flatMap(List::stream)
              .collect(Collectors.toList());
          return ModelUtils.newSnapshot(_snapshotClass, urn,
              Stream.concat(aspectsFromGms.stream(), aspectsFromLocalDao.stream()).collect(Collectors.toList()));
        }
      });
    }
  }

  /**
   * An action method for getting an asset of aspects for an entity.
   */
  @Action(name = ACTION_GET_ASSET)
  @Nonnull
  @Override
  public Task<ASSET> getAsset(@ActionParam(PARAM_URN) @Nonnull String urnString,
      @ActionParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    try {
      return RestliUtils.toTask(() -> {
        final URN urn = parseUrnParam(urnString);
        final Set<Class<? extends RecordTemplate>> aspectClasses = parseAspectsParam(aspectNames, true);

        if (!containsRoutingAspect(aspectClasses)) {
          // Get snapshot from Local DAO.
          final List<INTERNAL_ASPECT_UNION> aspectUnions = getInternalAspectsFromLocalDao(urn, aspectClasses);
          return ModelUtils.newAsset(_assetClass, urn, aspectUnions);
        } else {
          final Set<Class<? extends RecordTemplate>> nonRoutingAspects = getNonRoutingAspects(aspectClasses);
          final List<INTERNAL_ASPECT_UNION> aspectsFromLocalDao =
              getInternalAspectsFromLocalDao(urn, nonRoutingAspects);
          final Set<Class<? extends RecordTemplate>> routingAspects = getRoutingAspects(aspectClasses);
          final List<INTERNAL_ASPECT_UNION> aspectsFromGms = routingAspects.stream()
              .map(routingAspect -> getInternalAspectsFromGms(urn, routingAspect))
              .flatMap(List::stream)
              .collect(Collectors.toList());
          return ModelUtils.newAsset(_assetClass, urn,
              Stream.concat(aspectsFromGms.stream(), aspectsFromLocalDao.stream()).collect(Collectors.toList()));
        }
      });
    } catch (ModelValidationException e) {
      throw RestliUtils.invalidArgumentsException(e.getMessage());
    }
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

  @Nonnull
  private Task<BackfillResult> backfill(@ActionParam(PARAM_URNS) @Nonnull String[] urns,
      @ActionParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames, boolean isInternalModelsEnabled) {
    if (this._urnClass == null) {
      throw new IllegalStateException("URN class is not set for this resource");
    }
    final String entityType = getEntityTypeFromUrnClass(this._urnClass);

    return RestliUtils.toTask(() -> {
      final Set<URN> urnSet = Arrays.stream(urns).map(this::parseUrnParam).collect(Collectors.toSet());
      final Set<Class<? extends RecordTemplate>> aspectClasses =
          parseAspectsParam(aspectNames, isInternalModelsEnabled);
      Map<URN, Map<Class<? extends RecordTemplate>, java.util.Optional<? extends RecordTemplate>>> urnToAspect =
          new HashMap<>();

      if (!containsRoutingAspect(aspectClasses)) {
        // Backfill only needs local DAO.
        return RestliUtils.buildBackfillResult(getLocalDAO().backfill(aspectClasses, urnSet));
      }

      if (containsRoutingAspect(aspectClasses) && aspectClasses.size() == 1) {
        // Backfill only needs aspect GMS
        return backfillWithDefault(urnSet, entityType);
      }

      // Backfill needs both aspect GMS and local DAO.
      BackfillResult localDaoBackfillResult =
          RestliUtils.buildBackfillResult(getLocalDAO().backfill(getNonRoutingAspects(aspectClasses), urnSet));
      BackfillResult gmsBackfillResult = backfillWithDefault(urnSet, entityType);
      return merge(localDaoBackfillResult, gmsBackfillResult);
    });
  }

  /**
   * An action method for emitting MAE backfill messages with new value (old value will be set as null). This action
   * should be deprecated once the secondary store is moving away from elastic search, or the standard backfill
   * method starts to safely backfill against live index.
   *
   * <p>As a hack solution, we only cover the aspects that belong to the request serving gms.
   */
  @Action(name = ACTION_BACKFILL_WITH_NEW_VALUE)
  @Nonnull
  @Override
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
      final Set<URN> urnSet = Arrays.stream(urns).map(this::parseUrnParam).collect(Collectors.toSet());
      final Set<Class<? extends RecordTemplate>> aspectClasses =
          parseAspectsParam(aspectNames, isInternalModelsEnabled);
      return RestliUtils.buildBackfillResult(
          getLocalDAO().backfillWithNewValue(getNonRoutingAspects(aspectClasses), urnSet));
    });
  }

  /**
   * Internal ingest method for snapshots. First execute any pre-ingestion updates. Then, route any aspects which have a registered routing
   * GMS client to the respective GMS for ingestion. Finally, continue to save the aspect locally as well (i.e. dual write)
   * @param snapshot snapshot to process
   * @param aspectsToIgnore aspects to ignore
   * @param trackingContext context for tracking ingestion health
   * @param ingestionParams optional ingestion parameters
   * @return Restli Task for metadata ingestion
   */
  @Nonnull
  @Override
  protected Task<Void> ingestInternal(@Nonnull SNAPSHOT snapshot,
      @Nonnull Set<Class<? extends RecordTemplate>> aspectsToIgnore, @Nullable IngestionTrackingContext trackingContext,
      @Nullable IngestionParams ingestionParams) {
    // TODO: META-18950: add trackingContext to BaseAspectRoutingResource. currently the param is unused.
    return RestliUtils.toTask(() -> {
      final URN urn = (URN) ModelUtils.getUrnFromSnapshot(snapshot);
      final AuditStamp auditStamp = getAuditor().requestAuditStamp(getContext().getRawRequestContext());
      ModelUtils.getAspectsFromSnapshot(snapshot).forEach(aspect ->
        ingestAspect(aspectsToIgnore, urn, aspect, trackingContext, ingestionParams, auditStamp, false));
      return null;
    });
  }

  /**
   * Raw internal ingest method for snapshots which skips any pre-, intra-, or post-processing. Route any aspects which
   * have a registered routing GMS client to the respective GMS for ingestion. Finally, continue to save the aspect
   * locally as well (i.e. dual write)
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
    // TODO: META-18950: add trackingContext to BaseAspectRoutingResource. currently the param is unused.
    return RestliUtils.toTask(() -> {
      final URN urn = (URN) ModelUtils.getUrnFromSnapshot(snapshot);
      final AuditStamp auditStamp = getAuditor().requestAuditStamp(getContext().getRawRequestContext());
      ModelUtils.getAspectsFromSnapshot(snapshot).forEach(aspect ->
          ingestAspect(aspectsToIgnore, urn, aspect, trackingContext, ingestionParams, auditStamp, true));
      return null;
    });
  }

  /**
   * Internal ingest method for assets. First execute any pre-ingestion updates. Then, route any aspects which have a registered routing
   * GMS client to the respective GMS for ingestion. Finally, continue to save the aspect locally as well (i.e. dual write)
   * @param asset asset to process
   * @param aspectsToIgnore aspects to ignore
   * @param ingestionParams optional ingestion parameters
   * @return Restli Task for metadata ingestion
   */
  @Nonnull
  @Override
  protected Task<Void> ingestInternalAsset(@Nonnull ASSET asset,
      @Nonnull Set<Class<? extends RecordTemplate>> aspectsToIgnore,
      @Nullable IngestionParams ingestionParams) {
    // TODO: META-18950: add trackingContext to BaseAspectRoutingResource. currently the param is unused.
    return RestliUtils.toTask(() -> {
      final URN urn = (URN) ModelUtils.getUrnFromAsset(asset);
      final AuditStamp auditStamp = getAuditor().requestAuditStamp(getContext().getRawRequestContext());
      IngestionTrackingContext trackingContext =
          ingestionParams != null ? ingestionParams.getIngestionTrackingContext() : null;
      ModelUtils.getAspectsFromAsset(asset).forEach(aspect ->
          ingestAspect(aspectsToIgnore, urn, aspect, trackingContext, ingestionParams, auditStamp, false));
      return null;
    });
  }

  /**
   * Raw internal ingest method for assets which skips any pre-, intra-, or post-processing. Route any aspects which
   * have a registered routing GMS client to the respective GMS for ingestion. Finally, continue to save the aspect
   * locally as well (i.e. dual write)
   * @param asset asset to process
   * @param aspectsToIgnore aspects to ignore
   * @param ingestionParams optional ingestion parameters
   * @return Restli Task for metadata ingestion
   */
  @Nonnull
  protected Task<Void> rawIngestInternalAsset(@Nonnull ASSET asset,
      @Nonnull Set<Class<? extends RecordTemplate>> aspectsToIgnore,
      @Nullable IngestionParams ingestionParams) {
    // TODO: META-18950: add trackingContext to BaseAspectRoutingResource. currently the param is unused.
    return RestliUtils.toTask(() -> {
      final URN urn = (URN) ModelUtils.getUrnFromAsset(asset);
      final AuditStamp auditStamp = getAuditor().requestAuditStamp(getContext().getRawRequestContext());
      IngestionTrackingContext trackingContext =
          ingestionParams != null ? ingestionParams.getIngestionTrackingContext() : null;
      ModelUtils.getAspectsFromAsset(asset).forEach(aspect ->
          ingestAspect(aspectsToIgnore, urn, aspect, trackingContext, ingestionParams, auditStamp, true));
      return null;
    });
  }

  /**
   * Helper function to ingest an aspect either via routing or locally (or both). There is a flag that can be toggled
   * to indicate whether to execute pre-, intra-, or post-ingestion updates if they exist.
   * @param aspectsToIgnore set of aspect classes to ignore, if any
   * @param urn urn associated with the aspect to ingest
   * @param aspect aspect to ingest
   * @param trackingContext context for tracking ingestion health
   * @param ingestionParams optional ingestion parameters
   * @param auditStamp audit information of the update
   * @param skipExtraProcessing flag to indicate whether to execute pre-, intra-, or post-ingestion updates
   */
  private void ingestAspect(Set<Class<? extends RecordTemplate>> aspectsToIgnore, Urn urn, RecordTemplate aspect,
      IngestionTrackingContext trackingContext, IngestionParams ingestionParams, AuditStamp auditStamp,
      boolean skipExtraProcessing) {
    if (!aspectsToIgnore.contains(aspect.getClass())) {
      BaseLocalDAO<INTERNAL_ASPECT_UNION, URN> shadowLocalDao = getShadowLocalDAO();
      if (getAspectRoutingGmsClientManager().hasRegistered(aspect.getClass())) {
        try {
          // get the updated aspect if there is a preupdate routing lambda registered
          AspectCallbackRegistry registry = getLocalDAO().getAspectCallbackRegistry();
          if (!skipExtraProcessing && registry != null && registry.isRegistered(aspect.getClass(), urn.getEntityType())) {
            log.info(String.format("Executing registered pre-update routing lambda for aspect class %s.", aspect.getClass()));
            aspect = aspectCallbackHelper((URN) urn, aspect, registry, ingestionParams, auditStamp);
            log.info("PreUpdateRouting completed in ingestInternalAsset, urn: {}, updated aspect: {}", urn, aspect);
            // Get the fqcn of the aspect class
            String aspectFQCN = aspect.getClass().getCanonicalName();
            //TODO: META-21112: Remove this check after adding annotations at model level; to handle SKIP/PROCEED for preUpdateRouting
            if (SKIP_INGESTION_FOR_ASPECTS.contains(aspectFQCN)) {
              log.info("Skip ingestion in ingestInternalAsset for urn: {}, aspectFQCN: {}", urn, aspectFQCN);
              return;
            }
          }
          if (trackingContext != null) {
            getAspectRoutingGmsClientManager().getRoutingGmsClient(aspect.getClass())
                .ingestWithTracking(urn, aspect, trackingContext, ingestionParams);
          } else {
            getAspectRoutingGmsClientManager().getRoutingGmsClient(aspect.getClass()).ingest(urn, aspect);
          }
          // here, always call a simple version of BaseLocalDAO::add which skips pre-update lambdas regardless of
          // the value of param skipExtraProcessing since any pre-update lambdas would have already been executed
          // in the code above.
          getLocalDAO().rawAdd((URN) urn, aspect, auditStamp, trackingContext, ingestionParams);
          // if there is a shadow local DAO, also call rawAdd on it to write it to the shadow db
          if (shadowLocalDao != null) {
            shadowLocalDao.rawAdd((URN) urn, aspect, auditStamp, trackingContext, ingestionParams);
          }
        } catch (Exception exception) {
          log.error("Couldn't ingest routing aspect {} for {}", aspect.getClass().getSimpleName(), urn, exception);
          throw exception;
        }
      } else {
        if (skipExtraProcessing) {
          // call a simple version of BaseLocalDAO::add which skips pre-update lambdas.
          getLocalDAO().rawAdd((URN) urn, aspect, auditStamp, trackingContext, ingestionParams);
          if (shadowLocalDao != null) {
            shadowLocalDao.rawAdd((URN) urn, aspect, auditStamp, trackingContext, ingestionParams);
          }
        } else {
          getLocalDAO().add((URN) urn, aspect, auditStamp, trackingContext, ingestionParams);
          if (shadowLocalDao != null) {
            shadowLocalDao.add((URN) urn, aspect, auditStamp, trackingContext, ingestionParams);
          }
        }
      }
    }
  }

  /**
   * Whether given set of aspect classes contains routing aspect class.
   * @param aspectClasses A set of aspect classes
   * @return True if aspectClasses contains routing aspect class.
   */
  private boolean containsRoutingAspect(Set<Class<? extends RecordTemplate>> aspectClasses) {
    return aspectClasses.stream().anyMatch(aspectClass -> getAspectRoutingGmsClientManager().hasRegistered(aspectClass));
  }

  /**
   * Get non-routing aspects from aspectClasses.
   */
  @Nonnull
  @ParametersAreNonnullByDefault
  private Set<Class<? extends RecordTemplate>> getNonRoutingAspects(
      Set<Class<? extends RecordTemplate>> aspectClasses) {
    return aspectClasses.stream()
        .filter(aspectClass -> !getAspectRoutingGmsClientManager().hasRegistered(aspectClass))
        .collect(Collectors.toSet());
  }

  /**
   * Get routing aspects from aspectClasses.
   */
  @Nonnull
  @ParametersAreNonnullByDefault
  private Set<Class<? extends RecordTemplate>> getRoutingAspects(Set<Class<? extends RecordTemplate>> aspectClasses) {
    return aspectClasses.stream()
        .filter(aspectClass -> getAspectRoutingGmsClientManager().hasRegistered(aspectClass))
        .collect(Collectors.toSet());
  }

  /**
   * Get entity value from local DAO decorated with specified aspect classes.
   * @param id identifier of the entity.
   * @param aspectClasses Aspects to be decorated on the entity
   * @return Entity decorated with specified aspect classes.
   */
  @Nonnull
  @ParametersAreNonnullByDefault
  private VALUE getValueFromLocalDao(KEY id, Set<Class<? extends RecordTemplate>> aspectClasses,
      boolean isInternalModelsEnabled) {
    final URN urn = toUrn(id);
    final VALUE value = getInternal(Collections.singleton(urn), aspectClasses, isInternalModelsEnabled).get(urn);
    if (value == null) {
      throw RestliUtils.resourceNotFoundException();
    }
    return value;
  }

  /**
   * Get aspect values from local DAO for specified aspect classes.
   * @param urn identifier of the entity.
   * @param aspectClasses Aspects to be decorated on the entity
   * @return A list of aspects.
   */
  @Nonnull
  @ParametersAreNonnullByDefault
  private List<ASPECT_UNION> getAspectsFromLocalDao(URN urn, Set<Class<? extends RecordTemplate>> aspectClasses) {

    if (aspectClasses == null || aspectClasses.isEmpty()) {
      return Collections.emptyList();
    }
    final Set<AspectKey<URN, ? extends RecordTemplate>> keys = aspectClasses.stream()
        .map(aspectClass -> new AspectKey<>(aspectClass, urn, LATEST_VERSION))
        .collect(Collectors.toSet());
    return getLocalDAO().get(keys)
        .values()
        .stream()
        .filter(java.util.Optional::isPresent)
        .map(aspect -> ModelUtils.newAspectUnion(_aspectUnionClass, aspect.get()))
        .collect(Collectors.toList());
  }

  /**
   * Get internal aspect values from local DAO for specified aspect classes.
   * @param urn identifier of the entity.
   * @param aspectClasses Aspects to be decorated on the entity
   * @return A list of aspects.
   */
  @Nonnull
  @ParametersAreNonnullByDefault
  private List<INTERNAL_ASPECT_UNION> getInternalAspectsFromLocalDao(URN urn, Set<Class<? extends RecordTemplate>> aspectClasses) {

    if (aspectClasses == null || aspectClasses.isEmpty()) {
      return Collections.emptyList();
    }

    final Set<AspectKey<URN, ? extends RecordTemplate>> keys = aspectClasses.stream()
        .map(aspectClass -> new AspectKey<>(aspectClass, urn, LATEST_VERSION))
        .collect(Collectors.toSet());

    if (getShadowReadLocalDAO() == null) {
      return getLocalDAO().get(keys)
          .values()
          .stream()
          .filter(java.util.Optional::isPresent)
          .map(aspect -> ModelUtils.newAspectUnion(_internalAspectUnionClass, aspect.get()))
          .collect(Collectors.toList());
    }
    return getInternalAspectsWithShadowComparison(keys);
  }

  /**
   * Get internal aspect values from shadow DAO for specified aspect keys.
   * This method is used to retrieve aspects from shadow DAO when the local DAO does not have the shadow read capability.
   * @param keys Aspect keys to be retrieved from shadow DAO
   * @return A list of internal aspects.
   */
  private List<INTERNAL_ASPECT_UNION> getInternalAspectsWithShadowComparison(Set<AspectKey<URN, ? extends RecordTemplate>> keys) {

    Map<AspectKey<URN, ? extends RecordTemplate>, java.util.Optional<? extends RecordTemplate>> localResults =
        getLocalDAO().get(keys);
    Map<AspectKey<URN, ? extends RecordTemplate>, java.util.Optional<? extends RecordTemplate>> shadowResults =
        getShadowReadLocalDAO().get(keys);

    return keys.stream()
        .map(key -> {
          java.util.Optional<? extends RecordTemplate> local = localResults.getOrDefault(key, java.util.Optional.empty());
          java.util.Optional<? extends RecordTemplate> shadow = shadowResults.getOrDefault(key, java.util.Optional.empty());

          RecordTemplate valueToUse = null;
          if (shadow.isPresent() && local.isPresent() && !Objects.equals(local.get(), shadow.get())) {
            log.warn("Aspect mismatch for URN {} and aspect {}: local = {}, shadow = {}",
                key.getUrn(), key.getAspectClass().getSimpleName(), local.get(), shadow.get());
            valueToUse = local.get();
          } else if (shadow.isPresent()) {
            log.warn("Only shadow value present for URN {} and aspect {}", key.getUrn(), key.getAspectClass().getSimpleName());
            valueToUse = shadow.get();
          } else if (local.isPresent()) {
            log.info("Only local value present for URN {} and aspect {}. Using local.", key.getUrn(), key.getAspectClass().getSimpleName());
            valueToUse = local.get();
          }

          return valueToUse != null
              ? ModelUtils.newAspectUnion(_internalAspectUnionClass, valueToUse)
              : null;
        })
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  /**
   * Get aspect value from routing aspect GMS.
   */
  @Nonnull
  @ParametersAreNonnullByDefault
  private List<ASPECT_UNION> getAspectsFromGms(URN urn, Class aspectClass) {
    final List<? extends RecordTemplate> routingAspects =
        getValueFromRoutingGms(urn, Collections.singletonList(aspectClass));
    if (routingAspects.isEmpty()) {
      return new ArrayList<>();
    }
    return Collections.singletonList(ModelUtils.newAspectUnion(_aspectUnionClass, routingAspects.get(0)));
  }

  /**
   * Get internal aspect value from routing aspect GMS.
   */
  @Nonnull
  @ParametersAreNonnullByDefault
  private List<INTERNAL_ASPECT_UNION> getInternalAspectsFromGms(URN urn, Class aspectClass) {
    final List<? extends RecordTemplate> routingAspects =
        getValueFromRoutingGms(urn, Collections.singletonList(aspectClass));
    if (routingAspects.isEmpty()) {
      return new ArrayList<>();
    }
    return Collections.singletonList(ModelUtils.newAspectUnion(_internalAspectUnionClass, routingAspects.get(0)));
  }

  /**
   * Merge routing aspect value from GMS into entity value retrieved from Local DAO.
   * @param valueFromLocalDao Entity value retrieved from Local DAO
   * @param routingAspects Aspect values retrieved from GMS
   * @return Merged entity value which will contain routing aspect value
   */
  @Nonnull
  private VALUE merge(@Nonnull VALUE valueFromLocalDao, @Nullable List<? extends RecordTemplate> routingAspects) {
    for (RecordTemplate routingAspect : routingAspects) {
      try {
        String setterMethodName = getAspectRoutingGmsClientManager().getRoutingAspectSetterName(routingAspect.getClass());
        Method setter =  valueFromLocalDao.getClass().getMethod(setterMethodName, routingAspect.getClass(), SetMode.class);
        setter.invoke(valueFromLocalDao, routingAspect, SetMode.IGNORE_NULL);
      } catch (NoSuchMethodException e) {
        throw new RestLiServiceException(HttpStatus.S_500_INTERNAL_SERVER_ERROR,
            "Failed to get routing aspect setter method.", e);
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new RestLiServiceException(HttpStatus.S_500_INTERNAL_SERVER_ERROR, "Failed to set routing aspect.", e);
      }
    }
    return valueFromLocalDao;
  }

  /**
   * Merget BackfillResult from routing aspect GMS and Local DAO.
   * @param fromDao BackfillResult from Local DAO.
   * @param fromGms BackfillResult from routing aspect GMS.
   * @return merged BackfillResult
   */
  @Nonnull
  @ParametersAreNonnullByDefault
  private BackfillResult merge(final BackfillResult fromDao, final BackfillResult... fromGms) {
    Map<Urn, BackfillResultEntity> urnToEntityMap = new LinkedHashMap<>();
    if (fromDao != null) {
      fromDao.getEntities().forEach(backfillResultEntity -> {
        urnToEntityMap.put(backfillResultEntity.getUrn(), backfillResultEntity);
      });
    }

    if (fromGms != null) {
      for (BackfillResult backfillResult : fromGms) {
        if (backfillResult == null || !backfillResult.hasEntities()) {
          log.error("Encountered a null or empty backfill result: " + backfillResult);
          continue;
        }
        backfillResult.getEntities().forEach(backfillResultEntity -> {
          Urn urn = backfillResultEntity.getUrn();

          if (urnToEntityMap.containsKey(urn)) {
            urnToEntityMap.get(urn).getAspects().addAll(backfillResultEntity.getAspects());
          } else {
            urnToEntityMap.put(urn,
                new BackfillResultEntity().setUrn(urn).setAspects(backfillResultEntity.getAspects()));
          }
        });
      }
    }
    return new BackfillResult().setEntities(new BackfillResultEntityArray(urnToEntityMap.values()));
  }

  @Nonnull
  private BackfillResult backfillWithDefault(@Nonnull final Set<URN> urns, @Nonnull final String entityType) {
    try {
      List<BackfillResult> backfillResults = getAspectRoutingGmsClientManager().getRegisteredRoutingGmsClients()
          .stream()
          .filter(baseAspectRoutingGmsClient -> entityType.equals(baseAspectRoutingGmsClient.getEntityType()))
          .map(baseAspectRoutingGmsClient -> baseAspectRoutingGmsClient.backfill(urns))
          .collect(Collectors.toList());
      return merge(null, backfillResults.toArray(new BackfillResult[0]));
    } catch (Exception exception) {
      log.error(String.format("Couldn't backfill routing entities: %s",
          String.join(",", urns.stream().map(Urn::toString).collect(Collectors.toSet()))), exception);

      BackfillResultEntityArray backfillResultEntityArray = new BackfillResultEntityArray();
      for (Urn urn : urns) {
        BackfillResultEntity backfillResultEntity = new BackfillResultEntity();
        backfillResultEntity.setUrn(urn);
        backfillResultEntity.setAspects(new StringArray());
        backfillResultEntityArray.add(backfillResultEntity);
      }
      return new BackfillResult().setEntities(backfillResultEntityArray);
    }
  }

  @Nullable
  private List<? extends RecordTemplate> getValueFromRoutingGms(@Nonnull URN urn,
      Collection<Class<? extends RecordTemplate>> routeAspectClasses) {
    return routeAspectClasses.stream().map(routeAspectClass -> {
      try {
        return getAspectRoutingGmsClientManager().getRoutingGmsClient(routeAspectClass).get(urn);
      } catch (Exception e) {
        String logMessage = String.format("Couldn't find routing aspect %s for %s", routeAspectClass.getSimpleName(), urn);
        if (e instanceof RestLiServiceException && ((RestLiServiceException) e).getStatus() == S_404_NOT_FOUND) {
         log.warn(logMessage, e);
        } else {
          log.error(logMessage, e);
        }
        return null;
      }
    }).filter(Objects::nonNull).collect(Collectors.toList());
  }

  /**
   * This method routes the update request to the appropriate custom API for pre-ingestion processing.
   * @param urn the urn of the asset
   * @param aspect the new aspect value
   * @param registry the aspect callback registry
   * @return the updated aspect
   */
  private RecordTemplate aspectCallbackHelper(URN urn, RecordTemplate aspect, AspectCallbackRegistry registry,
      IngestionParams ingestionParams, AuditStamp auditStamp) {
    AspectCallbackRoutingClient preUpdateClient = registry.getAspectCallbackRoutingClient(aspect.getClass(), urn.getEntityType());
    AspectCallbackResponse aspectCallbackResponse = preUpdateClient.routeAspectCallback(urn, aspect, null, ingestionParams, auditStamp);
    return aspectCallbackResponse.getUpdatedAspect();
  }
}
