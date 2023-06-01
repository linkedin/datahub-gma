package com.linkedin.metadata.restli;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.metadata.dao.AspectKey;
import com.linkedin.metadata.dao.utils.ModelUtils;
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
import static com.linkedin.metadata.restli.RestliConstants.*;


/**
 * Extends {@link BaseBrowsableEntityResource} with aspect routing capability.
 * For certain aspect of an entity, incoming request will be routed to different GMS.
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
    ROUTING_ASPECT extends RecordTemplate>
    // @formatter:on
    extends BaseBrowsableEntityResource<KEY, VALUE, URN, SNAPSHOT, ASPECT_UNION, DOCUMENT> {

  // strong-typed (ROUTING_ASPECT) _routingAspectClass has been deprecated, using generic _routingAspectClasses instead
  @Deprecated
  private final Class<ROUTING_ASPECT> _routingAspectClass;
  private final Class<VALUE> _valueClass;
  private final Class<ASPECT_UNION> _aspectUnionClass;
  private final Class<SNAPSHOT> _snapshotClass;

  // strong-typed (ROUTING_ASPECT) constructor has been deprecated, using constructors with Set<Class> routingAspects instead
  @Deprecated
  public BaseAspectRoutingResource(@Nonnull Class<SNAPSHOT> snapshotClass,
      @Nonnull Class<ASPECT_UNION> aspectUnionClass, @Nonnull Class<ROUTING_ASPECT> routingAspect,
      @Nonnull Class<VALUE> valueClass) {
    super(snapshotClass, aspectUnionClass);
    _routingAspectClass = routingAspect;
    _valueClass = valueClass;
    _aspectUnionClass = aspectUnionClass;
    _snapshotClass = snapshotClass;
  }


  // strong-typed (ROUTING_ASPECT) constructor has been deprecated, using constructors with Set<Class> routingAspects instead
  @Deprecated
  public BaseAspectRoutingResource(@Nonnull Class<SNAPSHOT> snapshotClass,
      @Nonnull Class<ASPECT_UNION> aspectUnionClass, @Nonnull Class<URN> urnClass,
      @Nonnull Class<ROUTING_ASPECT> routingAspect, @Nonnull Class<VALUE> valueClass) {
    super(snapshotClass, aspectUnionClass, urnClass);
    _routingAspectClass = routingAspect;
    _valueClass = valueClass;
    _aspectUnionClass = aspectUnionClass;
    _snapshotClass = snapshotClass;
  }

  public BaseAspectRoutingResource(@Nonnull Class<SNAPSHOT> snapshotClass,
      @Nonnull Class<ASPECT_UNION> aspectUnionClass,
      @Nonnull Class<VALUE> valueClass) {
    super(snapshotClass, aspectUnionClass);
    _valueClass = valueClass;
    _aspectUnionClass = aspectUnionClass;
    _snapshotClass = snapshotClass;
    _routingAspectClass = null;
  }

  public BaseAspectRoutingResource(@Nonnull Class<SNAPSHOT> snapshotClass,
      @Nonnull Class<ASPECT_UNION> aspectUnionClass, @Nonnull Class<URN> urnClass,
      @Nonnull Class<VALUE> valueClass, @Nullable Object dummyObject) {
    super(snapshotClass, aspectUnionClass, urnClass);
    // "dummyObject" the dummyObject is used to avoid the conflict with the two deprecated constructors
    // TODO(yanyang) clean up dummyObject when removing the deprecated constructors
    _valueClass = valueClass;
    _aspectUnionClass = aspectUnionClass;
    _snapshotClass = snapshotClass;
    _routingAspectClass = null;
  }

  /**
   * Get routing aspect field name in the entity.
   * @return Routing aspect field name.
   */
  @Deprecated
  public abstract String getRoutingAspectFieldName();

  /**
   * Get the client of GMS that routing aspect will be routed to.
   * @return A client of the GMS for routing aspect.
   */
  @Deprecated
  public abstract BaseAspectRoutingGmsClient getGmsClient();

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

    return RestliUtils.toTask(() -> {
      final Set<Class<? extends RecordTemplate>> aspectClasses = parseAspectsParam(aspectNames);

      // Get entity from aspect GMS
      if (containsRoutingAspect(aspectClasses) && aspectClasses.size() == 1) {
        return merge(null, getRoutingAspects(toUrn(id), aspectClasses));
      }

      // The assumption is main GMS must have this entity.
      // If entity only has routing aspect, resourceNotFoundException will be thrown.
      if (!getLocalDAO().exists(toUrn(id))) {
        throw RestliUtils.resourceNotFoundException();
      }

      // Get entity from local DAO
      if (!containsRoutingAspect(aspectClasses)) {
        return getValueFromLocalDao(id, aspectClasses);
      }

      // Need to read from both aspect GMS and local DAO.
      final VALUE valueFromLocalDao = getValueFromLocalDao(id, getNonRoutingAspects(aspectClasses));
      return merge(valueFromLocalDao, getRoutingAspects(toUrn(id), aspectClasses));
    });
  }

  /**
   * An action method for getting a snapshot of aspects for an entity.
   */
  @Action(name = ACTION_GET_SNAPSHOT)
  @Nonnull
  @Override
  public Task<SNAPSHOT> getSnapshot(@ActionParam(PARAM_URN) @Nonnull String urnString,
      @ActionParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {

    return RestliUtils.toTask(() -> {
      final URN urn = parseUrnParam(urnString);
      final Set<Class<? extends RecordTemplate>> aspectClasses = parseAspectsParam(aspectNames);

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

  /**
   * An action method for emitting MAE backfill messages for a set of entities.
   */
  @Action(name = ACTION_BACKFILL_WITH_URNS)
  @Nonnull
  public Task<BackfillResult> backfill(@ActionParam(PARAM_URNS) @Nonnull String[] urns,
      @ActionParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {

    return RestliUtils.toTask(() -> {
      final Set<URN> urnSet = Arrays.stream(urns).map(this::parseUrnParam).collect(Collectors.toSet());
      final Set<Class<? extends RecordTemplate>> aspectClasses = parseAspectsParam(aspectNames);
      Map<URN, Map<Class<? extends RecordTemplate>, java.util.Optional<? extends RecordTemplate>>> urnToAspect =
          new HashMap<>();

      if (!containsRoutingAspect(aspectClasses)) {
        // Backfill only needs local DAO.
        return RestliUtils.buildBackfillResult(getLocalDAO().backfill(aspectClasses, urnSet));
      }

      if (containsRoutingAspect(aspectClasses) && aspectClasses.size() == 1) {
        // Backfill only needs aspect GMS
        return backfillWithDefault(urnSet);
      }

      // Backfill needs both aspect GMS and local DAO.
      BackfillResult localDaoBackfillResult =
          RestliUtils.buildBackfillResult(getLocalDAO().backfill(getNonRoutingAspects(aspectClasses), urnSet));
      BackfillResult gmsBackfillResult = backfillWithDefault(urnSet);
      return merge(localDaoBackfillResult, gmsBackfillResult);
    });
  }

  @Nonnull
  @Override
  protected Task<Void> ingestInternal(@Nonnull SNAPSHOT snapshot,
      @Nonnull Set<Class<? extends RecordTemplate>> aspectsToIgnore) {
    return RestliUtils.toTask(() -> {
      final URN urn = (URN) ModelUtils.getUrnFromSnapshot(snapshot);
      final AuditStamp auditStamp = getAuditor().requestAuditStamp(getContext().getRawRequestContext());
      ModelUtils.getAspectsFromSnapshot(snapshot).forEach(aspect -> {
        if (!aspectsToIgnore.contains(aspect.getClass())) {
          if (isLegacyRoutingLogic()) {
            if (aspect.getClass().equals(_routingAspectClass)) {
              try {
                getGmsClient().ingest(urn, aspect);
              } catch (Exception exception) {
                log.error(
                    String.format("Couldn't ingest routing aspect %s for %s", _routingAspectClass.getSimpleName(), urn),
                    exception);
              }
            } else {
              getLocalDAO().add(urn, aspect, auditStamp);
            }
          } else {
            // If using generic aspect routing logic
            if (getAspectRoutingGmsClientManager().hasRegistered(aspect.getClass())) {
              try {
                getAspectRoutingGmsClientManager().getRoutingGmsClient(aspect.getClass()).ingest(urn, aspect);

              } catch (Exception exception) {
                log.error(
                    String.format("Couldn't ingest routing aspect %s for %s", aspect.getClass().getSimpleName(), urn),
                    exception);
              }
            } else {
              getLocalDAO().add(urn, aspect, auditStamp);
            }
          }
        }
      });
      return null;
    });
  }

  /**
   * Detect if using legacy routing logic by checking if _routingAspectClass is assigned.
   * TODO(yanyang) all the logic using this logic should be removed after the migration
   */
  private boolean isLegacyRoutingLogic() {
    return _routingAspectClass != null;
  }

  /**
   * Whether given set of aspect classes contains routing aspect class.
   * @param aspectClasses A set of aspect classes
   * @return True if aspectClasses contains routing aspect class.
   */
  private boolean containsRoutingAspect(Set<Class<? extends RecordTemplate>> aspectClasses) {
    if (isLegacyRoutingLogic()) {
      return aspectClasses.stream().anyMatch(aspectClass -> aspectClass.equals(_routingAspectClass));
    } else {
      return aspectClasses.stream().anyMatch(aspectClass -> getAspectRoutingGmsClientManager().hasRegistered(aspectClass));
    }
  }

  /**
   * Get non-routing aspects from aspectClasses.
   */
  @Nonnull
  @ParametersAreNonnullByDefault
  private Set<Class<? extends RecordTemplate>> getNonRoutingAspects(
      Set<Class<? extends RecordTemplate>> aspectClasses) {
    if (isLegacyRoutingLogic()) {
      return aspectClasses.stream()
          .filter(aspectClass -> !aspectClass.equals(_routingAspectClass))
          .collect(Collectors.toSet());
    } else {
      return aspectClasses.stream()
          .filter(aspectClass -> !getAspectRoutingGmsClientManager().hasRegistered(aspectClass))
          .collect(Collectors.toSet());
    }
  }

  /**
   * Get routing aspects from aspectClasses.
   */
  @Nonnull
  @ParametersAreNonnullByDefault
  private Set<Class<? extends RecordTemplate>> getRoutingAspects(Set<Class<? extends RecordTemplate>> aspectClasses) {
    if (isLegacyRoutingLogic()) {
      return aspectClasses.stream()
          .filter(aspectClass -> aspectClass.equals(_routingAspectClass))
          .collect(Collectors.toSet());
    } else {
      return aspectClasses.stream()
          .filter(aspectClass -> getAspectRoutingGmsClientManager().hasRegistered(aspectClass))
          .collect(Collectors.toSet());
    }
  }

  /**
   * Get entity value from local DAO decorated with specified aspect classes.
   * @param id identifier of the entity.
   * @param aspectClasses Aspects to be decorated on the entity
   * @return Entity decorated with specified aspect classes.
   */
  @Nonnull
  @ParametersAreNonnullByDefault
  private VALUE getValueFromLocalDao(KEY id, Set<Class<? extends RecordTemplate>> aspectClasses) {
    final URN urn = toUrn(id);
    final VALUE value = getInternal(Collections.singleton(urn), aspectClasses).get(urn);
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
   * Get aspect value from routing aspect GMS.
   */
  @Nonnull
  @ParametersAreNonnullByDefault
  private List<ASPECT_UNION> getAspectsFromGms(URN urn, Class aspectClass) {
    final List<? extends RecordTemplate> routingAspects =
        getRoutingAspects(urn, Collections.singletonList(aspectClass));
    if (routingAspects.isEmpty()) {
      return new ArrayList<>();
    }
    return Collections.singletonList(ModelUtils.newAspectUnion(_aspectUnionClass, routingAspects.get(0)));
  }

  /**
   * Merge routing aspect value from GMS into entity value retrieved from Local DAO.
   * @param valueFromLocalDao Entity value retrieved from Local DAO
   * @param routingAspects Aspect values retrieved from GMS
   * @return Merged entity value which will contain routing aspect value
   */
  @Nonnull
  private VALUE merge(@Nullable VALUE valueFromLocalDao, @Nullable List<? extends RecordTemplate> routingAspects) {
    //final String setterMethodName = "set" + getRoutingAspectFieldName();
    if (valueFromLocalDao == null) {
      try {
        valueFromLocalDao = _valueClass.getDeclaredConstructor().newInstance();
      } catch (Exception e) {
        throw new RestLiServiceException(HttpStatus.S_500_INTERNAL_SERVER_ERROR,
            "Failed to create new instance of class " + _valueClass.getCanonicalName(), e);
      }
    }
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
  private BackfillResult backfillWithDefault(@Nonnull final Set<URN> urns) {
    try {
      if (isLegacyRoutingLogic()) {
        return getGmsClient().backfill(urns);
      } else {
        List<BackfillResult> backfillResults = getAspectRoutingGmsClientManager().getRegisteredRoutingGmsClients()
            .stream()
            .map(baseAspectRoutingGmsClient -> baseAspectRoutingGmsClient.backfill(urns))
            .collect(Collectors.toList());
        return merge(null, backfillResults.toArray(new BackfillResult[0]));
      }
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
  private List<? extends RecordTemplate> getRoutingAspects(@Nonnull URN urn,
      Collection<Class<? extends RecordTemplate>> routeAspectClasses) {
    return routeAspectClasses.stream().map(routeAspectClass -> {

      try {
        if (isLegacyRoutingLogic()) {
          return getGmsClient().get(urn);
        } else {
          return getAspectRoutingGmsClientManager().getRoutingGmsClient(routeAspectClass).get(urn);
        }
      } catch (Exception exception) {
        log.error(String.format("Couldn't get routing aspect %s for %s", routeAspectClass.getSimpleName(), urn),
            exception);
        return null;
      }
    }).filter(Objects::nonNull).collect(Collectors.toList());
  }
}
