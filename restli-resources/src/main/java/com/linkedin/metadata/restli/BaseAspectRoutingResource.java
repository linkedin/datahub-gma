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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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

  private final Class<ROUTING_ASPECT> _routingAspectClass;
  private final Class<VALUE> _valueClass;
  private final Class<ASPECT_UNION> _aspectUnionClass;
  private final Class<SNAPSHOT> _snapshotClass;

  public BaseAspectRoutingResource(@Nonnull Class<SNAPSHOT> snapshotClass, @Nonnull Class<ASPECT_UNION> aspectUnionClass,
      @Nonnull Class<ROUTING_ASPECT> routingAspect, @Nonnull Class<VALUE> valueClass) {
    super(snapshotClass, aspectUnionClass);
    _routingAspectClass = routingAspect;
    _valueClass = valueClass;
    _aspectUnionClass = aspectUnionClass;
    _snapshotClass = snapshotClass;
  }

  public BaseAspectRoutingResource(@Nonnull Class<SNAPSHOT> snapshotClass, @Nonnull Class<ASPECT_UNION> aspectUnionClass,
      @Nonnull Class<URN> urnClass, @Nonnull Class<ROUTING_ASPECT> routingAspect, @Nonnull Class<VALUE> valueClass) {
    super(snapshotClass, aspectUnionClass, urnClass);
    _routingAspectClass = routingAspect;
    _valueClass = valueClass;
    _aspectUnionClass = aspectUnionClass;
    _snapshotClass = snapshotClass;
  }

  /**
   * Get routing aspect field name in the entity.
   * @return Routing aspect field name.
   */
  public abstract String getRoutingAspectFieldName();

  /**
   * Get the client of GMS that routing aspect will be routed to.
   * @return A client of the GMS for routing aspect.
   */
  public abstract BaseAspectRoutingGmsClient<ROUTING_ASPECT> getGmsClient();

  /**
   * Retrieves the value for an entity that is made up of latest versions of specified aspects.
   */
  @RestMethod.Get
  @Nonnull
  @Override
  public Task<VALUE> get(@Nonnull KEY id,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {

    return RestliUtils.toTask(() -> {
      final Set<Class<? extends RecordTemplate>> aspectClasses = parseAspectsParam(aspectNames);

      // Get entity from aspect GMS
      if (containsRoutingAspect(aspectClasses) && aspectClasses.size() == 1) {
        return merge(null, getRoutingAspect(toUrn(id)));
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
      final VALUE valueFromLocalDao = getValueFromLocalDao(id, removeRoutingAspect(aspectClasses));
      return merge(valueFromLocalDao, getRoutingAspect(toUrn(id)));
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
        if (aspectClasses.size() == 1) {
          // Get snapshot from aspect GMS.
          final List<ASPECT_UNION> aspectUnions = getAspectsFromGms(urn);
          return ModelUtils.newSnapshot(_snapshotClass, urn, aspectUnions);
        } else {
          final Set<Class<? extends RecordTemplate>> withoutRoutingAspect = removeRoutingAspect(aspectClasses);
          final List<ASPECT_UNION> aspectsFromGms = getAspectsFromGms(urn);
          final List<ASPECT_UNION> aspectsFromLocalDao = getAspectsFromLocalDao(urn, withoutRoutingAspect);
          return ModelUtils.newSnapshot(_snapshotClass, urn,
              Stream.concat(aspectsFromGms.stream(), aspectsFromLocalDao.stream()).collect(Collectors.toList()));
        }
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
      Map<URN, Map<Class<? extends RecordTemplate>, java.util.Optional<? extends RecordTemplate>>> urnToAspect = new HashMap<>();

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
          RestliUtils.buildBackfillResult(getLocalDAO().backfill(removeRoutingAspect(aspectClasses), urnSet));
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
          if (aspect.getClass().equals(_routingAspectClass)) {
            try {
              getGmsClient().ingest(urn, (ROUTING_ASPECT) aspect);
            } catch (Exception exception) {
              log.error(String.format("Couldn't ingest routing aspect %s for %s", _routingAspectClass.getSimpleName(), urn), exception);
            }
          } else {
            getLocalDAO().add(urn, aspect, auditStamp);
          }
        }
      });
      return null;
    });
  }

  /**
   * Whether given set of aspect classes contains routing aspect class.
   * @param aspectClasses A set of aspect classes
   * @return True if aspectClasses contains routing aspect class.
   */
  private boolean containsRoutingAspect(Set<Class<? extends RecordTemplate>> aspectClasses) {
    return aspectClasses.stream().anyMatch(aspectClass -> aspectClass.equals(_routingAspectClass));
  }

  /**
   * Remove the routing aspest class from aspectClasses.
   */
  @Nonnull
  @ParametersAreNonnullByDefault
  private Set<Class<? extends RecordTemplate>> removeRoutingAspect(Set<Class<? extends RecordTemplate>> aspectClasses) {
    return aspectClasses.stream().filter(aspectClass -> !aspectClass.equals(_routingAspectClass)).collect(Collectors.toSet());
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
    final Set<AspectKey<URN, ? extends RecordTemplate>> keys = aspectClasses.stream()
        .map(aspectClass -> new AspectKey<>(aspectClass, urn, LATEST_VERSION))
        .collect(Collectors.toSet());

    return getLocalDAO().get(keys).values().stream()
        .filter(java.util.Optional::isPresent)
        .map(aspect -> ModelUtils.newAspectUnion(_aspectUnionClass, aspect.get()))
        .collect(Collectors.toList());
  }

  /**
   * Get aspect value from routing aspect GMS.
   */
  @Nonnull
  @ParametersAreNonnullByDefault
  private List<ASPECT_UNION> getAspectsFromGms(URN urn) {
    final ROUTING_ASPECT routingAspect = getRoutingAspect(urn);
    if (routingAspect == null) {
      return new ArrayList<>();
    }
    return Collections.singletonList(ModelUtils.newAspectUnion(_aspectUnionClass, routingAspect));
  }

  /**
   * Merge routing aspect value from GMS into entity value retrieved from Local DAO.
   * @param valueFromLocalDao Entity value retrieved from Local DAO
   * @param aspectFromGms Aspect value retrieved from GMS
   * @return Merged entity value which will contain routing aspect value
   */
  @Nonnull
  private VALUE merge(@Nullable VALUE valueFromLocalDao, @Nullable ROUTING_ASPECT aspectFromGms) {
    final String setterMethodName = "set" + getRoutingAspectFieldName();

    if (valueFromLocalDao == null) {
      try {
        valueFromLocalDao = _valueClass.getDeclaredConstructor().newInstance();
      } catch (Exception e) {
        throw new RestLiServiceException(HttpStatus.S_500_INTERNAL_SERVER_ERROR,
            "Failed to create new instance of class " + _valueClass.getCanonicalName(), e);
      }
    }

    Method setter = null;
    try {
      setter = valueFromLocalDao.getClass().getMethod(setterMethodName, _routingAspectClass, SetMode.class);
    } catch (NoSuchMethodException e) {
      throw new RestLiServiceException(HttpStatus.S_500_INTERNAL_SERVER_ERROR,
          "Failed to get routing aspect setter method.", e);
    }

    try {
      setter.invoke(valueFromLocalDao, aspectFromGms, SetMode.IGNORE_NULL);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new RestLiServiceException(HttpStatus.S_500_INTERNAL_SERVER_ERROR, "Failed to set routing aspect.", e);
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
  private BackfillResult merge(final BackfillResult fromDao, final BackfillResult fromGms) {
    Map<Urn, BackfillResultEntity> urnToEntityMap = new LinkedHashMap<>();
    fromDao.getEntities().forEach(backfillResultEntity -> {
      urnToEntityMap.put(backfillResultEntity.getUrn(), backfillResultEntity);
    });

    fromGms.getEntities().forEach(backfillResultEntity -> {
      Urn urn = backfillResultEntity.getUrn();

      if (urnToEntityMap.containsKey(urn)) {
        urnToEntityMap.get(urn).getAspects().addAll(backfillResultEntity.getAspects());
      } else {
        urnToEntityMap.put(urn, new BackfillResultEntity().setUrn(urn).setAspects(backfillResultEntity.getAspects()));
      }
    });

    return new BackfillResult().setEntities(new BackfillResultEntityArray(urnToEntityMap.values()));
  }

  @Nonnull
  private BackfillResult backfillWithDefault(@Nonnull final Set<URN> urns) {
    try {
      return getGmsClient().backfill(urns);
    } catch (Exception exception) {
      log.error(String.format("Couldn't backfill routing aspect %s for %s", _routingAspectClass.getSimpleName(),
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
  private ROUTING_ASPECT getRoutingAspect(@Nonnull URN urn) {
    try {
      return getGmsClient().get(urn);
    } catch (Exception exception) {
      log.error(String.format("Couldn't get routing aspect %s for %s", _routingAspectClass.getSimpleName(), urn), exception);
      return null;
    }
  }
}
