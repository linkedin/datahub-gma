package com.linkedin.metadata.restli;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.parseq.Task;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.RestLiServiceException;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.QueryParam;
import com.linkedin.restli.server.annotations.RestMethod;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.linkedin.metadata.restli.RestliConstants.*;


/**
 * Extends {@link BaseBrowsableEntityResource} with aspect routing capability.
 * For certain aspect of an entity, incoming request will be routed to different GMS.
 * See http://go/aspect-routing for more details
 */
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

  private Class<ROUTING_ASPECT> _routingAspectClass;
  private Class<VALUE> _valueClass;

  public BaseAspectRoutingResource(@Nonnull Class<SNAPSHOT> snapshotClass, @Nonnull Class<ASPECT_UNION> aspectUnionClass,
      @Nonnull Class<ROUTING_ASPECT> routingAspect, @Nonnull Class<VALUE> valueClass) {
    super(snapshotClass, aspectUnionClass);
    _routingAspectClass = routingAspect;
    _valueClass = valueClass;
  }

  public BaseAspectRoutingResource(@Nonnull Class<SNAPSHOT> snapshotClass, @Nonnull Class<ASPECT_UNION> aspectUnionClass,
      @Nonnull Class<URN> urnClass, @Nonnull Class<ROUTING_ASPECT> routingAspect, @Nonnull Class<VALUE> valueClass) {
    super(snapshotClass, aspectUnionClass, urnClass);
    _routingAspectClass = routingAspect;
    _valueClass = valueClass;
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
  public abstract BaseAspectRoutingGmsClient getGmsClient();

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

      // No need to read from Local DAO.
      if (containsRoutingAspect(aspectClasses) && aspectClasses.size() == 1) {
        return merge(null, getGmsClient().get(id));
      }

      // The assumption is main GMS must have this entity.
      // If entity only has routing aspect, resourceNotFoundException will be thrown.
      if (!getLocalDAO().exists(toUrn(id))) {
        throw RestliUtils.resourceNotFoundException();
      }

      if (!containsRoutingAspect(aspectClasses)) {
        return getValueFromLocalDao(id, aspectClasses);
      } else {
        final Set<Class<? extends RecordTemplate>> withoutRoutingAspect =
            aspectClasses.stream().filter(aspectClass -> !aspectClass.equals(_routingAspectClass)).collect(Collectors.toSet());

        final VALUE valueFromLocalDao = getValueFromLocalDao(id, withoutRoutingAspect);

        // TODO: Confirm ownership gms will return null value if there is no ownership aspect for id.
        final ROUTING_ASPECT aspectFromGms = getGmsClient().get(id);

        return merge(valueFromLocalDao, aspectFromGms);
      }
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
   * Get entity value from local DAO decorated with specified aspect classes.
   * @param id identifier of the entity.
   * @param aspectClasses Aspects to be decorated on the entity
   * @return Entity decorated with specified aspect classes.
   */
  private VALUE getValueFromLocalDao(KEY id, Set<Class<? extends RecordTemplate>> aspectClasses) {
    final URN urn = toUrn(id);
    final VALUE value = getInternal(Collections.singleton(urn), aspectClasses).get(urn);
    if (value == null) {
      throw RestliUtils.resourceNotFoundException();
    }
    return value;
  }

  /**
   * Merge routing aspect value from GMS into entity value retrieved from Local DAO.
   * @param valueFromLocalDao Entity value retrieved from Local DAO
   * @param aspectFromGms Aspect value retrieved from GMS
   * @return Merged entity value which will contain routing aspect value
   */
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
}
