package com.linkedin.metadata.restli;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.metadata.dao.AspectKey;
import com.linkedin.metadata.dao.utils.RecordUtils;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.linkedin.metadata.dao.BaseReadDAO.*;

/**
 * A base class for the single aspect entity rest.li resource that supports CRUD + search methods.
 *
 * <p>See http://go/gma for more details.
 *
 * @param <KEY> the resource's key type
 * @param <VALUE> the resource's value type
 * @param <URN> must be a valid {@link Urn} type for the snapshot
 * @param <ASPECT> must be a valid aspect of VALUE type
 * @param <ASPECT_UNION> must be a valid aspect union type supported by the snapshot
 * @param <SNAPSHOT> must be a valid snapshot type defined in com.linkedin.metadata.snapshot
 * @param <DOCUMENT> must be a valid search document type defined in com.linkedin.metadata.search
 * @param <INTERNAL_SNAPSHOT> must be a valid internal snapshot type defined in com.linkedin.metadata.snapshot
 * @param <INTERNAL_ASPECT_UNION> must be a valid internal aspect union type supported by the internal snapshot
 * @param <ASSET> must be a valid asset type defined in com.linkedin.metadata.asset
 */
public abstract class BaseSingleAspectSearchableEntityResource<
    // @formatter:off
    KEY,
    VALUE extends RecordTemplate,
    URN extends Urn,
    ASPECT extends RecordTemplate,
    ASPECT_UNION extends UnionTemplate,
    SNAPSHOT extends RecordTemplate,
    DOCUMENT extends RecordTemplate,
    ASSET extends RecordTemplate,
    INTERNAL_SNAPSHOT extends RecordTemplate,
    INTERNAL_ASPECT_UNION extends UnionTemplate>
    // @formatter:on
    extends
    BaseSearchableEntityResource<KEY, VALUE, URN, SNAPSHOT, ASPECT_UNION, DOCUMENT, INTERNAL_SNAPSHOT, INTERNAL_ASPECT_UNION, ASSET> {

  private final Class<ASPECT> _aspectClass;
  private final Class<VALUE> _valueClass;

  /**
   * Constructor.
   * */
  public BaseSingleAspectSearchableEntityResource(@Nonnull Class<ASPECT> aspectClass,
      @Nullable Class<ASPECT_UNION> aspectUnionClass, @Nonnull Class<VALUE> valueClass,
      @Nullable Class<SNAPSHOT> snapshotClass, @Nonnull Class<INTERNAL_SNAPSHOT> internalSnapshotClass,
      @Nonnull Class<INTERNAL_ASPECT_UNION> internalAspectUnionClass, @Nonnull Class<ASSET> assetClass) {

    super(snapshotClass, aspectUnionClass, internalSnapshotClass, internalAspectUnionClass, assetClass);
    _aspectClass = aspectClass;
    _valueClass = valueClass;
  }

  public BaseSingleAspectSearchableEntityResource(@Nonnull Class<ASPECT> aspectClass,
      @Nullable Class<ASPECT_UNION> aspectUnionClass, @Nonnull Class<URN> urnClass, @Nonnull Class<VALUE> valueClass,
      @Nullable Class<SNAPSHOT> snapshotClass, @Nonnull Class<INTERNAL_SNAPSHOT> internalSnapshotClass,
      @Nonnull Class<INTERNAL_ASPECT_UNION> internalAspectUnionClass, @Nonnull Class<ASSET> assetClass) {
    super(snapshotClass, aspectUnionClass, urnClass, internalSnapshotClass, internalAspectUnionClass, assetClass);
    _aspectClass = aspectClass;
    _valueClass = valueClass;
  }

  /**
   * Takes a partial entity created by {@link #createPartialEntityFromAspect(RecordTemplate)} and the urn and
   * creates the complete entity value.
   *
   * @param partialEntity the partial entity.
   * @param urn urn of the entity.
   * @return the complete entity.
   * */
  @Nonnull
  protected abstract VALUE createEntity(@Nonnull VALUE partialEntity, @Nonnull URN urn);

  /**
   * Override {@link BaseEntityResource}'s method to override the default logic of returning entity values
   * for each urn. The base classes assumes that the aspects are fields in the entity value whereas in this class
   * the aspect is included in the value.
   * */
  @Override
  @Nonnull
  protected Map<URN, VALUE> getInternal(@Nonnull Collection<URN> urns,
      @Nonnull Set<Class<? extends RecordTemplate>> aspectClasses, boolean isInternalModelsEnabled) {
    // ignore the second parameter as it is not required for single aspect entities
    return getUrnEntityMapInternal(urns);
  }

  /**
   * Override {@link BaseEntityResource}'s method to override the default logic of returning entity values
   * for each urn. The base classes assumes that the aspects are fields in the entity value whereas in this class
   * the aspect is included in the value.
   * */
  @Override
  @Nonnull
  protected Map<URN, VALUE> getInternalNonEmpty(@Nonnull Collection<URN> urns,
      @Nonnull Set<Class<? extends RecordTemplate>> aspectClasses, boolean isInternalModelsEnabled) {
    // ignore the second parameter as it is not required for single aspect entities
    return getUrnEntityMapInternal(urns);
  }

  @Nonnull
  private Map<URN, VALUE> getUrnEntityMapInternal(@Nonnull Collection<URN> urns) {
    final Set<AspectKey<URN, ? extends RecordTemplate>> aspectKeys = urns.stream()
        .map(urn -> new AspectKey<>(_aspectClass, urn, LATEST_VERSION))
        .collect(Collectors.toSet());

    final Map<AspectKey<URN, ? extends RecordTemplate>, Optional<? extends RecordTemplate>> aspectKeyOptionalAspects =
        getLocalDAO().get(aspectKeys);

    return aspectKeyOptionalAspects.entrySet()
        .stream()
        .filter(entry -> entry.getValue().isPresent())
        .collect(Collectors.toMap(entry -> entry.getKey().getUrn(), entry -> {
          final URN urn = entry.getKey().getUrn();
          @SuppressWarnings("unchecked") ASPECT aspect = (ASPECT) entry.getValue().get();
          return createEntity(createPartialEntityFromAspect(aspect), urn);
        }));
  }

  /**
   * Creates a partial entity value from the aspect. The other fields in the value are set using
   * the {@link #createEntity(ASPECT, URN)} method.
   * */
  @Nonnull
  private VALUE createPartialEntityFromAspect(@Nonnull ASPECT aspect) {
    try {
      // The fields of the aspect are included in the entity value.
      // Hence, the data map of the aspect can be used to set the data map for the entity value.
      final DataMap aspectDataMap = aspect.data().clone();
      return RecordUtils.toRecordTemplate(_valueClass, aspectDataMap);
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Throwing an exception with a `not implemented` error message as this method is only required
   * by parent class {@link BaseEntityResource} method- {@link #getInternalNonEmpty(Collection, Set, boolean)},
   * which has been overridden here.
   * */
  @Override
  @Nonnull
  protected VALUE toValue(@Nonnull SNAPSHOT snapshot) {
    throw new RuntimeException("Not implemented.");
  }

  /**
   * Throwing an exception with a `not implemented` error message as this method is only required
   * by parent class {@link BaseEntityResource} method- {@link #getInternalNonEmpty(Collection, Set, boolean)} (Collection, Set)},
   * which has been overridden here.
   * */
  @Override
  @Nonnull
  protected SNAPSHOT toSnapshot(@Nonnull VALUE value, @Nonnull URN urn) {
    throw new RuntimeException("Not implemented.");
  }
}
