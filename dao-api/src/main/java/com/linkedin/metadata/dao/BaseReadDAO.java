package com.linkedin.metadata.dao;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.metadata.dao.exception.InvalidMetadataType;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.validator.AspectValidator;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;


public abstract class BaseReadDAO<ASPECT_UNION extends UnionTemplate> {

  public static final long FIRST_VERSION = 0;
  public static final long LATEST_VERSION = 0;

  // A set of pre-computed valid metadata types
  private final Set<Class<? extends RecordTemplate>> _validMetadataAspects;

  public BaseReadDAO(@Nonnull Class<ASPECT_UNION> aspectUnionClass) {
    AspectValidator.validateAspectUnionSchema(aspectUnionClass);

    _validMetadataAspects = ModelUtils.getValidAspectTypes(aspectUnionClass);
  }

  public BaseReadDAO(@Nonnull Set<Class<? extends RecordTemplate>> aspects) {
    _validMetadataAspects = aspects;
  }

  /**
   * Batch retrieves metadata aspects using multiple {@link AspectKey}s.
   *
   * @param keys set of keys for the metadata to retrieve
   * @return a mapping of given keys to the corresponding metadata aspect.
   */
  @Nonnull
  public abstract Map<AspectKey<? extends RecordTemplate>, Optional<? extends RecordTemplate>> get(
      @Nonnull Set<AspectKey<? extends RecordTemplate>> keys);

  /**
   * Similar to {@link #get(Set)} but only using only one {@link AspectKey}.
   */
  @Nonnull
  public <ASPECT extends RecordTemplate> Optional<ASPECT> get(@Nonnull AspectKey<ASPECT> key) {
    return (Optional<ASPECT>) get(Collections.singleton(key)).get(key);
  }

  /**
   * Similar to {@link #get(AspectKey)} but with each component of the key broken out as arguments.
   */
  @Nonnull
  public <ASPECT extends RecordTemplate> Optional<ASPECT> get(@Nonnull Class<ASPECT> aspectClass, @Nonnull Urn urn,
      long version) {
    return get(new AspectKey<>(aspectClass, urn, version));
  }

  /**
   * Similar to {@link #get(Class, Urn, long)} but always retrieves the latest version.
   */
  @Nonnull
  public <ASPECT extends RecordTemplate> Optional<ASPECT> get(@Nonnull Class<ASPECT> aspectClass, @Nonnull Urn urn) {
    return get(aspectClass, urn, LATEST_VERSION);
  }

  /**
   * Similar to {@link #get(Class, Urn)} but retrieves multiple aspects latest versions associated with multiple URNs.
   *
   * <p>The returned {@link Map} contains all the .
   */
  @Nonnull
  public Map<Urn, Map<Class<? extends RecordTemplate>, Optional<? extends RecordTemplate>>> get(
      @Nonnull Set<Class<? extends RecordTemplate>> aspectClasses, @Nonnull Set<Urn> urns) {


    final Set<AspectKey<? extends RecordTemplate>> keys = new HashSet<>();
    for (Urn urn : urns) {
      for (Class<? extends RecordTemplate> aspect : aspectClasses) {
        keys.add(new AspectKey<>(aspect, urn, LATEST_VERSION));
      }
    }

    final Map<Urn, Map<Class<? extends RecordTemplate>, Optional<? extends RecordTemplate>>> results = new HashMap<>();
    get(keys).entrySet().forEach(entry -> {
      final AspectKey<? extends RecordTemplate> key = entry.getKey();
      final Urn urn = key.getUrn();
      results.putIfAbsent(urn, new HashMap<>());
      results.get(urn).put(key.getAspectClass(), entry.getValue());
    });

    return results;
  }

  /**
   * Similar to {@link #get(Set, Set)} but only for one URN.
   */
  @Nonnull
  public Map<Class<? extends RecordTemplate>, Optional<? extends RecordTemplate>> get(
      @Nonnull Set<Class<? extends RecordTemplate>> aspectClasses, @Nonnull Urn urn) {

    Map<Urn, Map<Class<? extends RecordTemplate>, Optional<? extends RecordTemplate>>> results =
        get(aspectClasses, Collections.singleton(urn));
    if (!results.containsKey(urn)) {
      throw new IllegalStateException("Results should contain " + urn);
    }

    return results.get(urn);
  }

  /**
   * Similar to {@link #get(Set, Set)} but only for one aspect.
   */
  @Nonnull
  public <ASPECT extends RecordTemplate> Map<Urn, Optional<ASPECT>> get(
      @Nonnull Class<ASPECT> aspectClass, @Nonnull Set<Urn> urns) {

    return get(Collections.singleton(aspectClass), urns).entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> (Optional<ASPECT>) entry.getValue().get(aspectClass)));
  }

  protected void checkValidAspect(@Nonnull Class<? extends RecordTemplate> aspectClass) {
    if (!_validMetadataAspects.contains(aspectClass)) {
      throw new InvalidMetadataType(aspectClass + " is not a supported metadata aspect type");
    }
  }

  protected void checkValidAspects(@Nonnull Set<Class<? extends RecordTemplate>> aspectClasses) {
    aspectClasses.forEach(aspectClass -> checkValidAspect(aspectClass));
  }
}
