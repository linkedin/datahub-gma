package com.linkedin.metadata.restli;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.utils.ModelUtils;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * An immutable class to store and access the mapping from the entity type string to the entity's LocalDao.
 */
public class LocalDaoRegistry {

  /**
   * Map where key is the string of an entity type defined in Urn class, and value is the {@link BaseLocalDAO}
   * registered on that entity.
   */
  private final Map<String, BaseLocalDAO<? extends UnionTemplate, ? extends Urn>> entityToLocalDaoMap;

  /**
   * Constructs a LocalDaoRegistry with a mapping of entity types to their respective BaseLocalDAOs. This constructor
   * ensures the consistency between the entity types specified in the keys of the provided map and the types expected
   * by the URN class of each BaseLocalDAO. If any inconsistency is detected (i.e., the key does not match the expected
   * entity type from the DAO's URN class), an IllegalArgumentException is thrown.
   *
   * @param entityToLocalDaoMap A non-null map associating entity type strings with their
   *                            corresponding BaseLocalDAO instances.
   * @throws IllegalArgumentException if there is a mismatch between an entity type key and its
   *                                  expected entity type from the DAO's URN class.
   */
  public LocalDaoRegistry(@Nonnull Map<String, BaseLocalDAO<? extends UnionTemplate, ? extends Urn>> entityToLocalDaoMap) {
    entityToLocalDaoMap.forEach((key, value) -> {
      Class<? extends Urn> urnClass = value.getUrnClass();
      if (urnClass == null) {
        throw new IllegalStateException("urnClass is null for localDao: " + value.getClass().getName());
      }
      String expectedEntityType = ModelUtils.getEntityTypeFromUrnClass(urnClass);
      if (!key.equals(expectedEntityType)) {
        throw new IllegalArgumentException(
            String.format("provided entity type: %s is not the same as defined in localDao: %s", key, expectedEntityType));
      }
    });
    this.entityToLocalDaoMap = entityToLocalDaoMap;
  }

  /**
   * Returns the {@link BaseLocalDAO} registered on the given entity type.
   *
   * @param entity the entity type string
   * @return the {@link BaseLocalDAO} registered on the given entity type, or null if no such registration.
   */
  @Nullable
  public BaseLocalDAO<? extends UnionTemplate, ? extends Urn> getLocalDaoByEntity(@Nonnull String entity) {
    return entityToLocalDaoMap.get(entity);
  }
}
