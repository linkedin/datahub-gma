package com.linkedin.metadata.restli.dao;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.metadata.dao.BaseLocalDAO;
import javax.annotation.Nonnull;


/**
 * An interface to store and access the mapping from the entity type string to the entity's LocalDao.
 */
public interface LocalDaoRegistry {

  /**
   * Returns the {@link BaseLocalDAO} registered on the given entity type.
   *
   * @param entity the entity type string
   * @return the {@link BaseLocalDAO} registered on the given entity type, or null if no such registration.
   */
  BaseLocalDAO<? extends UnionTemplate, ? extends Urn> getLocalDaoByEntity(@Nonnull String entity);
}

