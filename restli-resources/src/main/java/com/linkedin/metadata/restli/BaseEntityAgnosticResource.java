package com.linkedin.metadata.restli;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.metadata.backfill.BackfillMode;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.events.IngestionMode;
import com.linkedin.restli.client.RestLiResponseException;
import com.linkedin.restli.common.ErrorResponse;
import com.linkedin.restli.server.annotations.Action;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.linkedin.metadata.dao.utils.IngestionUtils.*;
import static com.linkedin.metadata.restli.RestliConstants.*;


/**
 * An base restli resource class with operations that are not tied to a specific entity.
 */
public abstract class BaseEntityAgnosticResource {

  /**
   * Returns the {@link LocalDaoRegistry} that contains the mapping from entity type to {@link BaseLocalDAO}.
   * @return {@link LocalDaoRegistry}
   */
  @Nonnull
  protected abstract LocalDaoRegistry getLocalDaoRegistry();

  /**
   * Backfill MAE for the given entity, urns, aspects and the ingestion mode. Only registered and present aspects
   * in database table will be backfilled.
   *
   * @param entity the entity type string, should be identical to the entity type defined in the URN class
   * @param urns the set of urns to backfill
   * @param aspects the set of aspects to backfill, each item should be the FQCN of the aspect class. If null, all
   *                registered entity aspects will be used.
   * @param ingestionMode {@link IngestionMode} to indicate the processing strategy. Live mode together with no-change
   *                                           should represent no-op, empty map will be returned. Backfill is to redo
   *                                           any metadata update that is missed or skipped in the past.
   *                                           Bootstrap indicates building the metadata from scratch.
   * @return a map of urn to the set of aspects that are backfilled
   * @throws RestLiResponseException for any error
   */
  @Action(name = ACTION_BACKFILL_MAE)
  @Nonnull
  public Map<String, Set<String>> backfillMAE(
      @Nonnull String entity,
      @Nonnull Set<String> urns,
      @Nullable Set<String> aspects,
      @Nonnull IngestionMode ingestionMode
  ) throws RestLiResponseException {
    final BaseLocalDAO<? extends UnionTemplate, ? extends Urn> dao = getLocalDaoByEntity(entity);
    final BackfillMode backfillMode = ALLOWED_INGESTION_BACKFILL_BIMAP.get(ingestionMode);
    if (backfillMode == null) {
      return Collections.emptyMap();
    }
    return dao.backfillMAE(backfillMode, aspects, urns);
  }

  protected BaseLocalDAO<? extends UnionTemplate, ? extends Urn> getLocalDaoByEntity(String entity)
      throws RestLiResponseException {
    LocalDaoRegistry localDaoRegistry = getLocalDaoRegistry();
    BaseLocalDAO<? extends UnionTemplate, ? extends Urn> dao = localDaoRegistry.getLocalDaoByEntity(entity);
    if (dao == null) {
      throw new RestLiResponseException(
          new ErrorResponse()
              .setStatus(404)
              .setMessage(String.format("Dao not found for the requested entity: %s", entity))
      );
    }
    return dao;
  }
}
