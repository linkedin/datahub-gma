package com.linkedin.metadata.restli;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.metadata.backfill.BackfillMode;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.events.IngestionMode;
import com.linkedin.restli.client.RestLiResponseException;
import com.linkedin.restli.common.ErrorResponse;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.linkedin.metadata.dao.utils.IngestionUtils.*;


/**
 * An base class that has entity agnostic operations such as backfill.
 */
public abstract class BaseEntityAgnosticResource {

  @Nonnull
  protected abstract LocalDaoRegistry getLocalDaoRegistry();

  public Map<String, Set<String>> backfillMAE(
      @Nonnull String entity,
      @Nonnull Set<String> urns,
      @Nullable Set<String> aspects,
      @Nonnull IngestionMode ingestionMode
  ) throws RestLiResponseException {
    LocalDaoRegistry localDaoRegistry = getLocalDaoRegistry();
    BaseLocalDAO<? extends UnionTemplate, ? extends Urn> dao = localDaoRegistry.getLocalDaoByEntity(entity);
    if (dao == null) {
      throw new RestLiResponseException(
        new ErrorResponse()
          .setStatus(404)
          .setMessage(String.format("Dao not found for the requested entity: %s", entity))
      );
    }

    BackfillMode backfillMode = ALLOWED_INGESTION_BACKFILL_BIMAP.get(ingestionMode);
    if (backfillMode == null) {
      throw new RestLiResponseException(
          new ErrorResponse()
              .setStatus(400)
              .setMessage(String.format("Ingestion mode has illegal value: %s", ingestionMode))
      );
    }

    return dao.backfillMAE(backfillMode, aspects, urns);
  }
}
