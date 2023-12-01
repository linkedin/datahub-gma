package com.linkedin.metadata.restli;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.metadata.backfill.BackfillMode;
import com.linkedin.metadata.backfill.BackfillItem;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.exception.InvalidMetadataType;
import com.linkedin.metadata.events.IngestionMode;
import com.linkedin.metadata.restli.dao.LocalDaoRegistry;
import com.linkedin.restli.server.annotations.Action;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.dao.utils.IngestionUtils.*;
import static com.linkedin.metadata.restli.RestliConstants.*;


/**
 * A base restli resource class with operations that are not tied to a specific entity.
 */
@Slf4j
public abstract class BaseEntityAgnosticResource {

  /**
   * Returns the {@link LocalDaoRegistry} that contains the mapping from entity type to {@link BaseLocalDAO}.
   * @return {@link LocalDaoRegistry}
   */
  @Nonnull
  protected abstract LocalDaoRegistry getLocalDaoRegistry();

  /**
   * Backfill MAE for the given {@link BackfillItem} and the ingestion mode. Only registered and present aspects
   * in database table will be backfilled.
   *
   * @param backfillRequests a list of {@link BackfillItem} to be backfilled
   * @param ingestionMode {@link IngestionMode} to indicate the processing strategy. Live mode together with no-change
   *                                           should represent no-op, empty map will be returned. Backfill is to redo
   *                                           any metadata update that is missed or skipped in the past.
   *                                           Bootstrap indicates building the metadata from scratch.
   * @return a list of {@link BackfillItem} that is backfilled, failed urns and aspects will be filtered out
   */
  @Action(name = ACTION_BACKFILL_MAE)
  @Nonnull
  public List<BackfillItem> backfillMAE(@Nonnull List<BackfillItem> backfillRequests, @Nonnull IngestionMode ingestionMode) {
    final List<BackfillItem> backfillResults = new ArrayList<>();
    final BackfillMode backfillMode = ALLOWED_INGESTION_BACKFILL_BIMAP.get(ingestionMode);
    if (backfillMode == null) {
      return backfillResults;
    }

    // Group requests by entity type
    final Map<String, List<BackfillItem>> entitytoRequestsMap = new HashMap<>();
    backfillRequests.forEach(request -> {
      try {
        final String entity = Urn.createFromString(request.getUrn()).getEntityType();
        entitytoRequestsMap.computeIfAbsent(entity, k -> new ArrayList<>()).add(request);
      } catch (URISyntaxException e) {
        log.warn("Failed casting string to Urn, request: " + request, e);
      }
    });

    // for each entity, backfill MAE for each urn in parallel
    for (String entity : entitytoRequestsMap.keySet()) {
      final Optional<BaseLocalDAO<? extends UnionTemplate, ? extends Urn>> dao = getLocalDaoByEntity(entity);
      if (!dao.isPresent()) {
        log.warn("LocalDAO not found for entity: " + entity);
        continue;
      }
      final List<BackfillItem> items = entitytoRequestsMap.get(entity);
      backfillResults.addAll(
          items.parallelStream()
              // immutable dao, should be thread-safe
              .map(item -> backfillMAEForUrn(item.getUrn(), item.getAspects(), backfillMode, dao.get()).orElse(null))
              .filter(Objects::nonNull)
              .collect(Collectors.toList())
      );
    }
    return backfillResults; // insert order is not guaranteed the same as input
  }

  protected Optional<BackfillItem> backfillMAEForUrn(@Nonnull String urn, @Nonnull List<String> aspectSet,
      @Nonnull BackfillMode backfillMode, @Nonnull BaseLocalDAO<? extends UnionTemplate, ? extends Urn> dao) {
    try {
      // set aspectSetToUse to null if empty to backfill all aspects
      Set<String> aspectSetToUse = aspectSet.isEmpty() ? null : new HashSet<>(aspectSet);
      Set<String> backfilledAspects = dao.backfillMAE(backfillMode, aspectSetToUse, Collections.singleton(urn)).get(urn);
      if (backfilledAspects == null || backfilledAspects.isEmpty()) {
        return Optional.empty();
      }
      return Optional.of(new BackfillItem().setUrn(urn).setAspects(new StringArray(backfilledAspects)));
    } catch (IllegalArgumentException | InvalidMetadataType e) {
      log.error("Illegal argument or invalid metadata type: ", e);
    } catch (IllegalStateException e) {
      log.error("Backfill failed for IllegalStateException: ", e);
    }
    return Optional.empty();
  }

  /**
   * Helper method to get the {@link BaseLocalDAO} from class {@link LocalDaoRegistry} for the given entity type.
   */
  protected Optional<BaseLocalDAO<? extends UnionTemplate, ? extends Urn>> getLocalDaoByEntity(String entity) {
    LocalDaoRegistry localDaoRegistry = getLocalDaoRegistry();
    BaseLocalDAO<? extends UnionTemplate, ? extends Urn> dao = localDaoRegistry.getLocalDaoByEntity(entity);
    return Optional.ofNullable(dao);
  }
}
