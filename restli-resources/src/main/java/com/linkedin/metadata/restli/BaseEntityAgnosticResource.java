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
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.annotations.Action;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
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
 * A base restli resource class with operations that are not tied to a specific entity type.
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
   * @param backfillRequests an array of {@link BackfillItem} to be backfilled. Empty aspect list means backfill all aspects.
   * @param ingestionMode {@link IngestionMode} to indicate the processing strategy. Live mode together with no-change
   *                                           should represent no-op, empty array will be returned. Backfill is to redo
   *                                           any metadata update that is missed or skipped in the past.
   *                                           Bootstrap indicates building the metadata from scratch.
   * @return an array of {@link BackfillItem} that is backfilled, failed urns and aspects will be filtered out
   */
  @Action(name = ACTION_BACKFILL_MAE)
  @Nonnull
  public Task<BackfillItem[]> backfillMAE(@Nonnull BackfillItem[] backfillRequests, @Nonnull IngestionMode ingestionMode) {
    return RestliUtils.toTask(() -> {
      final List<BackfillItem> backfillRequestList = Arrays.asList(backfillRequests);
      final BackfillMode backfillMode = ALLOWED_INGESTION_BACKFILL_BIMAP.get(ingestionMode);
      if (backfillMode == null) {
        return new BackfillItem[0];
      }

      // Group requests by entity type
      final List<BackfillItem> backfillResults = new ArrayList<>();
      final Map<String, List<BackfillItem>> entityTypeToRequestsMap = new HashMap<>();
      backfillRequestList.forEach(request -> {
        try {
          final String entityType = Urn.createFromString(request.getUrn()).getEntityType();
          entityTypeToRequestsMap.computeIfAbsent(entityType, k -> new ArrayList<>()).add(request);
        } catch (URISyntaxException e) {
          log.warn("Failed casting string to Urn, request: " + request, e);
        }
      });

      // for each entity type, backfill MAE for each urn in parallel
      for (String entityType : entityTypeToRequestsMap.keySet()) {
        final Optional<BaseLocalDAO<? extends UnionTemplate, ? extends Urn>> dao = getLocalDaoByEntity(entityType);
        if (!dao.isPresent()) {
          log.warn("LocalDAO not found for entity type: " + entityType);
          continue;
        }
        final List<BackfillItem> itemsToBackfill = entityTypeToRequestsMap.get(entityType);
        final List<BackfillItem> backfilledItems = itemsToBackfill.stream()
            .map(item -> backfillMAEForUrn(item.getUrn(), item.getAspects(), backfillMode, dao.get()).orElse(null))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
        log.info(String.format("Given requests: %s, backfill results: %s", itemsToBackfill, backfilledItems));
        backfillResults.addAll(backfilledItems);
      }
      return backfillResults.toArray(new BackfillItem[0]); // insert order is not guaranteed the same as input
    });
  }

  protected Optional<BackfillItem> backfillMAEForUrn(@Nonnull String urn, @Nonnull List<String> aspectSet,
      @Nonnull BackfillMode backfillMode, @Nonnull BaseLocalDAO<? extends UnionTemplate, ? extends Urn> dao) {
    try {
      log.info(String.format("Attempt to backfill MAE for urn: %s, aspectSet: %s, backfillMode: %s", urn, aspectSet, backfillMode));
      // set aspectSetToUse to null if empty to backfill all aspects
      Set<String> aspectSetToUse = aspectSet.isEmpty() ? null : new HashSet<>(aspectSet);
      Set<String> backfilledAspects = dao.backfillMAE(backfillMode, aspectSetToUse, Collections.singleton(urn)).get(urn);
      log.info(String.format("Backfilled aspects: %s, for urn: %s, aspectSet: %s, backfillMode: %s", backfilledAspects, urn, aspectSet, backfillMode));
      if (backfilledAspects == null || backfilledAspects.isEmpty()) {
        return Optional.empty();
      }
      return Optional.of(new BackfillItem().setUrn(urn).setAspects(new StringArray(backfilledAspects)));
    } catch (IllegalArgumentException | InvalidMetadataType e) {
      log.warn(String.format("Illegal argument or invalid metadata type, urn: %s, aspectSet: %s", urn, aspectSet), e);
    } catch (IllegalStateException e) {
      log.warn(String.format("Backfill failed for illegal state, urn: %s, aspectSet: %s", urn, aspectSet), e);
    }
    return Optional.empty();
  }

  /**
   * Helper method to get the {@link BaseLocalDAO} from class {@link LocalDaoRegistry} for the given entity type.
   */
  protected Optional<BaseLocalDAO<? extends UnionTemplate, ? extends Urn>> getLocalDaoByEntity(String entityType) {
    LocalDaoRegistry localDaoRegistry = getLocalDaoRegistry();
    return Optional.ofNullable(localDaoRegistry.getLocalDaoByEntityType(entityType));
  }
}
