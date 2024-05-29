package com.linkedin.metadata.restli;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.metadata.dao.AspectKey;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.events.IngestionTrackingContext;
import com.linkedin.metadata.internal.IngestionParams;
import com.linkedin.metadata.restli.dao.LocalDaoRegistry;
import com.linkedin.parseq.Task;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.RestLiServiceException;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.RestMethod;
import com.linkedin.restli.server.resources.CollectionResourceTaskTemplate;
import java.net.URISyntaxException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;

import static com.linkedin.metadata.dao.BaseReadDAO.*;
import static com.linkedin.metadata.restli.RestliConstants.*;


/**
 * This resource is intended to be used as top level resource, instead of a sub resource.
 * For sub-resource please use {@link BaseVersionedAspectResource}
 *
 * @param <ASPECT> must be a valid aspect type inside ASPECT_UNION
 */
public abstract class BaseAspectV3Resource<ASPECT extends RecordTemplate> extends CollectionResourceTaskTemplate<String, ASPECT> {

  private static final BaseRestliAuditor DUMMY_AUDITOR = new DummyRestliAuditor(Clock.systemUTC());

  private final Class<ASPECT> _aspectClass;

  public BaseAspectV3Resource(Class<ASPECT> aspectClass) {
    _aspectClass = aspectClass;
  }

  /**
   * Returns the {@link LocalDaoRegistry} that contains the mapping from entity type to {@link BaseLocalDAO}.
   * @return {@link LocalDaoRegistry}
   */
  @Nonnull
  protected abstract LocalDaoRegistry getLocalDaoRegistry();

  /**
   * Returns a {@link BaseRestliAuditor} for this resource.
   */
  @Nonnull
  protected BaseRestliAuditor getAuditor() {
    return DUMMY_AUDITOR;
  }

  /**
   * Get the ASPECT associated with URN.
   */
  @RestMethod.Get
  @Nonnull
  public Task<ASPECT> get(@Nonnull String urnStr) {

    try {
      final Urn urn = Urn.createFromString(urnStr);
      final String entityType = urn.getEntityType();
      final AspectKey<ASPECT> key = new AspectKey<>(_aspectClass, urn, LATEST_VERSION);
      return RestliUtils.toTask(() -> getLocalDao(entityType).get(key).orElseThrow(RestliUtils::resourceNotFoundException));

    } catch (URISyntaxException e) {
      throw new RestLiServiceException(HttpStatus.S_400_BAD_REQUEST,
          String.format("Failed to cast %s to Urn. Please check if urn if correctly formatted.", urnStr));
    }
  }

  /**
   * Create latest version of an aspect.
   */
  @RestMethod.Create
  @Nonnull
  public Task<CreateResponse> create(@Nonnull String urnStr, @Nonnull ASPECT aspect) {

    try {
      final Urn urn = Urn.createFromString(urnStr);
      final String entityType = urn.getEntityType();
      final AuditStamp auditStamp = getAuditor().requestAuditStamp(getContext().getRawRequestContext());
      getLocalDao(entityType).add(urn, aspect, auditStamp);
      return RestliUtils.toTask(() -> new CreateResponse(HttpStatus.S_201_CREATED));

    } catch (URISyntaxException e) {
      throw new RestLiServiceException(HttpStatus.S_400_BAD_REQUEST,
          String.format("Failed to cast %s to Urn. Please check if urn if correctly formatted.", urnStr));
    }

  }

  /**
   * Create latest version of an aspect with tracking context.
   */
  @RestMethod.Create
  @Nonnull
  public Task<CreateResponse> createWithTracking(@Nonnull String urnStr, @Nonnull ASPECT aspect,
      @Nonnull IngestionTrackingContext trackingContext, @Optional IngestionParams ingestionParams) {
    try {
      final Urn urn = Urn.createFromString(urnStr);
      final String entityType = urn.getEntityType();
      final AuditStamp auditStamp = getAuditor().requestAuditStamp(getContext().getRawRequestContext());
      getLocalDao(entityType).add(urn, aspect, auditStamp, trackingContext, ingestionParams);
      return RestliUtils.toTask(() -> new CreateResponse(HttpStatus.S_201_CREATED));
    } catch (URISyntaxException e) {
      throw new RestLiServiceException(HttpStatus.S_400_BAD_REQUEST,
          String.format("Failed to cast %s to Urn. Please check if urn if correctly formatted.", urnStr));
    }
  }

  /**
   * Backfill ASPECT for each entity identified by its URN.
   */
  @Action(name = ACTION_BACKFILL_WITH_URNS)
  @Nonnull
  public Task<BackfillResult> backfillWithUrns(@Nonnull Set<String> urns) {
    List<BackfillResult> backfillResultList = new ArrayList<>();

    for (String urnStr : urns) {
      try {
        final Urn urn = Urn.createFromString(urnStr);
        final String entityType = urn.getEntityType();

        backfillResultList.add(
            RestliUtils.buildBackfillResult(getLocalDao(entityType).backfill(ImmutableSet.of(_aspectClass), Collections.singleton(urn)))
        );

      } catch (URISyntaxException e) {
        throw new RestLiServiceException(HttpStatus.S_400_BAD_REQUEST,
            String.format("Failed to cast %s to Urn. Please check if urn if correctly formatted.", urnStr));
      }
    }

    // Merge the backfill result for each urn
    BackfillResultEntityArray backfillResultEntities = new BackfillResultEntityArray();
    backfillResultList.forEach(backfillResult -> backfillResultEntities.addAll(backfillResult.getEntities()));
    return RestliUtils.toTask(() -> new BackfillResult().setEntities(backfillResultEntities));
  }

  private BaseLocalDAO<? extends UnionTemplate, ? extends Urn> getLocalDao(String entityType) {
    final java.util.Optional<BaseLocalDAO<? extends UnionTemplate, ? extends Urn>> dao =
        getLocalDaoRegistry().getLocalDaoByEntityType(entityType);
    if (!dao.isPresent()) {
      throw new RestLiServiceException(HttpStatus.S_500_INTERNAL_SERVER_ERROR,
          String.format("LocalDAO not found for entity type: %s", entityType));
    }

    return dao.get();
  }
}