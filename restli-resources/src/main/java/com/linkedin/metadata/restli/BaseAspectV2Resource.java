package com.linkedin.metadata.restli;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.metadata.dao.AspectKey;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.ListResult;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.events.IngestionTrackingContext;
import com.linkedin.metadata.internal.IngestionParams;
import com.linkedin.metadata.query.ListResultMetadata;
import com.linkedin.metadata.validator.ValidationUtils;
import com.linkedin.parseq.Task;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.CollectionResult;
import com.linkedin.restli.server.CreateKVResponse;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.PagingContext;
import com.linkedin.restli.server.UpdateResponse;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.PagingContextParam;
import com.linkedin.restli.server.annotations.RestMethod;
import com.linkedin.restli.server.annotations.ReturnEntity;
import com.linkedin.restli.server.resources.CollectionResourceTaskTemplate;
import java.time.Clock;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import javax.annotation.Nonnull;

import static com.linkedin.metadata.dao.BaseReadDAO.*;
import static com.linkedin.metadata.restli.RestliConstants.*;


/**
 * This resource is intended to be used as top level resource, instead of a sub resource.
 * For sub-resource please use {@link BaseVersionedAspectResource}
 *
 * @param <URN> must be a valid {@link Urn} type
 * @param <ASPECT_UNION> must be a valid union of aspect models defined in com.linkedin.metadata.aspect
 * @param <ASPECT> must be a valid aspect type inside ASPECT_UNION
 */
public abstract class BaseAspectV2Resource<
    URN extends Urn,
    ASPECT_UNION extends UnionTemplate,
    ASPECT extends RecordTemplate> extends CollectionResourceTaskTemplate<URN, ASPECT> {

  private static final BaseRestliAuditor DUMMY_AUDITOR = new DummyRestliAuditor(Clock.systemUTC());

  private final Class<ASPECT> _aspectClass;

  public BaseAspectV2Resource(@Nonnull Class<ASPECT_UNION> aspectUnionClass, @Nonnull Class<ASPECT> aspectClass) {
    if (!ModelUtils.getValidAspectTypes(aspectUnionClass).contains(aspectClass)) {
      ValidationUtils.invalidSchema("Aspect '%s' is not in Union '%s'", aspectClass.getCanonicalName(),
          aspectUnionClass.getCanonicalName());
    }

    this._aspectClass = aspectClass;
  }

  /**
   * Returns a {@link BaseRestliAuditor} for this resource.
   */
  @Nonnull
  protected BaseRestliAuditor getAuditor() {
    return DUMMY_AUDITOR;
  }

  /**
   * Returns an aspect-specific {@link BaseLocalDAO}.
   */
  @Nonnull
  protected abstract BaseLocalDAO<ASPECT_UNION, URN> getLocalDAO();

  /**
   * Returns {@link BaseLocalDAO} for shadowing the aspect.
   */
  protected BaseLocalDAO<ASPECT_UNION, URN> getLocalShadowDAO() {
    return null;
  }

  /**
   * Get the ASPECT associated with URN.
   */
  @RestMethod.Get
  @Nonnull
  public Task<ASPECT> get(@Nonnull URN urn) {
    return RestliUtils.toTask(() -> getLocalDAO().get(new AspectKey<>(_aspectClass, urn, LATEST_VERSION))
        .orElseThrow(RestliUtils::resourceNotFoundException));
  }

  @RestMethod.BatchGet
  @Nonnull
  public Task<Map<URN, java.util.Optional<ASPECT>>> batchGet(@Nonnull Collection<URN> urns) {
    return RestliUtils.toTask(() -> getLocalDAO().get(_aspectClass, new HashSet<>(urns)));
  }

  @RestMethod.GetAll
  @Nonnull
  public Task<CollectionResult<ASPECT, ListResultMetadata>> getAllWithMetadata(@Nonnull URN urn,
      @PagingContextParam @Nonnull PagingContext pagingContext) {
    return RestliUtils.toTask(() -> {

      final ListResult<ASPECT> listResult =
          getLocalDAO().list(_aspectClass, urn, pagingContext.getStart(), pagingContext.getCount());
      return new CollectionResult<>(listResult.getValues(), listResult.getMetadata());
    });
  }

  @RestMethod.Create
  @Nonnull
  public Task<CreateResponse> create(@Nonnull URN urn, @Nonnull ASPECT aspect) {
    return RestliUtils.toTask(() -> {
      final AuditStamp auditStamp = getAuditor().requestAuditStamp(getContext().getRawRequestContext());
      getLocalDAO().add(urn, aspect, auditStamp);
      BaseLocalDAO<ASPECT_UNION, URN> shadowLocalDao = getLocalShadowDAO();
      if (shadowLocalDao != null) {
        shadowLocalDao.add(urn, aspect, auditStamp);
      }
      return new CreateResponse(HttpStatus.S_201_CREATED);
    });
  }

  @RestMethod.Create
  @Nonnull
  public Task<CreateResponse> createWithTracking(@Nonnull URN urn, @Nonnull ASPECT aspect,
      @Nonnull IngestionTrackingContext trackingContext, @Optional IngestionParams ingestionParams) {
    return RestliUtils.toTask(() -> {
      final AuditStamp auditStamp = getAuditor().requestAuditStamp(getContext().getRawRequestContext());
      getLocalDAO().add(urn, aspect, auditStamp, trackingContext, ingestionParams);
      BaseLocalDAO<ASPECT_UNION, URN> shadowLocalDao = getLocalShadowDAO();
      if (shadowLocalDao != null) {
        shadowLocalDao.add(urn, aspect, auditStamp, trackingContext, ingestionParams);
      }
      return new CreateResponse(HttpStatus.S_201_CREATED);
    });
  }

  /**
   * Create and get the latest version of aspect.
   */
  @RestMethod.Create
  @ReturnEntity
  @Nonnull
  public Task<CreateKVResponse<URN, ASPECT>> createAndGet(@Nonnull URN urn,
      @Nonnull Function<java.util.Optional<ASPECT>, ASPECT> createLambda) {
    return RestliUtils.toTask(() -> {
      final AuditStamp auditStamp = getAuditor().requestAuditStamp(getContext().getRawRequestContext());
      final ASPECT newValue = getLocalDAO().add(urn, _aspectClass, createLambda, auditStamp);
      BaseLocalDAO<ASPECT_UNION, URN> shadowLocalDao = getLocalShadowDAO();
      if (shadowLocalDao != null) {
        shadowLocalDao.add(urn, _aspectClass, createLambda, auditStamp);
      }
      return new CreateKVResponse<>(urn, newValue);
    });
  }

  /**
   * Soft deletes the latest version of aspect if it exists.
   *
   * @return {@link UpdateResponse} indicating the status code of the response.
   */
  @RestMethod.Delete
  @Nonnull
  public Task<UpdateResponse> delete(@Nonnull URN urn) {
    return RestliUtils.toTask(() -> {
      final AuditStamp auditStamp = getAuditor().requestAuditStamp(getContext().getRawRequestContext());
      getLocalDAO().delete(urn, this._aspectClass, auditStamp);
      BaseLocalDAO<ASPECT_UNION, URN> shadowLocalDao = getLocalShadowDAO();
      if (shadowLocalDao != null) {
        shadowLocalDao.delete(urn, this._aspectClass, auditStamp);
      }
      return new UpdateResponse(HttpStatus.S_200_OK);
    });
  }

  /**
   * Backfill ASPECT for each entity identified by its URN.
   * @param urns Identifies a set of entities for which its ASPECT will be backfilled.
   * @return BackfillResult for each entity identified by URN.
   */
  @Action(name = ACTION_BACKFILL_WITH_URNS)
  @Nonnull
  public Task<BackfillResult> backfillWithUrns(@Nonnull Set<URN> urns) {
    return RestliUtils.toTask(() ->
        RestliUtils.buildBackfillResult(getLocalDAO().backfill(ImmutableSet.of(_aspectClass), urns)));
  }
}