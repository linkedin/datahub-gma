package com.linkedin.metadata.restli;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.metadata.dao.AspectKey;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.ListResult;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.query.ListResultMetadata;
import com.linkedin.metadata.validator.ValidationUtils;
import com.linkedin.parseq.Task;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.CollectionResult;
import com.linkedin.restli.server.CreateKVResponse;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.PagingContext;
import com.linkedin.restli.server.PathKeys;
import com.linkedin.restli.server.UpdateResponse;
import com.linkedin.restli.server.annotations.PagingContextParam;
import com.linkedin.restli.server.annotations.RestMethod;
import com.linkedin.restli.server.annotations.ReturnEntity;
import com.linkedin.restli.server.resources.CollectionResourceTaskTemplate;
import java.time.Clock;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.dao.BaseReadDAO.*;


/**
 * A base class for an aspect rest.li subresource with versioning support.
 *
 * <p>See http://go/gma for more details
 *
 * @param <URN> must be a valid {@link Urn} type
 * @param <ASPECT_UNION> must be a valid union of aspect models defined in com.linkedin.metadata.aspect
 * @param <ASPECT> must be a valid aspect type inside ASPECT_UNION
 */
@Slf4j
public abstract class BaseVersionedAspectResource<URN extends Urn, ASPECT_UNION extends UnionTemplate, ASPECT extends RecordTemplate>
    extends CollectionResourceTaskTemplate<Long, ASPECT> {

  private static final BaseRestliAuditor DUMMY_AUDITOR = new DummyRestliAuditor(Clock.systemUTC());

  private final Class<ASPECT> _aspectClass;

  public BaseVersionedAspectResource(@Nonnull Class<ASPECT_UNION> aspectUnionClass, @Nonnull Class<ASPECT> aspectClass) {
    super();

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
   * Returns {@link BaseLocalDAO} for read-shadowing the aspect.
   */
  protected BaseLocalDAO<ASPECT_UNION, URN> getLocalReadShadowDAO() {
    return null;
  }

  /**
   * Constructs an entity-specific {@link Urn} based on the entity's {@link PathKeys}.
   */
  @Nonnull
  protected abstract URN getUrn(@Nonnull PathKeys entityPathKeys);

  @RestMethod.Get
  @Override
  @Nonnull
  public Task<ASPECT> get(@Nonnull Long version) {
    return RestliUtils.toTask(() -> {
      final URN urn = getUrn(getContext().getPathKeys());
      if (getLocalReadShadowDAO() == null) {
        return getLocalDAO().get(new AspectKey<>(_aspectClass, urn, version))
            .orElseThrow(RestliUtils::resourceNotFoundException);
      }
      return getAspectFromReadShadowDAO(urn, version);
    });
  }

  private ASPECT getAspectFromReadShadowDAO(URN urn, Long version) {
    AspectKey<URN, ASPECT> key = new AspectKey<>(_aspectClass, urn, version);

    Optional<ASPECT> localOpt = getLocalDAO().get(key);
    Optional<ASPECT> shadowOpt = getLocalReadShadowDAO().get(key);

    if (localOpt.isPresent() && shadowOpt.isPresent()) {
      ASPECT local = localOpt.get();
      ASPECT shadow = shadowOpt.get();

      if (!Objects.equals(local, shadow)) {
        log.warn("Aspect mismatch for URN {}, version {}: local = {}, shadow = {}", urn, version, local, shadow);
        return local; // fallback to primary
      } else {
        return shadow;
      }
    } else if (shadowOpt.isPresent()) {
      log.warn("Only shadow has value for URN {}, version {}", urn, version);
      return shadowOpt.get();
    } else if (localOpt.isPresent()) {
      log.info("Only local has value for URN {}, version {}", urn, version);
      return localOpt.get();
    }
    throw RestliUtils.resourceNotFoundException();
  }

  @RestMethod.GetAll
  @Nonnull
  public Task<CollectionResult<ASPECT, ListResultMetadata>> getAllWithMetadata(
      @PagingContextParam @Nonnull PagingContext pagingContext) {
    return RestliUtils.toTask(() -> {
      final URN urn = getUrn(getContext().getPathKeys());
      if (getLocalReadShadowDAO() == null) {
        final ListResult<ASPECT> listResult =
            getLocalDAO().list(_aspectClass, urn, pagingContext.getStart(), pagingContext.getCount());
        return new CollectionResult<>(listResult.getValues(), listResult.getMetadata());
      }
      return getAllWithMetadataFromReadShadowDAO(urn, pagingContext);
    });
  }

  private CollectionResult<ASPECT, ListResultMetadata> getAllWithMetadataFromReadShadowDAO(URN urn, PagingContext pagingContext) {
    ListResult<ASPECT> localResult =
        getLocalDAO().list(_aspectClass, urn, pagingContext.getStart(), pagingContext.getCount());

    ListResult<ASPECT> shadowResult =
        getLocalReadShadowDAO().list(_aspectClass, urn, pagingContext.getStart(), pagingContext.getCount());

    List<ASPECT> localValues = localResult.getValues();
    List<ASPECT> shadowValues = shadowResult.getValues();

    if (!Objects.equals(localValues, shadowValues)) {
      log.warn("Mismatch in getAllWithMetadata for URN {}: local = {}, shadow = {}", urn, localValues, shadowValues);
      return new CollectionResult<>(localValues, localResult.getMetadata()); // Fallback to local
    }

    return new CollectionResult<>(shadowValues, shadowResult.getMetadata());
  }

  @RestMethod.Create
  @Override
  @Nonnull
  public Task<CreateResponse> create(@Nonnull ASPECT aspect) {
    return RestliUtils.toTask(() -> {
      final URN urn = getUrn(getContext().getPathKeys());
      final AuditStamp auditStamp = getAuditor().requestAuditStamp(getContext().getRawRequestContext());
      getLocalDAO().add(urn, aspect, auditStamp);
      BaseLocalDAO<ASPECT_UNION, URN> shadowLocalDao = getLocalShadowDAO();
      if (shadowLocalDao != null) {
        shadowLocalDao.add(urn, aspect, auditStamp);
      }
      return new CreateResponse(HttpStatus.S_201_CREATED);
    });
  }

  /**
   * Soft deletes the latest version of aspect if it exists.
   *
   * @return {@link UpdateResponse} indicating the status code of the response.
   */
  @RestMethod.Delete
  @Nonnull
  public Task<UpdateResponse> delete() {
    return RestliUtils.toTask(() -> {
      final URN urn = getUrn(getContext().getPathKeys());
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
   * Similar to {@link #create(RecordTemplate)} but uses a create lambda instead.
   */
  @Nonnull
  public Task<CreateResponse> create(@Nonnull Class<ASPECT> aspectClass,
      @Nonnull Function<Optional<ASPECT>, ASPECT> createLambda) {
    return RestliUtils.toTask(() -> {
      final URN urn = getUrn(getContext().getPathKeys());
      final AuditStamp auditStamp = getAuditor().requestAuditStamp(getContext().getRawRequestContext());
      getLocalDAO().add(urn, aspectClass, createLambda, auditStamp);
      BaseLocalDAO<ASPECT_UNION, URN> shadowLocalDao = getLocalShadowDAO();
      if (shadowLocalDao != null) {
        shadowLocalDao.add(urn, aspectClass, createLambda, auditStamp);
      }
      return new CreateResponse(HttpStatus.S_201_CREATED);
    });
  }

  /**
   * Similar to {@link #create(Class, Function)} but returns {@link CreateKVResponse} containing latest version and
   * created aspect.
   */
  @RestMethod.Create
  @ReturnEntity
  @Nonnull
  public Task<CreateKVResponse<Long, ASPECT>> createAndGet(@Nonnull Class<ASPECT> aspectClass,
      @Nonnull Function<Optional<ASPECT>, ASPECT> createLambda) {
    return RestliUtils.toTask(() -> {
      final URN urn = getUrn(getContext().getPathKeys());
      final AuditStamp auditStamp = getAuditor().requestAuditStamp(getContext().getRawRequestContext());
      final ASPECT newValue = getLocalDAO().add(urn, aspectClass, createLambda, auditStamp);
      BaseLocalDAO<ASPECT_UNION, URN> shadowLocalDao = getLocalShadowDAO();
      if (shadowLocalDao != null) {
        shadowLocalDao.add(urn, aspectClass, createLambda, auditStamp);
      }
      return new CreateKVResponse<>(LATEST_VERSION, newValue);
    });
  }

  /**
   * Creates using the provided default value only if the aspect is not set already.
   *
   * @param defaultValue provided default value
   * @return {@link CreateKVResponse} containing lastest version and created aspect
   */
  @RestMethod.Create
  @ReturnEntity
  @Nonnull
  public Task<CreateKVResponse<Long, ASPECT>> createIfAbsent(@Nonnull ASPECT defaultValue) {
    return createAndGet((Class<ASPECT>) defaultValue.getClass(), ignored -> ignored.orElse(defaultValue));
  }
}
