package com.linkedin.metadata.restli;

import com.linkedin.common.AuditStamp;
import com.linkedin.metadata.dao.GenericLocalDAO;
import com.linkedin.parseq.Task;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.RestLiServiceException;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.resources.ResourceContextHolder;
import java.time.Clock;
import java.util.Optional;
import javax.annotation.Nonnull;

import static com.linkedin.metadata.restli.RestliConstants.*;


/**
 * This resource is intended to be used as top level resource, instead of a sub resource.
 * For sub-resource please use {@link BaseVersionedAspectResource}
 */
public abstract class BaseEntityAgnosticAspectResource extends ResourceContextHolder {

  private static final BaseRestliAuditor DUMMY_AUDITOR = new DummyRestliAuditor(Clock.systemUTC());

  /**
   * Returns an instance of {@link GenericLocalDAO}.
   */
  @Nonnull
  protected abstract GenericLocalDAO genericLocalDAO();

  /**
   * Returns a {@link BaseRestliAuditor} for this resource.
   */
  @Nonnull
  protected BaseRestliAuditor getAuditor() {
    return DUMMY_AUDITOR;
  }

  /**
   * Query the latest metadata associated with an entity.
   * @param urn The urn that identifies the entity.
   * @param aspectClass The canonical class name of the aspect.
   * @return The metadata aspect serialized as string in JSON format.
   */
  @Action(name = ACTION_QUERY)
  @Nonnull
  public Task<String> queryLatest(
      @ActionParam(PARAM_URN) @Nonnull String urn,
      @ActionParam(PARAM_ASPECT_CLASS) @Nonnull String aspectClass) {

    Class clazz;
    try {
      clazz = this.getClass().getClassLoader().loadClass(aspectClass);
    } catch (ClassNotFoundException e) {
      throw new RestLiServiceException(HttpStatus.S_400_BAD_REQUEST, String.format("No such class %s", aspectClass));
    }

    Optional<GenericLocalDAO.MetadataWithExtraInfo> nullableMetadata = genericLocalDAO().queryLatest(urn, clazz);
    if (nullableMetadata.isPresent()) {
      return RestliUtils.toTask(() -> nullableMetadata.get().getAspect());
    }

    throw new RestLiServiceException(HttpStatus.S_404_NOT_FOUND);
  }

  /**
   * Ingest the metadata into database.
   * @param urn The urn identified the entity for which the metadata is associated with.
   * @param aspect The metadata aspect serialized as string in JSON format.
   * @param aspectClass The canonical class name of the aspect.
   * @return CreateResponse if metadata is ingested successfully.
   */
  @Action(name = ACTION_INGEST)
  @Nonnull
  public Task<CreateResponse> ingest(
      @ActionParam(PARAM_URN) @Nonnull String urn,
      @ActionParam(PARAM_ASPECT) @Nonnull String aspect,
      @ActionParam(PARAM_ASPECT_CLASS) @Nonnull String aspectClass) {
    final AuditStamp auditStamp = getAuditor().requestAuditStamp(getContext().getRawRequestContext());
    Class clazz;

    try {
      clazz = this.getClass().getClassLoader().loadClass(aspectClass);
    } catch (ClassNotFoundException e) {
      throw new RestLiServiceException(HttpStatus.S_400_BAD_REQUEST, String.format("No such class %s", aspectClass));
    }

    genericLocalDAO().save(urn, clazz, aspect, auditStamp);
    return RestliUtils.toTask(() -> new CreateResponse(HttpStatus.S_201_CREATED));
  }
}