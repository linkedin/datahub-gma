package com.linkedin.metadata.restli;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.backfill.BackfillMode;
import com.linkedin.metadata.dao.GenericLocalDAO;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.events.IngestionMode;
import com.linkedin.metadata.events.IngestionTrackingContext;
import com.linkedin.metadata.internal.IngestionParams;
import com.linkedin.parseq.Task;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.RestLiServiceException;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.resources.ResourceContextHolder;
import java.net.URISyntaxException;
import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

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

    try {
      Class clazz = this.getClass().getClassLoader().loadClass(aspectClass);

      GenericLocalDAO.MetadataWithExtraInfo metadataWithExtraInfo =
          genericLocalDAO().queryLatest(Urn.createFromCharSequence(urn), clazz).orElse(null);

      if (metadataWithExtraInfo != null) {
        return RestliUtils.toTask(metadataWithExtraInfo::getAspect);
      }

      throw new RestLiServiceException(HttpStatus.S_404_NOT_FOUND);
    } catch (ClassNotFoundException e) {
      throw new RestLiServiceException(HttpStatus.S_400_BAD_REQUEST, String.format("No such class %s", aspectClass));
    } catch (URISyntaxException e) {
      throw new RestLiServiceException(HttpStatus.S_400_BAD_REQUEST, String.format("Urn %s is malformed.", urn));
    }
  }

  /**
   * Ingest the metadata into database.
   * @param urn The urn identified the entity for which the metadata is associated with.
   * @param aspect The metadata aspect serialized as string in JSON format.
   * @param aspectClass The canonical class name of the aspect.
   * @param trackingContext Nullable tracking context contains information passed from metadata events.
   * @param ingestionParams Different options for ingestion.
   */
  @Action(name = ACTION_INGEST)
  @Nonnull
  public Task<Void> ingest(
      @ActionParam(PARAM_URN) @Nonnull String urn,
      @ActionParam(PARAM_ASPECT) @Nonnull String aspect,
      @ActionParam(PARAM_ASPECT_CLASS) @Nonnull String aspectClass,
      @Optional @ActionParam(PARAM_TRACKING_CONTEXT) @Nullable IngestionTrackingContext trackingContext,
      @Optional @ActionParam(PARAM_INGESTION_PARAMS) @Nullable IngestionParams ingestionParams) {
    final AuditStamp auditStamp = getAuditor().requestAuditStamp(getContext().getRawRequestContext());

    try {
      Class clazz = this.getClass().getClassLoader().loadClass(aspectClass);
      IngestionMode ingestionMode = ingestionParams == null ? null : ingestionParams.getIngestionMode();
      genericLocalDAO().save(Urn.createFromCharSequence(urn), clazz, aspect, auditStamp, trackingContext, ingestionMode);
      return Task.value(null);
    } catch (ClassNotFoundException e) {
      throw new RestLiServiceException(HttpStatus.S_400_BAD_REQUEST, String.format("No such class %s.", aspectClass));
    } catch (URISyntaxException e) {
      throw new RestLiServiceException(HttpStatus.S_400_BAD_REQUEST, String.format("Urn %s is malformed.", urn));
    }
  }

  /**
   * Backfill secondary storage by triggering MAEs.
   * @param urn The urn identified the entity for which the metadata is associated with.
   * @param aspectNames A list of aspect's canonical names.
   */
  @Action(name = ACTION_BACKFILL_WITH_URN)
  @Nonnull
  public Task<BackfillResult> backfill(
      @ActionParam(PARAM_URN) @Nonnull String urn,
      @ActionParam(PARAM_ASPECTS) @Nonnull String[] aspectNames) {
    Set<Class<? extends RecordTemplate>> aspects = Arrays.stream(aspectNames).map(ModelUtils::getAspectClass).collect(Collectors.toSet());

    try {
      BackfillResult backfillResult = RestliUtils.buildBackfillResult(genericLocalDAO().backfill(
          BackfillMode.BACKFILL_ALL, Collections.singletonMap(Urn.createFromCharSequence(urn), aspects)));

      return RestliUtils.toTask(() -> backfillResult);
    } catch (URISyntaxException e) {
      throw new RestLiServiceException(HttpStatus.S_400_BAD_REQUEST, String.format("Urn %s is malformed.", urn));
    }
  }
}