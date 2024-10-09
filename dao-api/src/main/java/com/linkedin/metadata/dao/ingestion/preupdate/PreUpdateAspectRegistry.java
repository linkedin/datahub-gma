package com.linkedin.metadata.dao.ingestion.preupdate;

import com.linkedin.data.template.RecordTemplate;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;


/**
 * A registry which maintains mapping of aspects and their PreUpdateRoutingClient.
 */
@Slf4j
public class PreUpdateAspectRegistry<ASPECT extends RecordTemplate> {

  private Map<Class<? extends RecordTemplate>, PreRoutingInfo> _preUpdateLambdaMap = new ConcurrentHashMap<>();

  /**
   * Get GrpcPreUpdateRoutingClient for an aspect.
   */
  public PreRoutingInfo getPreUpdateRoutingClient(@Nonnull final ASPECT aspect) {
    return _preUpdateLambdaMap.get(aspect.getClass());
  }

  /**
   * Check if GrpcPreUpdateRoutingClient is registered for an aspect.
   */
  public boolean isRegistered(@Nonnull final Class<ASPECT> aspectClass) {
    return _preUpdateLambdaMap.containsKey(aspectClass);
  }

  /**
   * Register a pre update lambda for the given aspect.
   * @param aspectClass aspect class
   * @param preUpdateRoutingInfo pre update routing map
   */
  public void registerPreUpdateLambda(@Nonnull Class<? extends RecordTemplate> aspectClass,
      @Nonnull PreRoutingInfo preUpdateRoutingInfo) {
    log.info("Registering pre update lambda: {}, {}", aspectClass.getCanonicalName(), preUpdateRoutingInfo);
    _preUpdateLambdaMap.put(aspectClass, preUpdateRoutingInfo);
  }

}