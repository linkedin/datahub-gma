package com.linkedin.metadata.dao.ingestion.preupdate;

import com.linkedin.data.template.RecordTemplate;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;


/**
 * A registry which maintains mapping of aspects and their PreUpdateRoutingClient.
 */
@Slf4j
public class PreUpdateAspectRegistry {

  private final Map<Class<? extends RecordTemplate>, PreUpdateRoutingAccessor> _preUpdateLambdaMap;

  /**
   * Constructor to register pre-update routing accessors for multiple aspects at once.
   * @param preUpdateMap map containing aspect classes and their corresponding accessors
   */
  public PreUpdateAspectRegistry(@Nonnull Map<Class<? extends RecordTemplate>, PreUpdateRoutingAccessor> preUpdateMap) {
    _preUpdateLambdaMap = new HashMap<>(preUpdateMap);
    log.info("Registered pre-update routing accessors for aspects: {}", _preUpdateLambdaMap.keySet());
  }

  /**
   * Get Pre Update Routing Accessor for an aspect class.
   * @param aspectClass the class of the aspect to retrieve the accessor for
   * @return PreUpdateRoutingAccessor for the given aspect class, or null if not found
   */
  public <ASPECT extends RecordTemplate> PreUpdateRoutingAccessor getPreUpdateRoutingAccessor(
      @Nonnull Class<ASPECT> aspectClass) {
    return _preUpdateLambdaMap.get(aspectClass);
  }

  /**
   * Check if Pre Update Routing Accessor is registered for an aspect.
   */
  public <ASPECT extends RecordTemplate> boolean isRegistered(@Nonnull final Class<ASPECT> aspectClass) {
    return _preUpdateLambdaMap.containsKey(aspectClass);
  }

}