package com.linkedin.metadata.dao.ingestion;

import com.linkedin.data.template.RecordTemplate;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;


/**
 * A registry which maintains mapping of aspects and their Aspect Routing Client.
 */
@Slf4j
public class AspectCallbackRegistry {

  private final Map<Class<? extends RecordTemplate>, AspectCallbackRoutingClient> aspectCallbackMap;

  /**
   * Constructor to register aspect callback routing clients for aspects.
   * @param aspectCallbackMap map containing aspect classes and their corresponding cleints
   */
  public AspectCallbackRegistry(@Nonnull Map<Class<? extends RecordTemplate>, AspectCallbackRoutingClient> aspectCallbackMap) {
    this.aspectCallbackMap = new HashMap<>(aspectCallbackMap);
    log.info("Registered aspect callback clients for aspects: {}", aspectCallbackMap.keySet());
  }

  /**
   * Get Aspect Callback Routing Client for an aspect class.
   * @param aspectClass the class of the aspect to retrieve the client
   * @return AspectCallbackRoutingClient for the given aspect class, or null if not found
   */
  public <ASPECT extends RecordTemplate> AspectCallbackRoutingClient getAspectCallbackRoutingClient(
      @Nonnull Class<ASPECT> aspectClass) {
    return aspectCallbackMap.get(aspectClass);
  }

  /**
   * Check if Aspect Callback Routing Client is registered for an aspect.
   */
  public <ASPECT extends RecordTemplate> boolean isRegistered(@Nonnull final Class<ASPECT> aspectClass) {
    return aspectCallbackMap.containsKey(aspectClass);
  }

}