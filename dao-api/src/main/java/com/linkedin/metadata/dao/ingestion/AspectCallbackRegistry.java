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

  private final Map<Class<? extends RecordTemplate>, InUpdateRoutingClient> _inUpdateLambdaMap;

  /**
   * Constructor to register in-update routing clients for aspects.
   * @param inUpdateMap map containing aspect classes and their corresponding cleints
   */
  public AspectCallbackRegistry(@Nonnull Map<Class<? extends RecordTemplate>, InUpdateRoutingClient> inUpdateMap) {
    _inUpdateLambdaMap = new HashMap<>(inUpdateMap);
    log.info("Registered aspect callback clients for aspects: {}", _inUpdateLambdaMap.keySet());
  }

  /**
   * Get In Update Callback Client for an aspect class.
   * @param aspectClass the class of the aspect to retrieve the client
   * @return InUpdateRoutingClient for the given aspect class, or null if not found
   */
  public <ASPECT extends RecordTemplate> InUpdateRoutingClient getInUpdateRoutingClient(
      @Nonnull Class<ASPECT> aspectClass) {
    return _inUpdateLambdaMap.get(aspectClass);
  }

  /**
   * Check if In Update Callback Client is registered for an aspect.
   */
  public <ASPECT extends RecordTemplate> boolean isRegistered(@Nonnull final Class<ASPECT> aspectClass) {
    return _inUpdateLambdaMap.containsKey(aspectClass);
  }

}