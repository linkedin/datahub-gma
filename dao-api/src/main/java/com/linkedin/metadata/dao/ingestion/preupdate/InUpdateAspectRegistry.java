package com.linkedin.metadata.dao.ingestion.preupdate;

import com.linkedin.data.template.RecordTemplate;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;


/**
 * A registry which maintains mapping of aspects and their InUpdateRoutingClient.
 */
@Slf4j
public class InUpdateAspectRegistry {

  private final Map<Class<? extends RecordTemplate>, InUpdateRoutingAccessor> _inUpdateLambdaMap;

  /**
   * Constructor to register in-update routing accessors for multiple aspects at once.
   * @param inUpdateMap map containing aspect classes and their corresponding accessors
   */
  public InUpdateAspectRegistry(@Nonnull Map<Class<? extends RecordTemplate>, InUpdateRoutingAccessor> inUpdateMap) {
    _inUpdateLambdaMap = new HashMap<>(inUpdateMap);
    log.info("Registered in-update routing accessors for aspects: {}", _inUpdateLambdaMap.keySet());
  }

  /**
   * Get In Update Routing Accessor for an aspect class.
   * @param aspectClass the class of the aspect to retrieve the accessor for
   * @return InUpdateRoutingAccessor for the given aspect class, or null if not found
   */
  public <ASPECT extends RecordTemplate> InUpdateRoutingAccessor getInUpdateRoutingAccessor(
      @Nonnull Class<ASPECT> aspectClass) {
    return _inUpdateLambdaMap.get(aspectClass);
  }

  /**
   * Check if In Update Routing Accessor is registered for an aspect.
   */
  public <ASPECT extends RecordTemplate> boolean isRegistered(@Nonnull final Class<ASPECT> aspectClass) {
    return _inUpdateLambdaMap.containsKey(aspectClass);
  }

}