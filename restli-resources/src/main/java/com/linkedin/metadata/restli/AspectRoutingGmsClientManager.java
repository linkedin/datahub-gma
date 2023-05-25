package com.linkedin.metadata.restli;

import com.linkedin.data.template.RecordTemplate;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;


/**
 * Manager class which provide registration and retrieval service for {@link BaseAspectRoutingGmsClient}.
 */
public class AspectRoutingGmsClientManager {

  private Map<Class, RoutingAspectConfig> _routingGmsClientConfigMap = new ConcurrentHashMap<>();

  private static class RoutingAspectConfig {
    private final BaseAspectRoutingGmsClient gmsClient;
    private final String setterName;
    public RoutingAspectConfig(BaseAspectRoutingGmsClient gmsClient, String setterName) {
      this.gmsClient = gmsClient;
      this.setterName = setterName;
    }
  }

  public void registerRoutingGmsClient(@Nonnull Class routingAspectClass,
      @Nonnull String routingAspectSetterName, @Nonnull BaseAspectRoutingGmsClient routingGmsClient) {
    _routingGmsClientConfigMap.put(routingAspectClass, new RoutingAspectConfig(routingGmsClient, routingAspectSetterName));
  }

  public BaseAspectRoutingGmsClient getRoutingGmsClient(@Nonnull Class routingAspectClass) {
    return _routingGmsClientConfigMap.getOrDefault(routingAspectClass, new RoutingAspectConfig(null, null)).gmsClient;
  }

  /**
   * Get the setter method name of the routing aspect in entity value.
   * @param routingAspectClass routing aspect class.
   * @return the setter method name of the routing aspect on the entity value object.
   */
  public String getRoutingAspectSetterName(@Nonnull Class routingAspectClass) {
    return _routingGmsClientConfigMap.getOrDefault(routingAspectClass, new RoutingAspectConfig(null, null)).setterName;
  }

  /**
   * Check if a routing aspect has been registered.
   * @param routingAspectClass routing aspect class.
   * @param <ASPECT> type of courting aspect.
   * @return true if has been registered.
   */
  public <ASPECT extends RecordTemplate> boolean hasRegistered(@Nonnull Class<ASPECT> routingAspectClass) {
    return _routingGmsClientConfigMap.containsKey(routingAspectClass);
  }

  /**
   * get all the registered gms clients.
   * @return a list of {@link BaseAspectRoutingGmsClient}
   */
  public List<BaseAspectRoutingGmsClient> getRegisteredRoutingGmsClients() {
    return _routingGmsClientConfigMap.values().stream().map(routingAspectConfig -> routingAspectConfig.gmsClient).collect(
        Collectors.toList());
  }
}
