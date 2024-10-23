package com.linkedin.metadata.dao;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.validator.AssetValidator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.reflections.Reflections;


/**
 * This class tracks the (entityType, Asset) map.
 */
@Slf4j
public class GlobalAssetRegistry {
  private final Map<String, Class<? extends RecordTemplate>> registry = new ConcurrentHashMap<>();

  private GlobalAssetRegistry() {
  }

  // thread-safe, lazy-load singleton instance.
  // JVM guarantees static fields is only instantiated once and in a thread-safe manner when class is first being loaded.
  // Putting it in the inner class makes this inner only being loaded when getInstance() is called.
  private static class InnerHolder {
    private static final GlobalAssetRegistry INSTANCE = new GlobalAssetRegistry();

    static {
      try {
        INSTANCE.preLoadInternalAssets();
      } catch (Exception e) {
        log.error("Failed to pre-load internal assets", e);
      }
    }
  }

  private static GlobalAssetRegistry getInstance() {
    return InnerHolder.INSTANCE;
  }

  public static void register(@Nonnull String assetType, @Nonnull Class<? extends RecordTemplate> assetClass) {
    AssetValidator.validateAssetSchema(assetClass);
    getInstance().registry.put(assetType, assetClass);
  }

  public static Class<? extends RecordTemplate> get(@Nonnull String assetType) {
    return getInstance().registry.get(assetType);
  }

  /**
   * TODO: moving this loading logic into internal-models.
   */
  private void preLoadInternalAssets() {
    Reflections reflections = new Reflections(INTERNAL_ASSET_PACKAGE); // Change to your package
    Set<Class<? extends RecordTemplate>> assetClasses = reflections.getSubTypesOf(RecordTemplate.class);
    for (Class<? extends RecordTemplate> assetClass : assetClasses) {
      try {
        register(ModelUtils.getUrnTypeFromAsset(assetClass), assetClass);
      } catch (Exception e) {
        log.error("failed to load asset: " + assetClass, e);
      }
    }
  }

  private static final String INTERNAL_ASSET_PACKAGE = "pegasus.com.linkedin.metadata.asset";
}

