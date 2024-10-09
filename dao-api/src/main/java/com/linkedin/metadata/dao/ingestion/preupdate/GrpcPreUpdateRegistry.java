package com.linkedin.metadata.dao.ingestion.preupdate;

import com.linkedin.data.template.RecordTemplate;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;


/**
 * A registry which maintains mapping of aspects and their GrpcPreUpdateRoutingClient.
 */
@Slf4j
public class GrpcPreUpdateRegistry<ASPECT extends RecordTemplate> {

    private Map<Class<? extends RecordTemplate>, RoutingMap> _preUpdateLambdaMap =
        new ConcurrentHashMap<>();

    /**
    * Get GrpcPreUpdateRoutingClient for an aspect.
    */
    public RoutingMap getPreUpdateRoutingClient(@Nonnull final ASPECT aspect){
        return _preUpdateLambdaMap.get(aspect.getClass());
    }

    /**
    * Check if GrpcPreUpdateRoutingClient is registered for an aspect.
    */
    public boolean isRegistered(@Nonnull final Class<ASPECT> aspectClass){
        return _preUpdateLambdaMap.containsKey(aspectClass);
    }

    /**
     * Register a pre update lambda for the given aspect
     * @param aspectClass aspect class
     * @param preUpdateRoutingMap pre update routing map
     */
    public void registerPreUpdateLambda(@Nonnull Class<? extends RecordTemplate> aspectClass,
        @Nonnull RoutingMap preUpdateRoutingMap) {
        log.info("Registering pre update lambda: {}, {}", aspectClass.getCanonicalName(), preUpdateRoutingMap);
        _preUpdateLambdaMap.put(aspectClass, preUpdateRoutingMap);
    }
}
