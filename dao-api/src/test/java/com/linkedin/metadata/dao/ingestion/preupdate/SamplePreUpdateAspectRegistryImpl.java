package com.linkedin.metadata.dao.ingestion;

import com.google.common.collect.ImmutableMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.ingestion.preupdate.RestliCompliantPreUpdateRoutingClient;
import com.linkedin.metadata.dao.ingestion.preupdate.RestliPreUpdateAspectRegistry;
import com.linkedin.testing.AspectFoo;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


public class SamplePreUpdateAspectRegistryImpl implements RestliPreUpdateAspectRegistry {
  private final ImmutableMap<Class<? extends RecordTemplate>, RestliCompliantPreUpdateRoutingClient> registry;

  public SamplePreUpdateAspectRegistryImpl() {
    registry = new ImmutableMap.Builder<Class<? extends RecordTemplate>, RestliCompliantPreUpdateRoutingClient>()
        .put(AspectFoo.class, new SamplePreUpdateRoutingClient())
        .build();
  }
  @Nullable
  @Override
  public  <ASPECT extends RecordTemplate> RestliCompliantPreUpdateRoutingClient getPreUpdateRoutingClient(@Nonnull ASPECT aspect) {
    return registry.get(aspect.getClass());
  }

  @Override
  public <ASPECT extends RecordTemplate> boolean isRegistered(@Nonnull Class<ASPECT> aspectClass) {
    return registry.containsKey(aspectClass);
  }
}
