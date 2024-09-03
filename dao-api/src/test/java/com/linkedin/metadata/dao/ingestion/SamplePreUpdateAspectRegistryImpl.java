package com.linkedin.metadata.dao.ingestion;

import com.google.common.collect.ImmutableMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.testing.AspectFoo;
import javax.annotation.Nonnull;


public class SamplePreUpdateAspectRegistryImpl implements RestliPreUpdateAspectRegistry {
  private final ImmutableMap<Class<? extends RecordTemplate>, RestliCompliantPreUpdateRoutingClient> registry;

  public SamplePreUpdateAspectRegistryImpl() {
    registry = new ImmutableMap.Builder<Class<? extends RecordTemplate>, RestliCompliantPreUpdateRoutingClient>()
        .put(AspectFoo.class, new SamplePreUpdateRoutingClient())
        .build();
  }
  @Nonnull
  @Override
  public RestliCompliantPreUpdateRoutingClient getPreIngestionRouting(Class<? extends RecordTemplate> aspectClass) {
    return registry.get(aspectClass);
  }

  @Override
  public boolean isRegistered(Class<? extends RecordTemplate> aspectClass) {
    return registry.containsKey(aspectClass);
  }
}
