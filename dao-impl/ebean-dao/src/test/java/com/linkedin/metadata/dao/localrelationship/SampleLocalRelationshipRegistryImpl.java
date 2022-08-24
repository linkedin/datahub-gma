package com.linkedin.metadata.dao.localrelationship;

import com.google.common.collect.ImmutableMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.builder.BaseLocalRelationshipBuilder;
import com.linkedin.metadata.dao.builder.LocalRelationshipBuilderRegistry;
import com.linkedin.metadata.dao.localrelationship.builder.BelongsToLocalRelationshipBuilder;
import com.linkedin.testing.localrelationship.AspectFooBar;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


public class SampleLocalRelationshipRegistryImpl implements LocalRelationshipBuilderRegistry {
  private Map<Class<? extends RecordTemplate>, BaseLocalRelationshipBuilder> builders =
      new ImmutableMap.Builder().put(AspectFooBar.class, new BelongsToLocalRelationshipBuilder(AspectFooBar.class)).build();

  @Nullable
  @Override
  public <ASPECT extends RecordTemplate> BaseLocalRelationshipBuilder getLocalRelationshipBuilder(
      @Nonnull ASPECT aspect) {
    return builders.get(aspect.getClass());
  }

  @Override
  public <ASPECT extends RecordTemplate> boolean isRegistered(@Nonnull Class<ASPECT> aspectClass) {
    return builders.containsKey(aspectClass);
  }
}
