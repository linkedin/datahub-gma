package com.linkedin.metadata.annotations;

import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.data.schema.RecordDataSchema;
import java.util.Collection;
import javax.annotation.Nonnull;

/**
 * Simple allow list for the {@code @gma.aspect.entities} annotation.
 *
 * <p>This annotation is only used for helping us migrate old "common" aspects to v5 and should not be used for anything
 * new in v5.
 */
public final class GmaEntitiesAnnotationAllowListImpl implements GmaEntitiesAnnotationAllowList {
  public static final GmaEntitiesAnnotationAllowList DEFAULT =
      new GmaEntitiesAnnotationAllowListImpl(ImmutableSetMultimap.<String, String>builder() //
          .put("com.linkedin.common.InstitutionalMemory", "com.linkedin.metadata.aspect.DatasetUrn")
          .put("com.linkedin.common.InstitutionalMemory", "com.linkedin.metadata.aspect.MLFeatureAspect")
          .put("com.linkedin.common.InstitutionalMemory", "com.linkedin.metadata.aspect.MLModelAspect")
          .put("com.linkedin.common.Ownership", "com.linkedin.metadata.aspect.ChartUrn")
          .put("com.linkedin.common.Ownership", "com.linkedin.metadata.aspect.DataFlowUrn")
          .put("com.linkedin.common.Ownership", "com.linkedin.metadata.aspect.DataJobUrn")
          .put("com.linkedin.common.Ownership", "com.linkedin.metadata.aspect.DataProcessUrn")
          .put("com.linkedin.common.Ownership", "com.linkedin.metadata.aspect.DashboardUrn")
          .put("com.linkedin.common.Ownership", "com.linkedin.metadata.aspect.DatasetUrn")
          .put("com.linkedin.common.Ownership", "com.linkedin.metadata.aspect.MLFeatureAspect")
          .put("com.linkedin.common.Ownership", "com.linkedin.metadata.aspect.MLModelAspect")
          .put("com.linkedin.common.Status", "com.linkedin.metadata.aspect.ChartUrn")
          .put("com.linkedin.common.Status", "com.linkedin.metadata.aspect.DashboardUrn")
          .put("com.linkedin.common.Status", "com.linkedin.metadata.aspect.DatasetUrn")
          .put("com.linkedin.common.Status", "com.linkedin.metadata.aspect.MLFeatureAspect")
          .put("com.linkedin.common.Status", "com.linkedin.metadata.aspect.MLModelAspect")
          .build());

  private final Multimap<String, String> _allowedModelFqcnsToAllowedUrns;

  public GmaEntitiesAnnotationAllowListImpl(Multimap<String, String> allowedModelFqcnsToAllowedUrns) {
    _allowedModelFqcnsToAllowedUrns = allowedModelFqcnsToAllowedUrns;
  }

  @Override
  public void check(@Nonnull RecordDataSchema schema, @Nonnull AspectAnnotation aspectAnnotation) {
    final Collection<String> urns = _allowedModelFqcnsToAllowedUrns.get(schema.getFullName());

    if (urns.isEmpty()) {
      throw new AnnotationNotAllowedException(
          String.format("@gma.aspect.entities not allowed on %s", schema.getFullName()));
    }

    for (AspectEntityAnnotation entity : aspectAnnotation.getEntities()) {
      if (!urns.contains(entity.getUrn())) {
        throw new AnnotationNotAllowedException(
            String.format("URN %s is not allowed for entity %s", entity.getUrn(), schema.getFullName()));
      }
    }
  }
}
