package com.linkedin.metadata.annotations;

import com.google.common.annotations.VisibleForTesting;
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
final class DefaultGmaEntitiesAnnotationAllowList implements GmaEntitiesAnnotationAllowList {
  static final GmaEntitiesAnnotationAllowList DEFAULT =
      new DefaultGmaEntitiesAnnotationAllowList(ImmutableSetMultimap.<String, String>builder() //
          .put("com.linkedin.common.Follow", "com.linkedin.metadata.aspect.DatasetUrn")
          .put("com.linkedin.common.Follow", "com.linkedin.metadata.aspect.MetricUrn")
          .put("com.linkedin.common.Health", "com.linkedin.metadata.aspect.DatasetUrn")
          .put("com.linkedin.common.Health", "com.linkedin.metadata.aspect.MetricUrn")
          .put("com.linkedin.common.InstitutionalMemory", "com.linkedin.metadata.aspect.DataConceptUrn")
          .put("com.linkedin.common.InstitutionalMemory", "com.linkedin.metadata.aspect.DatasetUrn")
          .put("com.linkedin.common.InstitutionalMemory", "com.linkedin.metadata.aspect.MetricUrn")
          .put("com.linkedin.common.Likes", "com.linkedin.metadata.aspect.DatasetUrn")
          .put("com.linkedin.common.Likes", "com.linkedin.metadata.aspect.MetricUrn")
          .put("com.linkedin.common.Ownership", "com.linkedin.metadata.aspect.AzkabanFlowUrn")
          .put("com.linkedin.common.Ownership", "com.linkedin.metadata.aspect.AzkabanJobUrn")
          .put("com.linkedin.common.Ownership", "com.linkedin.metadata.aspect.DataConceptUrn")
          .put("com.linkedin.common.Ownership", "com.linkedin.metadata.aspect.DatasetUrn")
          .put("com.linkedin.common.Ownership", "com.linkedin.metadata.aspect.FeatureUrn")
          .put("com.linkedin.common.Ownership", "com.linkedin.metadata.aspect.InchartsChartUrn")
          .put("com.linkedin.common.Ownership", "com.linkedin.metadata.aspect.InchartsDashboardUrn")
          .put("com.linkedin.common.Ownership", "com.linkedin.metadata.aspect.InferenceUrn")
          .put("com.linkedin.common.Ownership", "com.linkedin.metadata.aspect.MetricUrn")
          .put("com.linkedin.common.Ownership", "com.linkedin.metadata.aspect.SamzaJobInstanceUrn")
          .put("com.linkedin.common.Ownership", "com.linkedin.metadata.aspect.TrexNotificationSubscriptionUrn")
          .put("com.linkedin.common.Status", "com.linkedin.metadata.aspect.DatasetInstanceUrn")
          .put("com.linkedin.common.Status", "com.linkedin.metadata.aspect.DatasetUrn")
          .put("com.linkedin.common.Status", "com.linkedin.metadata.aspect.FeatureUrn")
          .put("com.linkedin.common.Status", "com.linkedin.metadata.aspect.InchartsChartInstanceUrn")
          .put("com.linkedin.common.Status", "com.linkedin.metadata.aspect.InchartsChartUrn")
          .put("com.linkedin.common.Status", "com.linkedin.metadata.aspect.InchartsDashboardUrn")
          .put("com.linkedin.common.Status", "com.linkedin.metadata.aspect.MetricUrn")
          .build());

  private final Multimap<String, String> _allowedModelFqcnsToAllowedUrns;

  @VisibleForTesting
  DefaultGmaEntitiesAnnotationAllowList(Multimap<String, String> allowedModelFqcnsToAllowedUrns) {
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
