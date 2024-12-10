package com.linkedin.metadata.dao.utils;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.linkedin.metadata.dao.utils.ModelUtils.*;


public class GraphUtils {
  private GraphUtils() {
    // Util class
  }

  /**
   * Check if a group relationship shares the same source urn.
   * @param relationships  list of relationships
   * @param assetUrn source urn to compare. Optional for V1. Needed for V2.
   */
  public static void checkSameSourceUrn(@Nonnull final List<? extends RecordTemplate> relationships, @Nullable Urn assetUrn) {
    if (relationships.isEmpty()) {
      return;
    }

    // TODO: remove this temporary allow list after AIM no longer relies on ProducesLocalRelationshipBuilderFromFeatureDependencies
    if (assetUrn != null && assetUrn.getEntityType().equals("mlFeatureVersion") && relationships.get(0).getClass().getSimpleName().equals("Produces")) {
      return;
    }

    for (RecordTemplate relationship : relationships) {
      if (ModelUtils.isRelationshipInV2(relationship.schema())) {
        if (assetUrn == null) {
          throw new IllegalArgumentException("Something went wrong. The asset urn is missing which is required during "
              + "ingestion of a model 2.0 relationship. Relationship model: " + relationship);
        }
        // Skip source urn check for V2 relationships since they don't have source field
      } else {
        Urn compare = assetUrn == null ? getSourceUrnFromRelationship(relationships.get(0)) : assetUrn;
        Urn source = getSourceUrnFromRelationship(relationship);
        if (!compare.equals(source)) {
          throw new IllegalArgumentException(
              String.format("Relationships have different source urns. Urn being compared to: %s, Relationship source: %s", compare, source));
        }
      }
    }
  }

  /**
   * Get the source asset's urn for a given relationship.
   * @param relationship Relationship. The relationship can be in model V1 or V2.
   * @param urn The source asset urn. Optional for V1. Must for V2. Exception will be thrown if urn is not provided for V2.
   * @return The source asset urn.
   */
  @Nonnull
  public static <RELATIONSHIP extends RecordTemplate> Urn getSourceUrnBasedOnRelationshipVersion(
      @Nonnull RELATIONSHIP relationship, @Nullable Urn urn) {
    Urn sourceUrn;
    boolean isRelationshipInV2 = ModelUtils.isRelationshipInV2(relationship.schema());
    if (isRelationshipInV2 && urn != null) {
      // if relationship model in V2 and urn is not null, get the sourceUrn from the input urn
      sourceUrn = urn;
    } else if (!isRelationshipInV2) {
      // if relationship model in V1, get the sourceUrn from relationship
      sourceUrn = getSourceUrnFromRelationship(relationship);
    } else {
      // throw exception if relationship in V2 but source urn not provided
      throw new IllegalArgumentException("Source urn is needed for Relationship V2");
    }
    return sourceUrn;
  }
}
