package com.linkedin.metadata.dao.utils;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.internal.BaseGraphWriterDAO;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.linkedin.metadata.dao.utils.ModelUtils.*;


public class GraphUtils {
  private static final String SOURCE = "source";
  private GraphUtils() {
    // Util class
  }

  /**
   * Check if a group relationship shares the same source urn, destination urn or both based on the remove option.
   * @param relationships  list of relationships
   * @param removalOption  removal option to specify which relationships to be removed
   * @param sourceField    name of the source field
   * @param destinationField name of the destination field
   * @param urn  source urn to compare. Optional for V1. Needed for V2.
   */
  public static void checkSameUrn(@Nonnull final List<? extends RecordTemplate> relationships,
      @Nonnull final BaseGraphWriterDAO.RemovalOption removalOption, @Nonnull final String sourceField,
      @Nonnull final String destinationField, @Nullable Urn urn) {

    if (relationships.isEmpty()) {
      return;
    }

    final Urn sourceUrn = getSourceUrnBasedOnRelationshipVersion(relationships.get(0), urn);
    final Urn destinationUrn = getDestinationUrnFromRelationship(relationships.get(0));

    if (removalOption == BaseGraphWriterDAO.RemovalOption.REMOVE_ALL_EDGES_FROM_SOURCE) {
      checkSameUrn(relationships, sourceField, sourceUrn);
    } else if (removalOption == BaseGraphWriterDAO.RemovalOption.REMOVE_ALL_EDGES_TO_DESTINATION) {
      checkSameUrn(relationships, destinationField, destinationUrn);
    } else if (removalOption == BaseGraphWriterDAO.RemovalOption.REMOVE_ALL_EDGES_FROM_SOURCE_TO_DESTINATION) {
      checkSameUrn(relationships, sourceField, sourceUrn);
      checkSameUrn(relationships, destinationField, destinationUrn);
    }
  }

  /**
   * Get the source asset's urn for a given relationship.
   * @param relationship Relationship. The relationship can be in model V1 or V2.
   * @param urn The source asset urn. Optional for V1. Must for V2. Exception will be thrown if urn is not provided for V2.
   * @return The source asset urn.
   */
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

  public static void checkSameUrn(@Nonnull final List<? extends RecordTemplate> relationships,
      @Nonnull final BaseGraphWriterDAO.RemovalOption removalOption, @Nonnull final String sourceField,
      @Nonnull final String destinationField) {
    checkSameUrn(relationships, removalOption, sourceField, destinationField, null);
  }

  public static void checkSameUrn(@Nonnull List<? extends RecordTemplate> records, @Nonnull String field,
      @Nonnull Urn compare) {
    for (RecordTemplate relation : records) {
      if (ModelUtils.isRelationshipInV2(relation.schema()) && field.equals(SOURCE)) {
        // Skip source urn check for V2 relationships since they don't have source field
        // ToDo: enhance the source check for V2 relationships
        return;
      }
      if (!compare.equals(ModelUtils.getUrnFromRelationship(relation, field))) {
        throw new IllegalArgumentException("Records have different " + field + " urn");
      }
    }
  }
}
