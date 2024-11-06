package com.linkedin.metadata.dao.utils;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.internal.BaseGraphWriterDAO;
import java.util.List;
import javax.annotation.Nonnull;

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
      @Nonnull final BaseGraphWriterDAO.RemovalOption removalOption, final String sourceField,
      final String destinationField, Urn urn) {

    if (relationships.isEmpty()) {
      return;
    }

    Urn sourceUrn = urn;
    if (!ModelUtils.isRelationshipInV2(relationships.get(0).schema())) {
      // get the sourceUrn from relationship, if relationship model in V1
      sourceUrn = getSourceUrnFromRelationship(relationships.get(0));
    }
    if (sourceUrn == null) {
      throw new IllegalArgumentException("Source urn is needed for Relationship V2");
    }
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

  public static void checkSameUrn(@Nonnull final List<? extends RecordTemplate> relationships,
      @Nonnull final BaseGraphWriterDAO.RemovalOption removalOption, final String sourceField, final String destinationField) {
    checkSameUrn(relationships, removalOption, sourceField, destinationField, null);
  }

  private static void checkSameUrn(@Nonnull List<? extends RecordTemplate> records, @Nonnull String field,
      @Nonnull Urn compare) {
    for (RecordTemplate relation : records) {
      if (ModelUtils.isRelationshipInV2(relation.schema()) && field == SOURCE) {
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
