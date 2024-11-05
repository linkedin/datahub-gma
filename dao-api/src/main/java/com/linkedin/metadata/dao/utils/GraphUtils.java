package com.linkedin.metadata.dao.utils;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.internal.BaseGraphWriterDAO;
import java.util.List;
import javax.annotation.Nonnull;

import static com.linkedin.metadata.dao.utils.ModelUtils.*;


public class GraphUtils {
  private GraphUtils() {
    // Util class
  }

  /**
   * Check if a group relationship shares the same source urn, destination urn or both based on the remove option.
   */
  public static void checkSameUrn(@Nonnull final List<? extends RecordTemplate> relationships,
      @Nonnull final BaseGraphWriterDAO.RemovalOption removalOption, final String sourceField, final String destinationField) {

    if (relationships.isEmpty()) {
      return;
    }

    // ToDo: how to handle this for Relationship V2?
    final Urn sourceUrn = getSourceUrnFromRelationship(relationships.get(0));
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

  private static void checkSameUrn(@Nonnull List<? extends RecordTemplate> records, @Nonnull String field,
      @Nonnull Urn compare) {
    for (RecordTemplate relation : records) {
      if (!compare.equals(ModelUtils.getUrnFromRelationship(relation, field))) {
        throw new IllegalArgumentException("Records have different " + field + " urn");
      }
    }
  }
}
