package com.linkedin.metadata.dao.utils;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.internal.BaseGraphWriterDAO;
import java.util.List;
import javax.annotation.Nonnull;

import static com.linkedin.metadata.dao.utils.ModelUtils.*;
import static com.linkedin.metadata.dao.utils.RecordUtils.*;


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

    final Urn source0Urn = getSourceUrnFromRelationship(relationships.get(0));
    final Urn destination0Urn = getDestinationUrnFromRelationship(relationships.get(0));

    if (removalOption == BaseGraphWriterDAO.RemovalOption.REMOVE_ALL_EDGES_FROM_SOURCE) {
      checkSameUrn(relationships, sourceField, source0Urn);
    } else if (removalOption == BaseGraphWriterDAO.RemovalOption.REMOVE_ALL_EDGES_TO_DESTINATION) {
      checkSameUrn(relationships, destinationField, destination0Urn);
    } else if (removalOption == BaseGraphWriterDAO.RemovalOption.REMOVE_ALL_EDGES_FROM_SOURCE_TO_DESTINATION) {
      checkSameUrn(relationships, sourceField, source0Urn);
      checkSameUrn(relationships, destinationField, destination0Urn);
    }
  }

  private static void checkSameUrn(@Nonnull List<? extends RecordTemplate> records, @Nonnull String field,
      @Nonnull Urn compare) {
    for (RecordTemplate relation : records) {
      if (!compare.equals(getRecordTemplateField(relation, field, Urn.class))) {
        throw new IllegalArgumentException("Records have different " + field + " urn");
      }
    }
  }
}
