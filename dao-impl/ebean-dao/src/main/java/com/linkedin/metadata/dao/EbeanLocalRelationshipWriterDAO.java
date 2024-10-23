package com.linkedin.metadata.dao;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.builder.BaseLocalRelationshipBuilder.LocalRelationshipUpdates;
import com.linkedin.metadata.dao.internal.BaseGraphWriterDAO;
import com.linkedin.metadata.dao.utils.GraphUtils;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.dao.utils.SQLSchemaUtils;
import com.linkedin.metadata.dao.utils.SQLStatementUtils;
import com.linkedin.metadata.validator.RelationshipValidator;
import io.ebean.EbeanServer;
import io.ebean.SqlUpdate;
import io.ebean.annotation.Transactional;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

import static com.linkedin.metadata.dao.utils.ModelUtils.*;

public class EbeanLocalRelationshipWriterDAO extends BaseGraphWriterDAO {
  private static final String DEFAULT_ACTOR = "urn:li:principal:UNKNOWN";
  private final EbeanServer _server;

  // Common column names shared by all local relationship tables.
  private static class CommonColumnName {
    private static final String SOURCE = "source";
    private static final String DESTINATION = "destination";
    private static final String SOURCE_TYPE = "source_type";
    private static final String DESTINATION_TYPE = "destination_type";
    private static final String METADATA = "metadata";
    private static final String LAST_MODIFIED_ON = "lastmodifiedon";
    private static final String LAST_MODIFIED_BY = "lastmodifiedby";
    private static final String DELETED_TS = "deleted_ts";
    private static final String ASPECT = "aspect";
  }

  public EbeanLocalRelationshipWriterDAO(EbeanServer server) {
    _server = server;
  }

  /**
   * Process the local relationship updates with transaction guarantee.
   * @param relationshipUpdates Updates to local relationship tables.
   */
  @Transactional
  public void processLocalRelationshipUpdates(@Nonnull Urn urn,
      @Nonnull List<LocalRelationshipUpdates> relationshipUpdates, boolean isTestMode) {
    for (LocalRelationshipUpdates relationshipUpdate : relationshipUpdates) {
      if (relationshipUpdate.getRelationships().isEmpty()) {
        clearRelationshipsByEntity(urn, relationshipUpdate.getRelationshipClass(),
            relationshipUpdate.getRemovalOption(), isTestMode);
      } else {
        addRelationships(relationshipUpdate.getRelationships(), relationshipUpdate.getRemovalOption(), isTestMode);
      }
    }
  }

  /**
   * This method is to serve for the purpose to clear all the relationships from a source entity urn.
   * @param urn entity urn could be either source or destination, depends on the RemovalOption
   * @param relationshipClass relationship that needs to be cleared
   */
  public void clearRelationshipsByEntity(@Nonnull Urn urn,
      @Nonnull Class<? extends RecordTemplate> relationshipClass, @Nonnull RemovalOption removalOption, boolean isTestMode) {
    if (removalOption == RemovalOption.REMOVE_NONE
        || removalOption == RemovalOption.REMOVE_ALL_EDGES_FROM_SOURCE_TO_DESTINATION) {
      // this method is to handle the case of adding empty relationship list to clear relationships of an entity urn
      // REMOVE_NONE and REMOVE_ALL_EDGES_FROM_SOURCE_TO_DESTINATION won't apply for this case.
      return;
    }
    RelationshipValidator.validateRelationshipSchema(relationshipClass);
    SqlUpdate deletionSQL = _server.createSqlUpdate(SQLStatementUtils.deleteLocalRelationshipSQL(
        isTestMode ? SQLSchemaUtils.getTestRelationshipTableName(relationshipClass)
            : SQLSchemaUtils.getRelationshipTableName(relationshipClass), removalOption));
    if (removalOption == RemovalOption.REMOVE_ALL_EDGES_FROM_SOURCE) {
      deletionSQL.setParameter(CommonColumnName.SOURCE, urn.toString());
    } else if (removalOption == RemovalOption.REMOVE_ALL_EDGES_TO_DESTINATION) {
      deletionSQL.setParameter(CommonColumnName.DESTINATION, urn.toString());
    }
    deletionSQL.execute();
  }

  @Override
  public <RELATIONSHIP extends RecordTemplate> void addRelationships(@Nonnull List<RELATIONSHIP> relationships,
      @Nonnull RemovalOption removalOption, boolean isTestMode) {
    // split relationships by relationship type
    Map<String, List<RELATIONSHIP>> relationshipGroupMap = relationships.stream()
        .collect(Collectors.groupingBy(relationship -> relationship.getClass().getCanonicalName()));

    // validate if all relationship groups have valid urns
    relationshipGroupMap.values().forEach(relationshipGroup
        -> GraphUtils.checkSameUrn(relationshipGroup, removalOption, CommonColumnName.SOURCE, CommonColumnName.DESTINATION));

    relationshipGroupMap.values().forEach(relationshipGroup -> {
      addRelationshipGroup(relationshipGroup, removalOption, isTestMode);
    });
  }

  @Override
  public <RELATIONSHIP extends RecordTemplate> void removeRelationships(@Nonnull List<RELATIONSHIP> relationships) {
    for (RELATIONSHIP relationship : relationships) {
      _server.createSqlUpdate(SQLStatementUtils.deleteLocalRelationshipSQL(SQLSchemaUtils.getRelationshipTableName(relationship),
              RemovalOption.REMOVE_ALL_EDGES_FROM_SOURCE_TO_DESTINATION))
          .setParameter(CommonColumnName.SOURCE, getSourceUrnFromRelationship(relationship).toString())
          .setParameter(CommonColumnName.DESTINATION, getDestinationUrnFromRelationship(relationship).toString())
          .execute();
    }
  }

  @Override
  public <ENTITY extends RecordTemplate> void addEntities(@Nonnull List<ENTITY> entities) {
    throw new UnsupportedOperationException("Local relationship does not support adding entity. Please consider using metadata entity table.");
  }

  @Override
  public <URN extends Urn> void removeEntities(@Nonnull List<URN> urns) {
    throw new UnsupportedOperationException("Local relationship does not support removing entity. Please consider using metadata entity table.");
  }

  private <RELATIONSHIP extends RecordTemplate> void addRelationshipGroup(@Nonnull final List<RELATIONSHIP> relationshipGroup,
      @Nonnull RemovalOption removalOption, boolean isTestMode) {
    if (relationshipGroup.size() == 0) {
      return;
    }

    RELATIONSHIP firstRelationship = relationshipGroup.get(0);
    RelationshipValidator.validateRelationshipSchema(firstRelationship.getClass());

    // Process remove option to delete some local relationships if needed before adding new relationships.
    processRemovalOption(isTestMode ? SQLSchemaUtils.getTestRelationshipTableName(firstRelationship)
        : SQLSchemaUtils.getRelationshipTableName(firstRelationship), firstRelationship, removalOption);

    long now = Instant.now().toEpochMilli();

    for (RELATIONSHIP relationship : relationshipGroup) {
      Urn source = getSourceUrnFromRelationship(relationship);
      Urn destination = getDestinationUrnFromRelationship(relationship);

      _server.createSqlUpdate(SQLStatementUtils.insertLocalRelationshipSQL(
              isTestMode ? SQLSchemaUtils.getTestRelationshipTableName(relationship)
                  : SQLSchemaUtils.getRelationshipTableName(relationship)))
          .setParameter(CommonColumnName.METADATA, RecordUtils.toJsonString(relationship))
          .setParameter(CommonColumnName.SOURCE_TYPE, source.getEntityType())
          .setParameter(CommonColumnName.DESTINATION_TYPE, destination.getEntityType())
          .setParameter(CommonColumnName.SOURCE, source.toString())
          .setParameter(CommonColumnName.DESTINATION, destination.toString())
          .setParameter(CommonColumnName.LAST_MODIFIED_ON, new Timestamp(now))
          .setParameter(CommonColumnName.LAST_MODIFIED_BY, DEFAULT_ACTOR)
          .execute();
    }
  }

  @ParametersAreNonnullByDefault
  private <RELATIONSHIP extends RecordTemplate> void processRemovalOption(String tableName, RELATIONSHIP relationship,
      RemovalOption removalOption) {

    if (removalOption == RemovalOption.REMOVE_NONE) {
      return;
    }

    SqlUpdate deletionSQL = _server.createSqlUpdate(SQLStatementUtils.deleteLocalRelationshipSQL(tableName, removalOption));
    Urn source = getSourceUrnFromRelationship(relationship);
    Urn destination = getDestinationUrnFromRelationship(relationship);

    if (removalOption == RemovalOption.REMOVE_ALL_EDGES_FROM_SOURCE_TO_DESTINATION) {
      deletionSQL.setParameter(CommonColumnName.DESTINATION, destination.toString());
      deletionSQL.setParameter(CommonColumnName.SOURCE, source.toString());
    } else if (removalOption == RemovalOption.REMOVE_ALL_EDGES_FROM_SOURCE) {
      deletionSQL.setParameter(CommonColumnName.SOURCE, source.toString());
    } else if (removalOption == RemovalOption.REMOVE_ALL_EDGES_TO_DESTINATION) {
      deletionSQL.setParameter(CommonColumnName.DESTINATION, destination.toString());
    }

    deletionSQL.execute();
  }
}
