package com.linkedin.metadata.dao;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.builder.BaseLocalRelationshipBuilder.LocalRelationshipUpdates;
import com.linkedin.metadata.dao.exception.RetryLimitReached;
import com.linkedin.metadata.dao.internal.BaseGraphWriterDAO;
import com.linkedin.metadata.dao.utils.GraphUtils;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.dao.utils.SQLSchemaUtils;
import com.linkedin.metadata.dao.utils.SQLStatementUtils;
import com.linkedin.metadata.validator.RelationshipValidator;
import io.ebean.EbeanServer;
import io.ebean.SqlUpdate;
import io.ebean.Transaction;
import io.ebean.annotation.Transactional;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.dao.utils.ModelUtils.*;
@Slf4j
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
  }
  private static final int BATCH_SIZE = 10000; // Process rows in batches of 10,000
  private static final int MAX_BATCHES = 1000; // Maximum number of batches to process
  private static final String LIMIT = " LIMIT ";
  @Getter
  private int batchCount = 0;

  public EbeanLocalRelationshipWriterDAO(EbeanServer server) {
    _server = server;
  }

  /**
   * Process the local relationship updates with transaction guarantee.
   * @param urn Urn of the entity to update relationships.
   * @param relationshipUpdates Updates to local relationship tables.
   * @param isTestMode whether to use test schema
   */
  @Transactional
  public void processLocalRelationshipUpdates(@Nonnull Urn urn,
      @Nonnull List<LocalRelationshipUpdates> relationshipUpdates, boolean isTestMode) {
    for (LocalRelationshipUpdates relationshipUpdate : relationshipUpdates) {
      if (relationshipUpdate.getRelationships().isEmpty()) {
        clearRelationshipsByEntity(urn, relationshipUpdate.getRelationshipClass(), isTestMode);
      } else {
        addRelationships(relationshipUpdate.getRelationships(), isTestMode, urn);
      }
    }
  }

  /**
   * This method clears all the relationships from a source entity urn using REMOVE_ALL_EDGES_FROM_SOURCE.
   *
   * @param urn                      entity urn could be either source or destination, depends on the RemovalOption
   * @param relationshipClass        relationship that needs to be cleared
   * @param isTestMode               whether to use test schema
   */
  public void clearRelationshipsByEntity(@Nonnull Urn urn,
      @Nonnull Class<? extends RecordTemplate> relationshipClass, boolean isTestMode)  {
    RelationshipValidator.validateRelationshipSchema(relationshipClass, isRelationshipInV2(relationshipClass));
    String deletionQuery = SQLStatementUtils.deleteLocalRelationshipSQL(
        isTestMode ? SQLSchemaUtils.getTestRelationshipTableName(relationshipClass)
            : SQLSchemaUtils.getRelationshipTableName(relationshipClass), RemovalOption.REMOVE_ALL_EDGES_FROM_SOURCE) + LIMIT + BATCH_SIZE;
    SqlUpdate deletionSQL = _server.createSqlUpdate(deletionQuery);
    deletionSQL.setParameter(CommonColumnName.SOURCE, urn.toString());
    batchCount = 0;
    while (batchCount < MAX_BATCHES) {
      try {
        // Use the runInTransactionWithRetry method to handle retries in case of transaction failures
        int rowsAffected = runInTransactionWithRetry(deletionSQL::execute, 3); // Retry up to 3 times in case of transient failures
        batchCount++;

        if (log.isDebugEnabled()) {
          log.debug("Deleted {} rows in batch {}", rowsAffected, batchCount);
        }

        if (rowsAffected < BATCH_SIZE) {
          // Exit loop if fewer than BATCH_SIZE rows were affected, indicating all rows are processed
          break;
        }

        // Sleep for 1 millisecond to reduce load
        Thread.sleep(1);
      } catch (RetryLimitReached e) {
        log.error("Error while executing batch deletion after {} batches and retries", batchCount, e);
        throw new RuntimeException("Batch deletion failed due to retry limit", e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt(); // Restore interrupted status
        throw new RuntimeException("Batch deletion interrupted", e);
      } catch (Exception e) {
        log.error("Error while executing batch deletion after {} batches", batchCount, e);
        throw new RuntimeException("Batch deletion failed", e);
      }
  }

    if (batchCount >= MAX_BATCHES) {
      log.warn(
          "Reached maximum batch count of {}, consider increasing MAX_BATCH_COUNT or debugging the deletion logic.",
          MAX_BATCHES);
    }

    if (log.isDebugEnabled()) {
      log.info("Cleared relationships in {} batches", batchCount);
    }
  }


  /**
   * Persist the given list of relationships to the local relationship using REMOVE_ALL_EDGES_FROM_SOURCE.
   * @param relationships the list of relationships to be persisted
   * @param isTestMode whether to use test schema
   * @param urn Urn of the entity to update relationships.
   *            For Relationship V1: Optional, can be source or destination urn.
   *            For Relationship V2: Required, is the source urn.
   */
  public <RELATIONSHIP extends RecordTemplate> void addRelationships(@Nonnull List<RELATIONSHIP> relationships,
      boolean isTestMode, @Nullable Urn urn) {
    // split relationships by relationship type
    Map<String, List<RELATIONSHIP>> relationshipGroupMap = relationships.stream()
        .collect(Collectors.groupingBy(relationship -> relationship.getClass().getCanonicalName()));

    // validate if all relationship groups have valid urns
    relationshipGroupMap.values().forEach(relationshipGroup -> GraphUtils.checkSameSourceUrn(relationshipGroup, urn));

    relationshipGroupMap.values().forEach(relationshipGroup -> {
      addRelationshipGroup(relationshipGroup, isTestMode, urn);
    });
  }

  // This method only supports Relationship 1.0 (i.e. source present in model) ingestion using graph builders.
  @Override
  public <RELATIONSHIP extends RecordTemplate> void addRelationships(@Nonnull List<RELATIONSHIP> relationships,
      @Nonnull RemovalOption removalOption, boolean isTestMode) {
    addRelationships(relationships, isTestMode, null);
  }

  @Override
  public <RELATIONSHIP extends RecordTemplate> void removeRelationships(@Nonnull List<RELATIONSHIP> relationships) {
    removeRelationshipsV2(relationships, null);
  }

  public <RELATIONSHIP extends RecordTemplate> void removeRelationshipsV2(@Nonnull List<RELATIONSHIP> relationships, @Nullable Urn sourceUrn) {
    for (RELATIONSHIP relationship : relationships) {
      _server.createSqlUpdate(SQLStatementUtils.deleteLocalRelationshipSQL(SQLSchemaUtils.getRelationshipTableName(relationship),
              RemovalOption.REMOVE_ALL_EDGES_FROM_SOURCE_TO_DESTINATION))
          .setParameter(CommonColumnName.SOURCE, GraphUtils.getSourceUrnBasedOnRelationshipVersion(relationship, sourceUrn).toString())
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

  /**
   * Add the given list of relationships to the local relationship tables.
   * @param relationshipGroup the list of relationships to be persisted
   * @param isTestMode  whether to use test schema
   * @param urn the source urn to be used for the relationships. Optional for Relationship V1.
   *            Needed for Relationship V2 because source is not included in the relationshipV2 metadata.
   */
  private <RELATIONSHIP extends RecordTemplate> void addRelationshipGroup(@Nonnull final List<RELATIONSHIP> relationshipGroup,
      boolean isTestMode, @Nullable Urn urn) {
    if (relationshipGroup.size() == 0) {
      return;
    }

    RELATIONSHIP firstRelationship = relationshipGroup.get(0);
    RelationshipValidator.validateRelationshipSchema(firstRelationship.getClass(), isRelationshipInV2(firstRelationship.getClass()));

    // Remove some local relationships if needed before adding new relationships using REMOVE_ALL_EDGES_FROM_SOURCE.
    removeRelationshipsBySource(isTestMode ? SQLSchemaUtils.getTestRelationshipTableName(firstRelationship)
        : SQLSchemaUtils.getRelationshipTableName(firstRelationship), firstRelationship, urn);

    long now = Instant.now().toEpochMilli();

    for (RELATIONSHIP relationship : relationshipGroup) {
      // Relationship model V2 doesn't include source urn, it needs to be passed in.
      // For relationship model V1, this given urn can be source urn or destination urn.
      // For relationship model V2, this given urn can only be source urn.
      Urn source = GraphUtils.getSourceUrnBasedOnRelationshipVersion(relationship, urn);
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

  /**
   * Process the relationship removal in the DB tableName based on the removal option.
   * @param tableName the table name of the relationship
   * @param relationship the relationship to be removed
   * @param urn the source urn to be used for the relationships. Optional for Relationship V1.
   *            Needed for Relationship V2 because source is not included in the relationshipV2 metadata.
   */
  private <RELATIONSHIP extends RecordTemplate> void removeRelationshipsBySource(@Nonnull String tableName,
      @Nonnull RELATIONSHIP relationship, @Nullable Urn urn) {
    SqlUpdate deletionSQL = _server.createSqlUpdate(SQLStatementUtils.deleteLocalRelationshipSQL(tableName, RemovalOption.REMOVE_ALL_EDGES_FROM_SOURCE));
    Urn source = GraphUtils.getSourceUrnBasedOnRelationshipVersion(relationship, urn);
    deletionSQL.setParameter(CommonColumnName.SOURCE, source.toString());
    deletionSQL.execute();
  }

  @Nonnull
  protected <T> T runInTransactionWithRetry(@Nonnull Supplier<T> block, int maxTransactionRetry) {
    int retryCount = 0;
    RuntimeException lastException = null;
    while (retryCount <= maxTransactionRetry) {
      try (Transaction transaction = _server.beginTransaction()) {
        T result = block.get();
        transaction.commit();
        return result; // Successful execution, return result
      } catch (RuntimeException exception) {
        lastException = exception;
        retryCount++;
      }
    }
    // If we exhausted retries, throw an exception.
    if (lastException != null) {
      throw new RetryLimitReached("Failed to execute after " + maxTransactionRetry + " retries", lastException);
    } else {
      throw new RetryLimitReached("Failed to execute after " + maxTransactionRetry + " retries due to unknown reasons");
    }
  }
}
