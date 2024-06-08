package com.linkedin.metadata.dao;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.DataTemplateUtil;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.equality.GenericEqualityTester;
import com.linkedin.metadata.dao.exception.RetryLimitReached;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.query.ExtraInfo;
import io.ebean.DuplicateKeyException;
import io.ebean.EbeanServer;
import io.ebean.SqlUpdate;
import io.ebean.Transaction;
import io.ebean.config.ServerConfig;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.persistence.OptimisticLockException;
import javax.persistence.RollbackException;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.dao.EbeanLocalDAO.*;
import static com.linkedin.metadata.dao.EbeanMetadataAspect.*;
import static com.linkedin.metadata.dao.utils.EBeanDAOUtils.*;
import static com.linkedin.metadata.dao.utils.EbeanServerUtils.*;
import static com.linkedin.metadata.dao.utils.RecordUtils.toRecordTemplate;
import static com.linkedin.metadata.dao.utils.SQLStatementUtils.*;


/**
 * Ebean Generic DAO. This is to access metadata stored in legacy format e.g. metadata_aspect.
 * Please see {@link EbeanGenericLocalAccess} for DAO accessing the metadata stored in new format e.g. metadata_entity_dataset.
 */
@Slf4j
public class EbeanGenericLocalDAO implements GenericLocalDAO {

  private final EbeanServer _server;

  private Map<Class, GenericEqualityTester> _equalityTesters = new HashMap<>();

  public EbeanGenericLocalDAO(@Nonnull ServerConfig serverConfig) {
    _server = createServer(serverConfig);
  }

  /**
   * Set equality testers. An equality tester checks if two aspects are considered equal.
   */
  public void setEqualityTesters(Map<Class, GenericEqualityTester> equalityTesters) {
    _equalityTesters = equalityTesters;
  }

  /**
   * Save the metadata into database. High level persistence logic:
   * 1. Find the latest version of the metadata.
   * 2. If there is no such metadata, directly insert the metadata as the latest version.
   * 3. If there is such metadata, run equality check to see if current and new metadata are "equal".
   *    a. If they are equal, then skip since no need to store duplicates.
   *    b. If they are not equal, save the new metadata as the latest version and update the old metadata as old version.
   *
   * @param urn The identifier of the entity which the metadata is associated with.
   * @param aspectClass The aspect class for the metadata.
   * @param metadata The metadata serialized as JSON string.
   * @param auditStamp audit stamp containing information on who and when the metadata is saved.
   */
  public void save(@Nonnull Urn urn, @Nonnull Class aspectClass, @Nonnull String metadata, @Nonnull AuditStamp auditStamp) {
    runInTransactionWithRetry(() -> {
      final Optional<GenericLocalDAO.MetadataWithExtraInfo> latest = queryLatest(urn, aspectClass);
      RecordTemplate newValue = toRecordTemplate(aspectClass, metadata);

      if (!latest.isPresent()) {
        saveLatest(urn, aspectClass, newValue, null, auditStamp, null);
      } else {
        RecordTemplate currentValue = toRecordTemplate(aspectClass, latest.get().getAspect());

        // Skip update if current value and new value are equal.
        if (!areEqual(currentValue, newValue, _equalityTesters.get(aspectClass))) {
          saveLatest(urn, aspectClass, newValue, currentValue, auditStamp, latest.get().getExtraInfo().getAudit());
        }
      }
      return null;
    }, 5);
  }

  /**
   * Query the latest metadata from database.
   * @param urn The identifier of the entity which the metadata is associated with.
   * @param aspectClass The aspect class for the metadata.
   * @return The metadata with extra info regarding auditing.
   */
  public Optional<GenericLocalDAO.MetadataWithExtraInfo> queryLatest(@Nonnull Urn urn, @Nonnull Class aspectClass) {

    final String aspectName = ModelUtils.getAspectName(aspectClass);
    final PrimaryKey key = new PrimaryKey(urn.toString(), aspectName, LATEST_VERSION);
    EbeanMetadataAspect metadata = _server.find(EbeanMetadataAspect.class, key);

    if (metadata == null || metadata.getMetadata() == null) {
      return Optional.empty();
    }

    final ExtraInfo extraInfo = toExtraInfo(metadata);
    return Optional.of(new GenericLocalDAO.MetadataWithExtraInfo(metadata.getMetadata(), extraInfo));
  }

  /**
   * Save metadata into database.
   */
  private void saveLatest(@Nonnull Urn urn, @Nonnull Class aspectClass, @Nonnull RecordTemplate newValue,
      @Nullable RecordTemplate currentValue, @Nonnull AuditStamp newAuditStamp, @Nullable AuditStamp currentAuditStamp) {

    // Save oldValue as the largest version + 1
    long largestVersion = getNextVersion(urn, aspectClass);

    log.debug(String.format("The largest version of %s for entity %s is %d", aspectClass.getSimpleName(), urn, largestVersion));

    if (currentValue != null && currentAuditStamp != null) {
      // Move latest version to historical version by insert a new record only if we are not overwriting the latest version.
      insert(urn, currentValue, aspectClass, currentAuditStamp, largestVersion);

      // update latest version
      updateWithOptimisticLocking(urn, newValue, aspectClass, newAuditStamp, 0, new Timestamp(currentAuditStamp.getTime()));
    } else {
      // When for fresh ingestion or with changeLog disabled
      insert(urn, newValue, aspectClass, newAuditStamp, 0);
    }
  }

  private long getNextVersion(@Nonnull Urn urn, @Nonnull Class aspectClass) {
    final List<EbeanMetadataAspect.PrimaryKey> result = _server.find(EbeanMetadataAspect.class)
        .where()
        .eq(URN_COLUMN, urn.toString())
        .eq(ASPECT_COLUMN, ModelUtils.getAspectName(aspectClass))
        .orderBy()
        .desc(VERSION_COLUMN)
        .setMaxRows(1)
        .findIds();

    return result.isEmpty() ? 0 : result.get(0).getVersion() + 1L;
  }

  private void insert(@Nonnull Urn urn, @Nullable RecordTemplate value, @Nonnull Class aspectClass,
      @Nonnull AuditStamp auditStamp, long version) {
    final EbeanMetadataAspect aspect = buildMetadataAspectBean(urn, value, aspectClass, auditStamp, version);
    _server.insert(aspect);
  }

  @Nonnull
  private <ASPECT extends RecordTemplate> EbeanMetadataAspect buildMetadataAspectBean(@Nonnull Urn urn,
      @Nullable RecordTemplate value, @Nonnull Class<ASPECT> aspectClass, @Nonnull AuditStamp auditStamp, long version) {

    final String aspectName = ModelUtils.getAspectName(aspectClass);

    final EbeanMetadataAspect aspect = new EbeanMetadataAspect();
    aspect.setKey(new PrimaryKey(urn.toString(), aspectName, version));
    if (value != null) {
      aspect.setMetadata(RecordUtils.toJsonString(value));
    } else {
      aspect.setMetadata(DELETED_VALUE);
    }
    aspect.setCreatedOn(new Timestamp(auditStamp.getTime()));
    aspect.setCreatedBy(auditStamp.getActor().toString());

    final Urn impersonator = auditStamp.getImpersonator();
    if (impersonator != null) {
      aspect.setCreatedFor(impersonator.toString());
    }

    return aspect;
  }

  protected void updateWithOptimisticLocking(@Nonnull Urn urn, @Nullable RecordTemplate value, @Nonnull Class aspectClass,
      @Nonnull AuditStamp newAuditStamp, long version, @Nonnull Timestamp oldTimestamp) {

    final EbeanMetadataAspect aspect = buildMetadataAspectBean(urn, value, aspectClass, newAuditStamp, version);
    final SqlUpdate sqlUpdate = assembleSchemaSqlUpdate(aspect, oldTimestamp);
    final int numOfUpdatedRows = _server.execute(sqlUpdate);

    // If there is no single updated row, throw OptimisticLockException
    if (numOfUpdatedRows != 1) {
      throw new OptimisticLockException(String.format("%s rows updated during update on update: %s.", numOfUpdatedRows, aspect));
    }
  }

  private SqlUpdate assembleSchemaSqlUpdate(@Nonnull EbeanMetadataAspect aspect, @Nullable Timestamp oldTimestamp) {

    final SqlUpdate update;
    if (oldTimestamp == null) {
      update = _server.createSqlUpdate(OPTIMISTIC_LOCKING_UPDATE_SQL);
    } else {
      update = _server.createSqlUpdate(OPTIMISTIC_LOCKING_UPDATE_SQL + " and createdOn = :oldTimestamp");
      update.setParameter("oldTimestamp", oldTimestamp);
    }
    update.setParameter("urn", aspect.getKey().getUrn());
    update.setParameter("aspect", aspect.getKey().getAspect());
    update.setParameter("version", aspect.getKey().getVersion());
    update.setParameter("metadata", aspect.getMetadata());
    update.setParameter("createdOn", aspect.getCreatedOn());
    update.setParameter("createdBy", aspect.getCreatedBy());
    return update;
  }

  // TODO: This validation is still weak. It can only make sure urn is in "urn:li:entity:foo" format.
  private void validateUrn(String urn) {
    try {
      Urn.createFromCharSequence(urn);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid Urn format");
    }
  }

  private boolean areEqual(@Nonnull RecordTemplate r1, @Nonnull RecordTemplate r2, @Nullable GenericEqualityTester equalityTester) {
    if (equalityTester != null) {
      return equalityTester.equals(r1, r2);
    }

    return DataTemplateUtil.areEqual(r1, r2);
  }

  @Nonnull
  protected <T> T runInTransactionWithRetry(@Nonnull Supplier<T> block, int maxTransactionRetry) {
    int retryCount = 0;
    Exception lastException;

    T result = null;
    do {
      try (Transaction transaction = _server.beginTransaction()) {
        result = block.get();
        transaction.commit();
        lastException = null;
        break;
      } catch (RollbackException | DuplicateKeyException | OptimisticLockException exception) {
        lastException = exception;
      }
    } while (++retryCount <= maxTransactionRetry);

    if (lastException != null) {
      throw new RetryLimitReached("Failed to add after " + maxTransactionRetry + " retries", lastException);
    }

    return result;
  }
}
