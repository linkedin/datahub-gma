package com.linkedin.metadata.dao;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.metadata.dao.builder.LocalRelationshipBuilderRegistry;
import com.linkedin.metadata.dao.exception.ModelConversionException;
import com.linkedin.metadata.dao.exception.RetryLimitReached;
import com.linkedin.metadata.dao.producer.BaseMetadataEventProducer;
import com.linkedin.metadata.dao.retention.TimeBasedRetention;
import com.linkedin.metadata.dao.retention.VersionBasedRetention;
import com.linkedin.metadata.dao.scsi.EmptyPathExtractor;
import com.linkedin.metadata.dao.scsi.UrnPathExtractor;
import com.linkedin.metadata.dao.storage.LocalDAOStorageConfig;
import com.linkedin.metadata.dao.utils.EBeanDAOUtils;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.dao.utils.QueryUtils;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.dao.utils.SQLIndexFilterUtils;
import com.linkedin.metadata.query.Condition;
import com.linkedin.metadata.query.ExtraInfo;
import com.linkedin.metadata.query.ExtraInfoArray;
import com.linkedin.metadata.query.IndexCriterion;
import com.linkedin.metadata.query.IndexCriterionArray;
import com.linkedin.metadata.query.IndexFilter;
import com.linkedin.metadata.query.IndexGroupByCriterion;
import com.linkedin.metadata.query.IndexSortCriterion;
import com.linkedin.metadata.query.IndexValue;
import com.linkedin.metadata.query.ListResultMetadata;
import com.linkedin.metadata.query.SortOrder;
import io.ebean.DuplicateKeyException;
import io.ebean.EbeanServer;
import io.ebean.ExpressionList;
import io.ebean.PagedList;
import io.ebean.Query;
import io.ebean.SqlQuery;
import io.ebean.SqlRow;
import io.ebean.SqlUpdate;
import io.ebean.Transaction;
import io.ebean.config.ServerConfig;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.persistence.OptimisticLockException;
import javax.persistence.RollbackException;
import javax.persistence.Table;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.dao.EbeanMetadataAspect.*;
import static com.linkedin.metadata.dao.utils.EBeanDAOUtils.*;
import static com.linkedin.metadata.dao.utils.EbeanServerUtils.*;


/**
 * An Ebean implementation of {@link BaseLocalDAO}.
 */
@Slf4j
public class EbeanLocalDAO<ASPECT_UNION extends UnionTemplate, URN extends Urn>
    extends BaseLocalDAO<ASPECT_UNION, URN> {

  private static final int INDEX_QUERY_TIMEOUT_IN_SEC = 10;

  protected final EbeanServer _server;
  protected final Class<URN> _urnClass;

  private int _queryKeysCount = 0; // 0 means no pagination on keys
  private IEbeanLocalAccess<URN> _localAccess;
  private UrnPathExtractor<URN> _urnPathExtractor;
  private SchemaConfig _schemaConfig = SchemaConfig.OLD_SCHEMA_ONLY;

  // Flag for direct SQL execution for latest record retrieval
  private boolean _directSqlRetrieval = false;

  public enum SchemaConfig {
    OLD_SCHEMA_ONLY, // Default: read from and write to the old schema table
    NEW_SCHEMA_ONLY, // Read from and write to the new schema tables
    DUAL_SCHEMA // Write to both the old and new tables and perform a comparison between values when reading
  }

  @Value
  static class GMAIndexPair {
    public String valueType;
    public Object value;
  }

  private static final Map<Condition, String> CONDITION_STRING_MAP =
      Collections.unmodifiableMap(new HashMap<Condition, String>() {
        {
          put(Condition.EQUAL, "=");
          put(Condition.GREATER_THAN, ">");
          put(Condition.GREATER_THAN_OR_EQUAL_TO, ">=");
          put(Condition.IN, "IN");
          put(Condition.LESS_THAN, "<");
          put(Condition.LESS_THAN_OR_EQUAL_TO, "<=");
          put(Condition.START_WITH, "LIKE");
        }
      });

  @VisibleForTesting
  EbeanLocalDAO(@Nonnull Class<ASPECT_UNION> aspectUnionClass, @Nonnull BaseMetadataEventProducer producer,
      @Nonnull EbeanServer server, @Nonnull Class<URN> urnClass) {
    super(aspectUnionClass, producer);
    _server = server;
    _urnClass = urnClass;
    _urnPathExtractor = new EmptyPathExtractor<>();
  }

  @VisibleForTesting
  EbeanLocalDAO(@Nonnull Class<ASPECT_UNION> aspectUnionClass, @Nonnull BaseMetadataEventProducer producer,
      @Nonnull EbeanServer server, @Nonnull ServerConfig serverConfig, @Nonnull Class<URN> urnClass, @Nonnull SchemaConfig schemaConfig) {
    this(aspectUnionClass, producer, server, urnClass);
    _schemaConfig = schemaConfig;
    if (schemaConfig != SchemaConfig.OLD_SCHEMA_ONLY) {
      _localAccess = new EbeanLocalAccess<>(server, serverConfig, urnClass, _urnPathExtractor);
    }
  }

  /**
   * Constructor for EbeanLocalDAO.
   *
   * @param aspectUnionClass containing union of all supported aspects. Must be a valid aspect union defined in com.linkedin.metadata.aspect
   * @param producer {@link BaseMetadataEventProducer} for the metadata event producer
   * @param serverConfig {@link ServerConfig} that defines the configuration of EbeanServer instances
   * @param urnClass Class of the entity URN
   */
  public EbeanLocalDAO(@Nonnull Class<ASPECT_UNION> aspectUnionClass, @Nonnull BaseMetadataEventProducer producer,
      @Nonnull ServerConfig serverConfig, @Nonnull Class<URN> urnClass) {
    this(aspectUnionClass, producer, createServer(serverConfig), urnClass);
  }

  /**
   * Constructor for EbeanLocalDAO with the option to use the new schema and enable dual-read.
   *
   * @param aspectUnionClass containing union of all supported aspects. Must be a valid aspect union defined in com.linkedin.metadata.aspect
   * @param producer {@link BaseMetadataEventProducer} for the metadata event producer
   * @param serverConfig {@link ServerConfig} that defines the configuration of EbeanServer instances
   * @param urnClass Class of the entity URN
   * @param schemaConfig Enum indicating which schema(s)/table(s) to read from and write to
   */
  public EbeanLocalDAO(@Nonnull Class<ASPECT_UNION> aspectUnionClass, @Nonnull BaseMetadataEventProducer producer,
      @Nonnull ServerConfig serverConfig, @Nonnull Class<URN> urnClass, @Nonnull SchemaConfig schemaConfig) {
    this(aspectUnionClass, producer, createServer(serverConfig), serverConfig, urnClass, schemaConfig);
  }

  /**
   * Constructor for EbeanLocalDAO with a parameter to pass in the GMS name, used temporarily to isolate job-gms-specific changes for duplicity.
   *
   * @param aspectUnionClass containing union of all supported aspects. Must be a valid aspect union defined in com.linkedin.metadata.aspect
   * @param producer {@link BaseMetadataEventProducer} for the metadata event producer
   * @param serverConfig {@link ServerConfig} that defines the configuration of EbeanServer instances
   * @param urnClass Class of the entity URN
   * @param gmsName Name of the gms in dash (-) delimiter format (ex. job-gms) -- not case-sensitive
   */
  public EbeanLocalDAO(@Nonnull Class<ASPECT_UNION> aspectUnionClass, @Nonnull BaseMetadataEventProducer producer,
      @Nonnull ServerConfig serverConfig, @Nonnull Class<URN> urnClass, @Nonnull String gmsName) {
    this(aspectUnionClass, producer, createServer(serverConfig), urnClass);
    _directSqlRetrieval = gmsName.toLowerCase().equals("job-gms");
  }

  @VisibleForTesting
  EbeanLocalDAO(@Nonnull BaseMetadataEventProducer producer, @Nonnull EbeanServer server,
      @Nonnull LocalDAOStorageConfig storageConfig, @Nonnull Class<URN> urnClass,
      @Nonnull UrnPathExtractor<URN> urnPathExtractor) {
    super(producer, storageConfig);
    _server = server;
    _urnClass = urnClass;
    _urnPathExtractor = urnPathExtractor;
  }

  @VisibleForTesting
  EbeanLocalDAO(@Nonnull BaseMetadataEventProducer producer, @Nonnull EbeanServer server,
      @Nonnull ServerConfig serverConfig, @Nonnull LocalDAOStorageConfig storageConfig, @Nonnull Class<URN> urnClass,
      @Nonnull UrnPathExtractor<URN> urnPathExtractor, @Nonnull SchemaConfig schemaConfig) {
    this(producer, server, storageConfig, urnClass, urnPathExtractor);
    _schemaConfig = schemaConfig;
    if (schemaConfig != SchemaConfig.OLD_SCHEMA_ONLY) {
      _localAccess = new EbeanLocalAccess<>(server, serverConfig, urnClass, urnPathExtractor);
    }
  }

  @VisibleForTesting
  EbeanLocalDAO(@Nonnull BaseMetadataEventProducer producer, @Nonnull EbeanServer server,
      @Nonnull LocalDAOStorageConfig storageConfig, @Nonnull Class<URN> urnClass) {
    this(producer, server, storageConfig, urnClass, new EmptyPathExtractor<>());
  }

  @VisibleForTesting
  EbeanLocalDAO(@Nonnull BaseMetadataEventProducer producer, @Nonnull EbeanServer server, @Nonnull ServerConfig serverConfig,
      @Nonnull LocalDAOStorageConfig storageConfig, @Nonnull Class<URN> urnClass, @Nonnull SchemaConfig schemaConfig) {
    this(producer, server, serverConfig, storageConfig, urnClass, new EmptyPathExtractor<>(), schemaConfig);
  }

  /**
   * Constructor for EbeanLocalDAO.
   *
   * @param producer {@link BaseMetadataEventProducer} for the metadata event producer
   * @param serverConfig {@link ServerConfig} that defines the configuration of EbeanServer instances
   * @param storageConfig {@link LocalDAOStorageConfig} containing storage config of full list of supported aspects
   * @param urnClass class of the entity URN
   * @param urnPathExtractor path extractor to index parts of URNs to the secondary index
   */
  public EbeanLocalDAO(@Nonnull BaseMetadataEventProducer producer, @Nonnull ServerConfig serverConfig,
      @Nonnull LocalDAOStorageConfig storageConfig, @Nonnull Class<URN> urnClass,
      @Nonnull UrnPathExtractor<URN> urnPathExtractor) {
    this(producer, createServer(serverConfig), storageConfig, urnClass, urnPathExtractor);
  }

  /**
   * Constructor for EbeanLocalDAO with the option to use the new schema and enable dual-read.
   *
   * @param producer {@link BaseMetadataEventProducer} for the metadata event producer
   * @param serverConfig {@link ServerConfig} that defines the configuration of EbeanServer instances
   * @param storageConfig {@link LocalDAOStorageConfig} containing storage config of full list of supported aspects
   * @param urnClass class of the entity URN
   * @param urnPathExtractor path extractor to index parts of URNs to the secondary index
   * @param schemaConfig Enum indicating which schema(s)/table(s) to read from and write to
   */
  public EbeanLocalDAO(@Nonnull BaseMetadataEventProducer producer, @Nonnull ServerConfig serverConfig,
      @Nonnull LocalDAOStorageConfig storageConfig, @Nonnull Class<URN> urnClass,
      @Nonnull UrnPathExtractor<URN> urnPathExtractor, @Nonnull SchemaConfig schemaConfig) {
    this(producer, createServer(serverConfig), serverConfig, storageConfig, urnClass, urnPathExtractor, schemaConfig);
  }

  /**
   * Constructor for EbeanLocalDAO.
   *
   * @param producer {@link BaseMetadataEventProducer} for the metadata event producer
   * @param serverConfig {@link ServerConfig} that defines the configuration of EbeanServer instances
   * @param storageConfig {@link LocalDAOStorageConfig} containing storage config of full list of supported aspects
   * @param urnClass class of the entity URN
   */
  public EbeanLocalDAO(@Nonnull BaseMetadataEventProducer producer, @Nonnull ServerConfig serverConfig,
      @Nonnull LocalDAOStorageConfig storageConfig, @Nonnull Class<URN> urnClass) {
    this(producer, createServer(serverConfig), storageConfig, urnClass, new EmptyPathExtractor<>());
  }

  /**
   * Constructor for EbeanLocalDAO with the option to use the new schema and enable dual-read.
   *
   * @param producer {@link BaseMetadataEventProducer} for the metadata event producer
   * @param serverConfig {@link ServerConfig} that defines the configuration of EbeanServer instances
   * @param storageConfig {@link LocalDAOStorageConfig} containing storage config of full list of supported aspects
   * @param urnClass class of the entity URN
   * @param schemaConfig Enum indicating which schema(s)/table(s) to read from and write to
   */
  public EbeanLocalDAO(@Nonnull BaseMetadataEventProducer producer, @Nonnull ServerConfig serverConfig,
      @Nonnull LocalDAOStorageConfig storageConfig, @Nonnull Class<URN> urnClass, @Nonnull SchemaConfig schemaConfig) {
    this(producer, createServer(serverConfig), serverConfig, storageConfig, urnClass, new EmptyPathExtractor<>(), schemaConfig);
  }

  public void setUrnPathExtractor(@Nonnull UrnPathExtractor<URN> urnPathExtractor) {
    if (_schemaConfig != SchemaConfig.OLD_SCHEMA_ONLY) {
      _localAccess.setUrnPathExtractor(urnPathExtractor);
    }
    _urnPathExtractor = urnPathExtractor;
  }

  /**
   * Return the {@link EbeanServer} server instance used for customized queries.
   */
  public EbeanServer getServer() {
    return _server;
  }

  /**
   * Getter for which SchemaConfig this DAO is using.
   * @return _schemaConfig
   */
  public SchemaConfig getSchemaConfig() {
    return _schemaConfig;
  }

  /**
   * Ensure table schemas is up-to-date with db evolution scripts.
   */
  public void ensureSchemaUpToDate() {
    if (_schemaConfig.equals(SchemaConfig.OLD_SCHEMA_ONLY)) {
      throw new UnsupportedOperationException("DB evolution script is not supported in old schema mode.");
    }

    _localAccess.ensureSchemaUpToDate();
  }

  @Nonnull
  @Override
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

  @Override
  protected <ASPECT extends RecordTemplate> long saveLatest(@Nonnull URN urn, @Nonnull Class<ASPECT> aspectClass,
      @Nullable ASPECT oldValue, @Nullable AuditStamp oldAuditStamp, @Nullable ASPECT newValue,
      @Nonnull AuditStamp newAuditStamp, boolean isSoftDeleted) {
    // Save oldValue as the largest version + 1
    long largestVersion = 0;
    if ((isSoftDeleted || oldValue != null) && oldAuditStamp != null) {
      largestVersion = getNextVersion(urn, aspectClass);

      // TODO(yanyang) added for job-gms duplicity debug, throwaway afterwards
      if (log.isDebugEnabled()) {
        if ("AzkabanFlowInfo".equals(aspectClass.getSimpleName())) {
          log.debug("Insert: {} => oldValue = {}, latest version = {}", urn, oldValue, largestVersion);
        }
      }


      // Move latest version to historical version by insert a new record.
      insert(urn, oldValue, aspectClass, oldAuditStamp, largestVersion);
      // update latest version
      updateWithOptimisticLocking(urn, newValue, aspectClass, newAuditStamp, LATEST_VERSION, new Timestamp(oldAuditStamp.getTime()));
    } else {

      // TODO(yanyang) added for job-gms duplicity debug, throwaway afterwards
      if (log.isDebugEnabled()) {
        if ("AzkabanFlowInfo".equals(aspectClass.getSimpleName())) {
          log.debug("Insert: {} => newValue = {}", urn, newValue);
        }
      }

      insert(urn, newValue, aspectClass, newAuditStamp, LATEST_VERSION);
    }

    return largestVersion;
  }

  @Override
  public <ASPECT extends RecordTemplate> void updateLocalIndex(@Nonnull URN urn, @Nonnull ASPECT newValue,
      long version) {
    if (_schemaConfig == SchemaConfig.NEW_SCHEMA_ONLY) {
      throw new UnsupportedOperationException("Local secondary index isn't supported by new schema");
    }

    if (!isLocalSecondaryIndexEnabled()) {
      throw new UnsupportedOperationException("Local secondary index isn't supported");
    }

    // Process and save URN
    // Only do this with the first version of each aspect
    if (version == FIRST_VERSION) {
      updateUrnInLocalIndex(urn);
    }
    updateAspectInLocalIndex(urn, newValue);
  }

  @Override
  public <ASPECT extends RecordTemplate> void updateEntityTables(@Nonnull URN urn, @Nonnull Class<ASPECT> aspectClass) {
    if (_schemaConfig == SchemaConfig.OLD_SCHEMA_ONLY) {
      throw new UnsupportedOperationException("Entity tables cannot be used in OLD_SCHEMA_ONLY mode, so they cannot be backfilled.");
    }
    PrimaryKey key = new PrimaryKey(urn.toString(), aspectClass.getCanonicalName(), LATEST_VERSION);
    runInTransactionWithRetry(() -> {
      // use forUpdate() to lock the row during this transaction so that we can guarantee a consistent update
      EbeanMetadataAspect result = _server.createQuery(EbeanMetadataAspect.class).setId(key).forUpdate().findOne();
      if (result == null) {
        return null; // unused
      }
      AuditStamp auditStamp = makeAuditStamp(result);
      _localAccess.add(urn, toRecordTemplate(aspectClass, result).orElse(null), aspectClass, auditStamp);
      return null; // unused
    }, 1);
  }

  public <ASPECT extends RecordTemplate> void backfillLocalRelationshipsFromEntityTables(@Nonnull URN urn, @Nonnull Class<ASPECT> aspectClass) {
    if (_schemaConfig == SchemaConfig.OLD_SCHEMA_ONLY) {
      throw new UnsupportedOperationException("Local relationship tables cannot be used in OLD_SCHEMA_ONLY mode, so they cannot be backfilled.");
    }
    AspectKey<URN, ASPECT> key = new AspectKey<>(aspectClass, urn, LATEST_VERSION);
    runInTransactionWithRetry(() -> {
      List<EbeanMetadataAspect> results = _localAccess.batchGetUnion(Collections.singletonList(key), 1, 0);
      if (results.size() == 0) {
        return null; // unused
      }
      Optional<ASPECT> aspect = toRecordTemplate(aspectClass, results.get(0));
      aspect.ifPresent(value -> _localAccess.addRelationships(urn, value, aspectClass));
      return null; // unused
    }, 1);
  }

  private  <ASPECT extends RecordTemplate> EbeanMetadataAspect queryLatest(@Nonnull URN urn, @Nonnull Class<ASPECT> aspectClass) {
    final PrimaryKey key = new PrimaryKey(urn.toString(), ModelUtils.getAspectName(aspectClass), 0L);

    if (!_directSqlRetrieval) {
      return _server.find(EbeanMetadataAspect.class, key);
    } else {
      final String selectQuery = "SELECT * FROM metadata_aspect "
          + "WHERE urn = :urn and aspect = :aspect and version = 0 "
          + "ORDER BY createdOn DESC";

      final SqlQuery query = _server.createSqlQuery(selectQuery);
      query.setParameter("urn", urn.toString());
      query.setParameter("aspect", aspectClass.getCanonicalName());

      List<SqlRow> results = query.findList();

      if (!results.isEmpty()) {
        if (results.size() > 1) {
          log.warn("Two version=0 records found for {}, {}", urn, aspectClass.getSimpleName());
        }
        SqlRow latestResult = results.get(0);

        final EbeanMetadataAspect aspect = new EbeanMetadataAspect();
        aspect.setKey(key);
        aspect.setMetadata(latestResult.getString("metadata"));
        aspect.setCreatedOn(latestResult.getTimestamp("createdOn"));
        aspect.setCreatedBy(latestResult.getString("createdBy"));
        aspect.setCreatedFor(latestResult.getString("createdFor"));

        return aspect;
      }
      return null;
    }
  }

  @Override
  @Nonnull
  protected <ASPECT extends RecordTemplate> AspectEntry<ASPECT> getLatest(@Nonnull URN urn,
      @Nonnull Class<ASPECT> aspectClass) {
    EbeanMetadataAspect latest = queryLatest(urn, aspectClass);
    if (latest == null) {
      return new AspectEntry<>(null, null);
    }
    final ExtraInfo extraInfo = toExtraInfo(latest);

    if (isSoftDeletedAspect(latest, aspectClass)) {
      return new AspectEntry<>(null, extraInfo, true);
    }

    return new AspectEntry<>(RecordUtils.toRecordTemplate(aspectClass, latest.getMetadata()), extraInfo);
  }

  @Nonnull
  private <ASPECT extends RecordTemplate> EbeanMetadataAspect buildMetadataAspectBean(@Nonnull URN urn,
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

  @VisibleForTesting
  @Override
  protected <ASPECT extends RecordTemplate> void updateWithOptimisticLocking(@Nonnull URN urn,
      @Nullable RecordTemplate value, @Nonnull Class<ASPECT> aspectClass, @Nonnull AuditStamp newAuditStamp,
      long version, @Nonnull Timestamp oldTimestamp) {

    final EbeanMetadataAspect aspect = buildMetadataAspectBean(urn, value, aspectClass, newAuditStamp, version);

    // Build manual SQL update query to enable optimistic locking on a given column
    // Optimistic locking is supported on ebean using @version, see https://ebean.io/docs/mapping/jpa/version
    // But we can't use @version annotation for optimistic locking for two reasons:
    //   1. That prevents flag guarding optimistic locking feature
    //   2. When using @version annotation, Ebean starts to override all updates to that column
    //      by disregarding any user change.
    // Ideally, another column for the sake of optimistic locking would be preferred but that means a change to
    // metadata_aspect schema and we don't take this route here to keep this change backward compatible.
    final String updateQuery = "UPDATE metadata_aspect "
        + "SET urn = :urn, aspect = :aspect, version = :version, metadata = :metadata, createdOn = :createdOn, createdBy = :createdBy "
        + "WHERE urn = :urn and aspect = :aspect and version = :version and createdOn = :oldTimestamp";

    final SqlUpdate update = _server.createSqlUpdate(updateQuery);
    update.setParameter("urn", aspect.getKey().getUrn());
    update.setParameter("aspect", aspect.getKey().getAspect());
    update.setParameter("version", aspect.getKey().getVersion());
    update.setParameter("metadata", aspect.getMetadata());
    update.setParameter("createdOn", aspect.getCreatedOn());
    update.setParameter("createdBy", aspect.getCreatedBy());
    update.setParameter("oldTimestamp", oldTimestamp);

    int numOfUpdatedRows;
    if (_schemaConfig == SchemaConfig.NEW_SCHEMA_ONLY || _schemaConfig == SchemaConfig.DUAL_SCHEMA) {
      // ensure atomicity by running old schema update + new schema update in a transaction
      numOfUpdatedRows = runInTransactionWithRetry(() -> {
        _localAccess.add(urn, (ASPECT) value, aspectClass, newAuditStamp);
        return _server.execute(update);
      }, 1);
    } else {
      numOfUpdatedRows = _server.execute(update);
    }

    // If there is no single updated row, emit OptimisticLockException
    if (numOfUpdatedRows != 1) {
      throw new OptimisticLockException(
          numOfUpdatedRows + " rows updated during save query: " + update.getGeneratedSql());
    }
  }

  @Override
  protected <ASPECT extends RecordTemplate> void insert(@Nonnull URN urn, @Nullable RecordTemplate value,
      @Nonnull Class<ASPECT> aspectClass, @Nonnull AuditStamp auditStamp, long version) {

    final EbeanMetadataAspect aspect = buildMetadataAspectBean(urn, value, aspectClass, auditStamp, version);

    if (_schemaConfig != SchemaConfig.OLD_SCHEMA_ONLY && version == LATEST_VERSION) {
      // insert() could be called when updating log table (moving current versions into new history version)
      // the metadata entity tables shouldn't been updated.
      _localAccess.add(urn, (ASPECT) value, aspectClass, auditStamp);
    }

    _server.insert(aspect);
  }

  protected void saveRecordsToLocalIndex(@Nonnull URN urn, @Nonnull String aspect, @Nonnull String path,
      @Nonnull Object value) {
    if (value instanceof List) {
      for (Object obj : (List<?>) value) {
        saveSingleRecordToLocalIndex(urn, aspect, path, obj);
      }
    } else {
      saveSingleRecordToLocalIndex(urn, aspect, path, value);
    }
  }

  protected long saveSingleRecordToLocalIndex(@Nonnull URN urn, @Nonnull String aspect, @Nonnull String path,
      @Nonnull Object value) {

    final EbeanMetadataIndex record = new EbeanMetadataIndex().setUrn(urn.toString()).setAspect(aspect).setPath(path);
    if (value instanceof Integer || value instanceof Long) {
      record.setLongVal(Long.valueOf(value.toString()));
    } else if (value instanceof Float || value instanceof Double) {
      record.setDoubleVal(Double.valueOf(value.toString()));
    } else {
      record.setStringVal(value.toString());
    }

    _server.insert(record);
    return record.getId();
  }

  @Nonnull
  Map<Class<? extends RecordTemplate>, LocalDAOStorageConfig.AspectStorageConfig> getStrongConsistentIndexPaths() {
    return Collections.unmodifiableMap(new HashMap<>(_storageConfig.getAspectStorageConfigMap()));
  }

  private void updateUrnInLocalIndex(@Nonnull URN urn) {
    if (existsInLocalIndex(urn)) {
      return;
    }

    final Map<String, Object> pathValueMap = _urnPathExtractor.extractPaths(urn);
    pathValueMap.forEach((path, value) -> saveSingleRecordToLocalIndex(urn, _urnClass.getCanonicalName(), path, value));
  }

  private <ASPECT extends RecordTemplate> void updateAspectInLocalIndex(@Nonnull URN urn, @Nonnull ASPECT newValue) {

    if (!_storageConfig.getAspectStorageConfigMap().containsKey(newValue.getClass())
        || _storageConfig.getAspectStorageConfigMap().get(newValue.getClass()) == null) {
      return;
    }
    // step1: remove all rows from the index table corresponding to <urn, aspect> pair
    _server.find(EbeanMetadataIndex.class)
        .where()
        .eq(URN_COLUMN, urn.toString())
        .eq(ASPECT_COLUMN, ModelUtils.getAspectName(newValue.getClass()))
        .delete();

    // step2: add fields of the aspect that need to be indexed
    final Map<String, LocalDAOStorageConfig.PathStorageConfig> pathStorageConfigMap =
        _storageConfig.getAspectStorageConfigMap().get(newValue.getClass()).getPathStorageConfigMap();

    pathStorageConfigMap.keySet()
        .stream()
        .filter(path -> pathStorageConfigMap.get(path).isStrongConsistentSecondaryIndex())
        .collect(Collectors.toMap(Function.identity(), path -> RecordUtils.getFieldValue(newValue, path)))
        .forEach((k, v) -> v.ifPresent(
            value -> saveRecordsToLocalIndex(urn, newValue.getClass().getCanonicalName(), k, value)));
  }

  @Override
  protected <ASPECT extends RecordTemplate> long getNextVersion(@Nonnull URN urn, @Nonnull Class<ASPECT> aspectClass) {

    final List<PrimaryKey> result = _server.find(EbeanMetadataAspect.class)
        .where()
        .eq(URN_COLUMN, urn.toString())
        .eq(ASPECT_COLUMN, ModelUtils.getAspectName(aspectClass))
        .orderBy()
        .desc(VERSION_COLUMN)
        .setMaxRows(1)
        .findIds();

    return result.isEmpty() ? 0 : result.get(0).getVersion() + 1L;
  }

  @Override
  protected <ASPECT extends RecordTemplate> void applyVersionBasedRetention(@Nonnull Class<ASPECT> aspectClass,
      @Nonnull URN urn, @Nonnull VersionBasedRetention retention, long largestVersion) {

    _server.find(EbeanMetadataAspect.class)
        .where()
        .eq(URN_COLUMN, urn.toString())
        .eq(ASPECT_COLUMN, ModelUtils.getAspectName(aspectClass))
        .ne(VERSION_COLUMN, LATEST_VERSION)
        .le(VERSION_COLUMN, largestVersion - retention.getMaxVersionsToRetain() + 1)
        .delete();
  }

  @Override
  protected <ASPECT extends RecordTemplate> void applyTimeBasedRetention(@Nonnull Class<ASPECT> aspectClass,
      @Nonnull URN urn, @Nonnull TimeBasedRetention retention, long currentTime) {

    _server.find(EbeanMetadataAspect.class)
        .where()
        .eq(URN_COLUMN, urn.toString())
        .eq(ASPECT_COLUMN, ModelUtils.getAspectName(aspectClass))
        .lt(CREATED_ON_COLUMN, new Timestamp(currentTime - retention.getMaxAgeToRetain()))
        .delete();
  }

  @Override
  @Nonnull
  public Map<AspectKey<URN, ? extends RecordTemplate>, Optional<? extends RecordTemplate>> get(
      @Nonnull Set<AspectKey<URN, ? extends RecordTemplate>> keys) {
    if (keys.isEmpty()) {
      return Collections.emptyMap();
    }

    final List<EbeanMetadataAspect> records;

    if (_queryKeysCount == 0) {
      records = batchGet(keys, keys.size());
    } else {
      records = batchGet(keys, _queryKeysCount);
    }

    // TODO: Improve this O(n^2) search

    return keys.stream()
        .collect(Collectors.toMap(Function.identity(), key -> records.stream()
            .filter(record -> matchKeys(key, record.getKey()))
            .findFirst()
            .flatMap(record -> toRecordTemplate(key.getAspectClass(), record))));
  }

  @Override
  @Nonnull
  public Map<AspectKey<URN, ? extends RecordTemplate>, AspectWithExtraInfo<? extends RecordTemplate>> getWithExtraInfo(
      @Nonnull Set<AspectKey<URN, ? extends RecordTemplate>> keys) {
    if (keys.isEmpty()) {
      return Collections.emptyMap();
    }

    final List<EbeanMetadataAspect> records = batchGet(keys, keys.size());

    final Map<AspectKey<URN, ? extends RecordTemplate>, AspectWithExtraInfo<? extends RecordTemplate>> result =
        new HashMap<>();
    keys.forEach(key -> records.stream()
        .filter(record -> matchKeys(key, record.getKey()))
        .findFirst()
        .map(record -> {
          final Class<RecordTemplate> aspectClass = (Class<RecordTemplate>) key.getAspectClass();
          final Optional<AspectWithExtraInfo<RecordTemplate>> aspectWithExtraInfo = toRecordTemplateWithExtraInfo(aspectClass, record);
          aspectWithExtraInfo.ifPresent(
              recordTemplateAspectWithExtraInfo -> result.put(key, recordTemplateAspectWithExtraInfo));
          return null;
        }));
    return result;
  }

  @Override
  @SuppressWarnings({"checkstyle:FallThrough", "checkstyle:DefaultComesLast"})
  public boolean exists(@Nonnull URN urn) {
    switch (_schemaConfig) {
      case NEW_SCHEMA_ONLY:
        return _localAccess.exists(urn);
      case DUAL_SCHEMA:
        final boolean existsInNewSchema = _localAccess.exists(urn);
        final boolean existsInOldSchema = _server.find(EbeanMetadataAspect.class).where().eq(URN_COLUMN, urn.toString()).exists();
        if (existsInNewSchema != existsInOldSchema) {
          log.warn(String.format("The following urn does%s exist in the old schema but does%s exist in the new schema: %s",
              existsInOldSchema ? "" : " not", existsInNewSchema ? "" : " not", urn.toString()));
        }
        return existsInOldSchema;
      default:
        log.error("Please check that the SchemaConfig supplied to EbeanLocalDAO constructor is valid."
            + "Defaulting to using the old schema.");
        // FALLTHROUGH
      case OLD_SCHEMA_ONLY:
        return _server.find(EbeanMetadataAspect.class).where().eq(URN_COLUMN, urn.toString()).exists();
    }
  }

  public boolean existsInLocalIndex(@Nonnull URN urn) {
    if (_schemaConfig == SchemaConfig.NEW_SCHEMA_ONLY) {
      throw new UnsupportedOperationException("Local secondary index isn't supported when using only the new schema");
    }
    return _server.find(EbeanMetadataIndex.class).where().eq(URN_COLUMN, urn.toString()).exists();
  }

  /**
   * Sets the max keys allowed for each single query.
   */
  public void setQueryKeysCount(int keysCount) {
    if (keysCount < 0) {
      throw new IllegalArgumentException("Query keys count must be non-negative: " + keysCount);
    }
    _queryKeysCount = keysCount;
  }

  /**
   * Set a local relationship builder registry.
   */
  public void setLocalRelationshipBuilderRegistry(@Nonnull LocalRelationshipBuilderRegistry localRelationshipBuilderRegistry) {
    _localAccess.setLocalRelationshipBuilderRegistry(localRelationshipBuilderRegistry);
  }

  /**
   * BatchGet that allows pagination on keys to avoid large queries.
   * TODO: can further improve by running the sub queries in parallel
   *
   * @param keys a set of keys with urn, aspect and version
   * @param keysCount the max number of keys for each sub query
   */
  @Nonnull
  private List<EbeanMetadataAspect> batchGet(@Nonnull Set<AspectKey<URN, ? extends RecordTemplate>> keys,
      int keysCount) {

    int position = 0;
    final int totalPageCount = QueryUtils.getTotalPageCount(keys.size(), keysCount);

    List<EbeanMetadataAspect> finalResult = batchGetHelper(new ArrayList<>(keys), keysCount, position);
    while (QueryUtils.hasMore(position, keysCount, totalPageCount)) {
      position += keysCount;
      final List<EbeanMetadataAspect> oneStatementResult = batchGetHelper(new ArrayList<>(keys), keysCount, position);
      finalResult.addAll(oneStatementResult);
    }
    return finalResult;
  }

  /**
   * Builds a single SELECT statement for batch get, which selects one entity, and then can be UNION'd with other SELECT
   * statements.
   */
  private String batchGetSelect(@Nonnull String urn, @Nonnull String aspect, long version,
      @Nonnull List<Object> outputParams) {
    outputParams.add(urn);
    outputParams.add(aspect);
    outputParams.add(version);

    return String.format("SELECT t.urn, t.aspect, t.version, t.metadata, t.createdOn, t.createdBy, t.createdFor "
            + "FROM %s t WHERE urn = ? AND aspect = ? AND version = ?",
        EbeanMetadataAspect.class.getAnnotation(Table.class).name());
  }

  @Nonnull
  private List<EbeanMetadataAspect> batchGetUnion(@Nonnull List<AspectKey<URN, ? extends RecordTemplate>> keys,
      int keysCount, int position) {

    // Build one SELECT per key and then UNION ALL the results. This can be much more performant than OR'ing the
    // conditions together. Our query will look like:
    //   SELECT * FROM metadata_aspect WHERE urn = 'urn0' AND aspect = 'aspect0' AND version = 0
    //   UNION ALL
    //   SELECT * FROM metadata_aspect WHERE urn = 'urn0' AND aspect = 'aspect1' AND version = 0
    //   ...
    // Note: UNION ALL should be safe and more performant than UNION. We're selecting the entire entity key (as well
    // as data), so each result should be unique. No need to deduplicate.
    // Another note: ebean doesn't support UNION ALL, so we need to manually build the SQL statement ourselves.
    final StringBuilder sb = new StringBuilder();
    final int end = Math.min(keys.size(), position + keysCount);
    final List<Object> params = new ArrayList<>();
    for (int index = position; index < end; index++) {
      sb.append(batchGetSelect(keys.get(index).getUrn().toString(),
          ModelUtils.getAspectName(keys.get(index).getAspectClass()), keys.get(index).getVersion(), params));

      if (index != end - 1) {
        sb.append(" UNION ALL ");
      }
    }

    final Query<EbeanMetadataAspect> query = _server.findNative(EbeanMetadataAspect.class, sb.toString());

    for (int i = 1; i <= params.size(); i++) {
      query.setParameter(i, params.get(i - 1));
    }

    return query.findList();
  }

  @Nonnull
  private List<EbeanMetadataAspect> batchGetOr(@Nonnull List<AspectKey<URN, ? extends RecordTemplate>> keys,
      int keysCount, int position) {
    ExpressionList<EbeanMetadataAspect> query = _server.find(EbeanMetadataAspect.class).select(ALL_COLUMNS).where();

    // add or if it is not the last element
    if (position != keys.size() - 1) {
      query = query.or();
    }

    for (int index = position; index < keys.size() && index < position + keysCount; index++) {
      query = query.and()
          .eq(URN_COLUMN, keys.get(index).getUrn().toString())
          .eq(ASPECT_COLUMN, ModelUtils.getAspectName(keys.get(index).getAspectClass()))
          .eq(VERSION_COLUMN, keys.get(index).getVersion())
          .endAnd();
    }

    return query.findList();
  }

  @Nonnull
  @SuppressWarnings({"checkstyle:FallThrough", "checkstyle:DefaultComesLast"})
  List<EbeanMetadataAspect> batchGetHelper(@Nonnull List<AspectKey<URN, ? extends RecordTemplate>> keys,
      int keysCount, int position) {

    boolean nonLatestVersionFlag = keys.stream().anyMatch(key -> key.getVersion() != LATEST_VERSION);

    if (nonLatestVersionFlag || _schemaConfig == SchemaConfig.OLD_SCHEMA_ONLY) {
      return batchGetUnion(keys, keysCount, position);
    }

    if (_schemaConfig == SchemaConfig.NEW_SCHEMA_ONLY) {
      return _localAccess.batchGetUnion(keys, keysCount, position);
    }

    if (_schemaConfig == SchemaConfig.DUAL_SCHEMA) {
      // Compare results from both new and old schemas
      final List<EbeanMetadataAspect> resultsOldSchema = batchGetUnion(keys, keysCount, position);
      final List<EbeanMetadataAspect> resultsNewSchema = _localAccess.batchGetUnion(keys, keysCount, position);
      EBeanDAOUtils.compareResults(resultsOldSchema, resultsNewSchema, "batchGet");
      return resultsOldSchema;
    }

    log.error("Please check that the SchemaConfig supplied to EbeanLocalDAO constructor is valid.");
    return Collections.emptyList();
  }

  /**
   * Checks if an {@link AspectKey} and a {@link PrimaryKey} for Ebean are equivalent.
   *
   * @param aspectKey Urn needs to do a ignore case match
   */
  boolean matchKeys(@Nonnull AspectKey<URN, ? extends RecordTemplate> aspectKey, @Nonnull PrimaryKey pk) {
    return aspectKey.getUrn().toString().equalsIgnoreCase(pk.getUrn()) && aspectKey.getVersion() == pk.getVersion()
        && ModelUtils.getAspectName(aspectKey.getAspectClass()).equals(pk.getAspect());
  }

  @Override
  @Nonnull
  public <ASPECT extends RecordTemplate> ListResult<Long> listVersions(@Nonnull Class<ASPECT> aspectClass,
      @Nonnull URN urn, int start, int pageSize) {

    checkValidAspect(aspectClass);

    final PagedList<EbeanMetadataAspect> pagedList = _server.find(EbeanMetadataAspect.class)
        .select(KEY_ID)
        .where()
        .eq(URN_COLUMN, urn.toString())
        .eq(ASPECT_COLUMN, ModelUtils.getAspectName(aspectClass))
        .ne(METADATA_COLUMN, DELETED_VALUE)
        .setFirstRow(start)
        .setMaxRows(pageSize)
        .orderBy()
        .asc(VERSION_COLUMN)
        .findPagedList();

    final List<Long> versions =
        pagedList.getList().stream().map(a -> a.getKey().getVersion()).collect(Collectors.toList());
    return toListResult(versions, null, pagedList, start);
  }

  @Override
  @Nonnull
  public <ASPECT extends RecordTemplate> ListResult<URN> listUrns(@Nonnull Class<ASPECT> aspectClass, int start,
      int pageSize) {
    if (_schemaConfig == SchemaConfig.NEW_SCHEMA_ONLY) {
      return _localAccess.listUrns(aspectClass, start, pageSize);
    }
    checkValidAspect(aspectClass);

    final PagedList<EbeanMetadataAspect> pagedList = _server.find(EbeanMetadataAspect.class)
        .select(KEY_ID)
        .where()
        .eq(ASPECT_COLUMN, ModelUtils.getAspectName(aspectClass))
        .eq(VERSION_COLUMN, LATEST_VERSION)
        .ne(METADATA_COLUMN, DELETED_VALUE)
        .setFirstRow(start)
        .setMaxRows(pageSize)
        .orderBy()
        .asc(URN_COLUMN)
        .findPagedList();

    final List<URN> urns =
        pagedList.getList().stream().map(entry -> getUrn(entry.getKey().getUrn())).collect(Collectors.toList());
    final ListResult<URN> urnsOld = toListResult(urns, null, pagedList, start);
    if (_schemaConfig == SchemaConfig.DUAL_SCHEMA) {
      final ListResult<URN> urnsNew = _localAccess.listUrns(aspectClass, start, pageSize);
      EBeanDAOUtils.compareResults(urnsOld, urnsNew, "listUrns");
    }
    return urnsOld;
  }

  @Nonnull
  <ASPECT extends RecordTemplate> ListResult<ASPECT> getListResult(@Nonnull Class<ASPECT> aspectClass,
      @Nonnull PagedList<EbeanMetadataAspect> pagedList, int start) {
    final List<ASPECT> aspects = new ArrayList<>();
    final List<ExtraInfo> extraInfos = new ArrayList<>();
    pagedList.getList().forEach(a -> {
      final Optional<ASPECT> record = toRecordTemplate(aspectClass, a);
      record.ifPresent(r -> {
        aspects.add(r);
        extraInfos.add(EbeanLocalDAO.toExtraInfo(a));
      });
    });

    final ListResultMetadata listResultMetadata = makeListResultMetadata(extraInfos);
    return toListResult(aspects, listResultMetadata, pagedList, start);
  }

  @Override
  @Nonnull
  public <ASPECT extends RecordTemplate> ListResult<ASPECT> list(@Nonnull Class<ASPECT> aspectClass, @Nonnull URN urn,
      int start, int pageSize) {

    checkValidAspect(aspectClass);

    final PagedList<EbeanMetadataAspect> pagedList = _server.find(EbeanMetadataAspect.class)
        .select(ALL_COLUMNS)
        .where()
        .eq(URN_COLUMN, urn.toString())
        .eq(ASPECT_COLUMN, ModelUtils.getAspectName(aspectClass))
        .ne(METADATA_COLUMN, DELETED_VALUE)
        .setFirstRow(start)
        .setMaxRows(pageSize)
        .orderBy()
        .asc(VERSION_COLUMN)
        .findPagedList();

    return getListResult(aspectClass, pagedList, start);
  }

  @Override
  @Nonnull
  public <ASPECT extends RecordTemplate> ListResult<ASPECT> list(@Nonnull Class<ASPECT> aspectClass, long version,
      int start, int pageSize) {

    checkValidAspect(aspectClass);

    final PagedList<EbeanMetadataAspect> pagedList = _server.find(EbeanMetadataAspect.class)
        .select(ALL_COLUMNS)
        .where()
        .eq(ASPECT_COLUMN, ModelUtils.getAspectName(aspectClass))
        .eq(VERSION_COLUMN, version)
        .ne(METADATA_COLUMN, DELETED_VALUE)
        .setFirstRow(start)
        .setMaxRows(pageSize)
        .orderBy()
        .asc(URN_COLUMN)
        .findPagedList();

    return getListResult(aspectClass, pagedList, start);
  }

  @Override
  @Nonnull
  public <ASPECT extends RecordTemplate> ListResult<ASPECT> list(@Nonnull Class<ASPECT> aspectClass, int start,
      int pageSize) {
    return list(aspectClass, LATEST_VERSION, start, pageSize);
  }

  @Nonnull
  URN getUrn(@Nonnull String urn) {
    try {
      final Method getUrn = _urnClass.getMethod("createFromString", String.class);
      return _urnClass.cast(getUrn.invoke(null, urn));
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      throw new IllegalArgumentException("URN conversion error for " + urn, e);
    }
  }

  @Nonnull
  static <ASPECT extends RecordTemplate> Optional<ASPECT> toRecordTemplate(@Nonnull Class<ASPECT> aspectClass,
      @Nonnull EbeanMetadataAspect aspect) {
    if (isSoftDeletedAspect(aspect, aspectClass)) {
      return Optional.empty();
    }
    return Optional.of(RecordUtils.toRecordTemplate(aspectClass, aspect.getMetadata()));
  }

  @Nonnull
  static <ASPECT extends RecordTemplate> Optional<AspectWithExtraInfo<ASPECT>> toRecordTemplateWithExtraInfo(
      @Nonnull Class<ASPECT> aspectClass, @Nonnull EbeanMetadataAspect aspect) {
    if (aspect.getMetadata() == null || isSoftDeletedAspect(aspect, aspectClass)) {
      return Optional.empty();
    }
    final ExtraInfo extraInfo = toExtraInfo(aspect);
    return Optional.of(new AspectWithExtraInfo<>(RecordUtils.toRecordTemplate(aspectClass, aspect.getMetadata()),
        extraInfo));
  }

  @Nonnull
  private <T> ListResult<T> toListResult(@Nonnull List<T> values, @Nullable ListResultMetadata listResultMetadata,
      @Nonnull PagedList<?> pagedList, @Nullable Integer start) {
    final int nextStart =
        (start != null && pagedList.hasNext()) ? start + pagedList.getList().size() : ListResult.INVALID_NEXT_START;
    return ListResult.<T>builder()
        // Format
        .values(values)
        .metadata(listResultMetadata)
        .nextStart(nextStart)
        .havingMore(pagedList.hasNext())
        .totalCount(pagedList.getTotalCount())
        .totalPageCount(pagedList.getTotalPageCount())
        .pageSize(pagedList.getPageSize())
        .build();
  }

  @Nonnull
  static ExtraInfo toExtraInfo(@Nonnull EbeanMetadataAspect aspect) {
    final ExtraInfo extraInfo = new ExtraInfo();
    extraInfo.setVersion(aspect.getKey().getVersion());
    extraInfo.setAudit(makeAuditStamp(aspect));
    try {
      extraInfo.setUrn(Urn.createFromString(aspect.getKey().getUrn()));
    } catch (URISyntaxException e) {
      throw new ModelConversionException(e.getMessage());
    }

    return extraInfo;
  }

  @Nonnull
  static AuditStamp makeAuditStamp(@Nonnull EbeanMetadataAspect aspect) {
    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(aspect.getCreatedOn().getTime());

    try {
      auditStamp.setActor(new Urn(aspect.getCreatedBy()));
      if (aspect.getCreatedFor() != null) {
        auditStamp.setImpersonator(new Urn(aspect.getCreatedFor()));
      }
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    return auditStamp;
  }

  @Nonnull
  private ListResultMetadata makeListResultMetadata(@Nonnull List<ExtraInfo> extraInfos) {
    final ListResultMetadata listResultMetadata = new ListResultMetadata();
    listResultMetadata.setExtraInfos(new ExtraInfoArray(extraInfos));
    return listResultMetadata;
  }

  @Override
  public long newNumericId(@Nonnull String namespace, int maxTransactionRetry) {
    return runInTransactionWithRetry(() -> {
      final Optional<EbeanMetadataId> result = _server.find(EbeanMetadataId.class)
          .where()
          .eq(EbeanMetadataId.NAMESPACE_COLUMN, namespace)
          .orderBy()
          .desc(EbeanMetadataId.ID_COLUMN)
          .setMaxRows(1)
          .findOneOrEmpty();

      EbeanMetadataId id = result.orElse(new EbeanMetadataId(namespace, 0));
      id.setId(id.getId() + 1);
      _server.insert(id);
      return id;
    }, maxTransactionRetry).getId();
  }

  @Nonnull
  static GMAIndexPair getGMAIndexPair(@Nonnull IndexCriterion criterion) {
    final IndexValue indexValue = criterion.getPathParams().getValue();
    final Object object;
    if (indexValue.isBoolean()) {
      object = indexValue.getBoolean().toString();
      return new GMAIndexPair(EbeanMetadataIndex.STRING_COLUMN, object);
    } else if (indexValue.isDouble()) {
      object = indexValue.getDouble();
      return new GMAIndexPair(EbeanMetadataIndex.DOUBLE_COLUMN, object);
    } else if (indexValue.isFloat()) {
      object = (indexValue.getFloat()).doubleValue();
      return new GMAIndexPair(EbeanMetadataIndex.DOUBLE_COLUMN, object);
    } else if (indexValue.isInt()) {
      object = Long.valueOf(indexValue.getInt());
      return new GMAIndexPair(EbeanMetadataIndex.LONG_COLUMN, object);
    } else if (indexValue.isLong()) {
      object = indexValue.getLong();
      return new GMAIndexPair(EbeanMetadataIndex.LONG_COLUMN, object);
    } else if (indexValue.isString()) {
      object = getValueFromIndexCriterion(criterion);
      return new GMAIndexPair(EbeanMetadataIndex.STRING_COLUMN, object);
    } else if (indexValue.isArray() && indexValue.getArray().size() > 0
        && indexValue.getArray().get(0).getClass() == String.class) {
      object = indexValue.getArray();
      return new GMAIndexPair(EbeanMetadataIndex.STRING_COLUMN, object);
    } else {
      throw new IllegalArgumentException("Invalid index value " + indexValue);
    }
  }

  static String getValueFromIndexCriterion(@Nonnull IndexCriterion criterion) {
    final IndexValue indexValue = criterion.getPathParams().getValue();
    if (criterion.getPathParams().getCondition().equals(Condition.START_WITH)) {
      return indexValue.getString() + "%";
    }
    return indexValue.getString();
  }

  /**
   * Sets the values of parameters in metadata index query based on its position, values obtained from
   * {@link IndexCriterionArray} and last urn. Also sets the LIMIT of SQL query using the page size input.
   * For offset pagination, the limit will be set when the query gets executed.
   *
   * @param indexCriterionArray {@link IndexCriterionArray} whose values will be used to set parameters in metadata
   *                                                       index query based on its position
   * @param indexSortCriterion {@link IndexSortCriterion} whose values will be used to set parameters in query
   * @param indexQuery {@link Query} whose ordered parameters need to be set, based on it's position
   * @param lastUrn string representation of the urn whose value is used to set the last urn parameter in index query
   * @param pageSize maximum number of distinct urns to return which is essentially the LIMIT clause of SQL query
   * @param offsetPagination used to determine whether to used cursor or offset pagination
   */
  private static void setParameters(@Nonnull IndexCriterionArray indexCriterionArray,
      @Nullable IndexSortCriterion indexSortCriterion, @Nonnull Query<EbeanMetadataIndex> indexQuery,
      @Nonnull String lastUrn, int pageSize, boolean offsetPagination) {
    int pos = 1;
    if (!offsetPagination) {
      indexQuery.setParameter(pos++, lastUrn);
    }
    for (IndexCriterion criterion : indexCriterionArray) {
      indexQuery.setParameter(pos++, criterion.getAspect());
      if (criterion.getPathParams() != null) {
        indexQuery.setParameter(pos++, criterion.getPathParams().getPath());
        indexQuery.setParameter(pos++, getGMAIndexPair(criterion).value);
      }
    }
    if (indexSortCriterion != null) {
      indexQuery.setParameter(pos++, indexSortCriterion.getAspect());
      indexQuery.setParameter(pos++, indexSortCriterion.getPath());
    }
    if (!offsetPagination) {
      indexQuery.setParameter(pos, pageSize);
    }
  }

  @Nonnull
  private static String getStringForOperator(@Nonnull Condition condition) {
    if (!CONDITION_STRING_MAP.containsKey(condition)) {
      throw new UnsupportedOperationException(
          condition.toString() + " condition is not supported in local secondary index");
    }
    return CONDITION_STRING_MAP.get(condition);
  }

  @Nonnull
  static <ASPECT extends RecordTemplate> String getFieldColumn(@Nonnull String path, @Nonnull String aspectName) {
    final String[] pathSpecArray = RecordUtils.getPathSpecAsArray(path);

    // get nested field
    ASPECT aspect = RecordUtils.getAspectFromString(aspectName);

    final int pathSize = pathSpecArray.length;

    for (int i = 0; i < pathSize - 1; i++) {
      final String part = pathSpecArray[i];

      final RecordDataSchema.Field field = aspect.schema().getField(part);
      final DataSchema dataSchema = field.getType();

      if (dataSchema.getDereferencedType() == DataSchema.Type.RECORD) {
        final String nestedAspectName = ((RecordDataSchema) dataSchema).getBindingName();
        aspect = RecordUtils.getAspectFromString(nestedAspectName);
      } else if (dataSchema.getDereferencedType() == DataSchema.Type.ARRAY) {
        throw new UnsupportedOperationException(
            "Can not sort or group by an array for path: " + path + ", aspect: " + aspectName);
      } else {
        throw new IllegalArgumentException(
            "Invalid path field for aspect in sort or group by path: " + path + ", aspect: " + aspectName);
      }
    }

    final RecordDataSchema.Field field = aspect.schema().getField(pathSpecArray[pathSize - 1]);
    final DataSchema dataSchema = field.getType();
    final DataSchema.Type type = dataSchema.getDereferencedType();

    if (type == DataSchema.Type.INT || type == DataSchema.Type.LONG) {
      return EbeanMetadataIndex.LONG_COLUMN;
    } else if (type == DataSchema.Type.DOUBLE || type == DataSchema.Type.FLOAT) {
      return EbeanMetadataIndex.DOUBLE_COLUMN;
    } else if (type == DataSchema.Type.STRING || type == DataSchema.Type.BOOLEAN || type == DataSchema.Type.ENUM) {
      return EbeanMetadataIndex.STRING_COLUMN;
    } else {
      throw new UnsupportedOperationException(
          "The type stored in the path field of the aspect can not be sorted or grouped on for path: " + path
              + ", aspect: " + aspectName);
    }
  }

  private static String getPlaceholderStringForValue(@Nonnull IndexValue indexValue) {
    if (indexValue.isArray() && indexValue.getArray().size() > 0) {
      List<Object> values = Arrays.asList(indexValue.getArray().toArray());
      String placeholderString = "(";
      placeholderString += String.join(",", values.stream().map(value -> "?").collect(Collectors.toList()));
      placeholderString += ")";
      return placeholderString;
    }
    return "?";
  }

  /**
   * Constructs SQL query that contains positioned parameters (with `?`), based on whether {@link IndexCriterion} of
   * a given condition has field `pathParams`.
   * For offset pagination, the limit clause is empty because the limit will be set when the query
   * gets executed.
   *
   * @param indexCriterionArray {@link IndexCriterionArray} used to construct the SQL query
   * @param indexSortCriterion {@link IndexSortCriterion} used to construct the SQL query
   * @param offsetPagination used to determine whether to used cursor or offset pagination
   * @return String representation of SQL query
   */
  @Nonnull
  private static String constructSQLQuery(@Nonnull IndexCriterionArray indexCriterionArray,
      @Nullable IndexSortCriterion indexSortCriterion, boolean offsetPagination) {
    String sortColumn =
        indexSortCriterion != null ? getFieldColumn(indexSortCriterion.getPath(), indexSortCriterion.getAspect()) : "";
    String selectClause = "SELECT DISTINCT(t0.urn)";
    if (!sortColumn.isEmpty()) {
      selectClause += ", tsort.";
      selectClause += sortColumn;
    }
    selectClause += " FROM metadata_index t0";
    selectClause += IntStream.range(1, indexCriterionArray.size())
        .mapToObj(i -> " INNER JOIN metadata_index " + "t" + i + " ON t0.urn = " + "t" + i + ".urn")
        .collect(Collectors.joining(""));
    final StringBuilder whereClause = new StringBuilder("WHERE ");
    if (!offsetPagination) {
      whereClause.append("t0.urn > ?");
    }
    IntStream.range(0, indexCriterionArray.size()).forEach(i -> {
      final IndexCriterion criterion = indexCriterionArray.get(i);
      if (!offsetPagination || i > 0) {
        whereClause.append(" AND");
      }
      whereClause.append(" t").append(i).append(".aspect = ?");
      if (criterion.getPathParams() != null) {
        SQLIndexFilterUtils.validateConditionAndValue(criterion);
        whereClause.append(" AND t")
            .append(i)
            .append(".path = ? AND t")
            .append(i)
            .append(".")
            .append(getGMAIndexPair(criterion).valueType)
            .append(" ")
            .append(getStringForOperator(criterion.getPathParams().getCondition()))
            .append(getPlaceholderStringForValue(criterion.getPathParams().getValue()));
      }
    });
    final String orderByClause;
    if (indexSortCriterion != null && !sortColumn.isEmpty()) {
      String sortOrder = indexSortCriterion.getOrder() == SortOrder.ASCENDING ? "ASC" : "DESC";

      selectClause += " INNER JOIN metadata_index tsort ON t0.urn = tsort.urn";
      whereClause.append(" AND tsort.aspect = ? AND tsort.path = ? ");
      orderByClause = "ORDER BY tsort." + sortColumn + " " + sortOrder;
    } else {
      orderByClause = "ORDER BY urn ASC";
    }
    final String limitClause = offsetPagination ? "" : "LIMIT ?";
    return String.join(" ", selectClause, whereClause, orderByClause, limitClause);
  }

  void checkValidIndexCriterionArray(@Nonnull IndexCriterionArray indexCriterionArray) {
    if (indexCriterionArray.isEmpty()) {
      throw new UnsupportedOperationException("Empty Index Filter is not supported by EbeanLocalDAO");
    }
    if (indexCriterionArray.size() > 10) {
      throw new UnsupportedOperationException(
          "Currently more than 10 filter conditions is not supported by EbeanLocalDAO");
    }
  }

  void addEntityTypeFilter(@Nonnull IndexFilter indexFilter) {
    if (indexFilter.getCriteria().stream().noneMatch(x -> x.getAspect().equals(_urnClass.getCanonicalName()))) {
      indexFilter.getCriteria().add(new IndexCriterion().setAspect(_urnClass.getCanonicalName()));
    }
  }

  /**
   * Returns list of urns from strongly consistent secondary index that satisfy the given filter conditions.
   *
   * <p>Results are ordered by the sort criterion but defaults to sorting lexicographically by the string representation of the URN.
   *
   * <p>NOTE: Currently this works for upto 10 filter conditions.
   *
   * @param indexFilter {@link IndexFilter} containing filter conditions to be applied
   * @param indexSortCriterion {@link IndexSortCriterion} sorting criteria to be applied
   * @param lastUrn last urn of the previous fetched page. This eliminates the need to use offset which
   *                 is known to slow down performance of MySQL queries. For the first page, this should be set as NULL
   * @param pageSize maximum number of distinct urns to return
   * @return list of urns from strongly consistent secondary index that satisfy the given filter conditions
   */
  @Override
  @Nonnull
  public List<URN> listUrns(@Nonnull IndexFilter indexFilter, @Nullable IndexSortCriterion indexSortCriterion,
      @Nullable URN lastUrn, int pageSize) {
    if (_schemaConfig == SchemaConfig.NEW_SCHEMA_ONLY) {
      return _localAccess.listUrns(indexFilter, indexSortCriterion, lastUrn, pageSize);
    }

    if (!isLocalSecondaryIndexEnabled()) {
      throw new UnsupportedOperationException("Local secondary index isn't supported");
    }

    final IndexCriterionArray indexCriterionArray = indexFilter.getCriteria();
    checkValidIndexCriterionArray(indexCriterionArray);

    List<URN> urnsNew = null;
    if (_schemaConfig == SchemaConfig.DUAL_SCHEMA) {
      urnsNew = _localAccess.listUrns(indexFilter, indexSortCriterion, lastUrn, pageSize);
    }

    addEntityTypeFilter(indexFilter);

    final Query<EbeanMetadataIndex> query = _server.findNative(EbeanMetadataIndex.class, constructSQLQuery(indexCriterionArray,
        indexSortCriterion, false));

    query.setTimeout(INDEX_QUERY_TIMEOUT_IN_SEC);
    setParameters(indexCriterionArray, indexSortCriterion, query, lastUrn == null ? "" : lastUrn.toString(), pageSize, false);

    final List<URN> urnsOld = query.findList().stream().map(entry -> getUrn(entry.getUrn())).collect(Collectors.toList());

    if (_schemaConfig == SchemaConfig.DUAL_SCHEMA) {
      EBeanDAOUtils.compareResults(urnsOld, urnsNew, "listUrns");
    }

    return urnsOld;
  }

  /**
   *  Similar to {@link #listUrns(IndexFilter, IndexSortCriterion, Urn, int)} but returns a list result with pagination
   *  information.
   *
   * @param start the starting offset of the page
   * @return a {@link ListResult} containing a list of urns and other pagination information
   */
  @Override
  @Nonnull
  public ListResult<URN> listUrns(@Nonnull IndexFilter indexFilter, @Nullable IndexSortCriterion indexSortCriterion,
      int start, int pageSize) {

    if (_schemaConfig == SchemaConfig.NEW_SCHEMA_ONLY) {
      return _localAccess.listUrns(indexFilter, indexSortCriterion, start, pageSize);
    }

    if (!isLocalSecondaryIndexEnabled()) {
      throw new UnsupportedOperationException("Local secondary index isn't supported");
    }

    final IndexCriterionArray indexCriterionArray = indexFilter.getCriteria();
    checkValidIndexCriterionArray(indexCriterionArray);

    ListResult<URN> urnsNew = null;
    if (_schemaConfig == SchemaConfig.DUAL_SCHEMA) {
      urnsNew = _localAccess.listUrns(indexFilter, indexSortCriterion, start, pageSize);
    }

    addEntityTypeFilter(indexFilter);

    final Query<EbeanMetadataIndex> query = _server.findNative(EbeanMetadataIndex.class, constructSQLQuery(indexCriterionArray,
        indexSortCriterion, true));
    query.setTimeout(INDEX_QUERY_TIMEOUT_IN_SEC);
    setParameters(indexCriterionArray, indexSortCriterion, query, "", pageSize, true);

    final PagedList<EbeanMetadataIndex> pagedList = query.setFirstRow(start).setMaxRows(pageSize).findPagedList();

    pagedList.loadCount();

    final List<URN> urns = pagedList.getList().stream().map(entry -> getUrn(entry.getUrn())).collect(Collectors.toList());
    final ListResult<URN> urnsOld = toListResult(urns, null, pagedList, start);

    if (_schemaConfig == SchemaConfig.DUAL_SCHEMA) {
      EBeanDAOUtils.compareResults(urnsOld, urnsNew, "listUrns");
    }

    return urnsOld;
  }

  /**
   * Constructs SQL query to count agggregate urns that contains positioned parameters (with `?`),
   * based on whether {@link IndexCriterion} of a given condition has field `pathParams`.
   *
   * @param indexCriterionArray {@link IndexCriterionArray} used to construct the SQL query
   * @param indexGroupByCriterion {@link IndexGroupByCriterion} used to construct the SQL query
   * @return String representation of SQL query
   */
  @Nonnull
  private static String constructCountAggregateSQLQuery(@Nonnull IndexCriterionArray indexCriterionArray,
      @Nonnull IndexGroupByCriterion indexGroupByCriterion) {
    String groupByColumn = getFieldColumn(indexGroupByCriterion.getPath(), indexGroupByCriterion.getAspect());
    String selectClause = "SELECT COUNT(*), tgroup.";
    selectClause += groupByColumn;
    selectClause += " FROM metadata_index t0 INNER JOIN metadata_index tgroup on t0.urn = tgroup.urn";
    selectClause += IntStream.range(1, indexCriterionArray.size())
        .mapToObj(i -> " INNER JOIN metadata_index " + "t" + i + " ON t0.urn = " + "t" + i + ".urn")
        .collect(Collectors.joining(""));
    final StringBuilder whereClause = new StringBuilder("WHERE");
    IntStream.range(0, indexCriterionArray.size()).forEach(i -> {
      final IndexCriterion criterion = indexCriterionArray.get(i);

      if (i > 0) {
        whereClause.append(" AND");
      }
      whereClause.append(" t").append(i).append(".aspect = ?");
      if (criterion.getPathParams() != null) {
        SQLIndexFilterUtils.validateConditionAndValue(criterion);
        whereClause.append(" AND t")
            .append(i)
            .append(".path = ? AND t")
            .append(i)
            .append(".")
            .append(getGMAIndexPair(criterion).valueType)
            .append(" ")
            .append(getStringForOperator(criterion.getPathParams().getCondition()))
            .append(getPlaceholderStringForValue(criterion.getPathParams().getValue()));
      }
    });
    whereClause.append(" AND tgroup.aspect = ? AND tgroup.path = ? ");
    final String groupByClause = "GROUP BY tgroup." + groupByColumn;
    return String.join(" ", selectClause, whereClause, groupByClause);
  }

  /**
   * Sets the values of parameters in metadata index query based on its position, values obtained from
   * {@link IndexCriterionArray} and last urn. Also sets the LIMIT of SQL query using the page size input.
   *
   * @param indexCriterionArray {@link IndexCriterionArray} whose values will be used to set parameters in metadata
   *                                                       index query based on its position
   * @param indexGroupByCriterion {@link IndexGroupByCriterion} whose values will be used to set parameters in query
   * @param indexQuery {@link Query} whose ordered parameters need to be set, based on it's position
   */
  @Nonnull
  private static void setCountAggregateParameters(@Nonnull IndexCriterionArray indexCriterionArray,
      @Nonnull IndexGroupByCriterion indexGroupByCriterion, @Nonnull Query<EbeanMetadataIndex> indexQuery) {
    int pos = 1;
    for (IndexCriterion criterion : indexCriterionArray) {
      indexQuery.setParameter(pos++, criterion.getAspect());
      if (criterion.getPathParams() != null) {
        indexQuery.setParameter(pos++, criterion.getPathParams().getPath());
        indexQuery.setParameter(pos++, getGMAIndexPair(criterion).value);
      }
    }
    indexQuery.setParameter(pos++, indexGroupByCriterion.getAspect());
    indexQuery.setParameter(pos++, indexGroupByCriterion.getPath());
  }

  @Override
  @Nonnull
  public Map<String, Long> countAggregate(@Nonnull IndexFilter indexFilter,
      @Nonnull IndexGroupByCriterion indexGroupByCriterion) {
    if (_schemaConfig == SchemaConfig.NEW_SCHEMA_ONLY) {
      return _localAccess.countAggregate(indexFilter, indexGroupByCriterion);
    }

    if (!isLocalSecondaryIndexEnabled()) {
      throw new UnsupportedOperationException("Local secondary index isn't supported");
    }

    final IndexCriterionArray indexCriterionArray = indexFilter.getCriteria();
    checkValidIndexCriterionArray(indexCriterionArray);

    Map<String, Long> resultNew = null;
    if (_schemaConfig == SchemaConfig.DUAL_SCHEMA) {
      resultNew = _localAccess.countAggregate(indexFilter, indexGroupByCriterion);
    }

    addEntityTypeFilter(indexFilter);

    final Query<EbeanMetadataIndex> query = _server.findNative(EbeanMetadataIndex.class,
        constructCountAggregateSQLQuery(indexCriterionArray, indexGroupByCriterion));

    query.setTimeout(INDEX_QUERY_TIMEOUT_IN_SEC);

    setCountAggregateParameters(indexCriterionArray, indexGroupByCriterion, query);

    Map<String, Long> resultOld = new HashMap<>();
    query.setDistinct(true).findList().forEach(entry -> {
      if (entry.getStringVal() != null) {
        resultOld.put(entry.getStringVal(), entry.getTotalCount());
      } else if (entry.getDoubleVal() != null) {
        resultOld.put(entry.getDoubleVal().toString(), entry.getTotalCount());
      } else if (entry.getLongVal() != null) {
        resultOld.put(entry.getLongVal().toString(), entry.getTotalCount());
      }
    });

    if (_schemaConfig == SchemaConfig.DUAL_SCHEMA && !resultOld.equals(resultNew)) {
      // TODO: print info log with performance (response time) and values
      String message = String.format("Old result: %s. New result: %s", resultOld, resultNew);
      log.warn(String.format(EBeanDAOUtils.DIFFERENT_RESULTS_TEMPLATE, "countAggregate", message));
    }

    return resultOld;
  }
}