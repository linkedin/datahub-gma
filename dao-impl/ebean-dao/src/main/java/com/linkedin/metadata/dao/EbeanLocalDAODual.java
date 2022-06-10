package com.linkedin.metadata.dao;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.metadata.aspect.SoftDeletedAspect;
import com.linkedin.metadata.dao.producer.BaseMetadataEventProducer;
import com.linkedin.metadata.dao.retention.TimeBasedRetention;
import com.linkedin.metadata.dao.retention.VersionBasedRetention;
import com.linkedin.metadata.dao.scsi.EmptyPathExtractor;
import com.linkedin.metadata.dao.scsi.UrnPathExtractor;
import com.linkedin.metadata.dao.storage.LocalDAOStorageConfig;
import com.linkedin.metadata.dao.utils.QueryUtils;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.query.ExtraInfo;
import com.linkedin.metadata.query.IndexFilter;
import com.linkedin.metadata.query.IndexGroupByCriterion;
import com.linkedin.metadata.query.IndexSortCriterion;
import io.ebean.EbeanServer;
import io.ebean.EbeanServerFactory;
import io.ebean.SqlUpdate;
import io.ebean.config.ServerConfig;
import io.ebean.datasource.DataSourceConfig;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.persistence.OptimisticLockException;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.collections4.CollectionUtils;


/**
 * An Ebean implementation of {@link BaseLocalDAO}.
 */
@Slf4j
public class EbeanLocalDAODual<ASPECT_UNION extends UnionTemplate, URN extends Urn>
    extends BaseLocalDAO<ASPECT_UNION, URN> {

  // String stored in metadata_aspect table for soft deleted aspect
  private static final RecordTemplate DELETED_METADATA = new SoftDeletedAspect().setGma_deleted(true);
  public static final String DELETED_VALUE = RecordUtils.toJsonString(DELETED_METADATA);

  private static final String EBEAN_MODEL_PACKAGE = EbeanMetadataAspect.class.getPackage().getName();
  private static final String EBEAN_INDEX_PACKAGE = EbeanMetadataIndex.class.getPackage().getName();

  protected final EbeanServer _server;
  protected final Class<URN> _urnClass;
  private UrnPathExtractor<URN> _urnPathExtractor;
  private final EbeanLocalDAO<ASPECT_UNION, URN> _ebeanLocalDAO;
  private final IEBeanLocalAccess<URN> _localAccess;
  private int _queryKeysCount = 0; // 0 means no pagination on keys

  // TODO feature flags, remove when vetted.
  private boolean _useUnionForBatch = false;
  private boolean _useOptimisticLocking = false;

  @Value
  static class GMAIndexPair {
    public String valueType;
    public Object value;
  }

  @VisibleForTesting
  EbeanLocalDAODual(@Nonnull Class<ASPECT_UNION> aspectUnionClass, @Nonnull BaseMetadataEventProducer producer,
      @Nonnull EbeanServer server, @Nonnull Class<URN> urnClass) {
    super(aspectUnionClass, producer);
    _server = server;
    _urnClass = urnClass;
    _urnPathExtractor = new EmptyPathExtractor<>();
    _localAccess = new EBeanLocalAccess<>(server, urnClass);
    _ebeanLocalDAO = new EbeanLocalDAO<>(aspectUnionClass, producer, server, urnClass);
  }

  /**
   * Constructor for EbeanLocalDAO.
   *
   * @param aspectUnionClass containing union of all supported aspects. Must be a valid aspect union defined in com.linkedin.metadata.aspect
   * @param producer {@link BaseMetadataEventProducer} for the metadata event producer
   * @param serverConfig {@link ServerConfig} that defines the configuration of EbeanServer instances
   * @param urnClass Class of the entity URN
   */
  public EbeanLocalDAODual(@Nonnull Class<ASPECT_UNION> aspectUnionClass, @Nonnull BaseMetadataEventProducer producer,
      @Nonnull ServerConfig serverConfig, @Nonnull Class<URN> urnClass) {
    this(aspectUnionClass, producer, createServer(serverConfig), urnClass);
  }

  @VisibleForTesting
  EbeanLocalDAODual(@Nonnull BaseMetadataEventProducer producer, @Nonnull EbeanServer server,
      @Nonnull LocalDAOStorageConfig storageConfig, @Nonnull Class<URN> urnClass,
      @Nonnull UrnPathExtractor<URN> urnPathExtractor) {
    super(producer, storageConfig);
    _server = server;
    _urnClass = urnClass;
    _urnPathExtractor = urnPathExtractor;
    _localAccess = new EBeanLocalAccess<>(server, urnClass);
    _ebeanLocalDAO = new EbeanLocalDAO<>(producer, server, storageConfig, urnClass, urnPathExtractor);
  }

  @VisibleForTesting
  EbeanLocalDAODual(@Nonnull BaseMetadataEventProducer producer, @Nonnull EbeanServer server,
      @Nonnull LocalDAOStorageConfig storageConfig, @Nonnull Class<URN> urnClass) {
    this(producer, server, storageConfig, urnClass, new EmptyPathExtractor<>());
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
  public EbeanLocalDAODual(@Nonnull BaseMetadataEventProducer producer, @Nonnull ServerConfig serverConfig,
      @Nonnull LocalDAOStorageConfig storageConfig, @Nonnull Class<URN> urnClass,
      @Nonnull UrnPathExtractor<URN> urnPathExtractor) {
    this(producer, createServer(serverConfig), storageConfig, urnClass, urnPathExtractor);
  }

  /**
   * Constructor for EbeanLocalDAO.
   *
   * @param producer {@link BaseMetadataEventProducer} for the metadata event producer
   * @param serverConfig {@link ServerConfig} that defines the configuration of EbeanServer instances
   * @param storageConfig {@link LocalDAOStorageConfig} containing storage config of full list of supported aspects
   * @param urnClass class of the entity URN
   */
  public EbeanLocalDAODual(@Nonnull BaseMetadataEventProducer producer, @Nonnull ServerConfig serverConfig,
      @Nonnull LocalDAOStorageConfig storageConfig, @Nonnull Class<URN> urnClass) {
    this(producer, createServer(serverConfig), storageConfig, urnClass, new EmptyPathExtractor<>());
  }

  /**
   * Determines whether we should use UNION ALL statements for batch gets, rather than a large series of OR statements.
   *
   * <p>DO NOT USE THIS FLAG! This is for LinkedIn use to help us test this feature without a rollback. Once we've
   * vetted this in production we will be removing this flag and making the the default behavior. So if you set this
   * to true by calling this method, your code will break when we remove this method. Just wait a bit for us to turn
   * it on by default!
   *
   * <p>While this can increase performance, it can also cause a stack overflow error if {@link #setQueryKeysCount(int)}
   * is either not set or set too high. See https://groups.google.com/g/ebean/c/ILpii41dJPA/m/VxMbPlqEBwAJ.
   */
  public void setUseUnionForBatch(boolean useUnionForBatch) {
    _useUnionForBatch = useUnionForBatch;
    _ebeanLocalDAO.setUseUnionForBatch(useUnionForBatch);
  }

  /**
   * Determines whether we should use optimistic locking in updates. Optimistic locking is done on {@code createdOn} column
   *
   * <p>DO NOT USE THIS FLAG! This is for LinkedIn use to help us test this feature without a rollback. Once we've
   * vetted this in production we will be removing this flag and making the the default behavior. So if you set this
   * to true by calling this method, your code will break when we remove this method. Just wait a bit for us to turn
   * it on by default!
   */
  public void setUseOptimisticLocking(boolean useOptimisticLocking) {
    _useOptimisticLocking = useOptimisticLocking;
    _ebeanLocalDAO.setUseOptimisticLocking(useOptimisticLocking);
  }

  @Nonnull
  private static EbeanServer createServer(@Nonnull ServerConfig serverConfig) {
    // Make sure that the serverConfig includes the package that contains DAO's Ebean model.
    if (!serverConfig.getPackages().contains(EBEAN_MODEL_PACKAGE)) {
      serverConfig.getPackages().add(EBEAN_MODEL_PACKAGE);
    }
    if (!serverConfig.getPackages().contains(EBEAN_INDEX_PACKAGE)) {
      serverConfig.getPackages().add(EBEAN_INDEX_PACKAGE);
    }
    return EbeanServerFactory.create(serverConfig);
  }

  /**
   * Return the {@link EbeanServer} server instance used for customized queries.
   */
  public EbeanServer getServer() {
    return _server;
  }

  public void setUrnPathExtractor(@Nonnull UrnPathExtractor<URN> urnPathExtractor) {
    _urnPathExtractor = urnPathExtractor;
  }

  /**
   * Creates a private in-memory {@link EbeanServer} based on H2 for production.
   */
  @Nonnull
  public static ServerConfig createProductionH2ServerConfig(@Nonnull String dbName) {

    DataSourceConfig dataSourceConfig = new DataSourceConfig();
    dataSourceConfig.setUsername("tester");
    dataSourceConfig.setPassword("");
    String url = "jdbc:h2:mem:" + dbName + ";IGNORECASE=TRUE;DB_CLOSE_DELAY=-1;";
    dataSourceConfig.setUrl(url);
    dataSourceConfig.setDriver("org.h2.Driver");

    ServerConfig serverConfig = new ServerConfig();
    serverConfig.setName(dbName);
    serverConfig.setDataSourceConfig(dataSourceConfig);
    serverConfig.setDdlGenerate(false);
    serverConfig.setDdlRun(false);

    return serverConfig;
  }

  /**
   * Creates a private in-memory {@link EbeanServer} based on H2 for testing purpose.
   */
  @Nonnull
  public static ServerConfig createTestingH2ServerConfig() {
    DataSourceConfig dataSourceConfig = new DataSourceConfig();
    dataSourceConfig.setUsername("tester");
    dataSourceConfig.setPassword("");
    dataSourceConfig.setUrl("jdbc:h2:mem:testdb;IGNORECASE=TRUE;");
    dataSourceConfig.setDriver("org.h2.Driver");

    ServerConfig serverConfig = new ServerConfig();
    serverConfig.setName("gma");
    serverConfig.setDataSourceConfig(dataSourceConfig);
    serverConfig.setDdlGenerate(true);
    serverConfig.setDdlRun(true);

    return serverConfig;
  }

  @Nonnull
  @Override
  protected <T> T runInTransactionWithRetry(@Nonnull Supplier<T> block, int maxTransactionRetry) {
    return _ebeanLocalDAO.runInTransactionWithRetry(block, maxTransactionRetry);
  }

  @Override
  protected <ASPECT extends RecordTemplate> long saveLatest(@Nonnull URN urn, @Nonnull Class<ASPECT> aspectClass,
      @Nullable ASPECT oldValue, @Nullable AuditStamp oldAuditStamp, @Nullable ASPECT newValue,
      @Nonnull AuditStamp newAuditStamp, boolean isSoftDeleted) {
    // Save oldValue as the largest version + 1
    long largestVersion = 0;
    if ((isSoftDeleted || oldValue != null) && oldAuditStamp != null) {
      largestVersion = getNextVersion(urn, aspectClass);
      save(urn, oldValue, aspectClass, oldAuditStamp, largestVersion, true);

      // update latest version
      if (_useOptimisticLocking) {
        saveWithOptimisticLocking(urn, newValue, aspectClass, newAuditStamp, LATEST_VERSION, false,
            new Timestamp(oldAuditStamp.getTime()));
      } else {
        save(urn, newValue, aspectClass, newAuditStamp, LATEST_VERSION, false);
      }
    } else {
      save(urn, newValue, aspectClass, newAuditStamp, LATEST_VERSION, true);
    }

    return largestVersion;
  }

  @Override
  public <ASPECT extends RecordTemplate> void updateLocalIndex(@Nonnull URN urn, @Nonnull ASPECT newValue,
      long version) {
    _ebeanLocalDAO.updateLocalIndex(urn, newValue, version);
  }

  @Override
  @Nonnull
  protected <ASPECT extends RecordTemplate> AspectEntry<ASPECT> getLatest(@Nonnull URN urn,
      @Nonnull Class<ASPECT> aspectClass) {
    final List<EbeanMetadataAspect> result = batchGetHelper(
        Collections.singletonList(new AspectKey<>(aspectClass, urn, LATEST_VERSION)), 1, 0);
    final EbeanMetadataAspect latest = result.size() == 0 ? null : result.get(0);
    if (latest == null) {
      return new AspectEntry<>(null, null);
    }

    final ExtraInfo extraInfo = EbeanLocalDAO.toExtraInfo(latest);

    if (latest.getMetadata().equals(DELETED_VALUE)) {
      return new AspectEntry<>(null, extraInfo, true);
    }

    return new AspectEntry<>(RecordUtils.toRecordTemplate(aspectClass, latest.getMetadata()), extraInfo);
  }

  // visible for testing
  protected <ASPECT extends RecordTemplate> void saveWithOptimisticLocking(@Nonnull URN urn,
      @Nullable RecordTemplate value, @Nonnull Class<ASPECT> aspectClass, @Nonnull AuditStamp newAuditStamp,
      long version, boolean insert, @Nonnull Object oldTimestamp) {

    final EbeanMetadataAspect aspect = _ebeanLocalDAO.buildMetadataAspectBean(urn, value, aspectClass, newAuditStamp, version);

    if (insert) {
      runInTransactionWithRetry(() -> {
        _server.insert(aspect);
        return _localAccess.add(urn, value, newAuditStamp);
      }, 1);
      return;
    }

    // Build manual SQL update query to enable optimistic locking on a given column
    // Optimistic locking is supported on ebean using @version, see https://ebean.io/docs/mapping/jpa/version
    // But we can't use @version annotation for optimistic locking for two reasons:
    //   1. That prevents flag guarding optimistic locking feature
    //   2. When using @version annotation, Ebean starts to override all updates to that column
    //      by disregarding any user change.
    // Ideally, another column for the sake of optimistic locking would be preferred but that means a change to
    // metadata_aspect schema and we don't take this route here to keep this change backward compatible.
    final String updateQuery = String.format("UPDATE metadata_aspect "
        + "SET urn = :urn, aspect = :aspect, version = :version, metadata = :metadata, createdOn = :createdOn, createdBy = :createdBy "
        + "WHERE urn = :urn and aspect = :aspect and version = :version and createdOn = :oldTimestamp");

    final SqlUpdate update = _server.createSqlUpdate(updateQuery);
    update.setParameter("urn", aspect.getKey().getUrn());
    update.setParameter("aspect", aspect.getKey().getAspect());
    update.setParameter("version", aspect.getKey().getVersion());
    update.setParameter("metadata", aspect.getMetadata());
    update.setParameter("createdOn", aspect.getCreatedOn());
    update.setParameter("createdBy", aspect.getCreatedBy());
    update.setParameter("oldTimestamp", oldTimestamp);

    // If there is no single updated row, emit OptimisticLockException
    int numOfUpdatedRows = runInTransactionWithRetry(() -> {
      // ensure that update to old schema (change log) and new schema are in one atomic transaction
      final int numUpdated = _server.execute(update);
      _localAccess.add(urn, value, newAuditStamp);
      return numUpdated;
    }, 1);
    if (numOfUpdatedRows != 1) {
      throw new OptimisticLockException(
          numOfUpdatedRows + " rows updated during save query: " + update.getGeneratedSql());
    }
  }

  @Override
  protected <ASPECT extends RecordTemplate> void save(@Nonnull URN urn, @Nullable RecordTemplate value,
      @Nonnull Class<ASPECT> aspectClass, @Nonnull AuditStamp auditStamp, long version, boolean insert) {

    final EbeanMetadataAspect aspect = _ebeanLocalDAO.buildMetadataAspectBean(urn, value, aspectClass, auditStamp, version);

    // upsert into old schema
    if (insert) {
      _server.insert(aspect);
    } else {
      _server.update(aspect);
    }

    // upsert into new schema
    _localAccess.add(urn, value, auditStamp);
  }

  protected void saveRecordsToLocalIndex(@Nonnull URN urn, @Nonnull String aspect, @Nonnull String path,
      @Nonnull Object value) {
    _ebeanLocalDAO.saveRecordsToLocalIndex(urn, aspect, path, value);
  }

  protected long saveSingleRecordToLocalIndex(@Nonnull URN urn, @Nonnull String aspect, @Nonnull String path,
      @Nonnull Object value) {
    return _ebeanLocalDAO.saveSingleRecordToLocalIndex(urn, aspect, path, value);
  }

  @Nonnull
  Map<Class<? extends RecordTemplate>, LocalDAOStorageConfig.AspectStorageConfig> getStrongConsistentIndexPaths() {
    return _ebeanLocalDAO.getStrongConsistentIndexPaths();
  }

  @Override
  protected <ASPECT extends RecordTemplate> long getNextVersion(@Nonnull URN urn, @Nonnull Class<ASPECT> aspectClass) {
    return _ebeanLocalDAO.getNextVersion(urn, aspectClass);
  }

  @Override
  protected <ASPECT extends RecordTemplate> void applyVersionBasedRetention(@Nonnull Class<ASPECT> aspectClass,
      @Nonnull URN urn, @Nonnull VersionBasedRetention retention, long largestVersion) {
    _ebeanLocalDAO.applyVersionBasedRetention(aspectClass, urn, retention, largestVersion);
  }

  @Override
  protected <ASPECT extends RecordTemplate> void applyTimeBasedRetention(@Nonnull Class<ASPECT> aspectClass,
      @Nonnull URN urn, @Nonnull TimeBasedRetention retention, long currentTime) {
    _ebeanLocalDAO.applyTimeBasedRetention(aspectClass, urn, retention, currentTime);
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
            .filter(record -> _ebeanLocalDAO.matchKeys(key, record.getKey()))
            .findFirst()
            .flatMap(record -> EbeanLocalDAO.toRecordTemplate(key.getAspectClass(), record))));
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
        .filter(record -> _ebeanLocalDAO.matchKeys(key, record.getKey()))
        .findFirst()
        .map(record -> {
          final Class<RecordTemplate> aspectClass = (Class<RecordTemplate>) key.getAspectClass();
          final Optional<AspectWithExtraInfo<RecordTemplate>> aspectWithExtraInfo = EbeanLocalDAO.toRecordTemplateWithExtraInfo(aspectClass, record);
          aspectWithExtraInfo.ifPresent(
              recordTemplateAspectWithExtraInfo -> result.put(key, recordTemplateAspectWithExtraInfo));
          return null;
        }));
    return result;
  }

  @Override
  public boolean exists(@Nonnull URN urn) {
    return _ebeanLocalDAO.exists(urn) && _localAccess.exists(urn);
  }

  public boolean existsInLocalIndex(@Nonnull URN urn) {
    return _ebeanLocalDAO.existsInLocalIndex(urn);
  }

  /**
   * Sets the max keys allowed for each single query.
   */
  public void setQueryKeysCount(int keysCount) {
    if (keysCount < 0) {
      throw new IllegalArgumentException("Query keys count must be non-negative: " + keysCount);
    }
    _queryKeysCount = keysCount;
    _ebeanLocalDAO.setQueryKeysCount(keysCount);
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

  @Nonnull
  private List<EbeanMetadataAspect> batchGetUnion(@Nonnull List<AspectKey<URN, ? extends RecordTemplate>> keys,
      int keysCount, int position) {
    return _localAccess.batchGetUnion(keys, keysCount, position);
  }

  @Nonnull
  private List<EbeanMetadataAspect> batchGetOr(@Nonnull List<AspectKey<URN, ? extends RecordTemplate>> keys,
      int keysCount, int position) {
    return _localAccess.batchGetOr(keys, keysCount, position);
  }

  /**
   * Run batchGetUnion or batchGetOr on the passed in keys, depending on _useUnionForBatch variable, reading from both
   * the old schema and the new schema. Compare the results. If the results are not equal, log an error and default
   * to using the old schema's value(s).
   */
  @Nonnull
  private List<EbeanMetadataAspect> batchGetHelper(@Nonnull List<AspectKey<URN, ? extends RecordTemplate>> keys,
      int keysCount, int position) {
    // TODO remove batchGetOr, make batchGetUnion the only implementation.
    List<EbeanMetadataAspect> resultsOldSchema = _ebeanLocalDAO.batchGetHelper(keys, keysCount, position);
    List<EbeanMetadataAspect> resultsNewSchema = _useUnionForBatch
        ? batchGetUnion(keys, keysCount, position)
        : batchGetOr(keys, keysCount, position);
    // TODO: add comparator to compare objects within list:
    // https://commons.apache.org/proper/commons-collections/javadocs/api-4.4/org/apache/commons/collections4/CollectionUtils.html#isEqualCollection-java.util.Collection-java.util.Collection-org.apache.commons.collections4.Equator-
    if (!CollectionUtils.isEqualCollection(resultsOldSchema, resultsNewSchema)) {
      log.error(String.format("The results of batchGet from the new schema table and old schema table are not equal."
          + "Defaulting to using the value(s) from the old schema table."));
    }
    return resultsOldSchema;
  }

  @Override
  @Nonnull
  public <ASPECT extends RecordTemplate> ListResult<Long> listVersions(@Nonnull Class<ASPECT> aspectClass,
      @Nonnull URN urn, int start, int pageSize) {
    return _ebeanLocalDAO.listVersions(aspectClass, urn, start, pageSize);
  }

  @Override
  @Nonnull
  public <ASPECT extends RecordTemplate> ListResult<URN> listUrns(@Nonnull Class<ASPECT> aspectClass, int start,
      int pageSize) {
    return _localAccess.listUrns(aspectClass, start, pageSize);
  }

  @Override
  @Nonnull
  public <ASPECT extends RecordTemplate> ListResult<ASPECT> list(@Nonnull Class<ASPECT> aspectClass, @Nonnull URN urn,
      int start, int pageSize) {
    return _ebeanLocalDAO.list(aspectClass, urn, start, pageSize);
  }

  @Override
  @Nonnull
  public <ASPECT extends RecordTemplate> ListResult<ASPECT> list(@Nonnull Class<ASPECT> aspectClass, long version,
      int start, int pageSize) {
    return _ebeanLocalDAO.list(aspectClass, version, start, pageSize);
  }

  @Override
  @Nonnull
  public <ASPECT extends RecordTemplate> ListResult<ASPECT> list(@Nonnull Class<ASPECT> aspectClass, int start,
      int pageSize) {
    return _ebeanLocalDAO.list(aspectClass, start, pageSize);
  }

  @Override
  public long newNumericId(@Nonnull String namespace, int maxTransactionRetry) {
    return _ebeanLocalDAO.newNumericId(namespace, maxTransactionRetry);
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
    return _localAccess.listUrns(indexFilter, indexSortCriterion, lastUrn, pageSize);
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
    return _localAccess.listUrns(indexFilter, indexSortCriterion, start, pageSize);
  }


  @Override
  @Nonnull
  public Map<String, Long> countAggregate(@Nonnull IndexFilter indexFilter,
      @Nonnull IndexGroupByCriterion indexGroupByCriterion) {
    return _localAccess.countAggregate(indexFilter, indexGroupByCriterion);
  }
}