package com.linkedin.metadata.dao;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.metadata.dao.builder.BaseLocalRelationshipBuilder.LocalRelationshipUpdates;
import com.linkedin.metadata.dao.builder.LocalRelationshipBuilderRegistry;
import com.linkedin.metadata.dao.exception.ModelConversionException;
import com.linkedin.metadata.dao.exception.RetryLimitReached;
import com.linkedin.metadata.dao.internal.BaseGraphWriterDAO;
import com.linkedin.metadata.dao.producer.BaseMetadataEventProducer;
import com.linkedin.metadata.dao.producer.BaseTrackingMetadataEventProducer;
import com.linkedin.metadata.dao.retention.TimeBasedRetention;
import com.linkedin.metadata.dao.retention.VersionBasedRetention;
import com.linkedin.metadata.dao.storage.LocalDAOStorageConfig;
import com.linkedin.metadata.dao.tracking.BaseTrackingManager;
import com.linkedin.metadata.dao.urnpath.EmptyPathExtractor;
import com.linkedin.metadata.dao.urnpath.UrnPathExtractor;
import com.linkedin.metadata.dao.utils.EBeanDAOUtils;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.dao.utils.QueryUtils;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.events.IngestionTrackingContext;
import com.linkedin.metadata.query.Condition;
import com.linkedin.metadata.query.ExtraInfo;
import com.linkedin.metadata.query.ExtraInfoArray;
import com.linkedin.metadata.query.IndexCriterion;
import com.linkedin.metadata.query.IndexFilter;
import com.linkedin.metadata.query.IndexGroupByCriterion;
import com.linkedin.metadata.query.IndexSortCriterion;
import com.linkedin.metadata.query.IndexValue;
import com.linkedin.metadata.query.ListResultMetadata;
import io.ebean.DuplicateKeyException;
import io.ebean.EbeanServer;
import io.ebean.PagedList;
import io.ebean.Query;
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
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.persistence.OptimisticLockException;
import javax.persistence.RollbackException;
import javax.persistence.Table;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.dao.EbeanLocalAccess.*;
import static com.linkedin.metadata.dao.EbeanMetadataAspect.*;
import static com.linkedin.metadata.dao.utils.EBeanDAOUtils.*;
import static com.linkedin.metadata.dao.utils.EbeanServerUtils.*;


/**
 * An Ebean implementation of {@link BaseLocalDAO}.
 */
@Slf4j
public class EbeanLocalDAO<ASPECT_UNION extends UnionTemplate, URN extends Urn>
    extends BaseLocalDAO<ASPECT_UNION, URN> {

  protected final EbeanServer _server;
  protected final Class<URN> _urnClass;

  private final static int DEFAULT_BATCH_SIZE = 50;
  private int _queryKeysCount = DEFAULT_BATCH_SIZE;
  private IEbeanLocalAccess<URN> _localAccess;
  private EbeanLocalRelationshipWriterDAO _localRelationshipWriterDAO;
  private LocalRelationshipBuilderRegistry _localRelationshipBuilderRegistry = null;
  private SchemaConfig _schemaConfig = SchemaConfig.OLD_SCHEMA_ONLY;
  private final EBeanDAOConfig _eBeanDAOConfig = new EBeanDAOConfig();

  public enum SchemaConfig {
    OLD_SCHEMA_ONLY, // Default: read from and write to the old schema table
    NEW_SCHEMA_ONLY, // Read from and write to the new schema tables
    DUAL_SCHEMA // Write to both the old and new tables and perform a comparison between values when reading
  }

  // TODO: clean up once AIM is no longer using existing local relationships - they should make new relationship tables with the aspect column
  private boolean _useAspectColumnForRelationshipRemoval = false;

  // Which approach to be used for record retrieval when inserting a new record
  // See GCN-38382
  private FindMethodology _findMethodology = FindMethodology.UNIQUE_ID;

  // true if metadata change will be persisted into the change log table (metadata_aspect)
  private boolean _changeLogEnabled = true;

  // TODO: remove this logic once metadata_aspect has been completed removed from TMS
  // regarding metadata_aspect table:
  // false = read/bump 2nd latest version + insert latest version
  // true = overwrite 2nd latest version with latest version (equivalent to keeping only version = 0 rows in metadata_aspect)
  private boolean _overwriteLatestVersionEnabled = false;

  public void setChangeLogEnabled(boolean changeLogEnabled) {
    if (_schemaConfig == SchemaConfig.NEW_SCHEMA_ONLY) {
      _changeLogEnabled = changeLogEnabled;
    } else {
      // For non-new schema, _changeLog will be enforced to be true
      log.warn("You can only enable or disable the change log in new schema mode."
          + "In old and dual schema modes, this setting is always enabled.");
      _changeLogEnabled = true;
    }
  }

  public boolean isChangeLogEnabled() {
    return _changeLogEnabled;
  }

  /**
   * Set a flag to indicate whether to use the aspect column for relationship removal. If set to true, only relationships from
   * the same aspect class will be removed during ingestion or soft-deletion.
   */
  public void setUseAspectColumnForRelationshipRemoval(boolean useAspectColumnForRelationshipRemoval) {
    _useAspectColumnForRelationshipRemoval = useAspectColumnForRelationshipRemoval;
    _localRelationshipWriterDAO.setUseAspectColumnForRelationshipRemoval(useAspectColumnForRelationshipRemoval);
  }

  public void setOverwriteLatestVersionEnabled(boolean overwriteLatestVersionEnabled) {
    if (_schemaConfig == SchemaConfig.NEW_SCHEMA_ONLY) {
      if (isChangeLogEnabled()) {
        _overwriteLatestVersionEnabled = overwriteLatestVersionEnabled;
      } else {
        log.warn("You can only enable or disable overwriting the latest version when the change log is enabled as well.");
        _overwriteLatestVersionEnabled = false;
      }
    } else {
      // For non-new schema, _ovewriteLatestVersionEnabled will be enforced to be false
      log.warn("You can only enable or disable overwriting the latest version in new schema mode."
          + "In old and dual schema modes, this setting is always disabled.");
      _overwriteLatestVersionEnabled = false;
    }
  }

  public enum FindMethodology {
    UNIQUE_ID,      // (legacy) https://javadoc.io/static/io.ebean/ebean/11.19.2/io/ebean/EbeanServer.html#find-java.lang.Class-java.lang.Object-
    DIRECT_SQL,     // https://javadoc.io/static/io.ebean/ebean/11.19.2/io/ebean/EbeanServer.html#findNative-java.lang.Class-java.lang.String-
    QUERY_BUILDER   // https://javadoc.io/static/io.ebean/ebean/11.19.2/io/ebean/Ebean.html#find-java.lang.Class-
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
   * Constructor for EbeanLocalDAO.
   *
   * @param aspectUnionClass containing union of all supported aspects. Must be a valid aspect union defined in com.linkedin.metadata.aspect
   * @param producer {@link BaseTrackingMetadataEventProducer} for the metadata event producer
   * @param serverConfig {@link ServerConfig} that defines the configuration of EbeanServer instances
   * @param urnClass Class of the entity URN
   * @param trackingManager {@link BaseTrackingManager} tracking manager for producing tracking requests
   */
  public EbeanLocalDAO(@Nonnull Class<ASPECT_UNION> aspectUnionClass, @Nonnull BaseTrackingMetadataEventProducer producer,
      @Nonnull ServerConfig serverConfig, @Nonnull Class<URN> urnClass, @Nonnull BaseTrackingManager trackingManager) {
    this(aspectUnionClass, producer, createServer(serverConfig), urnClass, trackingManager);
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
   * Constructor for EbeanLocalDAO with the option to use the new schema and enable dual-read.
   *
   * @param aspectUnionClass containing union of all supported aspects. Must be a valid aspect union defined in com.linkedin.metadata.aspect
   * @param producer {@link BaseTrackingMetadataEventProducer} for the metadata event producer
   * @param serverConfig {@link ServerConfig} that defines the configuration of EbeanServer instances
   * @param urnClass Class of the entity URN
   * @param schemaConfig Enum indicating which schema(s)/table(s) to read from and write to
   * @param trackingManager {@link BaseTrackingManager} tracking manager for producing tracking requests
   */
  public EbeanLocalDAO(@Nonnull Class<ASPECT_UNION> aspectUnionClass, @Nonnull BaseTrackingMetadataEventProducer producer,
      @Nonnull ServerConfig serverConfig, @Nonnull Class<URN> urnClass, @Nonnull SchemaConfig schemaConfig,
      @Nonnull BaseTrackingManager trackingManager) {
    this(aspectUnionClass, producer, createServer(serverConfig), serverConfig, urnClass, schemaConfig, trackingManager);
  }

  /**
   * Constructor for EbeanLocalDAO with the option to use an alternate Ebean find methodology for record insertion.
   * See GCN-38382
   *
   * @param aspectUnionClass containing union of all supported aspects. Must be a valid aspect union defined in com.linkedin.metadata.aspect
   * @param producer {@link BaseMetadataEventProducer} for the metadata event producer
   * @param serverConfig {@link ServerConfig} that defines the configuration of EbeanServer instances
   * @param urnClass Class of the entity URN
   * @param findMethodology Enum indicating which find configuration to use
   */
  public EbeanLocalDAO(@Nonnull Class<ASPECT_UNION> aspectUnionClass, @Nonnull BaseMetadataEventProducer producer,
      @Nonnull ServerConfig serverConfig, @Nonnull Class<URN> urnClass, @Nonnull FindMethodology findMethodology) {
    this(aspectUnionClass, producer, createServer(serverConfig), serverConfig, urnClass, findMethodology);
  }

  /**
   * Constructor for EbeanLocalDAO with the option to use an alternate Ebean find methodology for record insertion.
   * See GCN-38382
   *
   * @param aspectUnionClass containing union of all supported aspects. Must be a valid aspect union defined in com.linkedin.metadata.aspect
   * @param producer {@link BaseTrackingMetadataEventProducer} for the metadata event producer
   * @param serverConfig {@link ServerConfig} that defines the configuration of EbeanServer instances
   * @param urnClass Class of the entity URN
   * @param findMethodology Enum indicating which find configuration to use
   * @param trackingManager {@link BaseTrackingManager} tracking manager for producing tracking requests
   */
  public EbeanLocalDAO(@Nonnull Class<ASPECT_UNION> aspectUnionClass,
      @Nonnull BaseTrackingMetadataEventProducer producer, @Nonnull ServerConfig serverConfig,
      @Nonnull Class<URN> urnClass, @Nonnull FindMethodology findMethodology,
      @Nonnull BaseTrackingManager trackingManager) {
    this(aspectUnionClass, producer, createServer(serverConfig), serverConfig, urnClass, findMethodology, trackingManager);
  }

  /**
   * Constructor for EbeanLocalDAO.
   *
   * @param producer {@link BaseMetadataEventProducer} for the metadata event producer
   * @param serverConfig {@link ServerConfig} that defines the configuration of EbeanServer instances
   * @param storageConfig {@link LocalDAOStorageConfig} containing storage config of full list of supported aspects
   * @param urnClass class of the entity URN
   * @param urnPathExtractor path extractor to index parts of URNs
   */
  public EbeanLocalDAO(@Nonnull BaseMetadataEventProducer producer, @Nonnull ServerConfig serverConfig,
      @Nonnull LocalDAOStorageConfig storageConfig, @Nonnull Class<URN> urnClass,
      @Nonnull UrnPathExtractor<URN> urnPathExtractor) {
    this(producer, createServer(serverConfig), storageConfig, urnClass, urnPathExtractor);
  }

  /**
   * Constructor for EbeanLocalDAO.
   *
   * @param producer {@link BaseTrackingMetadataEventProducer} for the metadata event producer
   * @param serverConfig {@link ServerConfig} that defines the configuration of EbeanServer instances
   * @param storageConfig {@link LocalDAOStorageConfig} containing storage config of full list of supported aspects
   * @param urnClass class of the entity URN
   * @param urnPathExtractor path extractor to index parts of URNs to
   * @param trackingManager {@link BaseTrackingManager} tracking manager for producing tracking requests
   */
  public EbeanLocalDAO(@Nonnull BaseTrackingMetadataEventProducer producer, @Nonnull ServerConfig serverConfig,
      @Nonnull LocalDAOStorageConfig storageConfig, @Nonnull Class<URN> urnClass,
      @Nonnull UrnPathExtractor<URN> urnPathExtractor, @Nonnull BaseTrackingManager trackingManager) {
    this(producer, createServer(serverConfig), storageConfig, urnClass, urnPathExtractor, trackingManager);
  }

  /**
   * Constructor for EbeanLocalDAO with the option to use the new schema and enable dual-read.
   *
   * @param producer {@link BaseMetadataEventProducer} for the metadata event producer
   * @param serverConfig {@link ServerConfig} that defines the configuration of EbeanServer instances
   * @param storageConfig {@link LocalDAOStorageConfig} containing storage config of full list of supported aspects
   * @param urnClass class of the entity URN
   * @param urnPathExtractor path extractor to index parts of URNs
   * @param schemaConfig Enum indicating which schema(s)/table(s) to read from and write to
   */
  public EbeanLocalDAO(@Nonnull BaseMetadataEventProducer producer, @Nonnull ServerConfig serverConfig,
      @Nonnull LocalDAOStorageConfig storageConfig, @Nonnull Class<URN> urnClass,
      @Nonnull UrnPathExtractor<URN> urnPathExtractor, @Nonnull SchemaConfig schemaConfig) {
    this(producer, createServer(serverConfig), serverConfig, storageConfig, urnClass, urnPathExtractor, schemaConfig);
  }

  /**
   * Constructor for EbeanLocalDAO with the option to use the new schema and enable dual-read.
   *
   * @param producer {@link BaseTrackingMetadataEventProducer} for the metadata event producer
   * @param serverConfig {@link ServerConfig} that defines the configuration of EbeanServer instances
   * @param storageConfig {@link LocalDAOStorageConfig} containing storage config of full list of supported aspects
   * @param urnClass class of the entity URN
   * @param urnPathExtractor path extractor to index parts of URNs
   * @param schemaConfig Enum indicating which schema(s)/table(s) to read from and write to
   * @param trackingManager {@link BaseTrackingManager} tracking manager for producing tracking requests
   */
  public EbeanLocalDAO(@Nonnull BaseTrackingMetadataEventProducer producer, @Nonnull ServerConfig serverConfig,
      @Nonnull LocalDAOStorageConfig storageConfig, @Nonnull Class<URN> urnClass,
      @Nonnull UrnPathExtractor<URN> urnPathExtractor, @Nonnull SchemaConfig schemaConfig, @Nonnull BaseTrackingManager trackingManager) {
    this(producer, createServer(serverConfig), serverConfig, storageConfig, urnClass, urnPathExtractor, schemaConfig, trackingManager);
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
   * Constructor for EbeanLocalDAO.
   *
   * @param producer {@link BaseTrackingMetadataEventProducer} for the tracking metadata event producer
   * @param serverConfig {@link ServerConfig} that defines the configuration of EbeanServer instances
   * @param storageConfig {@link LocalDAOStorageConfig} containing storage config of full list of supported aspects
   * @param urnClass class of the entity URN
   * @param trackingManager {@link BaseTrackingManager} tracking manager for producing tracking requests
   */
  public EbeanLocalDAO(@Nonnull BaseTrackingMetadataEventProducer producer, @Nonnull ServerConfig serverConfig,
      @Nonnull LocalDAOStorageConfig storageConfig, @Nonnull Class<URN> urnClass, @Nonnull BaseTrackingManager trackingManager) {
    this(producer, createServer(serverConfig), storageConfig, urnClass, new EmptyPathExtractor<>(), trackingManager);
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

  /**
   * Constructor for EbeanLocalDAO with the option to use the new schema and enable dual-read.
   *
   * @param producer {@link BaseTrackingMetadataEventProducer} for the metadata event producer
   * @param serverConfig {@link ServerConfig} that defines the configuration of EbeanServer instances
   * @param storageConfig {@link LocalDAOStorageConfig} containing storage config of full list of supported aspects
   * @param urnClass class of the entity URN
   * @param schemaConfig Enum indicating which schema(s)/table(s) to read from and write to
   * @param trackingManager {@link BaseTrackingManager} tracking manager for producing tracking requests
   */
  public EbeanLocalDAO(@Nonnull BaseTrackingMetadataEventProducer producer, @Nonnull ServerConfig serverConfig,
      @Nonnull LocalDAOStorageConfig storageConfig, @Nonnull Class<URN> urnClass, @Nonnull SchemaConfig schemaConfig,
      @Nonnull BaseTrackingManager trackingManager) {
    this(producer, createServer(serverConfig), serverConfig, storageConfig, urnClass, new EmptyPathExtractor<>(), schemaConfig, trackingManager);
  }

  public EbeanLocalDAO(@Nonnull Class<ASPECT_UNION> aspectUnionClass, @Nonnull BaseTrackingMetadataEventProducer producer,
      @Nonnull EbeanServer server, @Nonnull ServerConfig serverConfig, @Nonnull SchemaConfig schemaConfig, @Nonnull Class<URN> urnClass) {
    this(aspectUnionClass, producer, server, serverConfig, urnClass, schemaConfig, new EBeanDAOConfig());
  }

  private EbeanLocalDAO(@Nonnull Class<ASPECT_UNION> aspectUnionClass, @Nonnull BaseMetadataEventProducer producer,
      @Nonnull EbeanServer server, @Nonnull Class<URN> urnClass) {
    super(aspectUnionClass, producer, urnClass, new EmptyPathExtractor<>());
    _server = server;
    _urnClass = urnClass;
    _localRelationshipWriterDAO = new EbeanLocalRelationshipWriterDAO(_server);
  }

  private EbeanLocalDAO(@Nonnull Class<ASPECT_UNION> aspectUnionClass, @Nonnull BaseTrackingMetadataEventProducer producer,
      @Nonnull EbeanServer server, @Nonnull Class<URN> urnClass, @Nonnull BaseTrackingManager trackingManager) {
    super(aspectUnionClass, producer, trackingManager, urnClass, new EmptyPathExtractor<>());
    _server = server;
    _urnClass = urnClass;
    _localRelationshipWriterDAO = new EbeanLocalRelationshipWriterDAO(_server);
  }
  private EbeanLocalDAO(@Nonnull Class<ASPECT_UNION> aspectUnionClass, @Nonnull BaseMetadataEventProducer producer,
      @Nonnull EbeanServer server, @Nonnull ServerConfig serverConfig, @Nonnull Class<URN> urnClass, @Nonnull SchemaConfig schemaConfig) {
    this(aspectUnionClass, producer, server, urnClass);
    _schemaConfig = schemaConfig;
    if (schemaConfig != SchemaConfig.OLD_SCHEMA_ONLY) {
      _localAccess = new EbeanLocalAccess<>(server, serverConfig, urnClass, _urnPathExtractor, _eBeanDAOConfig.isNonDollarVirtualColumnsEnabled());
    }
  }

  private EbeanLocalDAO(@Nonnull Class<ASPECT_UNION> aspectUnionClass, @Nonnull BaseTrackingMetadataEventProducer producer,
      @Nonnull EbeanServer server, @Nonnull ServerConfig serverConfig, @Nonnull Class<URN> urnClass, @Nonnull SchemaConfig schemaConfig,
      @Nonnull BaseTrackingManager trackingManager) {
    this(aspectUnionClass, producer, server, urnClass, trackingManager);
    _schemaConfig = schemaConfig;
    if (schemaConfig != SchemaConfig.OLD_SCHEMA_ONLY) {
      _localAccess = new EbeanLocalAccess<>(server, serverConfig, urnClass, _urnPathExtractor, _eBeanDAOConfig.isNonDollarVirtualColumnsEnabled());
    }
  }

  private EbeanLocalDAO(@Nonnull Class<ASPECT_UNION> aspectUnionClass, @Nonnull BaseTrackingMetadataEventProducer producer,
      @Nonnull EbeanServer server, @Nonnull ServerConfig serverConfig, @Nonnull Class<URN> urnClass, @Nonnull SchemaConfig schemaConfig,
      @Nonnull EBeanDAOConfig ebeanDAOConfig) {
    this(aspectUnionClass, producer, server, urnClass);
    _schemaConfig = schemaConfig;
    if (schemaConfig != SchemaConfig.OLD_SCHEMA_ONLY) {
      _localAccess = new EbeanLocalAccess<>(server, serverConfig, urnClass, _urnPathExtractor, ebeanDAOConfig.isNonDollarVirtualColumnsEnabled());
    }
  }

  private EbeanLocalDAO(@Nonnull Class<ASPECT_UNION> aspectUnionClass, @Nonnull BaseMetadataEventProducer producer,
      @Nonnull EbeanServer server, @Nonnull ServerConfig serverConfig, @Nonnull Class<URN> urnClass, @Nonnull FindMethodology findMethodology) {
    this(aspectUnionClass, producer, server, urnClass);
    _findMethodology = findMethodology;
  }

  private EbeanLocalDAO(@Nonnull Class<ASPECT_UNION> aspectUnionClass,
      @Nonnull BaseTrackingMetadataEventProducer producer, @Nonnull EbeanServer server,
      @Nonnull ServerConfig serverConfig, @Nonnull Class<URN> urnClass, @Nonnull FindMethodology findMethodology,
      @Nonnull BaseTrackingManager trackingManager) {
    this(aspectUnionClass, producer, server, urnClass, trackingManager);
    _findMethodology = findMethodology;
  }

  // Only called in testing (test all possible combos of SchemaConfig, FindMethodology)
  @VisibleForTesting
  EbeanLocalDAO(@Nonnull Class<ASPECT_UNION> aspectUnionClass, @Nonnull BaseMetadataEventProducer producer,
      @Nonnull EbeanServer server, @Nonnull ServerConfig serverConfig, @Nonnull Class<URN> urnClass,
      @Nonnull SchemaConfig schemaConfig,
      @Nonnull FindMethodology findMethodology, @Nonnull EBeanDAOConfig ebeanDAOConfig) {
    this(aspectUnionClass, producer, server, serverConfig, urnClass, schemaConfig);
    _findMethodology = findMethodology;
    if (schemaConfig != SchemaConfig.OLD_SCHEMA_ONLY) {
      _localAccess = new EbeanLocalAccess<>(server, serverConfig, urnClass, _urnPathExtractor, ebeanDAOConfig.isNonDollarVirtualColumnsEnabled());
    }
  }

  @VisibleForTesting
  EbeanLocalDAO(@Nonnull BaseMetadataEventProducer producer, @Nonnull EbeanServer server,
      @Nonnull LocalDAOStorageConfig storageConfig, @Nonnull Class<URN> urnClass,
      @Nonnull UrnPathExtractor<URN> urnPathExtractor) {
    super(producer, storageConfig, urnClass, urnPathExtractor);
    _server = server;
    _urnClass = urnClass;
    _localRelationshipWriterDAO = new EbeanLocalRelationshipWriterDAO(_server);
  }

  private EbeanLocalDAO(@Nonnull BaseTrackingMetadataEventProducer producer, @Nonnull EbeanServer server,
      @Nonnull LocalDAOStorageConfig storageConfig, @Nonnull Class<URN> urnClass,
      @Nonnull UrnPathExtractor<URN> urnPathExtractor, @Nonnull BaseTrackingManager trackingManager) {
    super(producer, storageConfig, trackingManager, urnClass, urnPathExtractor);
    _server = server;
    _urnClass = urnClass;
    _localRelationshipWriterDAO = new EbeanLocalRelationshipWriterDAO(_server);
  }

  private EbeanLocalDAO(@Nonnull BaseMetadataEventProducer producer, @Nonnull EbeanServer server,
      @Nonnull ServerConfig serverConfig, @Nonnull LocalDAOStorageConfig storageConfig, @Nonnull Class<URN> urnClass,
      @Nonnull UrnPathExtractor<URN> urnPathExtractor, @Nonnull SchemaConfig schemaConfig) {
    this(producer, server, storageConfig, urnClass, urnPathExtractor);
    _schemaConfig = schemaConfig;
    if (schemaConfig != SchemaConfig.OLD_SCHEMA_ONLY) {
      _localAccess = new EbeanLocalAccess<>(server, serverConfig, urnClass, urnPathExtractor, _eBeanDAOConfig.isNonDollarVirtualColumnsEnabled());
    }
  }

  private EbeanLocalDAO(@Nonnull BaseTrackingMetadataEventProducer producer, @Nonnull EbeanServer server, @Nonnull ServerConfig serverConfig,
      @Nonnull LocalDAOStorageConfig storageConfig, @Nonnull Class<URN> urnClass, @Nonnull UrnPathExtractor<URN> urnPathExtractor,
      @Nonnull SchemaConfig schemaConfig, @Nonnull BaseTrackingManager trackingManager) {
    this(producer, server, storageConfig, urnClass, urnPathExtractor, trackingManager);
    _schemaConfig = schemaConfig;
    if (schemaConfig != SchemaConfig.OLD_SCHEMA_ONLY) {
      _localAccess = new EbeanLocalAccess<>(server, serverConfig, urnClass, urnPathExtractor, _eBeanDAOConfig.isNonDollarVirtualColumnsEnabled());
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

  public void setUrnPathExtractor(@Nonnull UrnPathExtractor<URN> urnPathExtractor) {
    if (_schemaConfig != SchemaConfig.OLD_SCHEMA_ONLY) {
      _localAccess.setUrnPathExtractor(urnPathExtractor);
    }
    _urnPathExtractor = urnPathExtractor;
  }

  @Nonnull
  public UrnPathExtractor<URN> getUrnPathExtractor() {
    return _urnPathExtractor;
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
   * Overwride schema config, unit-test only.
   * @param schemaConfig schema config
   */
  void setSchemaConfig(SchemaConfig schemaConfig) {
    _schemaConfig = schemaConfig;
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
      @Nonnull AuditStamp newAuditStamp, boolean isSoftDeleted, @Nullable IngestionTrackingContext trackingContext,
      boolean isTestMode) {
    // Save oldValue as the largest version + 1
    long largestVersion = 0;
    if ((isSoftDeleted || oldValue != null) && oldAuditStamp != null && _changeLogEnabled) {
      // When saving on entity which has history version (including being soft deleted), and changeLog is enabled,
      // the saveLatest will process the following steps:

      // 1. get the next version from the metadata_aspect table
      // 2. write value of latest version (version = 0) as a new version
      // 3. update the latest version (version = 0) with the new value. If the value of latest version has been
      //    changed during this process, then rollback by throwing OptimisticLockException
      largestVersion = getNextVersion(urn, aspectClass);
      // TODO(yanyang) added for job-gms duplicity debug, throwaway afterwards
      if (log.isDebugEnabled()) {
        if ("AzkabanFlowInfo".equals(aspectClass.getSimpleName())) {
          log.debug("Insert: {} => oldValue = {}, latest version = {}", urn, oldValue, largestVersion);
        }
      }
      // Move latest version to historical version by insert a new record only if we are not overwriting the latest version.
      if (!_overwriteLatestVersionEnabled) {
        insert(urn, oldValue, aspectClass, oldAuditStamp, largestVersion, trackingContext, isTestMode);
      }
      // update latest version
      updateWithOptimisticLocking(urn, newValue, aspectClass, newAuditStamp, LATEST_VERSION,
          new Timestamp(oldAuditStamp.getTime()), trackingContext, isTestMode);
    } else {
      // When for fresh ingestion or with changeLog disabled
      // TODO(yanyang) added for job-gms duplicity debug, throwaway afterwards
      if (log.isDebugEnabled()) {
        if ("AzkabanFlowInfo".equals(aspectClass.getSimpleName())) {
          log.debug("Insert: {} => newValue = {}", urn, newValue);
        }
      }

      insert(urn, newValue, aspectClass, newAuditStamp, LATEST_VERSION, trackingContext, isTestMode);
    }

    // This method will handle relationship ingestions and soft-deletions
    handleRelationshipIngestion(urn, newValue, oldValue, aspectClass, isTestMode);

    return largestVersion;
  }

  /**
   * Insert a new aspect record into the metadata_aspect table.
   *
   * @param urn                 entity urn
   * @param aspectCreateLambdas aspect create lambdas
   * @param aspectValues        aspect values
   * @param newAuditStamp       audit stamp
   * @param trackingContext     tracking context
   * @param isTestMode          test mode
   * @param <ASPECT_UNION>     aspect union type
   * @return the number of rows inserted
   */
  @Override
  protected <ASPECT_UNION extends RecordTemplate> int createNewAspect(@Nonnull URN urn,
      @Nonnull List<AspectCreateLambda<? extends RecordTemplate>> aspectCreateLambdas,
      @Nonnull List<? extends RecordTemplate> aspectValues, @Nonnull AuditStamp newAuditStamp,
      @Nullable IngestionTrackingContext trackingContext, boolean isTestMode) {
    return runInTransactionWithRetry(() ->
        _localAccess.create(urn, aspectValues, aspectCreateLambdas, newAuditStamp, trackingContext, isTestMode), 1);
  }

  @Override
  public <ASPECT extends RecordTemplate> void updateEntityTables(@Nonnull URN urn, @Nonnull Class<ASPECT> aspectClass) {
    if (_schemaConfig == SchemaConfig.OLD_SCHEMA_ONLY) {
      throw new UnsupportedOperationException("Entity tables cannot be used in OLD_SCHEMA_ONLY mode, so they cannot be backfilled.");
    }
    PrimaryKey key = new PrimaryKey(urn.toString(), aspectClass.getCanonicalName(), LATEST_VERSION);
    runInTransactionWithRetry(() -> {
      // use forUpdate() to lock the row during this transaction so that we can guarantee a consistent update.
      // order by createdon desc to get the latest value in the case where there are multiple results
      EbeanMetadataAspect result = _server.createQuery(EbeanMetadataAspect.class).setId(key).orderBy().desc("createdon").forUpdate().findOne();
      if (result == null) {
        return null; // unused
      }
      AuditStamp auditStamp = makeAuditStamp(result);
      ASPECT aspect = toRecordTemplate(aspectClass, result).orElse(null);
      _localAccess.add(urn, aspect, aspectClass, auditStamp, null, false);

      // also insert any relationships associated with this aspect
      handleRelationshipIngestion(urn, aspect, null, aspectClass, false);
      return null; // unused
    }, 1);
  }

  public <ASPECT extends RecordTemplate> List<LocalRelationshipUpdates> backfillLocalRelationships(
      @Nonnull URN urn, @Nonnull Class<ASPECT> aspectClass) {
    AspectKey<URN, ASPECT> key = new AspectKey<>(aspectClass, urn, LATEST_VERSION);
    return runInTransactionWithRetry(() -> {
          List<EbeanMetadataAspect> results = batchGet(Collections.singleton(key), 1);
      if (results.size() == 0) {
        return new ArrayList<>();
      }
      Optional<ASPECT> aspect = toRecordTemplate(aspectClass, results.get(0));
      if (aspect.isPresent()) {
        return handleRelationshipIngestion(urn, aspect.get(), null, aspectClass, false);
      }
      return Collections.emptyList();
    }, 1);
  }

  /**
   * Get latest metadata aspect record by urn and aspect.
   * @param urn entity urn
   * @param aspectClass aspect class
   * @param <ASPECT> aspect type
   * @return metadata aspect ebean model {@link EbeanMetadataAspect}
   */
  private @Nullable <ASPECT extends RecordTemplate> EbeanMetadataAspect queryLatest(@Nonnull URN urn,
      @Nonnull Class<ASPECT> aspectClass, boolean isTestMode) {

    EbeanMetadataAspect result;
    if (_schemaConfig == SchemaConfig.OLD_SCHEMA_ONLY || _schemaConfig == SchemaConfig.DUAL_SCHEMA) {
      final String aspectName = ModelUtils.getAspectName(aspectClass);
      final PrimaryKey key = new PrimaryKey(urn.toString(), aspectName, LATEST_VERSION);
      if (_findMethodology == FindMethodology.DIRECT_SQL) {
        result = findLatestMetadataAspect(_server, urn, aspectClass);
        if (result == null) {
          // Attempt 1: retry
          result = _server.find(EbeanMetadataAspect.class, key);
          if (log.isDebugEnabled()) {
            log.debug("Attempt 1: Retried on {}, {}", urn, result);
          }
        }
      } else {
        result = _server.find(EbeanMetadataAspect.class, key);
      }
    } else {
      // for new schema, get latest data from the new schema entity table. (Resolving the read de-coupling issue)
      final List<EbeanMetadataAspect> results =
          _localAccess.batchGetUnion(Collections.singletonList(new AspectKey<>(aspectClass, urn, LATEST_VERSION)), 1, 0,
              true, isTestMode);
      result = results.isEmpty() ? null : results.get(0);
    }
    return result;
  }

  @Override
  @Nonnull
  protected <ASPECT extends RecordTemplate> AspectEntry<ASPECT> getLatest(@Nonnull URN urn,
      @Nonnull Class<ASPECT> aspectClass, boolean isTestMode) {
    EbeanMetadataAspect latest = queryLatest(urn, aspectClass, isTestMode);
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

  // Build manual SQL update query to enable optimistic locking on a given column
  // Optimistic locking is supported on ebean using @version, see https://ebean.io/docs/mapping/jpa/version
  // But we can't use @version annotation for optimistic locking for two reasons:
  //   1. That prevents flag guarding optimistic locking feature
  //   2. When using @version annotation, Ebean starts to override all updates to that column
  //      by disregarding any user change.
  // Ideally, another column for the sake of optimistic locking would be preferred but that means a change to
  // metadata_aspect schema and we don't take this route here to keep this change backward compatible.
  private static final String OPTIMISTIC_LOCKING_UPDATE_SQL = "UPDATE metadata_aspect "
      + "SET urn = :urn, aspect = :aspect, version = :version, metadata = :metadata, createdOn = :createdOn, createdBy = :createdBy "
      + "WHERE urn = :urn and aspect = :aspect and version = :version and createdOn = :oldTimestamp";

  /**
   * Assembly SQL UPDATE script for old Schema.
   * @param aspect {@link EbeanMetadataAspect}
   * @param oldTimestamp old timestamp.The generated SQL will use optimistic locking and do compare-and-set
   *                     with oldTimestamp during the update.
   * @return {@link SqlUpdate} for SQL update execution
   */
  private SqlUpdate assembleOldSchemaSqlUpdate(@Nonnull EbeanMetadataAspect aspect, @Nonnull Timestamp oldTimestamp) {
    final SqlUpdate oldSchemaSqlUpdate = _server.createSqlUpdate(OPTIMISTIC_LOCKING_UPDATE_SQL);
    oldSchemaSqlUpdate.setParameter("oldTimestamp", oldTimestamp);
    oldSchemaSqlUpdate.setParameter("urn", aspect.getKey().getUrn());
    oldSchemaSqlUpdate.setParameter("aspect", aspect.getKey().getAspect());
    oldSchemaSqlUpdate.setParameter("version", aspect.getKey().getVersion());
    oldSchemaSqlUpdate.setParameter("metadata", aspect.getMetadata());
    oldSchemaSqlUpdate.setParameter("createdOn", aspect.getCreatedOn());
    oldSchemaSqlUpdate.setParameter("createdBy", aspect.getCreatedBy());
    return oldSchemaSqlUpdate;
  }

  @VisibleForTesting
  @Override
  protected <ASPECT extends RecordTemplate> void updateWithOptimisticLocking(@Nonnull URN urn,
      @Nullable RecordTemplate value, @Nonnull Class<ASPECT> aspectClass, @Nonnull AuditStamp newAuditStamp,
      long version, @Nonnull Timestamp oldTimestamp, @Nullable IngestionTrackingContext trackingContext,
      boolean isTestMode) {

    final EbeanMetadataAspect aspect = buildMetadataAspectBean(urn, value, aspectClass, newAuditStamp, version);

    if (!_changeLogEnabled) {
      throw new UnsupportedOperationException(
          String.format("updateWithOptimisticLocking should not be called when changeLog is disabled: %s", aspect));
    }

    int numOfUpdatedRows;
    // ensure atomicity by running old schema update + new schema update in a transaction

    final SqlUpdate oldSchemaSqlUpdate;
    if (_schemaConfig == SchemaConfig.NEW_SCHEMA_ONLY) {
      // In NEW_SCHEMA, the entity table is the SOT and getLatest (oldTimestamp) reads from the entity
      // table. Therefore, we will apply compare-and-set with oldTimestamp on entity table (addWithOptimisticLocking).
      // We will also apply an optimistic locking update over (urn, aspect, version) primary key combination to avoid duplicate
      // key exceptions when the primary key includes createdon.
      EbeanMetadataAspect result = findLatestMetadataAspect(_server, urn, aspectClass);
      if (result == null) {
        throw new IllegalStateException("No entry from aspect table found even though one was expected. Urn: " + urn + ", Aspect class:" + aspectClass);
      }
      oldSchemaSqlUpdate = assembleOldSchemaSqlUpdate(aspect, result.getCreatedOn());
      numOfUpdatedRows = runInTransactionWithRetry(() -> {
        // DUAL WRITE: 1) update aspect table, 2) update entity table.
        // Note: when cold-archive is enabled, this method: updateWithOptimisticLocking will not be called.
        _server.execute(oldSchemaSqlUpdate);
        return _localAccess.addWithOptimisticLocking(urn, (ASPECT) value, aspectClass, newAuditStamp, oldTimestamp,
            trackingContext, isTestMode);
      }, 1);
    } else {
      // In OLD_SCHEMA and DUAL_SCHEMA mode, the aspect table is the SOT and the getLatest (oldTimestamp) is from the aspect table.
      // Therefore, we will apply compare-and-set with oldTimestamp on aspect table (assemblyOldSchemaSqlUpdate)
      oldSchemaSqlUpdate = assembleOldSchemaSqlUpdate(aspect, oldTimestamp);
      numOfUpdatedRows = runInTransactionWithRetry(() -> {
        // Additionally, in DUAL_SCHEMA mode: apply a regular update (no optimistic locking) to the entity table
        if (_schemaConfig == SchemaConfig.DUAL_SCHEMA) {
          _localAccess.addWithOptimisticLocking(urn, (ASPECT) value, aspectClass, newAuditStamp, null,
              trackingContext, isTestMode);
        }
        return _server.execute(oldSchemaSqlUpdate);
      }, 1);
    }
    // If there is no single updated row, emit OptimisticLockException
    if (numOfUpdatedRows != 1) {
      throw new OptimisticLockException(
          String.format("%s rows updated during update on update: %s.", numOfUpdatedRows, aspect));
    }
  }

  @Override
  protected <ASPECT extends RecordTemplate> void insert(@Nonnull URN urn, @Nullable RecordTemplate value,
      @Nonnull Class<ASPECT> aspectClass, @Nonnull AuditStamp auditStamp, long version,
      @Nullable IngestionTrackingContext trackingContext, boolean isTestMode) {
    final EbeanMetadataAspect aspect = buildMetadataAspectBean(urn, value, aspectClass, auditStamp, version);
    if (_schemaConfig != SchemaConfig.OLD_SCHEMA_ONLY && version == LATEST_VERSION) {
      // insert() could be called when updating log table (moving current versions into new history version)
      // the metadata entity tables shouldn't been updated.
      _localAccess.add(urn, (ASPECT) value, aspectClass, auditStamp, trackingContext, isTestMode);
    }

    // DO append change log table (metadata_aspect) if:
    //   1. explicitly enabled
    //   AND
    //   2. if NOT in test mode
    //      -> which is ALWAYS a dual-write operation (meaning this insertion will already happen in the "other" write)
    if (_changeLogEnabled && !isTestMode) {
      try {
        _server.insert(aspect);
      } catch (Exception e) {
        if (e.getMessage() != null && e.getMessage().contains("Duplicate entry")) {
          // silently fail and log the error
          log.warn("Insert to metadata_aspect failed due to duplicate entry exception. Exception: {}", e.toString());
        } else {
          throw e;
        }
      }
    }
  }

  /**
   * If the aspect is associated with at least one relationship, upsert the relationship into the corresponding local
   * relationship table. Associated means that the aspect has a registered relationship build or it includes a relationship field.
   * If the new value is null and the old value exists, the aspect is being soft-deleted so remove any existing relationships
   * associated with that aspect.
   * It will first try to find a registered relationship builder; if one doesn't exist or returns no relationship updates,
   * try to find relationships from the aspect itself.
   * @param urn Urn of the metadata update
   * @param newValue new value of the aspect
   * @param oldValue previous value of the aspect
   * @param aspectClass Aspect class of the metadata update
   * @param isTestMode Whether the test mode is enabled or not
   * @return List of LocalRelationshipUpdates that were executed, or an empty list if soft-deleting relationships only
   */
  public <ASPECT extends RecordTemplate, RELATIONSHIP extends RecordTemplate> List<LocalRelationshipUpdates> handleRelationshipIngestion(
      @Nonnull URN urn, @Nullable ASPECT newValue, @Nullable ASPECT oldValue, @Nonnull Class<ASPECT> aspectClass, boolean isTestMode) {
    // Check if we're soft deleting newValue, which means we need to remove any relationships derived from oldValue
    boolean isSoftDeletion = false;
    if (newValue == null) {
      if (oldValue == null) {
        return Collections.emptyList();
      }
      isSoftDeletion = true;
    }

    // Get the relationships associated with the aspect. from newValue if inserting, from oldValue if soft-deleting.
    ASPECT aspect = isSoftDeletion ? oldValue : newValue;
    List<LocalRelationshipUpdates> localRelationshipUpdates = Collections.emptyList();
    // Try to get relationships using relationship builders first. If there is not a relationship builder registered
    // for the aspect class, try to get relationships from the aspect metadata instead. After most relationship models
    // are using Model 2.0, switch the priority i.e. try to get the relationship from the aspect first before falling back
    // on relationship builders.
    // TODO: fix the gap where users can define new relationships in the aspect while still using graph builders to extract existing relationships
    if (_localRelationshipBuilderRegistry != null && _localRelationshipBuilderRegistry.isRegistered(aspectClass)) {
      localRelationshipUpdates = _localRelationshipBuilderRegistry.getLocalRelationshipBuilder(aspect).buildRelationships(urn, aspect);
      // default all relationship updates to use REMOVE_ALL_EDGES_FROM_SOURCE
      localRelationshipUpdates.forEach(update -> update.setRemovalOption(BaseGraphWriterDAO.RemovalOption.REMOVE_ALL_EDGES_FROM_SOURCE));
    }
    // If no relationship updates were found using relationship builders, try to get them via the aspect.
    if (localRelationshipUpdates.isEmpty()) {
      Map<Class<?>, Set<RELATIONSHIP>> allRelationships = EBeanDAOUtils.extractRelationshipsFromAspect(aspect);
      localRelationshipUpdates = allRelationships.entrySet().stream()
          .filter(entry -> !entry.getValue().isEmpty()) // ensure at least 1 relationship in sublist to avoid index out of bounds
          .map(entry -> new LocalRelationshipUpdates(
              Arrays.asList(entry.getValue().toArray()), entry.getKey(), BaseGraphWriterDAO.RemovalOption.REMOVE_ALL_EDGES_FROM_SOURCE))
          .collect(Collectors.toList());
    }
    // process relationship soft-deletion if applicable
    if (isSoftDeletion) {
      List<RELATIONSHIP> relationships = new ArrayList<>();
      localRelationshipUpdates.forEach(localRelationshipUpdate -> relationships.addAll(localRelationshipUpdate.getRelationships()));
      _localRelationshipWriterDAO.removeRelationships(urn, aspectClass, relationships);
      return Collections.emptyList();
    }
    // process relationship ingestion
    _localRelationshipWriterDAO.processLocalRelationshipUpdates(urn, aspectClass, localRelationshipUpdates, isTestMode);
    return localRelationshipUpdates;
  }

  @Nonnull
  Map<Class<? extends RecordTemplate>, LocalDAOStorageConfig.AspectStorageConfig> getStrongConsistentIndexPaths() {
    return Collections.unmodifiableMap(new HashMap<>(_storageConfig.getAspectStorageConfigMap()));
  }

  @Override
  protected <ASPECT extends RecordTemplate> long getNextVersion(@Nonnull URN urn, @Nonnull Class<ASPECT> aspectClass) {
    if (!_changeLogEnabled) {
      throw new UnsupportedOperationException("getNextVersion shouldn't be called when changeLog is disabled");
    } else {
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
  }

  @Override
  protected <ASPECT extends RecordTemplate> void applyVersionBasedRetention(@Nonnull Class<ASPECT> aspectClass,
      @Nonnull URN urn, @Nonnull VersionBasedRetention retention, long largestVersion) {
    if (_changeLogEnabled) {
      // only apply version based retention when changeLog is enabled
      _server.find(EbeanMetadataAspect.class)
          .where()
          .eq(URN_COLUMN, urn.toString())
          .eq(ASPECT_COLUMN, ModelUtils.getAspectName(aspectClass))
          .ne(VERSION_COLUMN, LATEST_VERSION)
          .le(VERSION_COLUMN, largestVersion - retention.getMaxVersionsToRetain() + 1)
          .delete();
    }
  }

  @Override
  protected <ASPECT extends RecordTemplate> void applyTimeBasedRetention(@Nonnull Class<ASPECT> aspectClass,
      @Nonnull URN urn, @Nonnull TimeBasedRetention retention, long currentTime) {
    if (_changeLogEnabled) {
      // only apply time based retention when changeLog is enabled
      _server.find(EbeanMetadataAspect.class)
          .where()
          .eq(URN_COLUMN, urn.toString())
          .eq(ASPECT_COLUMN, ModelUtils.getAspectName(aspectClass))
          .lt(CREATED_ON_COLUMN, new Timestamp(currentTime - retention.getMaxAgeToRetain()))
          .delete();
    }
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
    final List<EbeanMetadataAspect> records;
    if (_queryKeysCount == 0) {
      records = batchGet(keys, keys.size());
    } else {
      records = batchGet(keys, _queryKeysCount);
    }
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

  /**
   * Sets the max keys allowed for each single query, not allowed more than the default batch size.
   */
  public void setQueryKeysCount(int keysCount) {
    if (keysCount < 0) {
      throw new IllegalArgumentException("Query keys count must be non-negative: " + keysCount);
    } else if (keysCount > DEFAULT_BATCH_SIZE) {
      log.warn("Setting query keys count greater than " + DEFAULT_BATCH_SIZE
          + " may cause performance issues. Defaulting to " + DEFAULT_BATCH_SIZE + ".");
      _queryKeysCount = DEFAULT_BATCH_SIZE;
    } else {
      _queryKeysCount = keysCount;
    }
  }

  /**
   * Provide a local relationship builder registry. Local relationships will be built based on the builders during data ingestion.
   * If set to null, local relationship ingestion will be turned off for this particular DAO instance. This is beneficial
   * in situations where some relationships are still in the process of onboarding (i.e. tables have not been created yet).
   * @param localRelationshipBuilderRegistry All local relationship builders should be registered in this registry.
   *                                         Can be set to null to turn off local relationship ingestion.
   */
  public void setLocalRelationshipBuilderRegistry(@Nullable LocalRelationshipBuilderRegistry localRelationshipBuilderRegistry) {
    _localRelationshipBuilderRegistry = localRelationshipBuilderRegistry;
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

    return String.format("(SELECT t.urn, t.aspect, t.version, t.metadata, t.createdOn, t.createdBy, t.createdFor "
            + "FROM %s t WHERE urn = ? AND aspect = ? AND version = ? ORDER BY t.createdOn DESC LIMIT 1)",
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
  @SuppressWarnings({"checkstyle:FallThrough", "checkstyle:DefaultComesLast"})
  List<EbeanMetadataAspect> batchGetHelper(@Nonnull List<AspectKey<URN, ? extends RecordTemplate>> keys,
      int keysCount, int position) {

    boolean nonLatestVersionFlag = keys.stream().anyMatch(key -> key.getVersion() != LATEST_VERSION);

    if (nonLatestVersionFlag || _schemaConfig == SchemaConfig.OLD_SCHEMA_ONLY) {
      return batchGetUnion(keys, keysCount, position);
    }

    if (_schemaConfig == SchemaConfig.NEW_SCHEMA_ONLY) {
      return _localAccess.batchGetUnion(keys, keysCount, position, false, false);
    }

    if (_schemaConfig == SchemaConfig.DUAL_SCHEMA) {
      // Compare results from both new and old schemas
      final List<EbeanMetadataAspect> resultsOldSchema = batchGetUnion(keys, keysCount, position);
      final List<EbeanMetadataAspect> resultsNewSchema =
          _localAccess.batchGetUnion(keys, keysCount, position, false, false);
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
    if (_changeLogEnabled) {
      PagedList<EbeanMetadataAspect> pagedList = _server.find(EbeanMetadataAspect.class)
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
    } else {
      ListResult<ASPECT> aspectListResult = _localAccess.list(aspectClass, urn, start, pageSize);
      return transformListResult(aspectListResult, aspect -> LATEST_VERSION);
    }
  }

  @Override
  @Nonnull
  public <ASPECT extends RecordTemplate> ListResult<URN> listUrns(@Nonnull Class<ASPECT> aspectClass, int start,
      int pageSize) {
    if (_schemaConfig != SchemaConfig.OLD_SCHEMA_ONLY) {
      // decouple from old schema
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

    final List<URN> urns = pagedList.getList().stream().map(entry -> getUrn(entry.getKey().getUrn())).collect(Collectors.toList());
    return toListResult(urns, null, pagedList, start);
  }

  /*
    * List all URNs of the entity registered in the DAO, paginated by last urn.
    *
    * This method is old schema mode use only. Seek alternatives in other schemas.
   */
  @Deprecated
  protected List<URN> listUrnsPaginatedByLastUrn(@Nullable URN lastUrn, int pageSize) {
    if (_schemaConfig != SchemaConfig.OLD_SCHEMA_ONLY) {
      throw new UnsupportedOperationException("this method is only allowed in OLD_SCHEMA_ONLY mode");
    }

    final String query = getDistinctUrnsOfEntitySqlQuery(lastUrn, pageSize);
    final List<SqlRow> sqlRows = _server.createSqlQuery(query).setFirstRow(0).findList();
    return sqlRows.stream().map(sqlRow -> getUrn(sqlRow.getString(URN_COLUMN))).collect(Collectors.toList());
  }

  private String getDistinctUrnsOfEntitySqlQuery(URN lastUrn, int pageSize) {
    final String entityType = ModelUtils.getEntityTypeFromUrnClass(_urnClass);
    final String entityUrnPrefix = "urn:li:" + entityType + ":%";

    StringBuilder sb = new StringBuilder();
    sb.append(String.format("SELECT DISTINCT(%s) FROM metadata_aspect ", URN_COLUMN));
    sb.append(String.format("WHERE %s LIKE '%s' ", URN_COLUMN, entityUrnPrefix));
    sb.append(String.format("AND version = %d ", LATEST_VERSION));
    sb.append(String.format("AND metadata != '%s' ", DELETED_VALUE));
    if (lastUrn != null) {
      sb.append(String.format("AND %s > '%s' ", URN_COLUMN, lastUrn));
    }
    sb.append(String.format("ORDER BY %s asc ", URN_COLUMN));
    sb.append(String.format("LIMIT %d ", pageSize));
    return sb.toString();
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
    PagedList<EbeanMetadataAspect> pagedList;
    if (_changeLogEnabled) {
      pagedList = _server.find(EbeanMetadataAspect.class)
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
    } else {
      // if changeLog is disabled, then list all the non-null,
      // non-solft deleted, version-0) aspects from new schema table
      return _localAccess.list(aspectClass, urn, start, pageSize);
    }
  }

  @Override
  @Nonnull
  public <ASPECT extends RecordTemplate> ListResult<ASPECT> list(@Nonnull Class<ASPECT> aspectClass, long version,
      int start, int pageSize) {

    checkValidAspect(aspectClass);

    if (_changeLogEnabled) {

      PagedList<EbeanMetadataAspect> pagedList = _server.find(EbeanMetadataAspect.class)
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
    } else {
      if (version != LATEST_VERSION) {
        throw new UnsupportedOperationException(
            "non-current version based list is not supported when ChangeLog is disabled");
      }

      return _localAccess.list(aspectClass, start, pageSize);
    }
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

  /**
   * Transform list result from type T to type R.
   * @param listResult input list result
   * @param function transform function
   * @param <T> input data type
   * @param <R> output data type
   * @return ListResult of type R
   */
  @Nonnull
  public static  <T, R> ListResult<R> transformListResult(@Nonnull ListResult<T> listResult,
      @Nonnull Function<T, R> function) {
    List<R> values = listResult.getValues().stream().map(function).collect(Collectors.toList());
    return ListResult.<R>builder().values(values)
        .metadata(listResult.getMetadata())
        .nextStart(listResult.getNextStart())
        .havingMore(listResult.isHavingMore())
        .totalCount(listResult.getTotalCount())
        .totalPageCount(listResult.getTotalPageCount())
        .pageSize(listResult.getPageSize())
        .build();
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
    extraInfo.setEmitTime(aspect.getEmitTime(), SetMode.IGNORE_NULL);
    extraInfo.setEmitter(aspect.getEmitter(), SetMode.IGNORE_NULL);
    try {
      extraInfo.setUrn(Urn.createFromString(aspect.getKey().getUrn()));
    } catch (URISyntaxException e) {
      throw new ModelConversionException(e.getMessage());
    }

    return extraInfo;
  }

  @Nonnull
  static AuditStamp makeAuditStamp(@Nonnull Timestamp timestamp, @Nonnull String actor, @Nullable String impersonator) {
    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(timestamp.getTime());
    try {
      auditStamp.setActor(new Urn(actor));
      if (impersonator != null) {
        auditStamp.setImpersonator(new Urn(impersonator));
      }
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    return auditStamp;
  }

  @Nonnull
  static AuditStamp makeAuditStamp(@Nonnull EbeanMetadataAspect aspect) {
    return makeAuditStamp(aspect.getCreatedOn(), aspect.getCreatedBy(), aspect.getCreatedFor());
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

  /**
   * Returns list of urns that satisfy the given filter conditions.
   *
   * <p>Results are ordered by the sort criterion but defaults to sorting lexicographically by the string representation of the URN.
   *
   * <p>NOTE: Currently this works for upto 10 filter conditions.
   *
   * <p>This method can only be used in old schema mode if the provided indexFilter and indexSortCriterion are null.
   *
   * @param indexFilter {@link IndexFilter} containing filter conditions to be applied
   * @param indexSortCriterion {@link IndexSortCriterion} sorting criteria to be applied
   * @param lastUrn last urn of the previous fetched page. This eliminates the need to use offset which
   *                 is known to slow down performance of MySQL queries. For the first page, this should be set as NULL
   * @param pageSize maximum number of distinct urns to return
   * @return list of urns that satisfy the given filter conditions
   */
  @Override
  @Nonnull
  public List<URN> listUrns(@Nullable IndexFilter indexFilter, @Nullable IndexSortCriterion indexSortCriterion,
      @Nullable URN lastUrn, int pageSize) {
    if (_schemaConfig == SchemaConfig.OLD_SCHEMA_ONLY && !(indexFilter == null && indexSortCriterion == null)) {
      throw new UnsupportedOperationException("listUrns with nonnull index filter is only supported in new schema.");
    }

    if (_schemaConfig == SchemaConfig.OLD_SCHEMA_ONLY) {
      return listUrnsPaginatedByLastUrn(lastUrn, pageSize);
    }
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
  public ListResult<URN> listUrns(@Nullable IndexFilter indexFilter, @Nullable IndexSortCriterion indexSortCriterion,
      int start, int pageSize) {

    if (_schemaConfig == SchemaConfig.OLD_SCHEMA_ONLY) {
      throw new UnsupportedOperationException("listUrns with index filter is only supported in new schema.");
    }

    return _localAccess.listUrns(indexFilter, indexSortCriterion, start, pageSize);
  }

  @Override
  @Nonnull
  public Map<String, Long> countAggregate(@Nullable IndexFilter indexFilter,
      @Nonnull IndexGroupByCriterion indexGroupByCriterion) {

    if (_schemaConfig == SchemaConfig.OLD_SCHEMA_ONLY) {
      throw new UnsupportedOperationException("countAggregate is only supported in new schema.");
    }
    return _localAccess.countAggregate(indexFilter, indexGroupByCriterion);
  }
}