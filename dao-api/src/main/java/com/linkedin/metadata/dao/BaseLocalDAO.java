package com.linkedin.metadata.dao;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.schema.validation.CoercionMode;
import com.linkedin.data.schema.validation.RequiredMode;
import com.linkedin.data.schema.validation.UnrecognizedFieldMode;
import com.linkedin.data.schema.validation.ValidateDataAgainstSchema;
import com.linkedin.data.schema.validation.ValidationOptions;
import com.linkedin.data.schema.validation.ValidationResult;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.metadata.annotations.AspectIngestionAnnotation;
import com.linkedin.metadata.annotations.AspectIngestionAnnotationArray;
import com.linkedin.metadata.annotations.Mode;
import com.linkedin.metadata.annotations.UrnFilter;
import com.linkedin.metadata.annotations.UrnFilterArray;
import com.linkedin.metadata.backfill.BackfillMode;
import com.linkedin.metadata.dao.builder.BaseLocalRelationshipBuilder.LocalRelationshipUpdates;
import com.linkedin.metadata.dao.equality.DefaultEqualityTester;
import com.linkedin.metadata.dao.equality.EqualityTester;
import com.linkedin.metadata.dao.exception.ModelValidationException;
import com.linkedin.metadata.dao.ingestion.BaseLambdaFunction;
import com.linkedin.metadata.dao.ingestion.LambdaFunctionRegistry;
import com.linkedin.metadata.dao.producer.BaseMetadataEventProducer;
import com.linkedin.metadata.dao.producer.BaseTrackingMetadataEventProducer;
import com.linkedin.metadata.dao.retention.IndefiniteRetention;
import com.linkedin.metadata.dao.retention.Retention;
import com.linkedin.metadata.dao.retention.TimeBasedRetention;
import com.linkedin.metadata.dao.retention.VersionBasedRetention;
import com.linkedin.metadata.dao.storage.LocalDAOStorageConfig;
import com.linkedin.metadata.dao.tracking.BaseTrackingManager;
import com.linkedin.metadata.dao.tracking.TrackingUtils;
import com.linkedin.metadata.dao.urnpath.UrnPathExtractor;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.events.IngestionMode;
import com.linkedin.metadata.events.IngestionTrackingContext;
import com.linkedin.metadata.internal.IngestionParams;
import com.linkedin.metadata.query.ExtraInfo;
import com.linkedin.metadata.query.IndexCriterion;
import com.linkedin.metadata.query.IndexCriterionArray;
import com.linkedin.metadata.query.IndexFilter;
import com.linkedin.metadata.query.IndexGroupByCriterion;
import com.linkedin.metadata.query.IndexSortCriterion;
import java.sql.Timestamp;
import java.time.Clock;
import java.util.Collections;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.dao.utils.IngestionUtils.*;
import static com.linkedin.metadata.dao.utils.ModelUtils.*;


/**
 * A base class for all Local DAOs.
 *
 * <p>Local DAO is a standardized interface to store and retrieve aspects from a document store.
 *
 * @param <ASPECT_UNION> must be a valid aspect union type defined in com.linkedin.metadata.aspect
 * @param <URN> must be the entity URN type in {@code ASPECT_UNION}
 */
@Slf4j
public abstract class BaseLocalDAO<ASPECT_UNION extends UnionTemplate, URN extends Urn>
    extends BaseReadDAO<ASPECT_UNION, URN> {

  private final Class<ASPECT_UNION> _aspectUnionClass;

  private final Class<URN> _urnClass;

  /**
   * Immutable class that corresponds to the metadata aspect along with {@link ExtraInfo} for the same metadata. It also
   * has a flag to indicate if this metadata is soft deleted.
   *
   * @param <ASPECT> must be a supported aspect type in {@code ASPECT_UNION}.
   */
  @Data
  @Builder
  @AllArgsConstructor
  static class AspectEntry<ASPECT extends RecordTemplate> {
    @Nullable
    ASPECT aspect;

    @Nullable
    ExtraInfo extraInfo;

    @Builder.Default
    boolean isSoftDeleted = false;

    public AspectEntry(@Nullable ASPECT aspect, @Nullable ExtraInfo extraInfo) {
      this.aspect = aspect;
      this.extraInfo = extraInfo;
      this.isSoftDeleted = false;
    }
  }

  @Value
  static class AddResult<ASPECT extends RecordTemplate> {
    ASPECT oldValue;
    ASPECT newValue;
    Class<ASPECT> klass;
  }

  /**
   * Immutable class to hold the details of an update to an aspect.
   *
   * <p>This class allows the wildcard capture in {@link #addMany(Urn, List, AuditStamp, IngestionTrackingContext)}</p>
   *
   * @param <ASPECT> the type of the aspect being updated
   */
  @AllArgsConstructor
  @Value
  public static class AspectUpdateLambda<ASPECT extends RecordTemplate> {
    @NonNull
    Class<ASPECT> aspectClass;

    @NonNull
    Function<Optional<ASPECT>, ASPECT> updateLambda;

    @NonNull
    IngestionParams ingestionParams;

    AspectUpdateLambda(ASPECT value) {
      this.aspectClass = (Class<ASPECT>) value.getClass();
      this.updateLambda = (ignored) -> value;
      this.ingestionParams = new IngestionParams().setIngestionMode(IngestionMode.LIVE);
    }

    AspectUpdateLambda(@NonNull Class<ASPECT> aspectClass, @NonNull Function<Optional<ASPECT>, ASPECT> updateLambda) {
      this.aspectClass = aspectClass;
      this.updateLambda = updateLambda;
      this.ingestionParams = new IngestionParams().setIngestionMode(IngestionMode.LIVE);
    }
  }

  private static final String DEFAULT_ID_NAMESPACE = "global";

  private static final String BACKFILL_EMITTER = "dao_backfill_endpoint";

  private static final String BASE_SEMANTIC_VERSION = "baseSemanticVersion";

  private static final String MAJOR = "major";

  private static final String MINOR = "minor";

  private static final String PATCH = "patch";

  private static final IndefiniteRetention INDEFINITE_RETENTION = new IndefiniteRetention();

  private static final int DEFAULT_MAX_TRANSACTION_RETRY = 3;

  protected final BaseMetadataEventProducer _producer;
  protected final BaseTrackingMetadataEventProducer _trackingProducer;
  protected final LocalDAOStorageConfig _storageConfig;
  protected final BaseTrackingManager _trackingManager;
  protected UrnPathExtractor<URN> _urnPathExtractor;

  private LambdaFunctionRegistry _lambdaFunctionRegistry;

  // Maps an aspect class to the corresponding retention policy
  private final Map<Class<? extends RecordTemplate>, Retention> _aspectRetentionMap = new HashMap<>();

  // Maps an aspect class to a list of pre-update hooks
  private final Map<Class<? extends RecordTemplate>, List<BiConsumer<Urn, RecordTemplate>>> _aspectPreUpdateHooksMap =
      new HashMap<>();

  // Maps an aspect class to a list of post-update hooks
  private final Map<Class<? extends RecordTemplate>, List<BiConsumer<Urn, RecordTemplate>>> _aspectPostUpdateHooksMap =
      new HashMap<>();

  // Maps an aspect class to the corresponding equality tester
  private final Map<Class<? extends RecordTemplate>, EqualityTester<? extends RecordTemplate>>
      _aspectEqualityTesterMap = new ConcurrentHashMap<>();

  private boolean _modelValidationOnWrite = true;

  // Always emit MAE on every update regardless if there's any actual change in value
  private boolean _alwaysEmitAuditEvent = false;

  private boolean _emitAspectSpecificAuditEvent = false;

  private boolean _alwaysEmitAspectSpecificAuditEvent = false;

  // Enable updating multiple aspects within a single transaction
  private boolean _enableAtomicMultipleUpdate = false;

  private boolean _emitAuditEvent = false;

  private Clock _clock = Clock.systemUTC();

  /**
   * Constructor for BaseLocalDAO.
   *
   * @param aspectUnionClass containing union of all supported aspects. Must be a valid aspect union defined in
   *     com.linkedin.metadata.aspect
   * @param producer {@link BaseMetadataEventProducer} for the metadata event producer
   * @param urnClass class of the URN type
   */
  public BaseLocalDAO(@Nonnull Class<ASPECT_UNION> aspectUnionClass, @Nonnull BaseMetadataEventProducer producer,
      @Nonnull Class<URN> urnClass, @Nonnull UrnPathExtractor<URN> urnPathExtractor) {
    super(aspectUnionClass);
    _producer = producer;
    _storageConfig = LocalDAOStorageConfig.builder().build();
    _aspectUnionClass = aspectUnionClass;
    _trackingManager = null;
    _trackingProducer = null;
    _urnClass = urnClass;
    _urnPathExtractor = urnPathExtractor;
  }

  /**
   * Constructor for BaseLocalDAO.
   *
   * @param aspectUnionClass containing union of all supported aspects. Must be a valid aspect union defined in
   *     com.linkedin.metadata.aspect
   * @param trackingProducer {@link BaseTrackingMetadataEventProducer} for producing metadata events with tracking
   * @param trackingManager {@link BaseTrackingManager} for managing tracking requests
   * @param urnClass class of the URN type
   */
  public BaseLocalDAO(@Nonnull Class<ASPECT_UNION> aspectUnionClass, @Nonnull BaseTrackingMetadataEventProducer trackingProducer,
      @Nonnull BaseTrackingManager trackingManager, @Nonnull Class<URN> urnClass, @Nonnull UrnPathExtractor<URN> urnPathExtractor) {
    super(aspectUnionClass);
    _producer = null;
    _storageConfig = LocalDAOStorageConfig.builder().build();
    _aspectUnionClass = aspectUnionClass;
    _trackingManager = trackingManager;
    _trackingProducer = trackingProducer;
    _urnClass = urnClass;
    _urnPathExtractor = urnPathExtractor;
  }

  /**
   * Constructor for BaseLocalDAO.
   *
   * @param producer {@link BaseMetadataEventProducer} for the metadata event producer
   * @param storageConfig {@link LocalDAOStorageConfig} containing storage config of full list of supported aspects
   * @param urnClass class of the URN type
   */
  public BaseLocalDAO(@Nonnull BaseMetadataEventProducer producer, @Nonnull LocalDAOStorageConfig storageConfig,
      @Nonnull Class<URN> urnClass, @Nonnull UrnPathExtractor<URN> urnPathExtractor) {
    super(storageConfig.getAspectStorageConfigMap().keySet());
    _producer = producer;
    _storageConfig = storageConfig;
    _aspectUnionClass = producer.getAspectUnionClass();
    _trackingManager = null;
    _trackingProducer = null;
    _urnClass = urnClass;
    _urnPathExtractor = urnPathExtractor;
  }

  /**
   * Constructor for BaseLocalDAO.
   *
   * @param trackingProducer {@link BaseTrackingMetadataEventProducer} for producing metadata events with tracking
   * @param storageConfig {@link LocalDAOStorageConfig} containing storage config of full list of supported aspects
   * @param trackingManager {@link BaseTrackingManager} for managing tracking requests
   * @param urnClass class of the URN type
   *
   */
  public BaseLocalDAO(@Nonnull BaseTrackingMetadataEventProducer trackingProducer, @Nonnull LocalDAOStorageConfig storageConfig,
      @Nonnull BaseTrackingManager trackingManager, @Nonnull Class<URN> urnClass, @Nonnull UrnPathExtractor<URN> urnPathExtractor) {
    super(storageConfig.getAspectStorageConfigMap().keySet());
    _producer = null;
    _storageConfig = storageConfig;
    _aspectUnionClass = trackingProducer.getAspectUnionClass();
    _trackingManager = trackingManager;
    _trackingProducer = trackingProducer;
    _urnClass = urnClass;
    _urnPathExtractor = urnPathExtractor;
  }

  /**
   * Get the urn class.
   *
   * @return the urn class
   */
  @Nullable
  public Class<URN> getUrnClass() {
    return this._urnClass;
  }

  /**
   * For tests to override the internal clock.
   */
  public void setClock(@Nonnull Clock clock) {
    _clock = clock;
  }

  /**
   * Sets {@link Retention} for a specific aspect type.
   */
  public <ASPECT extends RecordTemplate> void setRetention(@Nonnull Class<ASPECT> aspectClass,
      @Nonnull Retention retention) {
    checkValidAspect(aspectClass);
    _aspectRetentionMap.put(aspectClass, retention);
  }

  /**
   * Gets the {@link Retention} for an aspect type, or {@link IndefiniteRetention} if none is registered.
   */
  @Nonnull
  public <ASPECT extends RecordTemplate> Retention getRetention(@Nonnull Class<ASPECT> aspectClass) {
    checkValidAspect(aspectClass);
    return _aspectRetentionMap.getOrDefault(aspectClass, INDEFINITE_RETENTION);
  }

  /**
   * Helper function to add pre- and post-update hooks.
   *
   * <p>The hook will be invoked with the latest value of an aspect before (pre-) or after (post-) it's updated. There's
   * no guarantee on the order of invocation when multiple hooks are added for a single aspect. Adding the same hook
   * again will result in {@link IllegalArgumentException} thrown. Hooks are invoked in the order they're registered.
   */
  public <URN extends Urn, ASPECT extends RecordTemplate> void addHook(@Nonnull Class<ASPECT> aspectClass,
      @Nonnull BiConsumer<URN, ASPECT> hook,
      @Nonnull Map<Class<? extends RecordTemplate>, List<BiConsumer<Urn, RecordTemplate>>> hooksMap) {

    checkValidAspect(aspectClass);
    // TODO: Also validate Urn once we convert all aspect models to PDL with proper annotation

    final List<BiConsumer<Urn, RecordTemplate>> hooks =
        hooksMap.getOrDefault(aspectClass, new LinkedList<>());

    if (hooks.contains(hook)) {
      throw new IllegalArgumentException("Adding an already-registered hook");
    }

    hooks.add((BiConsumer<Urn, RecordTemplate>) hook);
    hooksMap.put(aspectClass, hooks);
  }

  /**
   * Add a pre-update hook for a specific aspect type. Warning: pre-update hooks are run within a transaction, so avoid
   * creating time- and/or resource-consuming pre-update hooks.
   */
  public <URN extends Urn, ASPECT extends RecordTemplate> void addPreUpdateHook(@Nonnull Class<ASPECT> aspectClass,
      @Nonnull BiConsumer<URN, ASPECT> preUpdateHook) {
    addHook(aspectClass, preUpdateHook, _aspectPreUpdateHooksMap);
  }

  /**
   * Add a post-update hook for a specific aspect type.
   */
  public <URN extends Urn, ASPECT extends RecordTemplate> void addPostUpdateHook(@Nonnull Class<ASPECT> aspectClass,
      @Nonnull BiConsumer<URN, ASPECT> postUpdateHook) {
    addHook(aspectClass, postUpdateHook, _aspectPostUpdateHooksMap);
  }

  /**
   * Sets the {@link EqualityTester} for a specific aspect type.
   */
  public <ASPECT extends RecordTemplate> void setEqualityTester(@Nonnull Class<ASPECT> aspectClass,
      @Nonnull EqualityTester<ASPECT> tester) {
    checkValidAspect(aspectClass);
    _aspectEqualityTesterMap.put(aspectClass, tester);
  }

  /**
   * Gets the {@link EqualityTester} for an aspect, or {@link DefaultEqualityTester} if none is registered.
   */
  @Nonnull
  public <ASPECT extends RecordTemplate> EqualityTester<ASPECT> getEqualityTester(@Nonnull Class<ASPECT> aspectClass) {
    checkValidAspect(aspectClass);
    return (EqualityTester<ASPECT>) _aspectEqualityTesterMap.computeIfAbsent(aspectClass,
        key -> new DefaultEqualityTester<ASPECT>());
  }

  /**
   * Set lambda function registry.
   */
  public void setLambdaFunctionRegistry(@Nullable LambdaFunctionRegistry lambdaFunctionRegistry) {
    _lambdaFunctionRegistry = lambdaFunctionRegistry;
  }

  /**
   * Enables or disables atomic updates of multiple aspects.
   */
  public void enableAtomicMultipleUpdate(boolean enabled) {
    _enableAtomicMultipleUpdate = enabled;
  }

  /**
   * Enables or disables model validation before persisting.
   */
  public void enableModelValidationOnWrite(boolean enabled) {
    _modelValidationOnWrite = enabled;
  }

  /**
   * Sets if MAE should be always emitted after each update even if there's no actual value change.
   */
  public void setAlwaysEmitAuditEvent(boolean alwaysEmitAuditEvent) {
    _alwaysEmitAuditEvent = alwaysEmitAuditEvent;
  }

  /**
   * Sets if aspect specific MAE should be enabled.
   */
  public void setEmitAspectSpecificAuditEvent(boolean emitAspectSpecificAuditEvent) {
    _emitAspectSpecificAuditEvent = emitAspectSpecificAuditEvent;
  }

  /**
   * Sets if aspect specific MAE should be always emitted after each update even if there's no actual value change.
   */
  public void setAlwaysEmitAspectSpecificAuditEvent(boolean alwaysEmitAspectSpecificAuditEvent) {
    _alwaysEmitAspectSpecificAuditEvent = alwaysEmitAspectSpecificAuditEvent;
  }

  public void setEmitAuditEvent(boolean emitAuditEvent) {
    _emitAuditEvent = emitAuditEvent;
  }


  /**
   * Logic common to both {@link #add(Urn, Class, Function, AuditStamp)} and {@link #delete(Urn, Class, AuditStamp, int)} methods.
   *
   * @param urn urn the URN for the entity the aspect is attached to
   * @param latest {@link AspectEntry} that corresponds to the latest metadata stored
   * @param newValue new metadata that needs to be added/stored
   * @param aspectClass aspectClass of the aspect being saved
   * @param auditStamp audit stamp for the operation
   * @param equalityTester {@link EqualityTester} that is an interface for testing equality between two objects of the same type
   * @param trackingContext {@link IngestionTrackingContext} which contains ingestion metadata used for tracking purposes
   * @param ingestionParams {@link IngestionParams} which indicates how the aspect should be ingested
   * @param <ASPECT> must be a supported aspect type in {@code ASPECT_UNION}
   * @return {@link AddResult} corresponding to the old and new value of metadata
   */
  private <ASPECT extends RecordTemplate> AddResult<ASPECT> addCommon(@Nonnull URN urn,
      @Nonnull AspectEntry<ASPECT> latest, @Nullable ASPECT newValue, @Nonnull Class<ASPECT> aspectClass,
      @Nonnull AuditStamp auditStamp, @Nonnull EqualityTester<ASPECT> equalityTester,
      @Nullable IngestionTrackingContext trackingContext, @Nonnull IngestionParams ingestionParams) {

    final ASPECT oldValue = latest.getAspect() == null ? null : latest.getAspect();
    final AuditStamp oldAuditStamp = latest.getExtraInfo() == null ? null : latest.getExtraInfo().getAudit();
    final Long oldEmitTime = latest.getExtraInfo() == null ? null : latest.getExtraInfo().getEmitTime();

    final boolean isBackfillEvent = trackingContext != null
        && trackingContext.hasBackfill() && trackingContext.isBackfill();
    if (isBackfillEvent) {
      boolean shouldBackfill =
          // new value is being inserted. We should backfill
          oldValue == null
              || (
              // tracking context should ideally always have emitTime. If it's not present, we will skip backfilling
              trackingContext.hasEmitTime()
                  && (
                  // old emit time is available so we'll use it for comparison
                  // if new event emit time > old event emit time, we'll backfill
                  (oldEmitTime != null && trackingContext.getEmitTime() > oldEmitTime)
                      // old emit time is not available, so we'll fall back to comparing new emit time against old audit time
                      // old audit time represents the last modified time of the aspect
                      || (oldEmitTime == null && oldAuditStamp != null && oldAuditStamp.hasTime() && trackingContext.getEmitTime() > oldAuditStamp.getTime())));

      log.info("Encounter backfill event. Old value = null: {}. Tracking context: {}. Urn: {}. Aspect class: {}. Old audit stamp: {}. "
              + "Old emit time: {}. "
              + "Based on this information, shouldBackfill = {}.",
          oldValue == null, trackingContext, urn, aspectClass, oldAuditStamp, oldEmitTime, shouldBackfill);

      if (!shouldBackfill) {
        return new AddResult<>(oldValue, oldValue, aspectClass);
      }
    }

    // TODO(yanyang) added for job-gms duplicity debug, throwaway afterwards
    if (log.isDebugEnabled()) {
      if ("AzkabanFlowInfo".equals(aspectClass.getSimpleName())) {
        log.debug("New Value: {} => {}", urn, newValue);
        log.debug("Old Value: {} => {}", urn, oldValue);
        log.debug("Quality: {} => {}", urn, equalityTester.equals(oldValue, newValue));
      }
    }

    // Logic determines whether an update to aspect should be persisted.
    if (!shouldUpdateAspect(ingestionParams.getIngestionMode(), urn, oldValue, newValue, aspectClass, auditStamp, equalityTester)) {
      return new AddResult<>(oldValue, oldValue, aspectClass);
    }

    // Save the newValue as the latest version
    long largestVersion = saveLatest(urn, aspectClass, oldValue, oldAuditStamp, newValue, auditStamp, latest.isSoftDeleted, trackingContext);

    // Apply retention policy
    applyRetention(urn, aspectClass, getRetention(aspectClass), largestVersion);

    return new AddResult<>(oldValue, newValue, aspectClass);
  }

  /**
   * Adds a new version of several aspects for an entity.
   *
   * <p>Each new aspect will have an automatically assigned version number, which is guaranteed to be positive and
   * monotonically increasing. Older versions of aspect will be purged automatically based on the retention setting. A
   * MetadataAuditEvent is also emitted if an actual update occurs.</p>
   *
   * <p><b>Important:</b> If {@link #_enableAtomicMultipleUpdate} is true, all updates will occur in a single transaction. Note that
   * pre-update hooks are fired between update statements, meaning that the behavior of which pre-update hooks fire when
   * an update partially fails will depend on the implementation.</p>
   *
   * @param urn the URN of the entity to which the aspects are attached
   * @param aspectUpdateLambdas a list of {@link AspectUpdateLambda} to execute
   * @param auditStamp the audit stamp for the operation
   * @param maxTransactionRetry the maximum number of times to retry the transaction
   * @return a list of the updated aspects, each wrapped in an instance of {@link ASPECT_UNION}
   */
  public List<ASPECT_UNION> addMany(@Nonnull URN urn,
      @Nonnull List<AspectUpdateLambda<? extends RecordTemplate>> aspectUpdateLambdas,
      @Nonnull AuditStamp auditStamp,
      int maxTransactionRetry) {
    return addMany(urn, aspectUpdateLambdas, auditStamp, maxTransactionRetry, null);
  }

  /**
   * Adds a new version of several aspects for an entity.
   *
   * <p>Each new aspect will have an automatically assigned version number, which is guaranteed to be positive and
   * monotonically increasing. Older versions of aspect will be purged automatically based on the retention setting. A
   * MetadataAuditEvent is also emitted if an actual update occurs.</p>
   *
   * <p><b>Important:</b> If {@link #_enableAtomicMultipleUpdate} is true, all updates will occur in a single transaction. Note that
   * pre-update hooks are fired between update statements, meaning that the behavior of which pre-update hooks fire when
   * an update partially fails will depend on the implementation.</p>
   *
   * @param urn the URN of the entity to which the aspects are attached
   * @param aspectUpdateLambdas a list of {@link AspectUpdateLambda} to execute
   * @param auditStamp the audit stamp for the operation
   * @param maxTransactionRetry the maximum number of times to retry the transaction
   * @return a list of the updated aspects, each wrapped in an instance of {@link ASPECT_UNION}
   */
  public List<ASPECT_UNION> addMany(@Nonnull URN urn,
      @Nonnull List<AspectUpdateLambda<? extends RecordTemplate>> aspectUpdateLambdas,
      @Nonnull AuditStamp auditStamp,
      int maxTransactionRetry, @Nullable IngestionTrackingContext trackingContext) {

    // first check that all the aspects are valid
    aspectUpdateLambdas.stream().map(AspectUpdateLambda::getAspectClass).forEach(this::checkValidAspect);

    final List<AddResult<? extends RecordTemplate>> results;
    if (_enableAtomicMultipleUpdate) {
      // atomic multiple update enabled: run in a single transaction
      results = runInTransactionWithRetry(() ->
              aspectUpdateLambdas.stream().map(x -> aspectUpdateHelper(urn, x, auditStamp, trackingContext)).collect(Collectors.toList()),
          maxTransactionRetry);
    } else {
      // no atomic multiple updates: run each in its own transaction. This is the same as repeated calls to add
      results = aspectUpdateLambdas.stream().map(x -> runInTransactionWithRetry(() ->
              aspectUpdateHelper(urn, x, auditStamp, trackingContext), maxTransactionRetry)).collect(Collectors.toList());
    }

    // send the audit events etc
    return results.stream().map(x -> unwrapAddResultToUnion(urn, x, auditStamp, trackingContext)).collect(Collectors.toList());
  }

  public List<ASPECT_UNION> addMany(@Nonnull URN urn, @Nonnull List<? extends RecordTemplate> aspectValues, @Nonnull AuditStamp auditStamp) {
    return addMany(urn, aspectValues, auditStamp, null);
  }

  public List<ASPECT_UNION> addMany(@Nonnull URN urn, @Nonnull List<? extends RecordTemplate> aspectValues, @Nonnull AuditStamp auditStamp,
      @Nullable IngestionTrackingContext trackingContext) {
    List<AspectUpdateLambda<? extends RecordTemplate>> aspectUpdateLambdas = aspectValues.stream()
        .map(AspectUpdateLambda::new)
        .collect(Collectors.toList());

    return addMany(urn, aspectUpdateLambdas, auditStamp, DEFAULT_MAX_TRANSACTION_RETRY, trackingContext);
  }

  private <ASPECT extends RecordTemplate> AddResult<ASPECT> aspectUpdateHelper(URN urn, AspectUpdateLambda<ASPECT> updateTuple,
      @Nonnull AuditStamp auditStamp, @Nullable IngestionTrackingContext trackingContext) {
    AspectEntry<ASPECT> latest = getLatest(urn, updateTuple.getAspectClass());

    // TODO(yanyang) added for job-gms duplicity debug, throwaway afterwards
    if (log.isDebugEnabled()) {
      if ("AzkabanFlowInfo".equals(updateTuple.getAspectClass().getSimpleName())) {
        log.debug("Latest: {} => {}", urn, latest);
      }
    }

    Optional<ASPECT> oldValue = Optional.ofNullable(latest.getAspect());
    ASPECT newValue = updateTuple.getUpdateLambda().apply(oldValue);
    if (newValue == null) {
      throw new UnsupportedOperationException(String.format("Attempted to update %s with null aspect %s", urn, updateTuple.getAspectClass().getName()));
    }

    if (_lambdaFunctionRegistry != null && _lambdaFunctionRegistry.isRegistered(updateTuple.getAspectClass())) {
      newValue = updatePreIngestionLambdas(urn, oldValue, newValue);
    }

    checkValidAspect(newValue.getClass());

    if (_modelValidationOnWrite) {
      validateAgainstSchema(newValue);
    }

    // Invoke pre-update hooks, if any
    if (_aspectPreUpdateHooksMap.containsKey(updateTuple.getAspectClass())) {
      for (final BiConsumer<Urn, RecordTemplate> hook : _aspectPreUpdateHooksMap.get(updateTuple.getAspectClass())) {
        hook.accept(urn, newValue);
      }
    }

    return addCommon(urn, latest, newValue, updateTuple.getAspectClass(), auditStamp, getEqualityTester(updateTuple.getAspectClass()),
        trackingContext, updateTuple.getIngestionParams());
  }

  private <ASPECT extends RecordTemplate> ASPECT_UNION unwrapAddResultToUnion(URN urn, AddResult<ASPECT> result,
      @Nonnull AuditStamp auditStamp, @Nullable IngestionTrackingContext trackingContext) {
    ASPECT rawResult = unwrapAddResult(urn, result, auditStamp, trackingContext);
    return ModelUtils.newEntityUnion(_aspectUnionClass, rawResult);
  }

  private <ASPECT extends RecordTemplate> ASPECT unwrapAddResult(URN urn, AddResult<ASPECT> result, @Nonnull AuditStamp auditStamp,
      @Nullable IngestionTrackingContext trackingContext) {
    if (trackingContext != null) {
      trackingContext.setBackfill(false); // reset backfill since MAE won't be a backfill event
    }

    final Class<ASPECT> aspectClass = result.getKlass();
    final ASPECT oldValue = result.getOldValue();
    final ASPECT newValue = result.getNewValue();
    final EqualityTester<ASPECT> equalityTester = getEqualityTester(aspectClass);
    final boolean oldAndNewEqual = (oldValue == null && newValue == null)
        || (oldValue != null && newValue != null && equalityTester.equals(oldValue, newValue));

    // Invoke post-update hooks if there's any
    if (_aspectPostUpdateHooksMap.containsKey(aspectClass)) {
      _aspectPostUpdateHooksMap.get(aspectClass).forEach(hook -> hook.accept(urn, newValue));
    }

    // Produce MAE after a successful update
    if (_emitAuditEvent) {
      // https://jira01.corp.linkedin.com:8443/browse/APA-80115
      if (_alwaysEmitAuditEvent || !oldAndNewEqual) {
        if (_trackingProducer != null) {
          _trackingProducer.produceMetadataAuditEvent(urn, oldValue, newValue);
        } else {
          _producer.produceMetadataAuditEvent(urn, oldValue, newValue);
        }
      }
    }

    // TODO: Replace the previous step with the step below, after pipeline is fully migrated to aspect specific events.
    // Produce aspect specific MAE after a successful update
    if (_emitAspectSpecificAuditEvent) {
      if (_alwaysEmitAspectSpecificAuditEvent || !oldAndNewEqual) {
        if (_trackingProducer != null) {
          _trackingProducer.produceAspectSpecificMetadataAuditEvent(urn, oldValue, newValue, auditStamp, trackingContext,
              IngestionMode.LIVE);
        } else {
          _producer.produceAspectSpecificMetadataAuditEvent(urn, oldValue, newValue, auditStamp, IngestionMode.LIVE);
        }
      }
    }

    return newValue;
  }

  /**
   * Adds a new version of aspect for an entity.
   *
   * <p>The new aspect will have an automatically assigned version number, which is guaranteed to be positive and
   * monotonically increasing. Older versions of aspect will be purged automatically based on the retention setting. A
   * MetadataAuditEvent is also emitted if there's an actual update.
   *
   * @param urn the URN for the entity the aspect is attached to
   * @param auditStamp the audit stamp for the operation
   * @param updateLambda a lambda expression that takes the previous version of aspect and returns the new version
   * @return {@link RecordTemplate} of the new value of aspect
   */
  @Nonnull
  public <ASPECT extends RecordTemplate> ASPECT add(@Nonnull URN urn, @Nonnull Class<ASPECT> aspectClass,
      @Nonnull Function<Optional<ASPECT>, ASPECT> updateLambda, @Nonnull AuditStamp auditStamp,
      int maxTransactionRetry) {
    return add(urn, aspectClass, updateLambda, auditStamp, maxTransactionRetry, null);
  }

  /**
   * Same as {@link #add(Urn, Class, Function, AuditStamp, int)} but with tracking context.
   */
  @Nonnull
  public <ASPECT extends RecordTemplate> ASPECT add(@Nonnull URN urn, @Nonnull Class<ASPECT> aspectClass,
      @Nonnull Function<Optional<ASPECT>, ASPECT> updateLambda, @Nonnull AuditStamp auditStamp,
      int maxTransactionRetry, @Nullable IngestionTrackingContext trackingContext) {
    return add(urn, new AspectUpdateLambda<>(aspectClass, updateLambda), auditStamp, maxTransactionRetry, trackingContext);
  }

  /**
   * Same as {@link #add(Urn, Class, Function, AuditStamp, int, IngestionTrackingContext)} but with ingestion parameters.
   */
  @Nonnull
  public <ASPECT extends RecordTemplate> ASPECT add(@Nonnull URN urn, @Nonnull Class<ASPECT> aspectClass,
      @Nonnull Function<Optional<ASPECT>, ASPECT> updateLambda, @Nonnull AuditStamp auditStamp,
      int maxTransactionRetry, @Nullable IngestionTrackingContext trackingContext, @Nonnull IngestionParams ingestionParams) {
    return add(urn, new AspectUpdateLambda<>(aspectClass, updateLambda, ingestionParams), auditStamp, maxTransactionRetry, trackingContext);
  }

  /**
   * Adds a new version of an aspect for an entity.
   *
   * <p>
   * The new aspect will have an automatically assigned version number, which is guaranteed to be positive and monotonically
   * increasing. Older versions of the aspect will be purged automatically based on the retention setting. A MetadataAuditEvent
   * is also emitted if an actual update occurs.
   * </p>
   *
   * @param urn the URN for the entity to which the aspect is attached
   * @param updateLambda the {@link AspectUpdateLambda} describing the update
   * @param auditStamp the audit stamp for the operation
   * @param maxTransactionRetry the maximum number of times to retry the transaction
   * @param <ASPECT> the type of the aspect being updated
   * @return the new value of the aspect
   */
  @Nonnull
  public <ASPECT extends RecordTemplate> ASPECT add(@Nonnull URN urn, AspectUpdateLambda<ASPECT> updateLambda,
      @Nonnull AuditStamp auditStamp, int maxTransactionRetry) {
    return add(urn, updateLambda, auditStamp, maxTransactionRetry, null);
  }

  /**
   * Same as above {@link #add(Urn, AspectUpdateLambda, AuditStamp, int)} but with tracking context.
   */
  @Nonnull
  public <ASPECT extends RecordTemplate> ASPECT add(@Nonnull URN urn, AspectUpdateLambda<ASPECT> updateLambda,
      @Nonnull AuditStamp auditStamp, int maxTransactionRetry, @Nullable IngestionTrackingContext trackingContext) {
    checkValidAspect(updateLambda.getAspectClass());

    final AddResult<ASPECT> result = runInTransactionWithRetry(() -> aspectUpdateHelper(urn, updateLambda, auditStamp, trackingContext),
        maxTransactionRetry);

    return unwrapAddResult(urn, result, auditStamp, trackingContext);
  }

  /**
   * Deletes the latest version of aspect for an entity.
   *
   * <p>The new aspect will have an automatically assigned version number, which is guaranteed to be positive and
   * monotonically increasing. Older versions of aspect will be purged automatically based on the retention setting.
   *
   * <p>Note that we do not support Post-update hooks while soft deleting an aspect
   *
   * @param urn urn the URN for the entity the aspect is attached to
   * @param aspectClass aspectClass of the aspect being saved
   * @param auditStamp the audit stamp of the previous latest aspect, null if new value is the first version
   * @param maxTransactionRetry maximum number of transaction retries before throwing an exception
   * @param <ASPECT> must be a supported aspect type in {@code ASPECT_UNION}
   */
  public <ASPECT extends RecordTemplate> void delete(@Nonnull URN urn, @Nonnull Class<ASPECT> aspectClass,
      @Nonnull AuditStamp auditStamp, int maxTransactionRetry) {
    delete(urn, aspectClass, auditStamp, maxTransactionRetry, null);
  }

  /**
   * Same as above {@link #delete(Urn, Class, AuditStamp, int)} but with tracking context.
   */
  public <ASPECT extends RecordTemplate> void delete(@Nonnull URN urn, @Nonnull Class<ASPECT> aspectClass,
      @Nonnull AuditStamp auditStamp, int maxTransactionRetry, @Nullable IngestionTrackingContext trackingContext) {

    checkValidAspect(aspectClass);

    runInTransactionWithRetry(() -> {
      final AspectEntry<ASPECT> latest = getLatest(urn, aspectClass);
      final IngestionParams ingestionParams = new IngestionParams().setIngestionMode(IngestionMode.LIVE);
      return addCommon(urn, latest, null, aspectClass, auditStamp, new DefaultEqualityTester<>(), trackingContext, ingestionParams);
    }, maxTransactionRetry);

    // TODO: add support for sending MAE for soft deleted aspects
  }

  /**
   * Similar to {@link #add(Urn, Class, Function, AuditStamp, int)} but uses the default maximum transaction retry.
   */
  @Nonnull
  public <ASPECT extends RecordTemplate> ASPECT add(@Nonnull URN urn, @Nonnull Class<ASPECT> aspectClass,
      @Nonnull Function<Optional<ASPECT>, ASPECT> updateLambda, @Nonnull AuditStamp auditStamp) {
    return add(urn, aspectClass, updateLambda, auditStamp, DEFAULT_MAX_TRANSACTION_RETRY, null);
  }

  /**
   * Same as above {@link #add(Urn, Class, Function, AuditStamp)} but with tracking context.
   */
  @Nonnull
  public <ASPECT extends RecordTemplate> ASPECT add(@Nonnull URN urn, @Nonnull Class<ASPECT> aspectClass,
      @Nonnull Function<Optional<ASPECT>, ASPECT> updateLambda, @Nonnull AuditStamp auditStamp,
      @Nullable IngestionTrackingContext trackingContext, @Nonnull IngestionParams ingestionParams) {
    return add(urn, aspectClass, updateLambda, auditStamp, DEFAULT_MAX_TRANSACTION_RETRY, trackingContext, ingestionParams);
  }

  /**
   * Similar to {@link #add(Urn, Class, Function, AuditStamp)} but takes the new value directly.
   */
  @VisibleForTesting
  @Nonnull
  public <ASPECT extends RecordTemplate> ASPECT add(@Nonnull URN urn, @Nonnull ASPECT newValue,
      @Nonnull AuditStamp auditStamp) {
    return add(urn, newValue, auditStamp, null, null);
  }

  /**
   * Same as above {@link #add(Urn, RecordTemplate, AuditStamp)} but with tracking context.
   */
  @Nonnull
  public <ASPECT extends RecordTemplate> ASPECT add(@Nonnull URN urn, @Nonnull ASPECT newValue,
      @Nonnull AuditStamp auditStamp, @Nullable IngestionTrackingContext trackingContext, @Nullable IngestionParams ingestionParams) {
    final IngestionParams nonNullIngestionParams = ingestionParams == null ? new IngestionParams().setIngestionMode(IngestionMode.LIVE) : ingestionParams;
    return add(urn, (Class<ASPECT>) newValue.getClass(), ignored -> newValue, auditStamp, trackingContext, nonNullIngestionParams);
  }

  /**
   * Similar to {@link #delete(Urn, Class, AuditStamp, int)} but uses the default maximum transaction retry.
   */
  @Nonnull
  public <ASPECT extends RecordTemplate> void delete(@Nonnull URN urn, @Nonnull Class<ASPECT> aspectClass,
      @Nonnull AuditStamp auditStamp) {
    delete(urn, aspectClass, auditStamp, null);
  }

  /**
   * Same as above {@link #delete(Urn, Class, AuditStamp)} but with tracking context.
   */
  @Nonnull
  public <ASPECT extends RecordTemplate> void delete(@Nonnull URN urn, @Nonnull Class<ASPECT> aspectClass,
      @Nonnull AuditStamp auditStamp, @Nullable IngestionTrackingContext trackingContext) {
    delete(urn, aspectClass, auditStamp, DEFAULT_MAX_TRANSACTION_RETRY, trackingContext);
  }

  private <ASPECT extends RecordTemplate> void applyRetention(@Nonnull URN urn, @Nonnull Class<ASPECT> aspectClass,
      @Nonnull Retention retention, long largestVersion) {
    if (retention instanceof IndefiniteRetention) {
      return;
    }

    if (retention instanceof VersionBasedRetention) {
      applyVersionBasedRetention(aspectClass, urn, (VersionBasedRetention) retention, largestVersion);
      return;
    }

    if (retention instanceof TimeBasedRetention) {
      applyTimeBasedRetention(aspectClass, urn, (TimeBasedRetention) retention, _clock.millis());
      return;
    }
  }

  /**
   * Saves the latest aspect.
   *
   * @param urn the URN for the entity the aspect is attached to
   * @param aspectClass the aspectClass of the aspect being saved
   * @param oldEntry {@link RecordTemplate} of the previous latest value of aspect, null if new value is the first version
   * @param oldAuditStamp the audit stamp of the previous latest aspect, null if new value is the first version
   * @param newEntry {@link RecordTemplate} of the new latest value of aspect
   * @param newAuditStamp the audit stamp for the operation
   * @param isSoftDeleted flag to indicate if the previous latest value of aspect was soft deleted
   * @return the largest version
   */
  protected abstract <ASPECT extends RecordTemplate> long saveLatest(@Nonnull URN urn,
      @Nonnull Class<ASPECT> aspectClass, @Nullable ASPECT oldEntry, @Nullable AuditStamp oldAuditStamp,
      @Nullable ASPECT newEntry, @Nonnull AuditStamp newAuditStamp, boolean isSoftDeleted,
      @Nullable IngestionTrackingContext trackingContext);

  /**
   * Saves the new value of an aspect to entity tables. This is used when backfilling metadata from the old schema to
   * the new schema.
   *
   * @param urn the URN for the entity the aspect is attached to
   * @param aspectClass class of the aspect to backfill
   */
  public abstract <ASPECT extends RecordTemplate> void updateEntityTables(@Nonnull URN urn, @Nonnull Class<ASPECT> aspectClass);

  /**
   * Backfill local relationships from the new schema entity tables. This method is new/dual schema only. It should NOT
   * be used to add local relationships under normal circumstances and should ONLY be used in the case where the aspects
   * exist in the entity tables but the relationships don't exist in the local relationship tables (e.g. if the local
   * relationship tables got dropped or if they were set up after data was already ingested into entity tables).
   *
   * @param urn the URN for the entity the aspect (which the local relationship is derived from) is attached to
   * @param aspectClass class of the aspect to backfill
   * @return A list of local relationship updates executed.
   */
  public abstract <ASPECT extends RecordTemplate> List<LocalRelationshipUpdates> backfillLocalRelationshipsFromEntityTables(
      @Nonnull URN urn, @Nonnull Class<ASPECT> aspectClass);

  /**
   * Returns list of urns from local secondary index that satisfy the given filter conditions.
   *
   * <p>Results are ordered by the order criterion but defaults to sorting lexicographically by the string
   * representation of the URN.
   *
   * @param indexFilter {@link IndexFilter} containing filter conditions to be applied
   * @param indexSortCriterion {@link IndexSortCriterion} sorting criterion to be applied
   * @param lastUrn last urn of the previous fetched page. For the first page, this should be set as NULL
   * @param pageSize maximum number of distinct urns to return
   * @return List of urns from local secondary index that satisfy the given filter conditions
   */
  @Nonnull
  public abstract List<URN> listUrns(@Nullable IndexFilter indexFilter, @Nullable IndexSortCriterion indexSortCriterion,
      @Nullable URN lastUrn, int pageSize);

  public List<URN> listUrns(@Nullable String lastUrn, int pageSize, @Nullable IndexFilter indexFilter, @Nullable IndexSortCriterion indexSortCriterion) {
    return listUrns(indexFilter, indexSortCriterion, getUrnFromString(lastUrn, _urnClass), pageSize);
  }

  /**
   * Similar to {@link #listUrns(IndexFilter, IndexSortCriterion, Urn, int)} but sorts lexicographically by the URN.
   */
  @Nonnull
  public List<URN> listUrns(@Nullable IndexFilter indexFilter, @Nullable URN lastUrn, int pageSize) {
    return listUrns(indexFilter, null, lastUrn, pageSize);
  }

  /**
   * Similar to {@link #listUrns(IndexFilter, IndexSortCriterion, Urn, int)} but returns a list result with pagination
   * information.
   *
   * @param start the starting offset of the page
   * @return a {@link ListResult} containing a list of urns and other pagination information
   */
  @Nonnull
  public abstract <ASPECT extends RecordTemplate> ListResult<URN> listUrns(@Nullable IndexFilter indexFilter,
      @Nullable IndexSortCriterion indexSortCriterion, int start, int pageSize);

  /**
   * Similar to {@link #listUrns(IndexFilter, Urn, int)}. This is to get all urns with type URN.
   */
  @Nonnull
  public List<URN> listUrns(@Nonnull Class<URN> urnClazz, @Nullable URN lastUrn, int pageSize) {
    final IndexFilter indexFilter = new IndexFilter().setCriteria(
        new IndexCriterionArray(new IndexCriterion().setAspect(urnClazz.getCanonicalName())));
    return listUrns(indexFilter, lastUrn, pageSize);
  }

  /**
   * Retrieves list of urn aspect entries corresponding to the aspect classes and urns.
   *
   * @param aspectClasses aspect classes whose latest versions need to be retrieved
   * @param urns corresponding urns to be retrieved
   * @return list of latest versions of aspects along with urns
   */
  @Nonnull
  private List<UrnAspectEntry<URN>> getUrnAspectEntries(@Nonnull Set<Class<? extends RecordTemplate>> aspectClasses,
      @Nonnull List<URN> urns) {
    final Map<URN, Map<Class<? extends RecordTemplate>, Optional<? extends RecordTemplate>>> urnAspectMap =
        get(aspectClasses, new HashSet<>(urns));

    final Map<URN, List<RecordTemplate>> urnListAspectMap = new LinkedHashMap<>();
    for (URN urn : urns) {
      final Map<Class<? extends RecordTemplate>, Optional<? extends RecordTemplate>> aspectMap = urnAspectMap.get(urn);
      urnListAspectMap.compute(urn, (k, v) -> {
        if (v == null) {
          v = new ArrayList<>();
        }
        return v;
      });
      for (Optional<? extends RecordTemplate> aspect : aspectMap.values()) {
        aspect.ifPresent(record -> urnListAspectMap.get(urn).add(record));
      }
    }

    return urnListAspectMap.entrySet()
        .stream()
        .map(entry -> new UrnAspectEntry<>(entry.getKey(), entry.getValue()))
        .collect(Collectors.toList());
  }

  /**
   * Retrieves list of {@link UrnAspectEntry} containing latest version of aspects along with the urn for the list of urns
   * returned from local secondary index that satisfy given filter conditions. The returned list is ordered by the
   * sort criterion but ordered lexicographically by the string representation of the URN by default.
   *
   * @param aspectClasses aspect classes whose latest versions need to be retrieved
   * @param indexFilter {@link IndexFilter} containing filter conditions to be applied
   * @param indexSortCriterion {@link IndexSortCriterion} sort conditions to be applied
   * @param lastUrn last urn of the previous fetched page. For the first page, this should be set as NULL
   * @param pageSize maximum number of distinct urns whose aspects need to be retrieved
   * @return ordered list of latest versions of aspects along with urns returned from local secondary index
   *        satisfying given filter conditions
   */
  @Nonnull
  public List<UrnAspectEntry<URN>> getAspects(@Nonnull Set<Class<? extends RecordTemplate>> aspectClasses,
      @Nullable IndexFilter indexFilter, @Nullable IndexSortCriterion indexSortCriterion, @Nullable URN lastUrn,
      int pageSize) {

    final List<URN> urns = listUrns(indexFilter, indexSortCriterion, lastUrn, pageSize);

    return getUrnAspectEntries(aspectClasses, urns);
  }

  /**
   * Similar to {@link #getAspects(Set, IndexFilter, IndexSortCriterion, Urn, int)}
   * but sorts lexicographically by the URN.
   */
  @Nonnull
  public List<UrnAspectEntry<URN>> getAspects(@Nonnull Set<Class<? extends RecordTemplate>> aspectClasses,
      @Nonnull IndexFilter indexFilter, @Nullable URN lastUrn, int pageSize) {
    return getAspects(aspectClasses, indexFilter, null, lastUrn, pageSize);
  }

  /**
   * Similar to {@link #getAspects(Set, IndexFilter, IndexSortCriterion, Urn, int)}
   * but returns a list of aspects with pagination information.
   *
   * @param start starting offset of the page
   * @return a {@link ListResult} containing an ordered list of latest versions of aspects along with urns returned from
   *        local secondary index satisfying given filter conditions and pagination information
   */
  @Nonnull
  public ListResult<UrnAspectEntry<URN>> getAspects(@Nonnull Set<Class<? extends RecordTemplate>> aspectClasses,
      @Nullable IndexFilter indexFilter, @Nullable IndexSortCriterion indexSortCriterion, int start, int pageSize) {

    final ListResult<URN> listResult = listUrns(indexFilter, indexSortCriterion, start, pageSize);
    final List<URN> urns = listResult.getValues();

    final List<UrnAspectEntry<URN>> urnAspectEntries = getUrnAspectEntries(aspectClasses, urns);

    return ListResult.<UrnAspectEntry<URN>>builder().values(urnAspectEntries)
        .metadata(listResult.getMetadata())
        .nextStart(listResult.getNextStart())
        .havingMore(listResult.isHavingMore())
        .totalCount(listResult.getTotalCount())
        .totalPageCount(listResult.getTotalPageCount())
        .pageSize(listResult.getPageSize())
        .build();
  }

  /**
   * Runs the given lambda expression in a transaction with a limited number of retries.
   *
   * @param block the lambda expression to run
   * @param maxTransactionRetry maximum number of transaction retries before throwing an exception
   * @param <T> type for the result object
   * @return the result object from a successfully committed transaction
   */
  @Nonnull
  protected abstract <T> T runInTransactionWithRetry(@Nonnull Supplier<T> block, int maxTransactionRetry);

  /**
   * Gets the latest version of a specific aspect type for an entity.
   *
   * @param urn {@link Urn} for the entity
   * @param aspectClass the type of aspect to get
   * @return {@link AspectEntry} corresponding to the latest version of specific aspect, if it exists
   */
  @Nonnull
  protected abstract <ASPECT extends RecordTemplate> AspectEntry<ASPECT> getLatest(@Nonnull URN urn,
      @Nonnull Class<ASPECT> aspectClass);

  /**
   * Gets the next version to use for an entity's specific aspect type.
   *
   * @param urn {@link Urn} for the entity
   * @param aspectClass the type of aspect to get
   * @return the next version number to use, or {@link #LATEST_VERSION} if there's no previous versions
   */
  protected abstract <ASPECT extends RecordTemplate> long getNextVersion(@Nonnull URN urn,
      @Nonnull Class<ASPECT> aspectClass);

  /**
   * Insert an aspect for an entity with specific version and {@link AuditStamp}.
   *
   * @param urn {@link Urn} for the entity
   * @param value the aspect to insert
   * @param aspectClass the type of aspect to insert
   * @param auditStamp the {@link AuditStamp} for the aspect
   * @param version the version for the aspect
   */
  protected abstract <ASPECT extends RecordTemplate> void insert(@Nonnull URN urn, @Nullable RecordTemplate value,
      @Nonnull Class<ASPECT> aspectClass, @Nonnull AuditStamp auditStamp, long version,
      @Nullable IngestionTrackingContext trackingContext);

  /**
   * Update an aspect for an entity with specific version and {@link AuditStamp} with optimistic locking.
   *
   * @param urn {@link Urn} for the entity
   * @param value the aspect to update
   * @param aspectClass the type of aspect to update
   * @param newAuditStamp the {@link AuditStamp} for the new aspect
   * @param version the version for the aspect
   * @param oldTimestamp the timestamp for the old aspect
   */
  protected abstract <ASPECT extends RecordTemplate> void updateWithOptimisticLocking(@Nonnull URN urn,
      @Nullable RecordTemplate value, @Nonnull Class<ASPECT> aspectClass, @Nonnull AuditStamp newAuditStamp,
      long version, @Nonnull Timestamp oldTimestamp, @Nullable IngestionTrackingContext trackingContext);

  /**
   * Returns a boolean representing if an Urn has any Aspects associated with it (i.e. if it exists in the DB).
   * @param urn {@link Urn} for the entity
   * @return boolean representing if entity associated with Urn exists
   */
  public abstract boolean exists(@Nonnull URN urn);

  /**
   * Applies version-based retention against a specific aspect type for an entity.
   *
   * @param aspectClass the type of aspect to apply retention to
   * @param urn {@link Urn} for the entity
   * @param retention the retention configuration
   * @param largestVersion the largest version number for the aspect type
   */
  protected abstract <ASPECT extends RecordTemplate> void applyVersionBasedRetention(@Nonnull Class<ASPECT> aspectClass,
      @Nonnull URN urn, @Nonnull VersionBasedRetention retention, long largestVersion);

  /**
   * Applies time-based retention against a specific aspect type for an entity.
   *
   * @param aspectClass the type of aspect to apply retention to
   * @param urn {@link Urn} for the entity
   * @param retention the retention configuration
   * @param currentTime the current timestamp
   */
  protected abstract <ASPECT extends RecordTemplate> void applyTimeBasedRetention(@Nonnull Class<ASPECT> aspectClass,
      @Nonnull URN urn, @Nonnull TimeBasedRetention retention, long currentTime);

  /**
   * Emits backfill MAE for the latest version of an aspect and also backfills SCSI (if it exists and is enabled).
   *
   * @param aspectClass the type of aspect to backfill
   * @param urn urn for the entity
   * @param <ASPECT> must be a supported aspect type in {@code ASPECT_UNION}.
   * @return backfilled aspect
   * @deprecated Use {@link #backfill(Set, Set)} instead
   */
  @Nonnull
  public <ASPECT extends RecordTemplate> Optional<ASPECT> backfill(@Nonnull Class<ASPECT> aspectClass,
      @Nonnull URN urn) {
    return backfill(BackfillMode.BACKFILL_ALL, aspectClass, urn);
  }

  /**
   * Similar to {@link #backfill(Class, URN)} but does a scoped backfill.
   *
   * @param mode backfill mode to scope the backfill process
   */
  @Nonnull
  private <ASPECT extends RecordTemplate> Optional<ASPECT> backfill(@Nonnull BackfillMode mode,
      @Nonnull Class<ASPECT> aspectClass, @Nonnull URN urn) {
    checkValidAspect(aspectClass);
    Optional<ASPECT> aspect = get(aspectClass, urn, LATEST_VERSION);
    aspect.ifPresent(value -> backfill(mode, value, urn));
    return aspect;
  }

  /**
   * Emits backfill MAE for the latest version of a set of aspects for a set of urns and also backfills SCSI (if it exists and is enabled).
   *
   * @param aspectClasses set of aspects to backfill
   * @param urns set of urns to backfill
   * @return map of urn to their backfilled aspect values
   */
  @Nonnull
  public Map<URN, Map<Class<? extends RecordTemplate>, Optional<? extends RecordTemplate>>> backfill(
      @Nonnull Set<Class<? extends RecordTemplate>> aspectClasses, @Nonnull Set<URN> urns) {
    return backfill(BackfillMode.BACKFILL_ALL, aspectClasses, urns);
  }

  /**
   * This method provides a hack solution to enable low volume backfill against secondary live index by setting oldValue
   * in mae payload as null. This method should be deprecated once the secondary store is moving away from elastic search,
   * or the standard backfill method starts to safely backfill against live index.
   *
   * @param aspectClasses set of aspects to backfill
   * @param urns  set of urns to backfill
   * @return map of urn to their backfilled aspect values
   */
  @Deprecated
  @Nonnull
  public Map<URN, Map<Class<? extends RecordTemplate>, Optional<? extends RecordTemplate>>> backfillWithNewValue(
      @Nonnull Set<Class<? extends RecordTemplate>> aspectClasses, @Nonnull Set<URN> urns) {
    return backfill(BackfillMode.MAE_ONLY_WITH_OLD_VALUE_NULL, aspectClasses, urns);
  }

  /**
   * Similar to {@link #backfill(Set, Set)} but does a scoped backfill.
   *
   * @param mode backfill mode to scope the backfill process
   * @param aspectClasses set of aspects to backfill, if null, all valid aspects inside the entity snapshot will be backfilled
   * @param urns  set of urns to backfill
   */
  @Nonnull
  public Map<URN, Map<Class<? extends RecordTemplate>, Optional<? extends RecordTemplate>>> backfill(
      @Nonnull BackfillMode mode, @Nullable Set<Class<? extends RecordTemplate>> aspectClasses, @Nonnull Set<URN> urns) {
    Set<Class<? extends RecordTemplate>> aspectToBackfill =
        aspectClasses == null ? getValidAspectTypes(_aspectUnionClass) : aspectClasses;
    checkValidAspects(aspectToBackfill);
    final Map<URN, Map<Class<? extends RecordTemplate>, Optional<? extends RecordTemplate>>> urnToAspects =
        get(aspectToBackfill, urns);
    urnToAspects.forEach((urn, aspects) -> {
      aspects.forEach((aspectClass, aspect) -> aspect.ifPresent(value -> backfill(mode, value, urn)));
    });
    return urnToAspects;
  }

  /**
   * Entity agnostic method to backfill MAEs given aspects and urns. Only registered and present aspects in database table
   * will be backfilled. Invalid aspects and urns will cause exception.
   *
   * @param mode backfill mode to scope the backfill process
   * @param aspects set of aspects to backfill
   * @param urns set of urns to backfill
   * @return map of urn to their backfilled aspect values
   */
  @Nonnull
  public Map<String, Set<String>> backfillMAE(@Nonnull BackfillMode mode, @Nullable Set<String> aspects,
      @Nonnull Set<String> urns) {
    // convert string to entity urn
    if (_urnClass == null) { // _urnClass can be null in testing scenarios
      throw new IllegalStateException("urn class is null, unable to convert string to urn");
    }
    final Set<URN> urnSet = urns.stream().map(x -> getUrnFromString(x, _urnClass)).collect(Collectors.toSet());

    // convert string to aspect class
    Set<Class<? extends RecordTemplate>> aspectSet = null;
    if (aspects != null) {
      aspectSet = aspects.stream().map(ModelUtils::getAspectClass).collect(Collectors.toSet());
    }

    // call type specific backfill method and transform results to string map
    return transformBackfillResultsToStringMap(backfill(mode, aspectSet, urnSet));
  }

  @Nonnull
  public Map<URN, Map<Class<? extends RecordTemplate>, Optional<? extends RecordTemplate>>> backfillEntityTables(
      @Nonnull Set<Class<? extends RecordTemplate>> aspectClasses, @Nonnull Set<URN> urns) {
    urns.forEach(urn -> aspectClasses.forEach(aspect -> updateEntityTables(urn, aspect)));
    return get(aspectClasses, urns);
  }

  /**
   * Emits backfill MAE for the latest version of a set of aspects for a set of urns
   * and also backfills SCSI (if it exists and is enabled) depending on the backfill mode.
   *
   * @param mode backfill mode to scope the backfill process
   * @param aspectClasses set of aspects to backfill
   * @param urnClazz the type of urn to backfill - needed to list urns using SCSI
   * @param lastUrn last urn of the previous backfilled page - needed to list urns using SCSI
   * @param pageSize the number of entities to backfill
   * @return map of urn to their backfilled aspect values
   */
  @Nonnull
  public Map<URN, Map<Class<? extends RecordTemplate>, Optional<? extends RecordTemplate>>> backfill(
      @Nonnull BackfillMode mode, @Nonnull Set<Class<? extends RecordTemplate>> aspectClasses,
      @Nonnull Class<URN> urnClazz, @Nullable URN lastUrn, int pageSize) {

    final List<URN> urnList = listUrns(urnClazz, lastUrn, pageSize);
    return backfill(mode, aspectClasses, new HashSet(urnList));
  }

  /**
   * Emits backfill MAE for an aspect of an entity depending on the backfill mode.
   *
   * @param mode backfill mode
   * @param aspect aspect to backfill
   * @param urn {@link Urn} for the entity
   * @param <ASPECT> must be a supported aspect type in {@code ASPECT_UNION}.
   */
  private <ASPECT extends RecordTemplate> void backfill(@Nonnull BackfillMode mode, @Nonnull ASPECT aspect,
      @Nonnull URN urn) {

    if (mode == BackfillMode.MAE_ONLY
        || mode == BackfillMode.BACKFILL_ALL
        || mode == BackfillMode.BACKFILL_INCLUDING_LIVE_INDEX) {
      IngestionMode ingestionMode = ALLOWED_INGESTION_BACKFILL_BIMAP.inverse().get(mode);
      if (_trackingProducer != null) {
        IngestionTrackingContext trackingContext = buildIngestionTrackingContext(
            TrackingUtils.getRandomUUID(), BACKFILL_EMITTER, System.currentTimeMillis());

        _trackingProducer.produceAspectSpecificMetadataAuditEvent(urn, aspect, aspect, null, trackingContext, ingestionMode);
      } else {
        _producer.produceAspectSpecificMetadataAuditEvent(urn, aspect, aspect, null, ingestionMode);
      }
    }
  }

  /**
   * Paginates over all available versions of an aspect for an entity. This does not include version of soft deleted aspect(s).
   *
   * @param aspectClass the type of the aspect to query
   * @param urn {@link Urn} for the entity
   * @param start the starting offset of the page
   * @param pageSize the size of the page
   * @param <ASPECT> must be a supported aspect type in {@code ASPECT_UNION}.
   * @return a {@link ListResult} containing a list of version numbers and other pagination information
   */
  @Nonnull
  public abstract <ASPECT extends RecordTemplate> ListResult<Long> listVersions(@Nonnull Class<ASPECT> aspectClass,
      @Nonnull URN urn, int start, int pageSize);

  /**
   * Paginates over all URNs for entities that have a specific aspect. This does not include the urn(s) for which the
   * aspect is soft deleted in the latest version.
   *
   * @param aspectClass the type of the aspect to query
   * @param start the starting offset of the page
   * @param pageSize the size of the page
   * @param <ASPECT> must be a supported aspect type in {@code ASPECT_UNION}.
   * @return a {@link ListResult} containing a list of URN and other pagination information
   */
  @Nonnull
  public abstract <ASPECT extends RecordTemplate> ListResult<URN> listUrns(@Nonnull Class<ASPECT> aspectClass,
      int start, int pageSize);

  /**
   * Paginates over all versions of an aspect for a specific Urn. It does not return metadata corresponding to versions
   * indicating soft deleted aspect(s).
   *
   * @param aspectClass the type of the aspect to query
   * @param urn {@link Urn} for the entity
   * @param start the starting offset of the page
   * @param pageSize the size of the page
   * @param <ASPECT> must be a supported aspect type in {@code ASPECT_UNION}.
   * @return a {@link ListResult} containing a list of aspects and other pagination information
   */
  @Nonnull
  public abstract <ASPECT extends RecordTemplate> ListResult<ASPECT> list(@Nonnull Class<ASPECT> aspectClass,
      @Nonnull URN urn, int start, int pageSize);

  /**
   * Paginates over a specific version of a specific aspect for all Urns. The result does not include soft deleted
   * aspect if the specific version of a specific aspect was soft deleted.
   *
   * @param aspectClass the type of the aspect to query
   * @param version the version of the aspect
   * @param start the starting offset of the page
   * @param pageSize the size of the page
   * @param <ASPECT> must be a supported aspect type in {@code ASPECT_UNION}.
   * @return a {@link ListResult} containing a list of aspects and other pagination information
   */
  @Nonnull
  public abstract <ASPECT extends RecordTemplate> ListResult<ASPECT> list(@Nonnull Class<ASPECT> aspectClass,
      long version, int start, int pageSize);

  /**
   * Paginates over the latest version of a specific aspect for all Urns. The result does not include soft deleted
   * aspect if the latest version of a specific aspect was soft deleted.
   *
   * @param aspectClass the type of the aspect to query
   * @param start the starting offset of the page
   * @param pageSize the size of the page
   * @param <ASPECT> must be a supported aspect type in {@code ASPECT_UNION}.
   * @return a {@link ListResult} containing a list of aspects and other pagination information
   */
  @Nonnull
  public abstract <ASPECT extends RecordTemplate> ListResult<ASPECT> list(@Nonnull Class<ASPECT> aspectClass, int start,
      int pageSize);

  /**
   *  Gets the count of an aggregation specified by the aspect and field to group on.
   * @param indexFilter {@link IndexFilter} that defines the filter conditions
   * @param indexGroupByCriterion {@link IndexGroupByCriterion} that defines the aspect to group by
   * @return map of the field to the count
   */
  @Nonnull
  public abstract Map<String, Long> countAggregate(@Nullable IndexFilter indexFilter,
      @Nonnull IndexGroupByCriterion indexGroupByCriterion);

  /**
   * Batch retrieves metadata aspects along with {@link ExtraInfo} using multiple {@link AspectKey}s.
   *
   * @param keys set of keys for the metadata to retrieve
   * @return a mapping of given keys to the corresponding metadata aspect and {@link ExtraInfo}.
   */
  @Nonnull
  public abstract Map<AspectKey<URN, ? extends RecordTemplate>, AspectWithExtraInfo<? extends RecordTemplate>> getWithExtraInfo(
      @Nonnull Set<AspectKey<URN, ? extends RecordTemplate>> keys);

  /**
   * Similar to {@link #getWithExtraInfo(Set)} but only using only one {@link AspectKey}.
   */
  @Nonnull
  public <ASPECT extends RecordTemplate> Optional<AspectWithExtraInfo<ASPECT>> getWithExtraInfo(
      @Nonnull AspectKey<URN, ASPECT> key) {
    if (getWithExtraInfo(Collections.singleton(key)).containsKey(key)) {
      return Optional.of((AspectWithExtraInfo<ASPECT>) getWithExtraInfo(Collections.singleton(key)).get(key));
    }
    return Optional.empty();
  }

  /**
   * Similar to {@link #getWithExtraInfo(AspectKey)} but with each component of the key broken out as arguments.
   */
  @Nonnull
  public <ASPECT extends RecordTemplate> Optional<AspectWithExtraInfo<ASPECT>> getWithExtraInfo(
      @Nonnull Class<ASPECT> aspectClass, @Nonnull URN urn, long version) {
    return getWithExtraInfo(new AspectKey<>(aspectClass, urn, version));
  }

  /**
   * Similar to {@link #getWithExtraInfo(Class, Urn, long)} but always retrieves the latest version.
   */
  @Nonnull
  public <ASPECT extends RecordTemplate> Optional<AspectWithExtraInfo<ASPECT>> getWithExtraInfo(
      @Nonnull Class<ASPECT> aspectClass, @Nonnull URN urn) {
    return getWithExtraInfo(aspectClass, urn, LATEST_VERSION);
  }

  /**
   * Generates a new string ID that's guaranteed to be globally unique.
   */
  @Nonnull
  public String newStringId() {
    return UUID.randomUUID().toString();
  }

  /**
   * Generates a new numeric ID that's guaranteed to increase monotonically within the given namespace.
   */
  public abstract long newNumericId(@Nonnull String namespace, int maxTransactionRetry);

  /**
   * Similar to {@link #newNumericId(String, int)} but uses default maximum transaction retry count.
   */
  public long newNumericId(@Nonnull String namespace) {
    return newNumericId(namespace, DEFAULT_MAX_TRANSACTION_RETRY);
  }

  /**
   * Similar to {@link #newNumericId(String, int)} but uses a single global namespace.
   */
  public long newNumericId() {
    return newNumericId(DEFAULT_ID_NAMESPACE);
  }

  /**
   * Validates a model against its schema.
   */
  protected void validateAgainstSchema(@Nonnull RecordTemplate model) {
    ValidationResult result = ValidateDataAgainstSchema.validate(model,
        new ValidationOptions(RequiredMode.CAN_BE_ABSENT_IF_HAS_DEFAULT, CoercionMode.NORMAL,
            UnrecognizedFieldMode.DISALLOW));

    if (!result.isValid()) {
      throw new ModelValidationException(result.getMessages().toString());
    }
  }

  /**
   * Maps backfill results of type map{urn, map{aspect, optional metadata}} to map{urn, aspect fqcn}.
   */
  @Nonnull
  protected Map<String, Set<String>> transformBackfillResultsToStringMap(
      @Nonnull Map<URN, Map<Class<? extends RecordTemplate>, Optional<? extends RecordTemplate>>> backfillResults) {
    Map<String, Set<String>> mapToReturn = new HashMap<>();
    for (URN urn: backfillResults.keySet()) {
      Set<String> aspectFqcnSetToReturn = new HashSet<>();
      Map<Class<? extends RecordTemplate>, Optional<? extends RecordTemplate>> aspectClassToMetadataMap = backfillResults.get(urn);

      for (Class<? extends RecordTemplate> aspectClass: aspectClassToMetadataMap.keySet()) {
        if (aspectClassToMetadataMap.get(aspectClass).isPresent()) {
          aspectFqcnSetToReturn.add(getAspectName(aspectClass));
        }
      }

      if (!aspectFqcnSetToReturn.isEmpty()) {
        mapToReturn.put(String.valueOf(urn), aspectFqcnSetToReturn);
      }
    }
    return mapToReturn;
  }

  /** Aspect Version Comparator.
   * @param newValue - Aspect that may have baseSemanticVersion field (by including BaseVersionedAspect).
   * @param oldValue - Aspect that may have baseSemanticVersion field (by including BaseVersionedAspect).
   * @return Return integer (-1, 0, 1) depending on if newValue version is (lesser than, equal to, greater than)
   *     oldValue version.
   */
  protected int aspectVersionComparator(@Nullable RecordTemplate newValue, @Nullable RecordTemplate oldValue) {
    // Attempt to extract baseSemanticVersion from incoming aspects
    // If aspect or version does not exist, set version as lowest ranking (null)
    final DataMap newVerMap = newValue != null ? newValue.data().getDataMap(BASE_SEMANTIC_VERSION) : null;
    final DataMap oldVerMap = oldValue != null ? oldValue.data().getDataMap(BASE_SEMANTIC_VERSION) : null;

    if (newVerMap == null && oldVerMap == null) { // Both inputs are either null or have no version.
      return 0;
    } else if (newVerMap == null && oldVerMap != null) { // Old value has version, but new one does not
      return -1;
    } else if (newVerMap != null && oldVerMap == null) { // New value has version, but old one does not
      return 1;
    } else { //newVerMap != null && oldVerMap != null
      // Translate baseSemanticVersion into array [major, minor, patch]
      final int[] newVerArr = { newVerMap.getInteger(MAJOR).intValue(), newVerMap.getInteger(MINOR).intValue(),
          newVerMap.getInteger(PATCH).intValue()};
      final int[] oldVerArr = { oldVerMap.getInteger(MAJOR).intValue(), oldVerMap.getInteger(MINOR).intValue(),
          oldVerMap.getInteger(PATCH).intValue()};

      // Iterate through version numbers from highest to lowest priority (major->minor->patch)
      for (int i = 0; i < newVerArr.length; i++) {
        // If version numbers are not equal, return appropriate result
        if (newVerArr[i] > oldVerArr[i]) {
          return 1;
        } else if (newVerArr[i] < oldVerArr[i]) {
          return -1;
        }
        // else version numbers are equal. Continue to version numbers of next priority
      }

      // newValue version == oldValue version
      return 0;

    }
  }

  /** Logic to check aspect versions and skip write if needed.
   * @param newValue - Aspect that may have baseSemanticVersion field (by including BaseVersionedAspect).
   * @param oldValue - Aspect that may have baseSemanticVersion field (by including BaseVersionedAspect).
   * @return Return true if we should skip writing newValue. Return false if we won't skip based on aspect version
   *     check.
   */
  protected boolean aspectVersionSkipWrite(@Nullable RecordTemplate newValue, @Nullable RecordTemplate oldValue) {
    /* In the scope of version check, the only case where we should skip writing is when comparator returns -1.
       This includes the following cases:
           - newValue version < oldValue version
           - newValue is null and oldValue is not null
           - newValue has no version, and oldValue has a version
     */
    return aspectVersionComparator(newValue, oldValue) == -1;
  }

  /**
   * The logic determines if we will update the aspect.
   */
  private <ASPECT extends RecordTemplate> boolean shouldUpdateAspect(IngestionMode ingestionMode, URN urn, ASPECT oldValue,
      ASPECT newValue, Class<ASPECT> aspectClass, AuditStamp auditStamp, EqualityTester<ASPECT> equalityTester) {

    final boolean oldAndNewEqual = (oldValue == null && newValue == null) || (oldValue != null && newValue != null && equalityTester.equals(
        oldValue, newValue));

    AspectIngestionAnnotationArray ingestionAnnotations = parseIngestionModeFromAnnotation(aspectClass);
    AspectIngestionAnnotation annotation = findIngestionAnnotationForEntity(ingestionAnnotations, urn);
    Mode mode = annotation == null || !annotation.hasMode() ? Mode.DEFAULT : annotation.getMode();

    // Skip saving for the following scenarios
    if (mode != Mode.FORCE_UPDATE
        && ingestionMode != IngestionMode.LIVE_OVERRIDE // ensure that the new metadata received is skippable (i.e. not marked as a forced write).
        && (oldAndNewEqual || aspectVersionSkipWrite(newValue, oldValue))) { // values are equal or newValue ver < oldValue ver
      return false;
    }

    if (ingestionMode == IngestionMode.LIVE_OVERRIDE) {
      log.info((String.format(
          "Received ingestion event with LIVE_OVERRIDE write mode. urn: %s, aspectClass: %s, auditStamp: %s,"
              + "newValue == oldValue: %b. An MAE will %sbe emitted.", urn, aspectClass, auditStamp, oldAndNewEqual,
          oldAndNewEqual ? "not " : "")));
      return true;
    }

    if (mode == Mode.FORCE_UPDATE) {
      // If no filters specified in the annotation, FORCE_UPDATE
      if (!annotation.hasFilter() || annotation.getFilter() == null) {
        log.info((String.format("@gma.aspect.ingestion is FORCE_UPDATE on aspect %s and no filters set in annotation."
            + " Force update aspect.", aspectClass.getCanonicalName())));
        return true;
      }

      UrnFilterArray filters = annotation.getFilter();
      Map<String, Object> urnPaths = _urnPathExtractor.extractPaths(urn);

      // If there are filters in annotation, at least one filter conditions has to be met.
      boolean atLeastOneFilterPass = false;
      for (UrnFilter filter : filters) {
        if (urnPaths.containsKey(filter.getPath())
            && urnPaths.get(filter.getPath()).toString().equals(filter.getValue())) {
          atLeastOneFilterPass = true;
          break;
        }
      }

      if (atLeastOneFilterPass) {
        return true;
      }
    }

    return !(oldAndNewEqual || aspectVersionSkipWrite(newValue, oldValue));
  }

  /**
   * Update the aspect value with pre-defined lambda functions.
   */
  @Nonnull
  protected <ASPECT extends RecordTemplate> ASPECT updatePreIngestionLambdas(@Nonnull URN urn,
      @Nullable Optional<ASPECT> oldValue, @Nonnull ASPECT newValue) {
    for (final BaseLambdaFunction function : _lambdaFunctionRegistry.getLambdaFunctions(newValue)) {
      newValue = (ASPECT) function.apply(urn, oldValue, newValue);
    }
    if (newValue == null) {
      throw new UnsupportedOperationException(String.format("Attempted to update %s with null aspect.", urn));
    }
    return newValue;
  }
}
