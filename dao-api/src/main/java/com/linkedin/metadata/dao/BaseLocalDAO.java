package com.linkedin.metadata.dao;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.validation.CoercionMode;
import com.linkedin.data.schema.validation.RequiredMode;
import com.linkedin.data.schema.validation.UnrecognizedFieldMode;
import com.linkedin.data.schema.validation.ValidateDataAgainstSchema;
import com.linkedin.data.schema.validation.ValidationOptions;
import com.linkedin.data.schema.validation.ValidationResult;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.metadata.backfill.BackfillMode;
import com.linkedin.metadata.dao.equality.DefaultEqualityTester;
import com.linkedin.metadata.dao.equality.EqualityTester;
import com.linkedin.metadata.dao.exception.ModelValidationException;
import com.linkedin.metadata.dao.producer.BaseMetadataEventProducer;
import com.linkedin.metadata.dao.retention.IndefiniteRetention;
import com.linkedin.metadata.dao.retention.Retention;
import com.linkedin.metadata.dao.retention.TimeBasedRetention;
import com.linkedin.metadata.dao.retention.VersionBasedRetention;
import com.linkedin.metadata.dao.storage.LocalDAOStorageConfig;
import com.linkedin.metadata.dao.utils.ModelUtils;
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


/**
 * A base class for all Local DAOs.
 *
 * <p>Local DAO is a standardized interface to store and retrieve aspects from a document store.
 *
 * @param <ASPECT_UNION> must be a valid aspect union type defined in com.linkedin.metadata.aspect
 * @param <URN> must be the entity URN type in {@code ASPECT_UNION}
 */
public abstract class BaseLocalDAO<ASPECT_UNION extends UnionTemplate, URN extends Urn>
    extends BaseReadDAO<ASPECT_UNION, URN> {

  private final Class<ASPECT_UNION> _aspectUnionClass;

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
   * <p>This class allows the wildcard capture in {@link #addMany(Urn, List, AuditStamp)}</p>
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

    AspectUpdateLambda(ASPECT value) {
      this.aspectClass = (Class<ASPECT>) value.getClass();
      this.updateLambda = (ignored) -> value;
    }
  }

  private static final String DEFAULT_ID_NAMESPACE = "global";

  private static final IndefiniteRetention INDEFINITE_RETENTION = new IndefiniteRetention();

  private static final int DEFAULT_MAX_TRANSACTION_RETRY = 3;

  protected final BaseMetadataEventProducer _producer;
  protected final LocalDAOStorageConfig _storageConfig;

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

  // Flag for enabling reads and writes to local secondary index
  private boolean _enableLocalSecondaryIndex = false;

  // Enable updating multiple aspects within a single transaction
  private boolean _enableAtomicMultipleUpdate = false;

  private Clock _clock = Clock.systemUTC();

  /**
   * Constructor for BaseLocalDAO.
   *
   * @param aspectUnionClass containing union of all supported aspects. Must be a valid aspect union defined in
   *     com.linkedin.metadata.aspect
   * @param producer {@link BaseMetadataEventProducer} for the metadata event producer
   */
  public BaseLocalDAO(@Nonnull Class<ASPECT_UNION> aspectUnionClass, @Nonnull BaseMetadataEventProducer producer) {
    super(aspectUnionClass);
    _producer = producer;
    _storageConfig = LocalDAOStorageConfig.builder().build();
    _aspectUnionClass = aspectUnionClass;
  }

  /**
   * Constructor for BaseLocalDAO.
   *
   * @param producer {@link BaseMetadataEventProducer} for the metadata event producer
   * @param storageConfig {@link LocalDAOStorageConfig} containing storage config of full list of supported aspects
   */
  public BaseLocalDAO(@Nonnull BaseMetadataEventProducer producer, @Nonnull LocalDAOStorageConfig storageConfig) {
    super(storageConfig.getAspectStorageConfigMap().keySet());
    _producer = producer;
    _storageConfig = storageConfig;
    _aspectUnionClass = producer.getAspectUnionClass();
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

  /**
   * Sets if writes to local secondary index enabled.
   *
   * @deprecated Use {@link #enableLocalSecondaryIndex(boolean)} instead
   */
  public void setWriteToLocalSecondaryIndex(boolean writeToLocalSecondaryIndex) {
    _enableLocalSecondaryIndex = writeToLocalSecondaryIndex;
  }

  /**
   * Enables reads from and writes to local secondary index.
   */
  public void enableLocalSecondaryIndex(boolean enableLocalSecondaryIndex) {
    _enableLocalSecondaryIndex = enableLocalSecondaryIndex;
  }

  /**
   * Gets if reads and writes to local secondary index are enabled.
   */
  public boolean isLocalSecondaryIndexEnabled() {
    return _enableLocalSecondaryIndex;
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
   * @param <ASPECT> must be a supported aspect type in {@code ASPECT_UNION}
   * @return {@link AddResult} corresponding to the old and new value of metadata
   */
  private <ASPECT extends RecordTemplate> AddResult<ASPECT> addCommon(@Nonnull URN urn,
      @Nonnull AspectEntry<ASPECT> latest, @Nullable ASPECT newValue, @Nonnull Class<ASPECT> aspectClass,
      @Nonnull AuditStamp auditStamp, @Nonnull EqualityTester<ASPECT> equalityTester) {

    final ASPECT oldValue = latest.getAspect() == null ? null : latest.getAspect();
    final AuditStamp oldAuditStamp = latest.getExtraInfo() == null ? null : latest.getExtraInfo().getAudit();

    // Skip saving if there's no actual change
    if ((oldValue == null && newValue == null) || oldValue != null && newValue != null
        && equalityTester.equals(oldValue, newValue)) {
      return new AddResult<>(oldValue, oldValue, aspectClass);
    }

    // Save the newValue as the latest version
    long largestVersion = saveLatest(urn, aspectClass, oldValue, oldAuditStamp, newValue, auditStamp, latest.isSoftDeleted);

    // Apply retention policy
    applyRetention(urn, aspectClass, getRetention(aspectClass), largestVersion);

    // Save to local secondary index
    // TODO: add support for soft deleted aspects in local secondary index
    if (_enableLocalSecondaryIndex && newValue != null) {
      updateLocalIndex(urn, newValue, largestVersion);
    }

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

    // first check that all the aspects are valid
    aspectUpdateLambdas.stream().map(AspectUpdateLambda::getAspectClass).forEach(this::checkValidAspect);

    final List<AddResult<? extends RecordTemplate>> results;
    if (_enableAtomicMultipleUpdate) {
      // atomic multiple update enabled: run in a single transaction
      results = runInTransactionWithRetry(
          () -> aspectUpdateLambdas.stream().map(x -> aspectUpdateHelper(urn, x, auditStamp)).collect(Collectors.toList()), maxTransactionRetry);
    } else {
      // no atomic multiple updates: run each in its own transaction. This is the same as repeated calls to add
      results = aspectUpdateLambdas.stream().map(x -> runInTransactionWithRetry(() -> aspectUpdateHelper(urn, x, auditStamp), maxTransactionRetry))
          .collect(Collectors.toList());
    }

    // send the audit events etc
    return results.stream().map(x -> unwrapAddResultToUnion(urn, x)).collect(Collectors.toList());
  }

  public List<ASPECT_UNION> addMany(@Nonnull URN urn, @Nonnull List<? extends RecordTemplate> aspectValues, AuditStamp auditStamp) {
    List<AspectUpdateLambda<? extends RecordTemplate>> aspectUpdateLambdas = aspectValues.stream()
        .map(AspectUpdateLambda::new)
        .collect(Collectors.toList());

    return addMany(urn, aspectUpdateLambdas, auditStamp, DEFAULT_MAX_TRANSACTION_RETRY);
  }

  private <ASPECT extends RecordTemplate> AddResult<ASPECT> aspectUpdateHelper(URN urn, AspectUpdateLambda<ASPECT> updateTuple, AuditStamp auditStamp) {
    AspectEntry<ASPECT> latest = getLatest(urn, updateTuple.getAspectClass());
    Optional<ASPECT> oldValue = Optional.ofNullable(latest.getAspect());
    ASPECT newValue = updateTuple.getUpdateLambda().apply(oldValue);
    if (newValue == null) {
      throw new UnsupportedOperationException(String.format("Attempted to update %s with null aspect %s", urn, updateTuple.getAspectClass().getName()));
    }

    checkValidAspect(newValue.getClass());

    if (_modelValidationOnWrite) {
      validateAgainstSchema(newValue);
    }

    // Invoke pre-update hooks, if any
    if (_aspectPreUpdateHooksMap.containsKey(updateTuple.getAspectClass())) {
      _aspectPreUpdateHooksMap.get(updateTuple.getAspectClass()).forEach(hook -> hook.accept(urn, newValue));
    }

    return addCommon(urn, latest, newValue, updateTuple.getAspectClass(), auditStamp, getEqualityTester(updateTuple.getAspectClass()));
  }

  private <ASPECT extends RecordTemplate> ASPECT_UNION unwrapAddResultToUnion(URN urn, AddResult<ASPECT> result) {
    ASPECT rawResult = unwrapAddResult(urn, result);
    return ModelUtils.newEntityUnion(_aspectUnionClass, rawResult);
  }

  private <ASPECT extends RecordTemplate> ASPECT unwrapAddResult(URN urn, AddResult<ASPECT> result) {
    Class<ASPECT> aspectClass = result.getKlass();
    final ASPECT oldValue = result.getOldValue();
    final ASPECT newValue = result.getNewValue();

    // Produce MAE after a successful update
    if (_alwaysEmitAuditEvent || oldValue != newValue) {
      _producer.produceMetadataAuditEvent(urn, oldValue, newValue);
    }

    // TODO: Replace the previous step with the step below, after pipeline is fully migrated to aspect specific events.
    // Produce aspect specific MAE after a successful update
    if (_emitAspectSpecificAuditEvent) {
      if (_alwaysEmitAspectSpecificAuditEvent || oldValue != newValue) {
        _producer.produceAspectSpecificMetadataAuditEvent(urn, oldValue, newValue);
      }
    }
    // Invoke post-update hooks if there's any
    if (_aspectPostUpdateHooksMap.containsKey(aspectClass)) {
      _aspectPostUpdateHooksMap.get(aspectClass).forEach(hook -> hook.accept(urn, newValue));
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
    return add(urn, new AspectUpdateLambda<>(aspectClass, updateLambda), auditStamp, maxTransactionRetry);
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
    checkValidAspect(updateLambda.getAspectClass());

    final AddResult<ASPECT> result = runInTransactionWithRetry(() -> aspectUpdateHelper(urn, updateLambda, auditStamp),
        maxTransactionRetry);

    return unwrapAddResult(urn, result);
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

    checkValidAspect(aspectClass);

    runInTransactionWithRetry(() -> {
      final AspectEntry<ASPECT> latest = getLatest(urn, aspectClass);

      return addCommon(urn, latest, null, aspectClass, auditStamp, new DefaultEqualityTester<>());
    }, maxTransactionRetry);

    // TODO: add support for sending MAE for soft deleted aspects
  }

  /**
   * Similar to {@link #add(Urn, Class, Function, AuditStamp, int)} but uses the default maximum transaction retry.
   */
  @Nonnull
  public <ASPECT extends RecordTemplate> ASPECT add(@Nonnull URN urn, @Nonnull Class<ASPECT> aspectClass,
      @Nonnull Function<Optional<ASPECT>, ASPECT> updateLambda, @Nonnull AuditStamp auditStamp) {
    return add(urn, aspectClass, updateLambda, auditStamp, DEFAULT_MAX_TRANSACTION_RETRY);
  }

  /**
   * Similar to {@link #add(Urn, Class, Function, AuditStamp)} but takes the new value directly.
   */
  @Nonnull
  public <ASPECT extends RecordTemplate> ASPECT add(@Nonnull URN urn, @Nonnull ASPECT newValue,
      @Nonnull AuditStamp auditStamp) {
    return add(urn, (Class<ASPECT>) newValue.getClass(), ignored -> newValue, auditStamp);
  }

  /**
   * Similar to {@link #delete(Urn, Class, AuditStamp, int)} but uses the default maximum transaction retry.
   */
  @Nonnull
  public <ASPECT extends RecordTemplate> void delete(@Nonnull URN urn, @Nonnull Class<ASPECT> aspectClass,
      @Nonnull AuditStamp auditStamp) {
    delete(urn, aspectClass, auditStamp, DEFAULT_MAX_TRANSACTION_RETRY);
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
      @Nullable ASPECT newEntry, @Nonnull AuditStamp newAuditStamp, boolean isSoftDeleted);

  /**
   * Saves the new value of an aspect to local secondary index.
   *
   * @param urn the URN for the entity the aspect is attached to
   * @param newValue {@link RecordTemplate} of the new value of aspect
   * @param version version of the aspect
   */
  public abstract <ASPECT extends RecordTemplate> void updateLocalIndex(@Nonnull URN urn, @Nullable ASPECT newValue,
      long version);

  /**
   * Saves the new value of an aspect to entity tables. This is used when backfilling metadata from the old schema to
   * the new schema.
   *
   * @param urn the URN for the entity the aspect is attached to
   * @param aspectClass class of the aspect to backfill
   */
  public <ASPECT extends RecordTemplate> void updateEntityTables(@Nonnull URN urn, @Nonnull Class<ASPECT> aspectClass) {
    // Needs to be implemented by child classes if they want to use this functionality.
  }

  /**
   * Backfill local relationships from the new schema entity tables. This method is new/dual schema only. It should NOT
   * be used to add local relationships under normal circumstances and should ONLY be used in the case where the aspects
   * exist in the entity tables but the relationships don't exist in the local relationship tables (e.g. if the local
   * relationship tables got dropped or if they were set up after data was already ingested into entity tables).
   *
   * @param urn the URN for the entity the aspect (which the local relationship is derived from) is attached to
   * @param aspectClass class of the aspect to backfill
   */
  public <ASPECT extends RecordTemplate> void backfillLocalRelationshipsFromEntityTables(@Nonnull URN urn, @Nonnull Class<ASPECT> aspectClass) {
    // Needs to be implemented by child classes if they want to use this functionality.
  }

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
  public abstract List<URN> listUrns(@Nonnull IndexFilter indexFilter, @Nullable IndexSortCriterion indexSortCriterion,
      @Nullable URN lastUrn, int pageSize);

  /**
   * Similar to {@link #listUrns(IndexFilter, IndexSortCriterion, Urn, int)} but sorts lexicographically by the URN.
   */
  @Nonnull
  public List<URN> listUrns(@Nonnull IndexFilter indexFilter, @Nullable URN lastUrn, int pageSize) {
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
  public abstract <ASPECT extends RecordTemplate> ListResult<URN> listUrns(@Nonnull IndexFilter indexFilter,
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
      @Nonnull IndexFilter indexFilter, @Nullable IndexSortCriterion indexSortCriterion, @Nullable URN lastUrn,
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
      @Nonnull IndexFilter indexFilter, @Nullable IndexSortCriterion indexSortCriterion, int start, int pageSize) {

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
      @Nonnull Class<ASPECT> aspectClass, @Nonnull AuditStamp auditStamp, long version);

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
      long version, @Nonnull Timestamp oldTimestamp);

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
   * Similar to {@link #backfill(Set, Set)} but does a scoped backfill.
   *
   * @param mode backfill mode to scope the backfill process
   */
  @Nonnull
  public Map<URN, Map<Class<? extends RecordTemplate>, Optional<? extends RecordTemplate>>> backfill(
      @Nonnull BackfillMode mode, @Nonnull Set<Class<? extends RecordTemplate>> aspectClasses, @Nonnull Set<URN> urns) {
    checkValidAspects(aspectClasses);
    final Map<URN, Map<Class<? extends RecordTemplate>, Optional<? extends RecordTemplate>>> urnToAspects =
        get(aspectClasses, urns);
    urnToAspects.forEach((urn, aspects) -> {
      aspects.forEach((aspectClass, aspect) -> aspect.ifPresent(value -> backfill(mode, value, urn)));
    });
    return urnToAspects;
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
   * Emits backfill MAE for an aspect of an entity and/or backfills SCSI depending on the backfill mode.
   *
   * @param mode backfill mode
   * @param aspect aspect to backfill
   * @param urn {@link Urn} for the entity
   * @param <ASPECT> must be a supported aspect type in {@code ASPECT_UNION}.
   */
  private <ASPECT extends RecordTemplate> void backfill(@Nonnull BackfillMode mode, @Nonnull ASPECT aspect,
      @Nonnull URN urn) {
    if (_enableLocalSecondaryIndex && (mode == BackfillMode.SCSI_ONLY || mode == BackfillMode.BACKFILL_ALL)) {
      updateLocalIndex(urn, aspect, FIRST_VERSION);
    }

    if (mode == BackfillMode.MAE_ONLY || mode == BackfillMode.BACKFILL_ALL) {
      _producer.produceMetadataAuditEvent(urn, aspect, aspect);
      _producer.produceAspectSpecificMetadataAuditEvent(urn, aspect, aspect);
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
  public abstract Map<String, Long> countAggregate(@Nonnull IndexFilter indexFilter,
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
}
