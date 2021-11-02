package com.linkedin.metadata.restli;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.LongMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.metadata.backfill.BackfillMode;
import com.linkedin.metadata.dao.AspectKey;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.ListResult;
import com.linkedin.metadata.dao.UrnAspectEntry;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.query.IndexCriterion;
import com.linkedin.metadata.query.IndexCriterionArray;
import com.linkedin.metadata.query.IndexFilter;
import com.linkedin.metadata.query.IndexGroupByCriterion;
import com.linkedin.metadata.query.IndexSortCriterion;
import com.linkedin.metadata.query.ListResultMetadata;
import com.linkedin.metadata.query.MapMetadata;
import com.linkedin.parseq.Task;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.server.CollectionResult;
import com.linkedin.restli.server.PagingContext;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.annotations.Finder;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.PagingContextParam;
import com.linkedin.restli.server.annotations.QueryParam;
import com.linkedin.restli.server.annotations.RestMethod;
import com.linkedin.restli.server.resources.CollectionResourceTaskTemplate;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.linkedin.metadata.dao.BaseReadDAO.*;
import static com.linkedin.metadata.restli.RestliConstants.*;


/**
 * A base class for the entity rest.li resource, that supports CRUD methods.
 *
 * <p>See http://go/gma for more details
 *
 * @param <KEY> the resource's key type
 * @param <VALUE> the resource's value type
 * @param <URN> must be a valid {@link Urn} type for the snapshot
 * @param <SNAPSHOT> must be a valid snapshot type defined in com.linkedin.metadata.snapshot
 * @param <ASPECT_UNION> must be a valid aspect union type supported by the snapshot
 */
public abstract class BaseEntityResource<
    // @formatter:off
    KEY,
    VALUE extends RecordTemplate,
    URN extends Urn,
    SNAPSHOT extends RecordTemplate,
    ASPECT_UNION extends UnionTemplate>
    // @formatter:on
    extends CollectionResourceTaskTemplate<KEY, VALUE> {

  private static final BaseRestliAuditor DUMMY_AUDITOR = new DummyRestliAuditor(Clock.systemUTC());

  private final Class<SNAPSHOT> _snapshotClass;
  private final Class<ASPECT_UNION> _aspectUnionClass;
  private final Set<Class<? extends RecordTemplate>> _supportedAspectClasses;
  private final Class<URN> _urnClass;

  public BaseEntityResource(@Nonnull Class<SNAPSHOT> snapshotClass, @Nonnull Class<ASPECT_UNION> aspectUnionClass) {
    this(snapshotClass, aspectUnionClass, null);
  }

  public BaseEntityResource(@Nonnull Class<SNAPSHOT> snapshotClass,
      @Nonnull Class<ASPECT_UNION> aspectUnionClass, @Nullable Class<URN> urnClass) {
    super();
    ModelUtils.validateSnapshotAspect(snapshotClass, aspectUnionClass);
    _snapshotClass = snapshotClass;
    _aspectUnionClass = aspectUnionClass;
    _supportedAspectClasses = ModelUtils.getValidAspectTypes(_aspectUnionClass);
    _urnClass = urnClass;
  }

  /**
   * Returns a {@link BaseRestliAuditor} for this resource.
   */
  @Nonnull
  protected BaseRestliAuditor getAuditor() {
    return DUMMY_AUDITOR;
  }

  /**
   * Returns an aspect-specific {@link BaseLocalDAO}.
   */
  @Nonnull
  protected abstract BaseLocalDAO<ASPECT_UNION, URN> getLocalDAO();

  /**
   * Creates an URN from its string representation.
   */
  @Nonnull
  protected abstract URN createUrnFromString(@Nonnull String urnString) throws Exception;

  /**
   * Converts a resource key to URN.
   */
  @Nonnull
  protected abstract URN toUrn(@Nonnull KEY key);

  /**
   * Converts a URN to a resource's key.
   */
  @Nonnull
  protected abstract KEY toKey(@Nonnull URN urn);

  /**
   * Converts a snapshot to resource's value.
   */
  @Nonnull
  protected abstract VALUE toValue(@Nonnull SNAPSHOT snapshot);

  /**
   * Converts a resource's value to a snapshot.
   */
  @Nonnull
  protected abstract SNAPSHOT toSnapshot(@Nonnull VALUE value, @Nonnull URN urn);

  /**
   * Retrieves the value for an entity that is made up of latest versions of specified aspects.
   */
  @RestMethod.Get
  @Nonnull
  public Task<VALUE> get(@Nonnull KEY id,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {

    return RestliUtils.toTask(() -> {
      final URN urn = toUrn(id);
      final VALUE value = getInternalNonEmpty(Collections.singleton(urn), parseAspectsParam(aspectNames)).get(urn);
      if (value == null) {
        throw RestliUtils.resourceNotFoundException();
      }
      return value;
    });
  }

  /**
   * Similar to {@link #get(Object, String[])} but for multiple entities.
   */
  @RestMethod.BatchGet
  @Nonnull
  public Task<Map<KEY, VALUE>> batchGet(
      @Nonnull Set<KEY> ids,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    return RestliUtils.toTask(() -> {
      final Map<URN, KEY> urnMap =
          ids.stream().collect(Collectors.toMap(id -> toUrn(id), Function.identity()));
      return getInternal(urnMap.keySet(), parseAspectsParam(aspectNames)).entrySet()
          .stream()
          .collect(
              Collectors.toMap(e -> urnMap.get(e.getKey()), e -> e.getValue()));
    });
  }

  /**
   * An action method for automated ingestion pipeline.
   */
  @Action(name = ACTION_INGEST)
  @Nonnull
  public Task<Void> ingest(@ActionParam(PARAM_SNAPSHOT) @Nonnull SNAPSHOT snapshot) {
    return ingestInternal(snapshot, Collections.emptySet());
  }

  @Nonnull
  protected Task<Void> ingestInternal(@Nonnull SNAPSHOT snapshot,
      @Nonnull Set<Class<? extends RecordTemplate>> aspectsToIgnore) {
    return RestliUtils.toTask(() -> {
      final URN urn = (URN) ModelUtils.getUrnFromSnapshot(snapshot);
      final AuditStamp auditStamp = getAuditor().requestAuditStamp(getContext().getRawRequestContext());
      ModelUtils.getAspectsFromSnapshot(snapshot).stream().forEach(aspect -> {
        if (!aspectsToIgnore.contains(aspect.getClass())) {
          getLocalDAO().add(urn, aspect, auditStamp);
        }
      });
      return null;
    });
  }

  /**
   * An action method for getting a snapshot of aspects for an entity.
   */
  @Action(name = ACTION_GET_SNAPSHOT)
  @Nonnull
  public Task<SNAPSHOT> getSnapshot(@ActionParam(PARAM_URN) @Nonnull String urnString,
      @ActionParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {

    return RestliUtils.toTask(() -> {
      final URN urn = parseUrnParam(urnString);
      final Set<AspectKey<URN, ? extends RecordTemplate>> keys = parseAspectsParam(aspectNames).stream()
          .map(aspectClass -> new AspectKey<>(aspectClass, urn, LATEST_VERSION))
          .collect(Collectors.toSet());

      final List<UnionTemplate> aspects = getLocalDAO().get(keys)
          .values()
          .stream()
          .filter(java.util.Optional::isPresent)
          .map(aspect -> ModelUtils.newAspectUnion(_aspectUnionClass, aspect.get()))
          .collect(Collectors.toList());

      return ModelUtils.newSnapshot(_snapshotClass, urn, aspects);
    });
  }

  /**
   * An action method for emitting MAE backfill messages for an entity.
   *
   * @deprecated Use {@link #backfill(String[], String[])} instead
   */
  @Action(name = ACTION_BACKFILL_LEGACY)
  @Nonnull
  public Task<BackfillResult> backfill(@ActionParam(PARAM_URN) @Nonnull String urnString,
      @ActionParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {

    return RestliUtils.toTask(() -> {
      final URN urn = parseUrnParam(urnString);
      final List<String> backfilledAspects = parseAspectsParam(aspectNames).stream()
          .map(aspectClass -> getLocalDAO().backfill(aspectClass, urn))
          .filter(optionalAspect -> optionalAspect.isPresent())
          .map(optionalAspect -> ModelUtils.getAspectName(optionalAspect.get().getClass()))
          .collect(Collectors.toList());
      return new BackfillResult().setEntities(new BackfillResultEntityArray(Collections.singleton(
          new BackfillResultEntity().setUrn(urn).setAspects(new StringArray(backfilledAspects))
      )));
    });
  }

  /**
   * An action method for emitting MAE backfill messages for a set of entities.
   */
  @Action(name = ACTION_BACKFILL_WITH_URNS)
  @Nonnull
  public Task<BackfillResult> backfill(@ActionParam(PARAM_URNS) @Nonnull String[] urns,
                                       @ActionParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {

    return RestliUtils.toTask(() -> {
      final Set<URN> urnSet = Arrays.stream(urns).map(urnString -> parseUrnParam(urnString)).collect(Collectors.toSet());
      return buildBackfillResult(getLocalDAO().backfill(parseAspectsParam(aspectNames), urnSet));
    });
  }

  /**
   * An action method for emitting MAE backfill messages for a set of entities using SCSI.
   */
  @Action(name = ACTION_BACKFILL)
  @Nonnull
  public Task<BackfillResult> backfill(@ActionParam(PARAM_MODE) @Nonnull BackfillMode mode,
      @ActionParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames,
      @ActionParam(PARAM_URN) @Optional @Nullable String lastUrn,
      @ActionParam(PARAM_LIMIT) int limit) {

    return RestliUtils.toTask(() ->
            buildBackfillResult(getLocalDAO().backfill(mode, parseAspectsParam(aspectNames),
                    _urnClass,
                    parseUrnParam(lastUrn),
                    limit)));
  }

  @Nonnull
  private BackfillResult buildBackfillResult(@Nonnull Map<URN, Map<Class<? extends RecordTemplate>,
          java.util.Optional<? extends RecordTemplate>>> backfilledAspects) {

    final Set<URN> urns = new TreeSet<>(Comparator.comparing(Urn::toString));
    urns.addAll(backfilledAspects.keySet());
    return new BackfillResult().setEntities(new BackfillResultEntityArray(
            urns.stream().map(urn -> buildBackfillResultEntity(urn, backfilledAspects.get(urn)))
                    .collect(Collectors.toList())));
  }

  @Nonnull
  private BackfillResultEntity buildBackfillResultEntity(@Nonnull URN urn, Map<Class<? extends RecordTemplate>,
          java.util.Optional<? extends RecordTemplate>> aspectMap) {

    return new BackfillResultEntity()
            .setUrn(urn)
            .setAspects(new StringArray(aspectMap.entrySet().stream()
                    .filter(aspect -> aspect.getValue().isPresent())
                    .map(aspect -> aspect.getKey().getCanonicalName())
                    .collect(Collectors.toList()))
            );
  }

  /**
   * For strongly consistent local secondary index, this provides {@link IndexFilter} which uses FQCN of the entity urn to filter
   * on the aspect field of the index table. This serves the purpose of returning urns that are of given entity type from index table.
   */
  @Nonnull
  private IndexFilter getDefaultIndexFilter() {
    if (_urnClass == null) {
      throw new UnsupportedOperationException("Urn class has not been defined in BaseEntityResource");
    }
    final IndexCriterion indexCriterion = new IndexCriterion().setAspect(_urnClass.getCanonicalName());
    return new IndexFilter().setCriteria(new IndexCriterionArray(indexCriterion));
  }

  /**
   * An action method for getting filtered urns from local secondary index.
   * If no filter conditions are provided, then it returns urns of given entity type.
   *
   * @param indexFilter {@link IndexFilter} that defines the filter conditions
   * @param lastUrn last urn of the previous fetched page. For the first page, this should be set as NULL
   * @param limit maximum number of distinct urns to return
   * @return Array of urns represented as string
   *
   * @deprecated Use {@link #filter(IndexFilter, String[], String, PagingContext)} instead
   */
  @Action(name = ACTION_LIST_URNS_FROM_INDEX)
  @Nonnull
  public Task<String[]> listUrnsFromIndex(@ActionParam(PARAM_FILTER) @Optional @Nullable IndexFilter indexFilter,
      @ActionParam(PARAM_URN) @Optional @Nullable String lastUrn, @ActionParam(PARAM_LIMIT) int limit) {

    final IndexFilter filter = indexFilter == null ? getDefaultIndexFilter() : indexFilter;

    return RestliUtils.toTask(() ->
        getLocalDAO()
            .listUrns(filter, parseUrnParam(lastUrn), limit)
            .stream()
            .map(Urn::toString)
            .collect(Collectors.toList())
            .toArray(new String[0]));
  }

  /**
   * Returns a list of values of multiple entities from urn aspect entries.
   *
   * @param urnAspectEntries entries used to make values
   * @return ordered list of values of multiple entities
   */
  @Nonnull
  private List<VALUE> getUrnAspectValues(List<UrnAspectEntry<URN>> urnAspectEntries) {
    final Map<URN, List<UnionTemplate>> urnAspectsMap = new LinkedHashMap<>();
    for (UrnAspectEntry<URN> entry : urnAspectEntries) {
      urnAspectsMap.compute(entry.getUrn(), (k, v) -> {
        if (v == null) {
          v = new ArrayList<>();
        }
        v.addAll(entry.getAspects()
            .stream()
            .map(recordTemplate -> ModelUtils.newAspectUnion(_aspectUnionClass, recordTemplate))
            .collect(Collectors.toList()));
        return v;
      });
    }

    return urnAspectsMap.entrySet()
        .stream()
        .map(e -> toValue(newSnapshot(e.getKey(), e.getValue())))
        .collect(Collectors.toList());
  }

  /**
   * Returns ordered list of values of multiple entities obtained after filtering urns
   * from local secondary index. The returned list is ordered by the sort criterion but defaults to sorting
   * lexicographically by the string representation of the URN.
   * The list of values is in the same order as the list of urns contained in {@link ListResultMetadata}.
   *
   * @param aspectClasses set of aspect classes that needs to be populated in the values
   * @param filter {@link IndexFilter} that defines the filter conditions
   * @param indexSortCriterion {@link IndexSortCriterion} that defines the sort conditions
   * @param lastUrn last urn of the previous fetched page. For the first page, this should be set as NULL
   * @param count defining the maximum number of values returned
   * @return ordered list of values of multiple entities
   */
  @Nonnull
  private List<VALUE> filterAspects(
      @Nonnull Set<Class<? extends RecordTemplate>> aspectClasses, @Nonnull IndexFilter filter,
      @Nullable IndexSortCriterion indexSortCriterion, @Nullable String lastUrn, int count) {

    final List<UrnAspectEntry<URN>> urnAspectEntries =
        getLocalDAO().getAspects(aspectClasses, filter, indexSortCriterion, parseUrnParam(lastUrn), count);

    return getUrnAspectValues(urnAspectEntries);
  }

  /**
   * Similar to {@link #filterAspects(Set, IndexFilter, IndexSortCriterion, String, int)} but
   * takes in a start offset and returns a list result with pagination information.
   *
   * @param start defining the paging start
   * @param count defining the maximum number of values returned
   * @return a {@link ListResult} containing a list of version numbers and other pagination information
   */
  @Nonnull
  private ListResult<VALUE> filterAspects(
      @Nonnull Set<Class<? extends RecordTemplate>> aspectClasses, @Nonnull IndexFilter filter,
      @Nullable IndexSortCriterion indexSortCriterion, int start, int count) {

    final ListResult<UrnAspectEntry<URN>> listResult =
        getLocalDAO().getAspects(aspectClasses, filter, indexSortCriterion, start, count);
    final List<UrnAspectEntry<URN>> urnAspectEntries = listResult.getValues();
    final List<VALUE> values = getUrnAspectValues(urnAspectEntries);

    return ListResult.<VALUE>builder()
        .values(values)
        .metadata(listResult.getMetadata())
        .nextStart(listResult.getNextStart())
        .havingMore(listResult.isHavingMore())
        .totalCount(listResult.getTotalCount())
        .totalPageCount(listResult.getTotalPageCount())
        .pageSize(listResult.getPageSize())
        .build();
  }

  /**
   * Returns ordered list of values of multiple entities obtained after filtering urns
   * from local secondary index. The returned list is ordered by the sort criterion but defaults to
   * being ordered lexicographically by the string representation of the URN.
   * The values returned do not contain any metadata aspect, only parts of the urn (if applicable).
   * The list of values is in the same order as the list of urns contained in {@link ListResultMetadata}.
   *
   * @param filter {@link IndexFilter} that defines the filter conditions
   * @param indexSortCriterion {@link IndexSortCriterion} that defines the sorting conditions
   * @param lastUrn last urn of the previous fetched page
   * @param count defining the maximum number of values returned
   * @return ordered list of values of multiple entities
   */
  @Nonnull
  private List<VALUE> filterUrns(@Nonnull IndexFilter filter, @Nullable IndexSortCriterion indexSortCriterion,
      @Nullable String lastUrn, int count) {

    final List<URN> urns = getLocalDAO().listUrns(filter, indexSortCriterion, parseUrnParam(lastUrn), count);
    return urns.stream().map(urn -> toValue(newSnapshot(urn))).collect(Collectors.toList());
  }

  /**
   * Similar to {@link #filterUrns(IndexFilter, IndexSortCriterion, String, int)} but
   * takes in a start offset and returns a list result with pagination information.
   *
   * @param start defining the paging start
   * @param count defining the maximum number of values returned
   * @return a {@link ListResult} containing an ordered list of values of multiple entities and other pagination information
   */
  @Nonnull
  private ListResult<VALUE> filterUrns(@Nonnull IndexFilter filter, @Nullable IndexSortCriterion indexSortCriterion,
      int start, int count) {

    final ListResult<URN> listResult = getLocalDAO().listUrns(filter, indexSortCriterion, start, count);
    final List<URN> urns = listResult.getValues();
    final List<VALUE> urnValues = urns.stream().map(urn -> toValue(newSnapshot(urn))).collect(Collectors.toList());

    return ListResult.<VALUE>builder()
        .values(urnValues)
        .metadata(listResult.getMetadata())
        .nextStart(listResult.getNextStart())
        .havingMore(listResult.isHavingMore())
        .totalCount(listResult.getTotalCount())
        .totalPageCount(listResult.getTotalPageCount())
        .pageSize(listResult.getPageSize())
        .build();
  }

  /**
   * Retrieves the values for multiple entities obtained after filtering urns from local secondary index. Here the value is
   * made up of latest versions of specified aspects. If no aspects are provided, value model will not contain any metadata aspect.
   * {@link ListResultMetadata} contains relevant list of urns.
   *
   * <p>If no filter conditions are provided, then it returns values of given entity type.
   *
   * <p>Note: Only one of the filter finders should be implemented in your resource implementation.
   *
   * @param indexFilter {@link IndexFilter} that defines the filter conditions
   * @param indexSortCriterion {@link IndexSortCriterion} that defines the sorting conditions
   * @param aspectNames list of aspects to be returned in the VALUE model
   * @param lastUrn last urn of the previous fetched page. For the first page, this should be set as NULL
   * @param count defining the maximum number of urns to return
   * @return {@link CollectionResult} containing values along with the associated urns in {@link ListResultMetadata}
   */
  @Finder(FINDER_FILTER)
  @Nonnull
  public Task<List<VALUE>> filter(
      @QueryParam(PARAM_FILTER) @Optional @Nullable IndexFilter indexFilter,
      @QueryParam(PARAM_SORT) @Optional @Nullable IndexSortCriterion indexSortCriterion,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames,
      @QueryParam(PARAM_URN) @Optional @Nullable String lastUrn,
      @QueryParam(PARAM_COUNT) @Optional("10") int count) {

    final IndexFilter filter = indexFilter == null ? getDefaultIndexFilter() : indexFilter;

    return RestliUtils.toTask(() -> {
      final Set<Class<? extends RecordTemplate>> aspectClasses = parseAspectsParam(aspectNames);
      if (aspectClasses.isEmpty()) {
        return filterUrns(filter, indexSortCriterion, lastUrn, count);
      } else {
        return filterAspects(aspectClasses, filter, indexSortCriterion, lastUrn, count);
      }
    });
  }

  /**
   * Similar to {@link #filter(IndexFilter, IndexSortCriterion, String[], String, int)} but
   * uses null sorting criterion.
   *
   * <p>Note: Only one of the filter finders should be implemented in your resource implementation.
   *
   * @deprecated Use {@link #filter(IndexFilter, IndexSortCriterion, String[], String, int)} instead
   */
  @Finder(FINDER_FILTER)
  @Nonnull
  public Task<List<VALUE>> filter(
      @QueryParam(PARAM_FILTER) @Optional @Nullable IndexFilter indexFilter,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames,
      @QueryParam(PARAM_URN) @Optional @Nullable String lastUrn,
      @PagingContextParam @Nonnull PagingContext pagingContext) {
    return filter(indexFilter, null, aspectNames, lastUrn, pagingContext.getCount());
  }

  /**
   * Similar to {@link #filter(IndexFilter, IndexSortCriterion, String[], String, int)} but
   * returns a list result with pagination information.
   *
   * <p>Note: Only one of the filter finders should be implemented in your resource implementation.
   *
   * @param pagingContext {@link PagingContext} defines the paging start and count
   * @return {@link ListResult} containing values along with the associated urns in {@link ListResultMetadata} and
   *        pagination information
   */
  @Finder(FINDER_FILTER)
  @Nonnull
  public Task<ListResult<VALUE>> filter(
      @QueryParam(PARAM_FILTER) @Optional @Nullable IndexFilter indexFilter,
      @QueryParam(PARAM_SORT) @Optional @Nullable IndexSortCriterion indexSortCriterion,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames,
      @PagingContextParam @Nonnull PagingContext pagingContext) {

    final IndexFilter filter = indexFilter == null ? getDefaultIndexFilter() : indexFilter;

    return RestliUtils.toTask(() -> {
      final Set<Class<? extends RecordTemplate>> aspectClasses = parseAspectsParam(aspectNames);
      if (aspectClasses.isEmpty()) {
        return filterUrns(filter, indexSortCriterion, pagingContext.getStart(), pagingContext.getCount());
      } else {
        return filterAspects(aspectClasses, filter, indexSortCriterion, pagingContext.getStart(), pagingContext.getCount());
      }
    });
  }

  /**
   * Gets a collection result with count aggregate metadata, which has the count of an aggregation
   * specified by the aspect and field to group by.
   *
   * @param indexFilter {@link IndexFilter} that defines the filter conditions
   * @param indexGroupByCriterion {@link IndexGroupByCriterion} that defines the aspect to group by
   * @return {@link CollectionResult} containing metadata that has a map of the field values to their count
   */
  @Finder(FINDER_COUNT_AGGREGATE)
  @Nonnull
  public Task<CollectionResult<EmptyRecord, MapMetadata>> countAggregateFilter(
      @QueryParam(PARAM_FILTER) @Optional @Nullable IndexFilter indexFilter,
      @QueryParam(PARAM_GROUP) IndexGroupByCriterion indexGroupByCriterion
  ) {
    final IndexFilter filter = indexFilter == null ? getDefaultIndexFilter() : indexFilter;

    return RestliUtils.toTask(() -> {
      Map<String, Long> countAggregateMap = getLocalDAO().countAggregate(indexFilter, indexGroupByCriterion);
      MapMetadata mapMetadata = new MapMetadata().setLongMap(new LongMap(countAggregateMap));
      return new CollectionResult<EmptyRecord, MapMetadata>(new ArrayList<>(), mapMetadata);
    });
  }

  /**
   * Gets the count of an aggregation specified by the aspect and field to group by.
   * @param indexFilter {@link IndexFilter} that defines the filter conditions
   * @param indexGroupByCriterion {@link IndexGroupByCriterion} that defines the aspect to group by
   * @return map of the field values to their count
   *
   * @deprecated Use {@link #countAggregateFilter(IndexFilter, IndexGroupByCriterion)} instead
   */
  @Action(name = ACTION_COUNT_AGGREGATE)
  @Nonnull
  public Task<Map<String, Long>> countAggregate(
      @ActionParam(PARAM_FILTER) @Optional @Nullable IndexFilter indexFilter,
      @ActionParam(PARAM_GROUP) IndexGroupByCriterion indexGroupByCriterion
  ) {
    final IndexFilter filter = indexFilter == null ? getDefaultIndexFilter() : indexFilter;

    return RestliUtils.toTask(() -> getLocalDAO().countAggregate(indexFilter, indexGroupByCriterion));
  }

  @Nonnull
  protected Set<Class<? extends RecordTemplate>> parseAspectsParam(@Nullable String[] aspectNames) {
    if (aspectNames == null) {
      return _supportedAspectClasses;
    }
    return Arrays.asList(aspectNames).stream().map(ModelUtils::getAspectClass).collect(Collectors.toSet());
  }

  /**
   * Returns a map of {@link VALUE} models given the collection of {@link URN}s and set of aspect classes.
   *
   * @param urns collection of urns
   * @param aspectClasses set of aspect classes
   * @return All {@link VALUE} objects keyed by {@link URN} obtained from DB
   */
  @Nonnull
  protected Map<URN, VALUE> getInternal(@Nonnull Collection<URN> urns,
      @Nonnull Set<Class<? extends RecordTemplate>> aspectClasses) {
    return getUrnAspectMap(urns, aspectClasses).entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> toValue(newSnapshot(e.getKey(), e.getValue()))));
  }

  /**
   * Similar to {@link #getInternal(Collection, Set)} but filter out {@link URN}s which are not in the DB.
   */
  @Nonnull
  protected Map<URN, VALUE> getInternalNonEmpty(@Nonnull Collection<URN> urns,
      @Nonnull Set<Class<? extends RecordTemplate>> aspectClasses) {
    return getUrnAspectMap(urns, aspectClasses).entrySet()
        .stream()
        .filter(e -> !e.getValue().isEmpty())
        .collect(Collectors.toMap(Map.Entry::getKey, e -> toValue(newSnapshot(e.getKey(), e.getValue()))));
  }

  @Nonnull
  private Map<URN, List<UnionTemplate>> getUrnAspectMap(@Nonnull Collection<URN> urns,
      @Nonnull Set<Class<? extends RecordTemplate>> aspectClasses) {
    // Construct the keys to retrieve latest version of all supported aspects for all URNs.
    final Set<AspectKey<URN, ? extends RecordTemplate>> keys = urns.stream()
        .map(urn -> aspectClasses.stream()
            .map(clazz -> new AspectKey<>(clazz, urn, LATEST_VERSION))
            .collect(Collectors.toList()))
        .flatMap(List::stream)
        .collect(Collectors.toSet());

    final Map<URN, List<UnionTemplate>> urnAspectsMap =
        urns.stream().collect(Collectors.toMap(Function.identity(), urn -> new ArrayList<>()));

    getLocalDAO().get(keys)
        .forEach((key, aspect) -> aspect.ifPresent(
            metadata -> urnAspectsMap.get(key.getUrn()).add(ModelUtils.newAspectUnion(_aspectUnionClass, metadata))));

    return urnAspectsMap;
  }

  @Nonnull
  private SNAPSHOT newSnapshot(@Nonnull URN urn, @Nonnull List<UnionTemplate> aspects) {
    return ModelUtils.newSnapshot(_snapshotClass, urn, aspects);
  }

  /**
   * Creates a snapshot of the entity with no aspects set, just the URN.
   */
  @Nonnull
  private SNAPSHOT newSnapshot(@Nonnull URN urn) {
    return ModelUtils.newSnapshot(_snapshotClass, urn, Collections.emptyList());
  }

  @Nullable
  private URN parseUrnParam(@Nullable String urnString) {
    if (urnString == null) {
      return null;
    }

    try {
      return createUrnFromString(urnString);
    } catch (Exception e) {
      throw RestliUtils.badRequestException("Invalid URN: " + urnString);
    }
  }
}
