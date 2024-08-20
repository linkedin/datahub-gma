package com.linkedin.metadata.restli;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.metadata.dao.BaseSearchDAO;
import com.linkedin.metadata.dao.SearchResult;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.dao.utils.QueryUtils;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.SearchResultMetadata;
import com.linkedin.metadata.query.SortCriterion;
import com.linkedin.metadata.query.SortOrder;
import com.linkedin.metadata.restli.lix.DummyResourceLix;
import com.linkedin.metadata.restli.lix.ResourceLix;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.CollectionResult;
import com.linkedin.restli.server.PagingContext;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.annotations.Finder;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.PagingContextParam;
import com.linkedin.restli.server.annotations.QueryParam;
import com.linkedin.restli.server.annotations.RestMethod;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.linkedin.metadata.restli.RestliConstants.*;


/**
 * A base class for the entity rest.li resource that supports CRUD + search methods.
 *
 * <p>See http://go/gma for more details
 *
 * @param <KEY> the resource's key type
 * @param <VALUE> the resource's value type
 * @param <URN> must be a valid {@link Urn} type for the snapshot
 * @param <SNAPSHOT> must be a valid snapshot type defined in com.linkedin.metadata.snapshot
 * @param <ASPECT_UNION> must be a valid aspect union type supported by the snapshot
 * @param <DOCUMENT> must be a valid search document type defined in com.linkedin.metadata.search
 * @param <INTERNAL_SNAPSHOT> must be a valid internal snapshot type defined in com.linkedin.metadata.snapshot
 * @param <INTERNAL_ASPECT_UNION> must be a valid internal aspect union type supported by the internal snapshot
 * @param <ASSET> must be a valid asset type defined in com.linkedin.metadata.asset
 */
public abstract class BaseSearchableEntityResource<
    // @formatter:off
    KEY,
    VALUE extends RecordTemplate,
    URN extends Urn,
    SNAPSHOT extends RecordTemplate,
    ASPECT_UNION extends UnionTemplate,
    DOCUMENT extends RecordTemplate,
    INTERNAL_SNAPSHOT extends RecordTemplate,
    INTERNAL_ASPECT_UNION extends UnionTemplate,
    ASSET extends RecordTemplate>
    // @formatter:on
    extends
    BaseEntityResource<KEY, VALUE, URN, SNAPSHOT, ASPECT_UNION, INTERNAL_SNAPSHOT, INTERNAL_ASPECT_UNION, ASSET> {

  private static final String DEFAULT_SORT_CRITERION_FIELD = "urn";

  public BaseSearchableEntityResource(@Nullable Class<SNAPSHOT> snapshotClass,
      @Nullable Class<ASPECT_UNION> aspectUnionClass, @Nonnull Class<INTERNAL_SNAPSHOT> internalSnapshotClass,
      @Nonnull Class<INTERNAL_ASPECT_UNION> internalAspectUnionClass, @Nonnull Class<ASSET> assetClass) {
    super(snapshotClass, aspectUnionClass, internalSnapshotClass, internalAspectUnionClass, assetClass);
  }

  public BaseSearchableEntityResource(@Nullable Class<SNAPSHOT> snapshotClass,
      @Nullable Class<ASPECT_UNION> aspectUnionClass, @Nonnull Class<URN> urnClass,
      @Nonnull Class<INTERNAL_SNAPSHOT> internalSnapshotClass,
      @Nonnull Class<INTERNAL_ASPECT_UNION> internalAspectUnionClass, @Nonnull Class<ASSET> assetClass) {
    super(snapshotClass, aspectUnionClass, urnClass, internalSnapshotClass, internalAspectUnionClass, assetClass,
        new DummyResourceLix());
  }

  public BaseSearchableEntityResource(@Nullable Class<SNAPSHOT> snapshotClass,
      @Nullable Class<ASPECT_UNION> aspectUnionClass, @Nonnull Class<URN> urnClass,
      @Nonnull Class<INTERNAL_SNAPSHOT> internalSnapshotClass,
      @Nonnull Class<INTERNAL_ASPECT_UNION> internalAspectUnionClass, @Nonnull Class<ASSET> assetClass,
      @Nonnull ResourceLix resourceLix) {
    super(snapshotClass, aspectUnionClass, urnClass, internalSnapshotClass, internalAspectUnionClass, assetClass,
        resourceLix);
  }


  /**
   * Returns a document-specific {@link BaseSearchDAO}.
   */
  @Nonnull
  protected abstract BaseSearchDAO<DOCUMENT> getSearchDAO();

  /**
   * Returns all {@link VALUE} objects from DB which are also available in the search index for the corresponding entity.
   * By default the list is sorted in ascending order of urn
   *
   * @param pagingContext pagination context
   * @param aspectNames list of aspect names that need to be returned
   * @param filter {@link Filter} to filter the search results
   * @param sortCriterion {@link SortCriterion} to sort the search results
   * @return list of all {@link VALUE} objects obtained from DB
   */
  @RestMethod.GetAll
  @Nonnull
  public Task<List<VALUE>> getAll(@Nonnull PagingContext pagingContext,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames,
      @QueryParam(PARAM_FILTER) @Optional @Nullable Filter filter,
      @QueryParam(PARAM_SORT) @Optional @Nullable SortCriterion sortCriterion) {
    final String urnClassName = _urnClass == null ? null : _urnClass.getCanonicalName();
    return getAll(pagingContext, aspectNames, filter, sortCriterion, _resourceLix.testGetAll(urnClassName));
  }

  @Nonnull
  protected Task<List<VALUE>> getAll(@Nonnull PagingContext pagingContext,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames,
      @QueryParam(PARAM_FILTER) @Optional @Nullable Filter filter,
      @QueryParam(PARAM_SORT) @Optional @Nullable SortCriterion sortCriterion, boolean isInternalModelsEnabled) {

    final Filter searchFilter = filter != null ? filter : QueryUtils.EMPTY_FILTER;
    final SortCriterion searchSortCriterion = sortCriterion != null ? sortCriterion
        : new SortCriterion().setField(DEFAULT_SORT_CRITERION_FIELD).setOrder(SortOrder.ASCENDING);
    final SearchResult<DOCUMENT> filterResult =
        getSearchDAO().filter(searchFilter, searchSortCriterion, pagingContext.getStart(), pagingContext.getCount());
    return RestliUtils.toTask(
        () -> getSearchQueryCollectionResult(filterResult, aspectNames, isInternalModelsEnabled).getElements());
  }

  @Finder(FINDER_SEARCH)
  @Nonnull
  public Task<CollectionResult<VALUE, SearchResultMetadata>> search(@QueryParam(PARAM_INPUT) @Nonnull String input,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames,
      @QueryParam(PARAM_FILTER) @Optional @Nullable Filter filter,
      @QueryParam(PARAM_SORT) @Optional @Nullable SortCriterion sortCriterion,
      @PagingContextParam @Nonnull PagingContext pagingContext) {
    final String urnClassName = _urnClass == null ? null : _urnClass.getCanonicalName();
    return search(input, aspectNames, filter, sortCriterion, pagingContext, _resourceLix.testSearch(urnClassName));
  }

  @Nonnull
  private Task<CollectionResult<VALUE, SearchResultMetadata>> search(@QueryParam(PARAM_INPUT) @Nonnull String input,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames,
      @QueryParam(PARAM_FILTER) @Optional @Nullable Filter filter,
      @QueryParam(PARAM_SORT) @Optional @Nullable SortCriterion sortCriterion,
      @PagingContextParam @Nonnull PagingContext pagingContext, boolean isInternalModelsEnabled) {

    final Filter searchFilter = filter != null ? filter : QueryUtils.EMPTY_FILTER;
    final SearchResult<DOCUMENT> searchResult =
        getSearchDAO().search(input, searchFilter, sortCriterion, pagingContext.getStart(), pagingContext.getCount());
    return RestliUtils.toTask(
        () -> getSearchQueryCollectionResult(searchResult, aspectNames, isInternalModelsEnabled));
  }

  /**
   * The v2 version of search method which supports preference parameter and returns the right search result metadata
   * when there are more than one filter criteria.
   * @param input search query
   * @param aspectNames list of aspect names that need to be returned
   * @param filter {@link Filter} to filter the search results
   * @param sortCriterion {@link SortCriterion} to sort the search results
   * @param preference preference of the shard copy on which to execute the search
   * @param pagingContext pagination context
   * @return list of all {@link VALUE} objects along with search result metadata
   */
  @Finder(FINDER_SEARCH_V2)
  @Nonnull
  public Task<CollectionResult<VALUE, SearchResultMetadata>> searchV2(@QueryParam(PARAM_INPUT) @Nonnull String input,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames,
      @QueryParam(PARAM_FILTER) @Optional @Nullable Filter filter,
      @QueryParam(PARAM_SORT) @Optional @Nullable SortCriterion sortCriterion,
      @QueryParam(PARAM_PREFERENCE) @Optional @Nullable String preference,
      @PagingContextParam @Nonnull PagingContext pagingContext) {
    final String urnClassName = _urnClass == null ? null : _urnClass.getCanonicalName();
    return searchV2(input, aspectNames, filter, sortCriterion, preference, pagingContext,
        _resourceLix.testSearchV2(urnClassName));
  }

  @Nonnull
  private Task<CollectionResult<VALUE, SearchResultMetadata>> searchV2(@QueryParam(PARAM_INPUT) @Nonnull String input,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames,
      @QueryParam(PARAM_FILTER) @Optional @Nullable Filter filter,
      @QueryParam(PARAM_SORT) @Optional @Nullable SortCriterion sortCriterion,
      @QueryParam(PARAM_PREFERENCE) @Optional @Nullable String preference,
      @PagingContextParam @Nonnull PagingContext pagingContext, boolean isInternalModelsEnabled) {

    final Filter searchFilter = filter != null ? filter : QueryUtils.EMPTY_FILTER;
    final SearchResult<DOCUMENT> searchResult =
        getSearchDAO().searchV2(input, searchFilter, sortCriterion, preference, pagingContext.getStart(), pagingContext.getCount());
    return RestliUtils.toTask(
        () -> getSearchQueryCollectionResult(searchResult, aspectNames, isInternalModelsEnabled));
  }

  @Action(name = ACTION_AUTOCOMPLETE)
  @Nonnull
  public Task<AutoCompleteResult> autocomplete(@ActionParam(PARAM_QUERY) @Nonnull String query,
      @ActionParam(PARAM_FIELD) @Optional @Nullable String field,
      @ActionParam(PARAM_FILTER) @Optional @Nullable Filter filter,
      @ActionParam(PARAM_LIMIT) int limit) {
    return RestliUtils.toTask(() -> getSearchDAO().autoComplete(query, field, filter, limit));
  }

  /**
   * Fetch aspect values from MySQL DB based on search result.
   * @param searchResult Search result returned from search infra, such as Elasticsearch.
   * @return CollectionResult which contains: 1. aspect values fetched from MySQL DB, 2. Total count 3. Search result metadata.
   */
  @Nonnull
  private CollectionResult<VALUE, SearchResultMetadata> getSearchQueryCollectionResult(@Nonnull SearchResult<DOCUMENT> searchResult,
      @Nullable String[] aspectNames, boolean isInternalModelsEnabled) {

    final List<URN> matchedUrns = searchResult.getDocumentList()
        .stream()
        .map(d -> (URN) ModelUtils.getUrnFromDocument(d))
        .collect(Collectors.toList());
    final Map<URN, VALUE> urnValueMap =
        getInternalNonEmpty(matchedUrns, parseAspectsParam(aspectNames, isInternalModelsEnabled),
            isInternalModelsEnabled);
    final List<URN> existingUrns = matchedUrns.stream().filter(urn -> urnValueMap.containsKey(urn)).collect(Collectors.toList());
    return new CollectionResult<>(
        existingUrns.stream().map(urn -> urnValueMap.get(urn)).collect(Collectors.toList()),
        searchResult.getTotalCount(),
        searchResult.getSearchResultMetadata().setUrns(new UrnArray(existingUrns.stream().map(urn -> (Urn) urn).collect(Collectors.toList())))
    );
  }
}
