package com.linkedin.metadata.dao.search;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataList;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.LongMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.BaseSearchDAO;
import com.linkedin.metadata.dao.SearchResult;
import com.linkedin.metadata.dao.exception.ESQueryException;
import com.linkedin.metadata.dao.tracking.BaseTrackingManager;
import com.linkedin.metadata.dao.tracking.DummyTrackingManager;
import com.linkedin.metadata.dao.tracking.TrackingUtils;
import com.linkedin.metadata.dao.utils.ESUtils;
import com.linkedin.metadata.dao.utils.QueryUtils;
import com.linkedin.metadata.query.AggregationMetadata;
import com.linkedin.metadata.query.AggregationMetadataArray;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.Criterion;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.MatchMetadata;
import com.linkedin.metadata.query.MatchMetadataArray;
import com.linkedin.metadata.query.MatchedField;
import com.linkedin.metadata.query.MatchedFieldArray;
import com.linkedin.metadata.query.SearchResultMetadata;
import com.linkedin.metadata.query.SortCriterion;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.filter.ParsedFilter;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;

import static com.linkedin.metadata.dao.tracking.TrackingUtils.random;
import static com.linkedin.metadata.dao.tracking.TrackingUtils.ProcessType.*;
import static com.linkedin.metadata.dao.utils.SearchUtils.getQueryBuilderFromCriterion;


/**
 * A search DAO for Elasticsearch backend.
 */
@Slf4j
public class ESSearchDAO<DOCUMENT extends RecordTemplate> extends BaseSearchDAO<DOCUMENT> {

  private static final Integer DEFAULT_TERM_BUCKETS_SIZE_100 = 100;
  private static final String URN_FIELD = "urn";

  private RestHighLevelClient _client;
  private BaseSearchConfig<DOCUMENT> _config;
  private BaseESAutoCompleteQuery _autoCompleteQueryForLowCardFields;
  private BaseESAutoCompleteQuery _autoCompleteQueryForHighCardFields;
  private BaseTrackingManager _baseTrackingManager;
  private int _maxTermBucketSize = DEFAULT_TERM_BUCKETS_SIZE_100;
  private int _lowerBoundHits = Integer.MAX_VALUE;

  // Regex patterns for matching original field names to the highlighted field name returned by elasticsearch
  private Map<String, Pattern> _highlightedFieldNamePatterns;
  // @formatter:off
  private static final ImmutableList<TrackingUtils.ProcessType> PROCESS_STATES =
      ImmutableList.of(
          AUTOCOMPLETE_QUERY_END,
          AUTOCOMPLETE_QUERY_FAIL,
          AUTOCOMPLETE_QUERY_START,
          FILTER_QUERY_END,
          FILTER_QUERY_FAIL,
          FILTER_QUERY_START,
          SEARCH_QUERY_END,
          SEARCH_QUERY_FAIL,
          SEARCH_QUERY_START);
  // @formatter:on

  // TODO: Currently takes elastic search client, in future, can take other clients such as galene
  // TODO: take params and settings needed to create the client
  public ESSearchDAO(@Nonnull RestHighLevelClient esClient, @Nonnull Class<DOCUMENT> documentClass,
      @Nonnull BaseSearchConfig<DOCUMENT> config, @Nonnull BaseTrackingManager baseTrackingManager) {
    super(documentClass);
    _baseTrackingManager = baseTrackingManager;
    // Register all tracking process types.
    PROCESS_STATES.forEach(_baseTrackingManager::register);
    _client = esClient;
    _config = config;
    _autoCompleteQueryForLowCardFields = new ESAutoCompleteQueryForLowCardinalityFields(_config);
    _autoCompleteQueryForHighCardFields = new ESAutoCompleteQueryForHighCardinalityFields(_config);
    // Add regex pattern that checks whether the field name from elasticsearch
    // matches the original field name or any sub-fields i.e. name, name.delimited, name.edge_ngram
    _highlightedFieldNamePatterns = config.getFieldsToHighlightMatch()
        .stream()
        .collect(Collectors.toMap(Function.identity(), fieldName -> Pattern.compile(fieldName + "(\\..+)?")));
  }

  public ESSearchDAO(@Nonnull RestHighLevelClient esClient, @Nonnull Class<DOCUMENT> documentClass,
      @Nonnull BaseSearchConfig<DOCUMENT> config) {
    this(esClient, documentClass, config, new DummyTrackingManager());
  }

  /**
   * Set "track_total_hits" query parameter to a custom lower bound if you do not need accurate results. It is a good
   * trade off to speed up searches if you don’t need the accurate number of hits after a certain threshold.
   */
  public void setTrackTotalHits(int lowermost) {
    _lowerBoundHits = lowermost;
  }

  @Nonnull
  protected BaseESAutoCompleteQuery getAutocompleteQueryGenerator(@Nonnull String field) {
    if (_config.getLowCardinalityFields() != null && _config.getLowCardinalityFields().contains(field)) {
      return _autoCompleteQueryForLowCardFields;
    }
    return _autoCompleteQueryForHighCardFields;
  }

  /**
   * Constructs the base query string given input.
   *
   * @param input the search input text
   * @return built query
   */
  @Nonnull
  QueryBuilder buildQueryString(@Nonnull String input) {
    final String query = _config.getSearchQueryTemplate().replace("$INPUT", input);
    return QueryBuilders.wrapperQuery(query);
  }

  @Nonnull
  private SearchResult<DOCUMENT> executeAndExtract(@Nonnull SearchRequest searchRequest, int from, int size,
      @Nonnull byte[] id, @Nonnull TrackingUtils.ProcessType processType) {
    try {
      final SearchResponse searchResponse = _client.search(searchRequest, RequestOptions.DEFAULT);
      // extract results, validated against document model as well
      return extractQueryResult(searchResponse, from, size);
    } catch (Exception e) {
      log.error("Search query failed:" + e.getMessage());
      _baseTrackingManager.trackRequest(id, processType);
      throw new ESQueryException("Search query failed:", e);
    }
  }

  /**
   * TODO: This part will be replaced by searchTemplateAPI when the elastic is upgraded to 6.4 or later
   */
  @Override
  @Nonnull
  public SearchResult<DOCUMENT> search(@Nonnull String input, @Nullable Filter postFilters,
      @Nullable SortCriterion sortCriterion, int from, int size) {
    return search(input, postFilters, sortCriterion, null, from, size);
  }

  /**
   * Gets a list of documents that match given search request. The results are aggregated and filters are applied to the
   * search hits and not the aggregation results.
   *
   * <p>This method uses preference parameter to control the shard copy on which to execute the search operation.
   * The string used as preference can be a user ID or session ID for instance. This ensures that all queries of a
   * given user are always going to hit the same shards, so scores can remain more consistent across queries. Using a
   * preference value that identifies the current user or session could help optimize usage of the caches.
   *
   * <p>WARNING: using a preference parameter that is same for all queries will lead to hot spots that could
   * potentially impact latency, hence choose this parameter judiciously.
   *
   * @param input the search input text
   * @param postFilters the request map with fields and values as filters to be applied to search hits
   * @param sortCriterion {@link SortCriterion} to be applied to search results
   * @param preference controls a preference of the shard copy on which to execute the search
   * @param from index to start the search from
   * @param size the number of search hits to return
   * @return a {@link SearchResult} that contains a list of matched documents and related search result metadata
   */
  @Nonnull
  public SearchResult<DOCUMENT> search(@Nonnull String input, @Nullable Filter postFilters,
      @Nullable SortCriterion sortCriterion, @Nullable String preference, int from, int size) {
    // Step 0: TODO: Add type casting if needed and  add request params validation against the model
    final byte[] id = random(new byte[16]);
    _baseTrackingManager.trackRequest(id, SEARCH_QUERY_START);
    // Step 1: construct the query
    final SearchRequest req = constructSearchQuery(input, postFilters, sortCriterion, preference, from, size);
    // Step 2: execute the query and extract results, validated against document model as well
    final SearchResult<DOCUMENT> searchResult = executeAndExtract(req, from, size, id, SEARCH_QUERY_FAIL);
    _baseTrackingManager.trackRequest(id, SEARCH_QUERY_END);
    return searchResult;
  }

  @Override
  @Nonnull
  public SearchResult<DOCUMENT> filter(@Nullable Filter filters, @Nullable SortCriterion sortCriterion, int from,
      int size) {
    final byte[] id = random(new byte[16]);
    _baseTrackingManager.trackRequest(id, FILTER_QUERY_START);
    final SearchRequest searchRequest = getFilteredSearchQuery(filters, sortCriterion, from, size);
    final SearchResult<DOCUMENT> searchResult = executeAndExtract(searchRequest, from, size, id, FILTER_QUERY_FAIL);
    _baseTrackingManager.trackRequest(id, FILTER_QUERY_END);
    return searchResult;
  }

  /**
   * Returns a {@link SearchRequest} given filters to be applied to search query and sort criterion to be applied to
   * search results.
   *
   * @param filters {@link Filter} list of conditions with fields and values
   * @param sortCriterion {@link SortCriterion} to be applied to the search results
   * @param from index to start the search from
   * @param size the number of search hits to return
   * @return {@link SearchRequest} that contains the filtered query
   */
  @Nonnull
  SearchRequest getFilteredSearchQuery(@Nullable Filter filters, @Nullable SortCriterion sortCriterion, int from,
      int size) {

    final BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
    if (filters != null) {
      filters.getCriteria().forEach(criterion -> {
        if (!criterion.getValue().trim().isEmpty()) {
          boolQueryBuilder.filter(getQueryBuilderFromCriterion(criterion));
        }
      });
    }
    final SearchRequest searchRequest = new SearchRequest(_config.getIndexName());
    final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.trackTotalHitsUpTo(_lowerBoundHits);
    searchSourceBuilder.query(boolQueryBuilder);
    searchSourceBuilder.from(from).size(size);
    ESUtils.buildSortOrder(searchSourceBuilder, sortCriterion);
    searchRequest.source(searchSourceBuilder);

    return searchRequest;
  }

  /**
   * Constructs the search query based on the query request.
   *
   * <p>TODO: This part will be replaced by searchTemplateAPI when the elastic is upgraded to 6.4 or later
   *
   * @param input the search input text
   * @param filter the search filter
   * @param from index to start the search from
   * @param size the number of search hits to return
   * @return a valid search request
   * @deprecated please use {@link #constructSearchQuery(String, Filter, SortCriterion, String, int, int)} instead
   */
  @Nonnull
  public SearchRequest constructSearchQuery(@Nonnull String input, @Nullable Filter filter,
      @Nullable SortCriterion sortCriterion, int from, int size) {

    return constructSearchQuery(input, filter, sortCriterion, null, from, size);
  }

  /**
   * Constructs the search query based on the query request.
   *
   * <p>TODO: This part will be replaced by searchTemplateAPI when the elastic is upgraded to 6.4 or later
   *
   * @param input the search input text
   * @param filter the search filter
   * @param preference controls a preference of the shard copy on which to execute the search
   * @param from index to start the search from
   * @param size the number of search hits to return
   * @return a valid search request
   */
  @Nonnull
  SearchRequest constructSearchQuery(@Nonnull String input, @Nullable Filter filter,
      @Nullable SortCriterion sortCriterion, @Nullable String preference, int from, int size) {

    SearchRequest searchRequest = new SearchRequest(_config.getIndexName());
    if (preference != null) {
      searchRequest.preference(preference);
    }
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

    searchSourceBuilder.from(from);
    searchSourceBuilder.size(size);

    searchSourceBuilder.trackTotalHitsUpTo(_lowerBoundHits);

    searchSourceBuilder.query(buildQueryString(input));
    searchSourceBuilder.postFilter(ESUtils.buildFilterQuery(filter));
    buildAggregations(searchSourceBuilder, filter);
    buildHighlights(searchSourceBuilder, _config.getFieldsToHighlightMatch());
    ESUtils.buildSortOrder(searchSourceBuilder, sortCriterion);

    searchRequest.source(searchSourceBuilder);
    log.debug("Search request is: " + searchRequest.toString());
    return searchRequest;
  }

  /**
   * Constructs the aggregations and sub-aggregations by adding other facets' filters if they are set in request.
   *
   * <p>Retrieves dynamic aggregation bucket values when the selections change on the fly
   *
   * @param searchSourceBuilder the builder to build search source for search request
   * @param filter the search filters
   */
  private void buildAggregations(@Nonnull SearchSourceBuilder searchSourceBuilder, @Nullable Filter filter) {
    Set<String> facetFields = _config.getFacetFields();
    for (String facet : facetFields) {
      AggregationBuilder aggBuilder = AggregationBuilders.terms(facet).field(facet).size(_maxTermBucketSize);
      Optional.ofNullable(filter).map(Filter::getCriteria).ifPresent(criteria -> {
        for (Criterion criterion : criteria) {
          if (!facetFields.contains(criterion.getField()) || criterion.getField().equals(facet)) {
            continue;
          }
          QueryBuilder filterQueryBuilder = ESUtils.getQueryBuilderFromCriterionForSearch(criterion);
          aggBuilder.subAggregation(AggregationBuilders.filter(criterion.getField(), filterQueryBuilder));
        }
      });
      searchSourceBuilder.aggregation(aggBuilder);
    }
  }

  /**
   * Constructs the highlighter based on the list of fields to highlight.
   *
   * @param searchSourceBuilder the builder to build search source for search request
   * @param fieldsToHighlight list of fields to highlight
   */
  private void buildHighlights(@Nonnull SearchSourceBuilder searchSourceBuilder,
      @Nullable List<String> fieldsToHighlight) {
    if (fieldsToHighlight == null || fieldsToHighlight.isEmpty()) {
      return;
    }
    HighlightBuilder highlightBuilder = new HighlightBuilder();
    highlightBuilder.preTags("");
    highlightBuilder.postTags("");
    fieldsToHighlight.forEach(field -> highlightBuilder.field(field).field(field + ".*"));
    searchSourceBuilder.highlighter(highlightBuilder);
  }

  /**
   * Extracts a list of documents from the raw search response.
   *
   * @param searchResponse the raw search response from search engine
   * @param from offset from the first result you want to fetch
   * @param size page size
   * @return collection of a list of documents and related search result metadata
   */
  @Nonnull
  public SearchResult<DOCUMENT> extractQueryResult(@Nonnull SearchResponse searchResponse, int from, int size) {

    int totalCount = (int) searchResponse.getHits().getTotalHits().value;
    int totalPageCount = QueryUtils.getTotalPageCount(totalCount, size);

    return SearchResult.<DOCUMENT>builder()
        // format
        .documentList(getDocuments(searchResponse))
        .searchResultMetadata(extractSearchResultMetadata(searchResponse))
        .from(from)
        .pageSize(size)
        .havingMore(QueryUtils.hasMore(from, size, totalPageCount))
        .totalCount(totalCount)
        .totalPageCount(totalPageCount)
        .build();
  }

  /**
   * Gets list of documents from search hits.
   *
   * @param searchResponse the raw search response from search engine
   * @return List of documents
   */
  @Nonnull
  List<DOCUMENT> getDocuments(@Nonnull SearchResponse searchResponse) {
    return (Arrays.stream(searchResponse.getHits().getHits())).map(
        hit -> newDocument(buildDocumentsDataMap(hit.getSourceAsMap()))).collect(Collectors.toList());
  }

  /**
   * Builds data map for documents.
   *
   * @param objectMap an object map represents one raw search hit
   * @return a data map
   */
  @Nonnull
  DataMap buildDocumentsDataMap(@Nonnull Map<String, Object> objectMap) {

    final DataMap dataMap = new DataMap();
    for (Map.Entry<String, Object> entry : objectMap.entrySet()) {
      if (entry.getValue() instanceof ArrayList) {
        dataMap.put(entry.getKey(), new DataList((ArrayList<String>) entry.getValue()));
      } else if (entry.getValue() != null) {
        dataMap.put(entry.getKey(), entry.getValue());
      }
    }
    return dataMap;
  }

  @Override
  @Nonnull
  public AutoCompleteResult autoComplete(@Nonnull String query, @Nullable String field, @Nullable Filter requestParams,
      int limit) {
    final byte[] id = random(new byte[16]);
    _baseTrackingManager.trackRequest(id, AUTOCOMPLETE_QUERY_START);
    if (field == null) {
      field = _config.getDefaultAutocompleteField();
    }
    try {
      SearchRequest req = constructAutoCompleteQuery(query, field, requestParams);
      SearchResponse searchResponse = _client.search(req, RequestOptions.DEFAULT);
      final AutoCompleteResult autoCompleteResult = extractAutoCompleteResult(searchResponse, query, field, limit);
      _baseTrackingManager.trackRequest(id, AUTOCOMPLETE_QUERY_END);
      return autoCompleteResult;
    } catch (Exception e) {
      log.error("Auto complete query failed:" + e.getMessage());
      _baseTrackingManager.trackRequest(id, AUTOCOMPLETE_QUERY_FAIL);
      throw new ESQueryException("Auto complete query failed:", e);
    }
  }

  @Nonnull
  public AutoCompleteResult extractAutoCompleteResult(@Nonnull SearchResponse searchResponse, @Nonnull String input,
      @Nonnull String field, int limit) {
    return getAutocompleteQueryGenerator(field).extractAutoCompleteResult(searchResponse, input, field, limit);
  }

  @Nonnull
  public SearchRequest constructAutoCompleteQuery(@Nonnull String input, @Nonnull String field,
      @Nullable Filter requestParams) {
    return getAutocompleteQueryGenerator(field).constructAutoCompleteQuery(input, field, requestParams);
  }

  /**
   * Extracts SearchResultMetadata section.
   *
   * @param searchResponse the raw {@link SearchResponse} as obtained from the search engine
   * @return {@link SearchResultMetadata} with aggregation and list of urns obtained from {@link SearchResponse}
   */
  @Nonnull
  SearchResultMetadata extractSearchResultMetadata(@Nonnull SearchResponse searchResponse) {
    final SearchResultMetadata searchResultMetadata =
        new SearchResultMetadata().setSearchResultMetadatas(new AggregationMetadataArray()).setUrns(new UrnArray());

    try {
      // populate the urns from search response
      if (searchResponse.getHits() != null && searchResponse.getHits().getHits() != null) {
        searchResultMetadata.setUrns(Arrays.stream(searchResponse.getHits().getHits())
            .map(this::getUrnFromSearchHit)
            .collect(Collectors.toCollection(UrnArray::new)));
      }
    } catch (NullPointerException e) {
      throw new RuntimeException("Missing urn field in search document " + e);
    }

    final Aggregations aggregations = searchResponse.getAggregations();
    if (aggregations != null) {
      final AggregationMetadataArray aggregationMetadataArray = new AggregationMetadataArray();

      for (Map.Entry<String, Aggregation> entry : aggregations.getAsMap().entrySet()) {
        final Map<String, Long> oneTermAggResult = extractTermAggregations((ParsedTerms) entry.getValue());
        final AggregationMetadata aggregationMetadata =
            new AggregationMetadata().setName(entry.getKey()).setAggregations(new LongMap(oneTermAggResult));
        aggregationMetadataArray.add(aggregationMetadata);
      }

      searchResultMetadata.setSearchResultMetadatas(aggregationMetadataArray);
    }

    if (searchResponse.getHits() != null && searchResponse.getHits().getHits() != null) {
      boolean hasMatch = false;
      final List<MatchMetadata> highlightMetadataList = new ArrayList<>(searchResponse.getHits().getHits().length);
      for (SearchHit hit : searchResponse.getHits().getHits()) {
        if (!hit.getHighlightFields().isEmpty()) {
          hasMatch = true;
        }
        highlightMetadataList.add(extractMatchMetadata(hit.getHighlightFields()));
      }
      if (hasMatch) {
        searchResultMetadata.setMatches(new MatchMetadataArray(highlightMetadataList));
      }
    }

    return searchResultMetadata;
  }

  /**
   * Extracts term aggregations give a parsed term.
   *
   * @param terms an abstract parse term, input can be either ParsedStringTerms ParsedLongTerms
   * @return a map with aggregation key and corresponding doc counts
   */
  @Nonnull
  private Map<String, Long> extractTermAggregations(@Nonnull ParsedTerms terms) {

    final Map<String, Long> aggResult = new HashMap<>();
    List<? extends Terms.Bucket> bucketList = terms.getBuckets();

    for (Terms.Bucket bucket : bucketList) {
      String key = bucket.getKeyAsString();
      ParsedFilter parsedFilter = extractBucketAggregations(bucket);
      // Gets filtered sub aggregation doc count if exist
      Long docCount = parsedFilter != null ? parsedFilter.getDocCount() : bucket.getDocCount();
      if (docCount > 0) {
        aggResult.put(key, docCount);
      }
    }

    return aggResult;
  }

  /**
   * Extracts sub aggregations from one term bucket.
   *
   * @param bucket a term bucket
   * @return a parsed filter if exist
   */
  @Nullable
  private ParsedFilter extractBucketAggregations(@Nonnull Terms.Bucket bucket) {

    ParsedFilter parsedFilter = null;
    Map<String, Aggregation> bucketAggregations = bucket.getAggregations().getAsMap();
    for (Map.Entry<String, Aggregation> entry : bucketAggregations.entrySet()) {
      parsedFilter = (ParsedFilter) entry.getValue();
      // TODO: implement and test multi parsed filters
    }

    return parsedFilter;
  }

  /**
   * Extracts highlight metadata from highlighted fields returned by Elasticsearch.
   *
   * @param highlightedFields map of matched field name to list of field values
   * @return highlight metadata
   */
  @Nonnull
  private MatchMetadata extractMatchMetadata(@Nonnull Map<String, HighlightField> highlightedFields) {
    Map<String, Set<String>> highlightedFieldNamesAndValues = new HashMap<>();
    for (Map.Entry<String, HighlightField> entry : highlightedFields.entrySet()) {
      // Get the field name from source e.g. name.delimited -> name
      Optional<String> fieldName = getFieldName(entry.getKey());
      if (!fieldName.isPresent()) {
        continue;
      }
      if (!highlightedFieldNamesAndValues.containsKey(fieldName.get())) {
        highlightedFieldNamesAndValues.put(fieldName.get(), new HashSet<>());
      }
      for (Text fieldValue : entry.getValue().getFragments()) {
        highlightedFieldNamesAndValues.get(fieldName.get()).add(fieldValue.string());
      }
    }
    // Rank the highlights based on the order of field names in the config
    return new MatchMetadata().setMatchedFields(new MatchedFieldArray(_config.getFieldsToHighlightMatch()
        .stream()
        .filter(highlightedFieldNamesAndValues::containsKey)
        .flatMap(fieldName -> highlightedFieldNamesAndValues.get(fieldName)
            .stream()
            .map(value -> new MatchedField().setName(fieldName).setValue(value)))
        .collect(Collectors.toList())));
  }

  /**
   * Get the original field name that matches the given highlighted field name.
   * i.e. name.delimited, name.ngram, name -> name
   *
   * @param highlightedFieldName highlighted field name from search response
   * @return original field name
   */
  @Nonnull
  private Optional<String> getFieldName(String highlightedFieldName) {
    return _highlightedFieldNamePatterns.entrySet()
        .stream()
        .filter(entry -> entry.getValue().matcher(highlightedFieldName).matches())
        .map(Map.Entry::getKey)
        .findFirst();
  }

  @Nonnull
  private Urn getUrnFromSearchHit(@Nonnull SearchHit hit) {
    try {
      return Urn.createFromString(hit.getSourceAsMap().get(URN_FIELD).toString());
    } catch (URISyntaxException e) {
      throw new RuntimeException("Invalid urn in search document " + e);
    }
  }

  /**
   * Sets max term bucket size in the aggregation results.
   *
   * <p>The default value might not always be good enough when aggregation happens on a high cardinality field.
   * Using a high default instead is also not ideal because of potential query performance degradation.
   * Instead, entities which have a rare use case of aggregating over high cardinality fields can use this method
   * to configure the aggregation behavior.
   */
  public void setMaxTermBucketSize(int maxTermBucketSize) {
    _maxTermBucketSize = maxTermBucketSize;
  }
}
