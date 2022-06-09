package com.linkedin.metadata.restli;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.metadata.dao.BaseSearchDAO;
import com.linkedin.metadata.dao.SearchResult;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.dao.utils.QueryUtils;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.SearchResultMetadata;
import com.linkedin.metadata.query.SortCriterion;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.CollectionResult;
import com.linkedin.restli.server.PagingContext;
import com.linkedin.restli.server.annotations.Finder;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.PagingContextParam;
import com.linkedin.restli.server.annotations.QueryParam;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.linkedin.metadata.restli.RestliConstants.*;

/**
 * A base class for the aspect rest.li resource that supports CRUD + search capability.
 **
 * @param <URN> must be a valid {@link Urn} type for the snapshot
 * @param <ASPECT_UNION> must be a valid aspect union type supported by the snapshot
 * @param <ASPECT> the resource's aspect type
 * @param <DOCUMENT> must be a valid search document type defined in com.linkedin.metadata.search
 */
public abstract class BaseSearchableAspectResource<
    URN extends Urn,
    ASPECT_UNION extends UnionTemplate,
    ASPECT extends RecordTemplate,
    DOCUMENT extends RecordTemplate> extends BaseAspectV2Resource<URN, ASPECT_UNION, ASPECT> {

  private final Class<ASPECT> _aspectClass;

  public BaseSearchableAspectResource(@Nonnull Class<ASPECT_UNION> aspectUnionClass,
      @Nonnull Class<ASPECT> aspectClass) {
    super(aspectUnionClass, aspectClass);
    _aspectClass = aspectClass;
  }

  /**
   * Returns a document-specific {@link BaseSearchDAO}.
   */
  @Nonnull
  protected abstract BaseSearchDAO<DOCUMENT> getSearchDAO();

  @Finder(FINDER_SEARCH)
  @Nonnull
  public Task<CollectionResult<ASPECT, SearchResultMetadata>> search(@QueryParam(PARAM_INPUT) @Nonnull String input,
      @QueryParam(PARAM_FILTER) @Optional @Nullable Filter filter,
      @QueryParam(PARAM_SORT) @Optional @Nullable SortCriterion sortCriterion,
      @PagingContextParam @Nonnull PagingContext pagingContext) {

    final Filter searchFilter = filter != null ? filter : QueryUtils.EMPTY_FILTER;
    final SearchResult<DOCUMENT> searchResult =
        getSearchDAO().search(input, searchFilter, sortCriterion, pagingContext.getStart(), pagingContext.getCount());
    return RestliUtils.toTask(
        () -> getSearchQueryCollectionResult(searchResult));
  }

  @Nonnull
  private CollectionResult<ASPECT, SearchResultMetadata> getSearchQueryCollectionResult(@Nonnull SearchResult<DOCUMENT> searchResult) {

    final Set<URN> matchedUrns = searchResult.getDocumentList()
        .stream()
        .map(d -> (URN) ModelUtils.getUrnFromDocument(d))
        .collect(Collectors.toSet());

    final Map<URN, java.util.Optional<ASPECT>> urnAspectMap = getLocalDAO().get(_aspectClass, matchedUrns);

    final List<URN> existingUrns = matchedUrns.stream().filter(urnAspectMap::containsKey).collect(Collectors.toList());

    final List<ASPECT> aspects = urnAspectMap.values().stream().filter(java.util.Optional::isPresent).map(
        java.util.Optional::get).collect(Collectors.toList());

    return new CollectionResult<>(
        aspects,
        searchResult.getTotalCount(),
        searchResult.getSearchResultMetadata().setUrns(new UrnArray(existingUrns.stream().map(urn -> (Urn) urn).collect(Collectors.toList()))));
  }
}
