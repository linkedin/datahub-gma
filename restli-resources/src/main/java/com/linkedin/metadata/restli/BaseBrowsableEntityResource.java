package com.linkedin.metadata.restli;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.metadata.dao.BaseBrowseDAO;
import com.linkedin.metadata.dao.utils.QueryUtils;
import com.linkedin.metadata.query.BrowseResult;
import com.linkedin.metadata.query.Filter;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.annotations.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.linkedin.metadata.restli.RestliConstants.*;


/**
 * A base class for the entity rest.li resource that supports CRUD + search + browse methods.
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
public abstract class BaseBrowsableEntityResource<
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
    BaseSearchableEntityResource<KEY, VALUE, URN, SNAPSHOT, ASPECT_UNION, DOCUMENT, INTERNAL_SNAPSHOT, INTERNAL_ASPECT_UNION, ASSET> {

  public BaseBrowsableEntityResource(@Nullable Class<SNAPSHOT> snapshotClass,
      @Nullable Class<ASPECT_UNION> aspectUnionClass, @Nonnull Class<INTERNAL_SNAPSHOT> internalSnapshotClass,
      @Nonnull Class<INTERNAL_ASPECT_UNION> internalAspectUnionClass, @Nonnull Class<ASSET> assetClass) {
    super(snapshotClass, aspectUnionClass, internalSnapshotClass, internalAspectUnionClass, assetClass);
  }

  public BaseBrowsableEntityResource(@Nullable Class<SNAPSHOT> snapshotClass,
      @Nullable Class<ASPECT_UNION> aspectUnionClass, @Nonnull Class<URN> urnClass,
      @Nonnull Class<INTERNAL_SNAPSHOT> internalSnapshotClass,
      @Nonnull Class<INTERNAL_ASPECT_UNION> internalAspectUnionClass, @Nonnull Class<ASSET> assetClass) {
    super(snapshotClass, aspectUnionClass, urnClass, internalSnapshotClass, internalAspectUnionClass, assetClass);
  }

  /**
   * Returns a {@link BaseBrowseDAO}.
   */
  @Nonnull
  protected abstract BaseBrowseDAO getBrowseDAO();

  @Action(name = ACTION_BROWSE)
  @Nonnull
  public Task<BrowseResult> browse(@ActionParam(PARAM_PATH) @Nonnull String path,
      @ActionParam(PARAM_FILTER) @Optional @Nullable Filter filter, @ActionParam(PARAM_START) int start,
      @ActionParam(PARAM_LIMIT) int limit) {

    final Filter browseFilter = filter == null ? QueryUtils.EMPTY_FILTER : filter;
    return RestliUtils.toTask(() -> getBrowseDAO().browse(path, browseFilter, start, limit));
  }

  @Action(name = ACTION_GET_BROWSE_PATHS)
  @Nonnull
  public Task<StringArray> getBrowsePaths(
      @ActionParam(value = "urn", typeref = com.linkedin.common.Urn.class) @Nonnull Urn urn) {

    return RestliUtils.toTask(() -> new StringArray(getBrowseDAO().getBrowsePaths(urn)));
  }
}
