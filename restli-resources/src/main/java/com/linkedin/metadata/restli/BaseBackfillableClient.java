package com.linkedin.metadata.restli;

import com.linkedin.common.urn.Urn;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.Client;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


public abstract class BaseBackfillableClient<URN extends Urn> extends BaseClient {
  public BaseBackfillableClient(@Nonnull Client restliClient) {
    super(restliClient);
  }

  /**
   * Similar to {@link #backfill(URN, List)} but does backfill for all available aspects.
   */
  @Nonnull
  public List<String> backfill(@Nonnull URN urn) throws RemoteInvocationException {
    return backfill(urn, null);
  }

  /**
   * Similar to {@link #backfill(Set, List)} but does backfill for a single dataset.
   */
  @Nonnull
  public List<String> backfill(@Nonnull URN urn, @Nullable List<String> aspects)
      throws RemoteInvocationException {
    return new ArrayList<>(backfill(Collections.singleton(urn), aspects).getEntities().get(0).getAspects());
  }

  /**
   * Similar to {@link #backfill(Set, List)} but does backfill for all available aspects.
   */
  @Nonnull
  public BackfillResult backfill(@Nonnull Set<URN> urnSet) throws RemoteInvocationException {
    return backfill(urnSet, null);
  }

  /**
   * Backfills given aspects of a set of entities.
   *
   * @param urnSet set of {@link URN}s
   * @param aspects canonical names of the aspects to be backfilled
   * @return {@link BackfillResult}
   * @throws RemoteInvocationException throw exception when rest call fails
   */
  @Nonnull
  public abstract BackfillResult backfill(@Nonnull Set<URN> urnSet, @Nullable List<String> aspects)
      throws RemoteInvocationException;
}
