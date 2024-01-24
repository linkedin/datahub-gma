package com.linkedin.metadata.dao.localrelationship.builder;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.dao.builder.BaseLocalRelationshipBuilder;
import com.linkedin.metadata.dao.internal.BaseGraphWriterDAO;
import com.linkedin.testing.localrelationship.AspectFooBar;
import com.linkedin.testing.localrelationship.PairsWith;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;


public class PairsWithLocalRelationshipBuilder extends BaseLocalRelationshipBuilder<AspectFooBar> {
  public PairsWithLocalRelationshipBuilder(@Nonnull Class<AspectFooBar> aspectFooBarClass) {
    super(aspectFooBarClass);
  }

  @Nonnull
  @Override
  public <URN extends Urn> List<LocalRelationshipUpdates> buildRelationships(@Nonnull URN urn,
      @Nonnull AspectFooBar aspectFooBar) {
    List<PairsWith> pairsWithRelationships = new ArrayList<>();
    for (Urn barUrn : aspectFooBar.getBars()) {
      pairsWithRelationships.add(new PairsWith().setSource(barUrn).setDestination(urn));
    }

    LocalRelationshipUpdates localRelationshipUpdates = new LocalRelationshipUpdates(pairsWithRelationships,
        PairsWith.class,
        BaseGraphWriterDAO.RemovalOption.REMOVE_ALL_EDGES_FROM_SOURCE_TO_DESTINATION);

    return Collections.singletonList(localRelationshipUpdates);
  }
}
