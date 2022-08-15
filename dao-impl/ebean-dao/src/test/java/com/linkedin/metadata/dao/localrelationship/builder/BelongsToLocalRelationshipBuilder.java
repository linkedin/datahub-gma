package com.linkedin.metadata.dao.localrelationship.builder;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.dao.builder.BaseLocalRelationshipBuilder;
import com.linkedin.metadata.dao.internal.BaseGraphWriterDAO;
import com.linkedin.testing.localrelationship.AspectFooBar;
import com.linkedin.testing.localrelationship.BelongsTo;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;

public class BelongsToLocalRelationshipBuilder extends BaseLocalRelationshipBuilder<AspectFooBar> {
  public BelongsToLocalRelationshipBuilder(@Nonnull Class<AspectFooBar> aspectFooBarClass) {
    super(aspectFooBarClass);
  }

  @Nonnull
  @Override
  public <URN extends Urn> List<LocalRelationshipUpdates> buildRelationships(@Nonnull URN urn,
      @Nonnull AspectFooBar aspectFooBar) {
    List<BelongsTo> belongsToRelationships = new ArrayList<>();
    for (Urn barUrn : aspectFooBar.getBars()) {
      belongsToRelationships.add(new BelongsTo().setSource(barUrn).setDestination(urn));
    }

    LocalRelationshipUpdates localRelationshipUpdates = new LocalRelationshipUpdates(belongsToRelationships,
        BaseGraphWriterDAO.RemovalOption.REMOVE_ALL_EDGES_TO_DESTINATION);

    return Collections.singletonList(localRelationshipUpdates);
  }
}
