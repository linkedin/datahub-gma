package com.linkedin.metadata.dao.localrelationship.builder;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.dao.builder.BaseLocalRelationshipBuilder;
import com.linkedin.metadata.dao.internal.BaseGraphWriterDAO;
import com.linkedin.testing.localrelationship.AspectFooBaz;
import com.linkedin.testing.localrelationship.BelongsToV2;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;


public class BelongsToV2LocalRelationshipBuilder extends BaseLocalRelationshipBuilder<AspectFooBaz> {
  public BelongsToV2LocalRelationshipBuilder(@Nonnull Class<AspectFooBaz> aspectFooBazClass) {
    super(aspectFooBazClass);
  }

  @Nonnull
  @Override
  public <URN extends Urn> List<BaseLocalRelationshipBuilder.LocalRelationshipUpdates> buildRelationships(@Nonnull URN urn,
      @Nonnull AspectFooBaz aspectFooBaz) {
    List<BelongsToV2> belongsToRelationships = new ArrayList<>();
    if (!aspectFooBaz.hasBars()) {
      return Collections.emptyList();
    }
    for (Urn barUrn : aspectFooBaz.getBars()) {
      belongsToRelationships.add(new BelongsToV2().setDestination(BelongsToV2.Destination.create(barUrn.toString())));
    }

    BaseLocalRelationshipBuilder.LocalRelationshipUpdates localRelationshipUpdates =
        new BaseLocalRelationshipBuilder.LocalRelationshipUpdates(belongsToRelationships, BelongsToV2.class,
            BaseGraphWriterDAO.RemovalOption.REMOVE_ALL_EDGES_FROM_SOURCE);

    return Collections.singletonList(localRelationshipUpdates);
  }
}
