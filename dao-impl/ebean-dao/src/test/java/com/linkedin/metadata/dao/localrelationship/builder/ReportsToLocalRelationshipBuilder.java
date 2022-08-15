package com.linkedin.metadata.dao.localrelationship.builder;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.dao.builder.BaseLocalRelationshipBuilder;
import com.linkedin.metadata.dao.internal.BaseGraphWriterDAO;
import com.linkedin.testing.localrelationship.AspectFooBar;
import com.linkedin.testing.localrelationship.ReportsTo;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;


public class ReportsToLocalRelationshipBuilder extends BaseLocalRelationshipBuilder<AspectFooBar> {
  public ReportsToLocalRelationshipBuilder(@Nonnull Class<AspectFooBar> aspectFooBarClass) {
    super(aspectFooBarClass);
  }

  @Nonnull
  @Override
  public <URN extends Urn> List<LocalRelationshipUpdates> buildRelationships(@Nonnull URN urn,
      @Nonnull AspectFooBar aspectFooBar) {
    List<ReportsTo> reportsToRelationships = new ArrayList<>();
    for (Urn barUrn : aspectFooBar.getBars()) {
      reportsToRelationships.add(new ReportsTo().setSource(barUrn).setDestination(urn));
    }

    LocalRelationshipUpdates localRelationshipUpdates = new LocalRelationshipUpdates(reportsToRelationships,
        BaseGraphWriterDAO.RemovalOption.REMOVE_NONE);

    return Collections.singletonList(localRelationshipUpdates);
  }
}
