package com.linkedin.metadata.dao.builder;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.internal.BaseGraphWriterDAO;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.Value;


/**
 * Build a local relationship based on an ASPECT.
 */
public abstract class BaseLocalRelationshipBuilder<ASPECT extends RecordTemplate> {

  private final Class<ASPECT> _aspectClass;

  @Value
  public static class LocalRelationshipUpdates {
    List<? extends RecordTemplate> relationships;
    Class<? extends RecordTemplate>[] relationshipClasses;
    BaseGraphWriterDAO.RemovalOption removalOption;
  }

  public BaseLocalRelationshipBuilder(@Nonnull Class<ASPECT> aspectClass) {
    _aspectClass = aspectClass;
  }

  /**
   * Returns the aspect class this {@link BaseLocalRelationshipBuilder} supports.
   */
  @Nonnull
  public Class<ASPECT> supportedAspectClass() {
    return _aspectClass;
  }

  /**
   * Returns a list of corresponding relationship updates for the given metadata aspect.
   */
  @Nonnull
  public abstract <URN extends Urn> List<LocalRelationshipUpdates> buildRelationships(@Nonnull URN urn, @Nonnull ASPECT aspect);
}
