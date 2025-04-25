package com.linkedin.metadata.dao.utils;

import lombok.Getter;


/**
 * Context object for relationship lookup operations.
 * This class allows configuration of whether to include non-current relationships
 * (such as historical or soft-deleted relationships) during lookup operations.
 */
public class RelationshipLookUpContext {

  @Getter
  boolean includeNonCurrentRelationships;
  public RelationshipLookUpContext(boolean includeNonCurrentRelationships) {
    this.includeNonCurrentRelationships = includeNonCurrentRelationships;
  }

  public RelationshipLookUpContext() {
    // By default, not include the nonCurrentRelationship
    this(false);
  }
}
