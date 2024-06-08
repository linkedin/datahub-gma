package com.linkedin.metadata.dao.equality;

import com.linkedin.data.template.RecordTemplate;
import javax.annotation.Nonnull;


/**
 * A generic equality tester to check whether two RecordTemplates are equal.
 */
public interface GenericEqualityTester {

  /**
   * Return true only if r1 and r2 are considered "equal". Otherwise, return false.
   */
  boolean equals(@Nonnull RecordTemplate r1, @Nonnull RecordTemplate r2);
}
