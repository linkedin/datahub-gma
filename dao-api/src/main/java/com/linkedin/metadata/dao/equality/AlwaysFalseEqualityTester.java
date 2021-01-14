package com.linkedin.metadata.dao.equality;

import com.linkedin.data.template.RecordTemplate;
import javax.annotation.Nonnull;


/**
 * A {@link EqualityTester} that always returns false.
 */
public class AlwaysFalseEqualityTester<T extends RecordTemplate> implements EqualityTester<T> {
  private static final AlwaysFalseEqualityTester<?> INSTANCE = new AlwaysFalseEqualityTester<>();

  /**
   * Returns the singleton instance of {@link AlwaysFalseEqualityTester}.
   */
  @SuppressWarnings("unchecked")
  public static <T extends RecordTemplate> AlwaysFalseEqualityTester<T> instance() {
    return (AlwaysFalseEqualityTester<T>) INSTANCE;
  }

  @Override
  public boolean equals(@Nonnull RecordTemplate o1, @Nonnull RecordTemplate o2) {
    return false;
  }
}
