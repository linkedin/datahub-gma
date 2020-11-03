package com.linkedin.metadata.dao.equality;

import com.linkedin.data.template.DataTemplate;
import com.linkedin.data.template.DataTemplateUtil;
import com.linkedin.data.template.RecordTemplate;
import javax.annotation.Nonnull;


/**
 * A {@link EqualityTester} that uses {@link DataTemplateUtil#areEqual(DataTemplate, DataTemplate)} to check for
 * semantic equality.
 */
public class DefaultEqualityTester<T extends RecordTemplate> implements EqualityTester<T> {
  private static final DefaultEqualityTester<?> INSTANCE = new DefaultEqualityTester<>();

  /**
   * Returns the singleton instance of {@link DefaultEqualityTester}.
   */
  @SuppressWarnings("unchecked")
  public static <T extends RecordTemplate> DefaultEqualityTester<T> instance() {
    return (DefaultEqualityTester<T>) INSTANCE;
  }

  @Override
  public boolean equals(@Nonnull T o1, @Nonnull T o2) {
    return DataTemplateUtil.areEqual(o1, o2);
  }
}
