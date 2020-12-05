package com.linkedin.metadata.annotations.testing;

import java.io.InputStream;
import javax.annotation.Nonnull;


public final class TestModels {
  private TestModels() {
  }

  public static InputStream getTestModelStream(@Nonnull String path) {
    final InputStream resourceStream = TestModels.class.getClassLoader().getResourceAsStream(path);

    if (resourceStream == null) {
      throw new NullPointerException("Could not load resource " + path);
    }

    return resourceStream;
  }
}
