package com.linkedin.metadata.generator;

import com.linkedin.metadata.annotations.testing.TestModels;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import javax.annotation.Nonnull;
import org.apache.commons.io.FileUtils;


public final class TestUtils {
  private TestUtils() {
  }

  static void copyTestResources(@Nonnull File tempDir) throws IOException, URISyntaxException {
    FileUtils.copyDirectory(new File(TestModels.class.getClassLoader().getResource("com").toURI()),
        tempDir.toPath().resolve("com").toFile());
  }
}
