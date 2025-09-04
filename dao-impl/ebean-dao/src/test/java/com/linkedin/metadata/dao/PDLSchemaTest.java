package com.linkedin.metadata.dao;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.*;


public class PDLSchemaTest {
  /**
   * This test checks that all PDL models defined in this repo have valid comments which can be compiled by python libs.
   * Refer to #incident-6877 or ACTIONITEM-9287 for more details.
   * If this test is failing for you, then please check the violating PDL file and make sure all comments do not end with the " character
   * @throws IOException ignored
   */
  @Test
  public void testNoCommentsEndWithDoubleQuote() throws IOException {
    Path currentDir = Paths.get("").toAbsolutePath().normalize();
    Path repoRoot = findRepoRoot(currentDir);

    System.out.println("Scanning repo root: " + repoRoot);

    // traverse all files from the repository root
    try (Stream<Path> files = Files.walk(repoRoot)) {
      // identify pdl files
      List<Path> pdlFiles = files.filter(path -> path.toString().endsWith(".pdl")).collect(Collectors.toList());

      // for each pdl file, check that it has valid comments i.e. it does not end with a double quote.
      for (Path pdlFile : pdlFiles) {
        List<String> lines = Files.readAllLines(pdlFile);

        boolean insideBlockComment = false;

        for (int i = 0; i < lines.size(); i++) {
          String line = lines.get(i).trim();

          // handle block comments
          if (line.startsWith("/*")) {
            insideBlockComment = true;
          }
          if (insideBlockComment) {
            if (line.endsWith("\"")) {
              fail("Block comment ends with a double quote in file " + pdlFile + " at line " + (i + 1));
            }
            if (line.endsWith("*/")) {
              insideBlockComment = false;
            }
          } else {
            // handle single line comment
            if (line.startsWith("//") && line.endsWith("\"")) {
              fail("Single line comment ends with a double quote in file " + pdlFile + " at line " + (i + 1));
            }
          }
        }
      }
    }
  }

  // helper function to get to the root directory of datahub-gma, indicated by the presence of the .git file.
  private static Path findRepoRoot(Path startDir) {
    Path dir = startDir;
    while (dir != null && !Files.exists(dir.resolve(".git"))) {
      dir = dir.getParent();
    }
    if (dir == null) {
      throw new IllegalStateException("Could not find repo root from " + startDir);
    }
    return dir;
  }
}