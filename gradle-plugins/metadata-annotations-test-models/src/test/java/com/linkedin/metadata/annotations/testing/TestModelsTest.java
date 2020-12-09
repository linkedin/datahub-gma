package com.linkedin.metadata.annotations.testing;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;


public class TestModelsTest {
  @Test
  public void canLoadResource() {
    assertThat(TestModels.getTestModelStream("com/linkedin/testing/AnnotatedAspectBar.pdl")).isNotNull();
  }
}
