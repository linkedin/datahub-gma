package com.linkedin.metadata.dao.tracking;

import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class NoOpDaoBenchmarkMetricsTest {

  @Test
  public void testNoOpBehavior() {
    NoOpDaoBenchmarkMetrics metrics = new NoOpDaoBenchmarkMetrics();

    // Should not throw on any dimension combination
    metrics.recordOperation("add", "dataset", "AspectFoo", null, "success", null, 42L);
    metrics.recordOperation("batchUpsert", "corpuser", null, "3", "failure", "SQLException", 15L);

    assertFalse(metrics.isEnabled());
  }
}
