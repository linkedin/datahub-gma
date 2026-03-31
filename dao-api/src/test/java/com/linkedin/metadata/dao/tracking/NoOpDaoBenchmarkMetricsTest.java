package com.linkedin.metadata.dao.tracking;

import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class NoOpDaoBenchmarkMetricsTest {

  @Test
  public void testNoOpBehavior() {
    NoOpDaoBenchmarkMetrics metrics = new NoOpDaoBenchmarkMetrics();

    // Should not throw
    metrics.recordOperationLatency("add", "dataset", 42L);
    metrics.recordOperationError("add", "dataset", "SQLException");

    assertFalse(metrics.isEnabled());
  }
}
