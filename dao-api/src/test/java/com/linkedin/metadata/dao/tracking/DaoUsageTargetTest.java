package com.linkedin.metadata.dao.tracking;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class DaoUsageTargetTest {

  @Test
  public void testAspectsAreCopiedNotWrapped() {
    List<String> source = new ArrayList<>(Arrays.asList("AspectFoo", "AspectBar"));
    DaoUsageTarget target = new DaoUsageTarget("urn:li:foo:1", source);

    // unmodifiableList alone is only a view, so mutating the caller's list would change the
    // already-constructed target. Targets are handed to an asynchronous emitter, so they can be
    // read on another thread well after the DAO call returns.
    source.add("AspectBaz");

    assertEquals(target.getAspects(), Arrays.asList("AspectFoo", "AspectBar"));
  }
}
