package com.linkedin.metadata.dao.tracking;

import java.util.Collections;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class NoOpDaoUsageEmitterTest {

  @Test
  public void testNoOpBehavior() {
    NoOpDaoUsageEmitter emitter = new NoOpDaoUsageEmitter();

    // Should not throw for any operation shape, including null caller and empty targets.
    emitter.emit("READ", "dataset", "batchGetUnion", null, null,
        Collections.singletonList(new DaoUsageTarget("urn:li:dataset:foo",
            Collections.singletonList("AspectFoo"))));
    emitter.emit("WRITE", "corpuser", "add", "urn:li:corpuser:bar", "urn:li:service:baz",
        Collections.emptyList());

    assertFalse(emitter.isEnabled());
  }
}
