package com.linkedin.metadata.dao.internal;

import com.linkedin.testing.EntityBar;
import com.linkedin.testing.EntityFoo;
import org.testng.annotations.Test;

import static com.linkedin.metadata.dao.Neo4jUtil.*;
import static com.linkedin.testing.TestUtils.*;
import static org.testng.Assert.*;


public class Neo4jQueriesTransformerTest {

  @Test
  public void testGetNodeTypeFromUrn() {
    final Neo4jQueriesTransformer transformer = new Neo4jQueriesTransformer(getAllTestEntities());

    assertEquals(transformer.getNodeType(makeBarUrn(1)), ":`com.linkedin.testing.EntityBar`");
    assertEquals(transformer.getNodeType(makeFooUrn(1)), ":`com.linkedin.testing.EntityFoo`");
    assertEquals(transformer.getNodeType(makeUrn(1, "foo")), ":`com.linkedin.testing.EntityFoo`");
    assertEquals(transformer.getNodeType(makeUrn("1")), ":UNKNOWN");

    // test consistency !!
    assertEquals(transformer.getNodeType(makeBarUrn(1)), getTypeOrEmptyString(EntityBar.class));
    assertEquals(transformer.getNodeType(makeFooUrn(1)), getTypeOrEmptyString(EntityFoo.class));
  }
}