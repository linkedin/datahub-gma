package com.linkedin.metadata.dao.utils;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.testing.RelationshipBar;
import com.linkedin.testing.RelationshipFoo;
import com.linkedin.testing.RelationshipV2Bar;
import com.linkedin.testing.urn.BarUrn;
import com.linkedin.testing.urn.BazUrn;
import com.linkedin.testing.urn.FooUrn;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import static org.testng.Assert.*;

public class GraphUtilsTest {

  @Test
  public void testCheckSameSourceUrnWithEmptyRelationships() {
    List<RecordTemplate> relationships = Collections.emptyList();
    GraphUtils.checkSameSourceUrn(relationships, null);
    // No exception should be thrown
  }

  @Test
  public void testCheckSameSourceUrnWithSameSourceUrn() throws URISyntaxException {
    // Test cases for relationship V1
    RelationshipFoo relationship;
    try {
      relationship = mockRelationshipFoo(new FooUrn(1), new BarUrn(2));
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }

    List<RecordTemplate> relationships = Lists.newArrayList(relationship, relationship);
    // when the assetUrn is not provided for relationship v1, throw an IllegalArgumentException
    try {
      GraphUtils.checkSameSourceUrn(relationships, null);
      fail("Expected an IllegalArgumentException to be thrown, but it wasn't");
    } catch (IllegalArgumentException e) {
      // do nothing
    }

    // when the assetUrn is provided for relationship V1, check all relationships to see if they have the same source
    try {
      GraphUtils.checkSameSourceUrn(relationships, new FooUrn(1));
    } catch (IllegalArgumentException e) {
      fail("Expected no IllegalArgumentException to be thrown, but got: " + e.getMessage());
    }

    // given an assetUrn that is not the same as the source urns of the relationships, throw an exception
    try {
      GraphUtils.checkSameSourceUrn(relationships, new BarUrn(10));
      fail("Expected an IllegalArgumentException to be thrown, but it wasn't");
    } catch (IllegalArgumentException e) {
      // do nothing
    }

    // Test cases for relationship V2
    RelationshipV2Bar relationshipV2 = mockRelationshipV2Bar(new BarUrn(2));
    List<RecordTemplate> relationshipsV2 = Lists.newArrayList(relationshipV2, relationshipV2);
    // when urn is provided for relationship V2, the check should pass
    try {
      GraphUtils.checkSameSourceUrn(relationshipsV2, new BarUrn(2));
    } catch (IllegalArgumentException e) {
      fail("Expected no IllegalArgumentException to be thrown, but got: " + e.getMessage());
    }
    // when urn is not provided for relationship V2, it should throw IllegalArgumentException
    assertThrows(IllegalArgumentException.class,
        () -> GraphUtils.checkSameSourceUrn(relationshipsV2, null)
    );
  }

  @Test
  public void testCheckSameSourceUrnWithDifferentSourceUrn() {
    // Test cases for relationship V1 (not applicable to relationship v2)
    RecordTemplate relationship1;
    RecordTemplate relationship2;
    try {
      relationship1 = mockRelationshipFoo(new FooUrn(1), new BarUrn(2));
      relationship2 = mockRelationshipFoo(new FooUrn(3), new BarUrn(2));
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }

    List<RecordTemplate> relationships = Lists.newArrayList(relationship1, relationship2);
    assertThrows(IllegalArgumentException.class,
        () -> GraphUtils.checkSameSourceUrn(relationships, null)
    );
    // when urn is provided for relationship V1, the provided urn should be ignored
    assertThrows(IllegalArgumentException.class,
        () -> GraphUtils.checkSameSourceUrn(relationships, new BarUrn(10))
    );
  }

  @Test
  public void testCheckSameSourceUrnWithDifferentDestinationUrn() {
    // Test cases for relationship V1 (already tested v2 case in testCheckSameSourceUrnWithSameSourceUrn)
    RelationshipFoo relationship1;
    RelationshipBar relationship2;
    try {
      relationship1 = mockRelationshipFoo(new FooUrn(1), new BarUrn(2));
      relationship2 = new RelationshipBar().setSource(new FooUrn(4)).setDestination(new BazUrn(2));
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }

    List<RecordTemplate> relationships1 = Lists.newArrayList(relationship1, relationship2);
    assertThrows(IllegalArgumentException.class,
        () -> GraphUtils.checkSameSourceUrn(relationships1, null)
    );

    assertThrows(IllegalArgumentException.class,
        () -> GraphUtils.checkSameSourceUrn(relationships1, new BarUrn(10))
    );
  }

  private RelationshipFoo mockRelationshipFoo(FooUrn expectedSource, BarUrn expectedDestination) {
    return new RelationshipFoo().setSource(expectedSource).setDestination(expectedDestination);
  }

  private RelationshipV2Bar mockRelationshipV2Bar(BarUrn barUrn) {
    RelationshipV2Bar.Destination destination = new RelationshipV2Bar.Destination();
    destination.setDestinationBar(barUrn);
    return new RelationshipV2Bar().setDestination(destination);
  }
}