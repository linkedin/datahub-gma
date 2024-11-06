package com.linkedin.metadata.dao.utils;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.internal.BaseGraphWriterDAO;
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
  public void testCheckSameUrnWithEmptyRelationships() {
    List<RecordTemplate> relationships = Collections.emptyList();
    GraphUtils.checkSameUrn(relationships, BaseGraphWriterDAO.RemovalOption.REMOVE_ALL_EDGES_FROM_SOURCE, "source", "destination");
    GraphUtils.checkSameUrn(relationships, BaseGraphWriterDAO.RemovalOption.REMOVE_ALL_EDGES_TO_DESTINATION, "source", "destination", new BarUrn(1));
    // No exception should be thrown
  }

  @Test
  public void testCheckSameUrnWithSameSourceUrn() {
    // Test cases for relationship V1
    RelationshipFoo relationship;
    try {
      relationship = mockRelationshipFoo(new FooUrn(1), new BarUrn(2));
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }

    List<RecordTemplate> relationships = Lists.newArrayList(relationship, relationship);
    try {
      GraphUtils.checkSameUrn(relationships, BaseGraphWriterDAO.RemovalOption.REMOVE_ALL_EDGES_FROM_SOURCE, "source", "destination");
    } catch (IllegalArgumentException e) {
      fail("Expected no IllegalArgumentException to be thrown, but got: " + e.getMessage());
    }

    // when urn is provided for relationship V1, the provided urn should be ignored
    try {
      GraphUtils.checkSameUrn(relationships, BaseGraphWriterDAO.RemovalOption.REMOVE_ALL_EDGES_FROM_SOURCE, "source", "destination", new BarUrn(10));
    } catch (IllegalArgumentException e) {
      fail("Expected no IllegalArgumentException to be thrown, but got: " + e.getMessage());
    }

    // Test cases for relationship V2
    RelationshipV2Bar relationshipV2 = mockRelationshipV2Bar(new BarUrn(2));
    List<RecordTemplate> relationshipsV2 = Lists.newArrayList(relationshipV2, relationshipV2);
    // when urn is provided for relationship V2, the check should pass
    try {
      GraphUtils.checkSameUrn(relationshipsV2, BaseGraphWriterDAO.RemovalOption.REMOVE_ALL_EDGES_FROM_SOURCE, "source", "destination", new BarUrn(2));
    } catch (IllegalArgumentException e) {
      fail("Expected no IllegalArgumentException to be thrown, but got: " + e.getMessage());
    }
    // when urn is not provided for relationship V2, it should throw IllegalArgumentException
    assertThrows(IllegalArgumentException.class,
        () -> GraphUtils.checkSameUrn(
            relationshipsV2,
            BaseGraphWriterDAO.RemovalOption.REMOVE_ALL_EDGES_FROM_SOURCE,
            "source",
            "destination")
    );
  }

  @Test
  public void testCheckSameUrnWithDifferentSourceUrn() {
    // Test cases for relationship V1
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
        () -> GraphUtils.checkSameUrn(
            relationships,
            BaseGraphWriterDAO.RemovalOption.REMOVE_ALL_EDGES_FROM_SOURCE,
            "source",
            "destination")
    );
    // when urn is provided for relationship V1, the provided urn should be ignored
    assertThrows(IllegalArgumentException.class,
        () -> GraphUtils.checkSameUrn(
            relationships,
            BaseGraphWriterDAO.RemovalOption.REMOVE_ALL_EDGES_FROM_SOURCE,
            "source",
            "destination",
            new BarUrn(10))
    );

    // ToDo: add test cases for V2. Right now it check if a list of relationships have the same source urn.
  }

  @Test
  public void testCheckSameUrnWithSameDestinationUrn() {
    // Test cases for relationship V1
    RelationshipFoo relationship1;
    try {
      relationship1 = mockRelationshipFoo(new FooUrn(1), new BarUrn(2));
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    List<RecordTemplate> relationships1 = Lists.newArrayList(relationship1, relationship1);

    try {
      GraphUtils.checkSameUrn(relationships1, BaseGraphWriterDAO.RemovalOption.REMOVE_ALL_EDGES_TO_DESTINATION, "source",
          "destination");
    } catch (IllegalArgumentException e) {
      fail("Expected no IllegalArgumentException to be thrown, but got: " + e.getMessage());
    }

    // when urn is provided for relationship V1, the provided urn should be ignored
    try {
      GraphUtils.checkSameUrn(relationships1, BaseGraphWriterDAO.RemovalOption.REMOVE_ALL_EDGES_TO_DESTINATION, "source",
          "destination", new BarUrn(10));
    } catch (IllegalArgumentException e) {
      fail("Expected no IllegalArgumentException to be thrown, but got: " + e.getMessage());
    }

    // Test cases for relationship V2
    RelationshipV2Bar relationship2 = mockRelationshipV2Bar(new BarUrn(2));
    List<RecordTemplate> relationships2 = Lists.newArrayList(relationship2, relationship2);

    // throws exception if V2 relationships without source urn provided
    assertThrows(IllegalArgumentException.class,
        () -> GraphUtils.checkSameUrn(
            relationships2,
            BaseGraphWriterDAO.RemovalOption.REMOVE_ALL_EDGES_TO_DESTINATION,
            "source",
            "destination")
    );

    try {
      GraphUtils.checkSameUrn(relationships2, BaseGraphWriterDAO.RemovalOption.REMOVE_ALL_EDGES_TO_DESTINATION, "source",
          "destination", new BarUrn(10));
    } catch (IllegalArgumentException e) {
      fail("Expected no IllegalArgumentException to be thrown, but got: " + e.getMessage());
    }
  }

  @Test
  public void testCheckSameUrnWithDifferentDestinationUrn() {
    // Test cases for relationship V1
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
        () -> GraphUtils.checkSameUrn(
            relationships1,
            BaseGraphWriterDAO.RemovalOption.REMOVE_ALL_EDGES_TO_DESTINATION,
            "source",
            "destination")
    );

    assertThrows(IllegalArgumentException.class,
        () -> GraphUtils.checkSameUrn(
            relationships1,
            BaseGraphWriterDAO.RemovalOption.REMOVE_ALL_EDGES_TO_DESTINATION,
            "source",
            "destination",
            new BarUrn(10))
    );

    // Test cases for relationship V2
    RelationshipV2Bar relationship3 = mockRelationshipV2Bar(new BarUrn(3));
    RelationshipV2Bar relationship4 = mockRelationshipV2Bar(new BarUrn(4));
    List<RecordTemplate> relationships2 = Lists.newArrayList(relationship3, relationship4);
    assertThrows(IllegalArgumentException.class,
        () -> GraphUtils.checkSameUrn(
            relationships2,
            BaseGraphWriterDAO.RemovalOption.REMOVE_ALL_EDGES_TO_DESTINATION,
            "source",
            "destination")
    );

    assertThrows(IllegalArgumentException.class,
        () -> GraphUtils.checkSameUrn(
            relationships2,
            BaseGraphWriterDAO.RemovalOption.REMOVE_ALL_EDGES_TO_DESTINATION,
            "source",
            "destination",
            new BarUrn(10))
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