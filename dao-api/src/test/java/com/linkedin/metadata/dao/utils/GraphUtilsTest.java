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
    // No exception should be thrown
  }

  @Test
  public void testCheckSameUrnWithSameSourceUrn() {
    // ToDo: Add test cases for relationship V2

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
  }

  @Test
  public void testCheckSameUrnWithDifferentSourceUrn() {
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
  }

  @Test
  public void testCheckSameUrnWithSameDestinationUrn() {
    RelationshipFoo relationship1;
    RelationshipV2Bar relationship2;
    try {
      relationship1 = mockRelationshipFoo(new FooUrn(1), new BarUrn(2));
      relationship2 = mockRelationshipV2Bar(new BarUrn(2));
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    List<RecordTemplate> relationships = Lists.newArrayList(relationship1, relationship2);

    try {
      GraphUtils.checkSameUrn(relationships, BaseGraphWriterDAO.RemovalOption.REMOVE_ALL_EDGES_TO_DESTINATION, "source",
          "destination");
    } catch (IllegalArgumentException e) {
      fail("Expected no IllegalArgumentException to be thrown, but got: " + e.getMessage());
    }
  }

  @Test
  public void testCheckSameUrnWithDifferentDestinationUrn() {
    RelationshipFoo relationship1;
    RelationshipBar relationship2;
    RelationshipV2Bar relationship3;
    try {
      relationship1 = mockRelationshipFoo(new FooUrn(1), new BarUrn(2));
      relationship2 = new RelationshipBar().setSource(new FooUrn(4)).setDestination(new BazUrn(2));
      relationship3 = mockRelationshipV2Bar(new BarUrn(3));
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }

    List<RecordTemplate> relationships = Lists.newArrayList(relationship1, relationship2, relationship3);
    assertThrows(IllegalArgumentException.class,
        () -> GraphUtils.checkSameUrn(
            relationships,
            BaseGraphWriterDAO.RemovalOption.REMOVE_ALL_EDGES_TO_DESTINATION,
            "source",
            "destination")
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