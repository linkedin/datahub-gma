package com.linkedin.metadata.dao.internal;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.Neo4jQueryDAO;
import com.linkedin.metadata.dao.Neo4jTestServerBuilder;
import com.linkedin.testing.TestUtils;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class Neo4jGraphWriterDAOTest extends BaseGraphWriterDAOTestBase<Neo4jGraphWriterDAO, Neo4jQueryDAO> {

  private Neo4jTestServerBuilder _serverBuilder;
  private Driver _driver;
  private Neo4jTestHelper _helper;
  private TestMetricListener _testMetricListener;

  private static class TestMetricListener implements Neo4jGraphWriterDAO.MetricListener {
    int entitiesAdded = 0;
    int entityAddedEvents = 0;
    int entitiesRemoved = 0;
    int entityRemovedEvents = 0;
    int relationshipsAdded = 0;
    int relationshipAddedEvents = 0;
    int relationshipsRemoved = 0;
    int relationshipRemovedEvents = 0;

    @Override
    public void onEntitiesAdded(int entityCount, long updateTimeMs, int retries) {
      entityAddedEvents++;
      entitiesAdded += entityCount;
    }

    @Override
    public void onRelationshipsAdded(int relationshipCount, long updateTimeMs, int retries) {
      relationshipAddedEvents++;
      relationshipsAdded += relationshipCount;
    }

    @Override
    public void onEntitiesRemoved(int entityCount, long updateTimeMs, int retries) {
      entityRemovedEvents++;
      entitiesRemoved += entityCount;
    }

    @Override
    public void onRelationshipsRemoved(int relationshipCount, long updateTimeMs, int retries) {
      relationshipRemovedEvents++;
      relationshipsRemoved += relationshipCount;
    }
  }

  @Nonnull
  @Override
  Neo4jGraphWriterDAO newBaseGraphWriterDao() {
    Neo4jGraphWriterDAO dao = new Neo4jGraphWriterDAO(_driver, TestUtils.getAllTestEntities());
    dao.addMetricListener(_testMetricListener);
    return dao;
  }

  @Nonnull
  @Override
  Neo4jQueryDAO newBaseQueryDao() {
    return new Neo4jQueryDAO(_driver);
  }

  @Nonnull
  @Override
  public Optional<Map<String, Object>> getNode(@Nonnull Urn urn) {
    return _helper.getNode(urn);
  }

  @Nonnull
  @Override
  public List<Map<String, Object>> getAllNodes(@Nonnull Urn urn) {
    return _helper.getAllNodes(urn);
  }

  @Nonnull
  @Override
  public List<Map<String, Object>> getEdges(@Nonnull RecordTemplate relationship) {
    return _helper.getEdges(relationship);
  }

  @Nonnull
  @Override
  public List<Map<String, Object>> getEdgesFromSource(@Nonnull Urn sourceUrn, @Nonnull Class<? extends RecordTemplate> relationshipClass) {
    return _helper.getEdgesFromSource(sourceUrn, relationshipClass);
  }

  @Override
  @BeforeMethod
  public void init() {
    _serverBuilder = new Neo4jTestServerBuilder();
    _serverBuilder.newServer();
    _testMetricListener = new TestMetricListener();
    _driver = GraphDatabase.driver(_serverBuilder.boltURI());
    _helper = new Neo4jTestHelper(_driver, TestUtils.getAllTestEntities());

    super.init();
  }

  @AfterMethod
  public void tearDown() {
    _serverBuilder.shutdown();
  }

  @Test
  @Override
  public void testAddEntity() throws Exception {
    super.testAddEntity();
    assertEquals(_testMetricListener.entitiesAdded, 1);
    assertEquals(_testMetricListener.entityAddedEvents, 1);
  }

  @Test
  @Override
  public void testRemoveEntity() throws Exception {
    super.testRemoveEntity();
    assertEquals(_testMetricListener.entitiesRemoved, 1);
    assertEquals(_testMetricListener.entityRemovedEvents, 1);
  }

  @Test
  @Override
  public void testAddEntities() throws Exception {
    super.testAddEntities();
    assertEquals(_testMetricListener.entitiesAdded, 3);
    assertEquals(_testMetricListener.entityAddedEvents, 1);
  }

  @Test
  @Override
  public void testRemoveEntities() throws Exception {
    super.testRemoveEntities();
    assertEquals(_testMetricListener.entitiesRemoved, 2);
    assertEquals(_testMetricListener.entityRemovedEvents, 1);
  }

  @Test
  @Override
  public void testAddRelationshipNodeNonExist() throws Exception {
    super.testAddRelationshipNodeNonExist();
    assertEquals(_testMetricListener.relationshipsAdded, 1);
    assertEquals(_testMetricListener.relationshipAddedEvents, 1);
  }

  @Test
  @Override
  public void testAddRemoveRelationships() throws Exception {
    super.testAddRemoveRelationships();

    assertEquals(_testMetricListener.relationshipsAdded, 3);
    assertEquals(_testMetricListener.relationshipAddedEvents, 3);

    assertEquals(_testMetricListener.relationshipsRemoved, 3);
    assertEquals(_testMetricListener.relationshipRemovedEvents, 2);
  }
}
