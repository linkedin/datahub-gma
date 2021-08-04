package com.linkedin.metadata.dao;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.internal.Neo4jGraphWriterDAO;
import com.linkedin.metadata.dao.utils.Statement;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.RelationshipDirection;
import com.linkedin.metadata.query.RelationshipFilter;
import com.linkedin.testing.EntityBar;
import com.linkedin.testing.EntityFoo;
import com.linkedin.testing.RelationshipBar;
import com.linkedin.testing.RelationshipFoo;
import com.linkedin.testing.TestUtils;
import com.linkedin.testing.urn.BarUrn;
import com.linkedin.testing.urn.FooUrn;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.linkedin.metadata.dao.utils.QueryUtils.*;
import static com.linkedin.metadata.dao.utils.QueryUtils.EMPTY_FILTER;
import static com.linkedin.testing.TestUtils.makeBarUrn;
import static com.linkedin.testing.TestUtils.makeFooUrn;
import static org.testng.Assert.assertEquals;


public class Neo4jQueryDAOTest extends BaseQueryDAOTestBase<Neo4jQueryDAO, Neo4jGraphWriterDAO> {

  private Neo4jTestServerBuilder _serverBuilder;
  private Driver _driver;

  @Nonnull
  @Override
  Neo4jQueryDAO newBaseQueryDao() {
    return new Neo4jQueryDAO(_driver);
  }

  @Nonnull
  @Override
  Neo4jGraphWriterDAO newBaseGraphWriterDao() {
    return new Neo4jGraphWriterDAO(_driver, TestUtils.getAllTestEntities());
  }

  @Override
  @BeforeMethod
  public void init() {
    _serverBuilder = new Neo4jTestServerBuilder();
    _serverBuilder.newServer();
    _driver = GraphDatabase.driver(_serverBuilder.boltURI());

    super.init();
  }

  @AfterMethod
  public void tearDown() {
    _serverBuilder.shutdown();
  }


  @Test
  public void testFindNodesInPath() throws Exception {
    FooUrn urn1 = makeFooUrn(1);
    EntityFoo entity1 = new EntityFoo().setUrn(urn1).setValue("fooDirector");
    _writer.addEntity(entity1);

    FooUrn urn2 = makeFooUrn(2);
    EntityFoo entity2 = new EntityFoo().setUrn(urn2).setValue("fooManager1");
    _writer.addEntity(entity2);

    FooUrn urn3 = makeFooUrn(3);
    EntityFoo entity3 = new EntityFoo().setUrn(urn3).setValue("fooManager2");
    _writer.addEntity(entity3);

    FooUrn urn4 = makeFooUrn(4);
    EntityFoo entity4 = new EntityFoo().setUrn(urn3).setValue("fooReport1ofManager1");
    _writer.addEntity(entity4);

    FooUrn urn5 = makeFooUrn(5);
    EntityFoo entity5 = new EntityFoo().setUrn(urn3).setValue("fooReport1ofManager2");
    _writer.addEntity(entity5);

    FooUrn urn6 = makeFooUrn(6);
    EntityFoo entity6 = new EntityFoo().setUrn(urn3).setValue("fooReport2ofManager2");
    _writer.addEntity(entity6);

    // Create relationships - simulate reportsto use case
    _writer.addRelationship(new RelationshipFoo().setSource(urn6).setDestination(urn3));
    _writer.addRelationship(new RelationshipFoo().setSource(urn5).setDestination(urn3));
    _writer.addRelationship(new RelationshipFoo().setSource(urn4).setDestination(urn2));
    _writer.addRelationship(new RelationshipFoo().setSource(urn3).setDestination(urn1));
    _writer.addRelationship(new RelationshipFoo().setSource(urn2).setDestination(urn1));

    // Get reports roll-up - 2 levels
    Filter sourceFilter = newFilter("urn", urn1.toString());
    RelationshipFilter relationshipFilter = newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING);
    List<List<RecordTemplate>> paths = _dao.findPaths(EntityFoo.class, sourceFilter, null,
        EMPTY_FILTER, RelationshipFoo.class, relationshipFilter, 1, 2, -1, -1);
    assertEquals(paths.size(), 5);
    assertEquals(paths.stream().filter(l -> l.size() == 3).collect(Collectors.toList()).size(), 2);
    assertEquals(paths.stream().filter(l -> l.size() == 5).collect(Collectors.toList()).size(), 3);

    // Get reports roll-up - 1 level
    paths = _dao.findPaths(EntityFoo.class, sourceFilter, null,
        EMPTY_FILTER, RelationshipFoo.class, relationshipFilter, 1, 1, -1, -1);
    assertEquals(paths.size(), 2);
    assertEquals(paths.stream().filter(l -> l.size() == 3).collect(Collectors.toList()).size(), 2);
    assertEquals(paths.stream().filter(l -> l.size() == 5).collect(Collectors.toList()).size(), 0);
  }

  @Test
  public void testFindPaths() throws Exception {
    BarUrn srcUrn = makeBarUrn(0);
    BarUrn des1Urn = makeBarUrn(1);
    BarUrn des2Urn = makeBarUrn(2);

    FooUrn srcField1Urn = makeFooUrn(1);
    FooUrn srcField2Urn = makeFooUrn(2);
    FooUrn des1Field1Urn = makeFooUrn(11);
    FooUrn des1Field2Urn = makeFooUrn(12);
    FooUrn des2Field1Urn = makeFooUrn(21);

    EntityBar src = new EntityBar().setUrn(srcUrn);
    EntityBar des1 = new EntityBar().setUrn(des1Urn);
    EntityBar des2 = new EntityBar().setUrn(des2Urn);
    EntityFoo srcField1 = new EntityFoo().setUrn(srcField1Urn);
    EntityFoo srcField2 = new EntityFoo().setUrn(srcField2Urn);
    EntityFoo des1Field1 = new EntityFoo().setUrn(des1Field1Urn);
    EntityFoo des1Field2 = new EntityFoo().setUrn(des1Field2Urn);
    EntityFoo des2Field1 = new EntityFoo().setUrn(des2Field1Urn);

    _writer.addEntity(src);
    _writer.addEntity(des1);
    _writer.addEntity(des2);
    _writer.addEntity(srcField1);
    _writer.addEntity(srcField2);
    _writer.addEntity(des1Field1);
    _writer.addEntity(des1Field2);
    _writer.addEntity(des2Field1);

    String commandText = "MATCH p=(n1 {urn:$src})-"
        + "[r1]->(n2)-[r2:`com.linkedin.testing.RelationshipFoo`*0..100]->(n3)<-[r3]-() return p";
    Map<String, Object> params = new HashMap<>();
    params.put("src", srcUrn.toString());
    Statement statement = new Statement(commandText, params);

    // Test one path
    createBarRelationship(srcUrn, srcField1Urn);
    createBarRelationship(des1Urn, des1Field1Urn);
    createFooRelationship(srcField1Urn, des1Field1Urn);

    List<List<RecordTemplate>> paths = _dao.findPaths(statement);
    assertEquals(paths.size(), 1);

    List<RecordTemplate> path = paths.get(0);
    assertEquals(path.size(), 7);

    assertEquals(path.get(0), src);
    assertEquals(path.get(2), srcField1);
    assertEquals(path.get(4), des1Field1);
    assertEquals(path.get(6), des1);

    // Test multiple paths
    createBarRelationship(des1Urn, des1Field2Urn);
    createBarRelationship(des2Urn, des2Field1Urn);
    createBarRelationship(srcUrn, srcField2Urn);
    createFooRelationship(srcField2Urn, des1Field2Urn);
    createFooRelationship(srcField2Urn, des2Field1Urn);

    paths = _dao.findPaths(statement);

    assertEquals(paths.size(), 3);
    paths.forEach(p -> assertEquals(p.size(), 7));
  }

  @Test
  public void testRunFreeFormQuery() throws Exception {
    FooUrn urn1 = makeFooUrn(1);
    FooUrn urn2 = makeFooUrn(2);
    EntityFoo entity1 = new EntityFoo().setUrn(urn1).setValue("foo");
    EntityFoo entity2 = new EntityFoo().setUrn(urn2).setValue("foo");
    _writer.addEntity(entity1);
    _writer.addEntity(entity2);

    String cypherQuery = "MATCH (n {value:\"foo\"}) RETURN n ORDER BY n.urn";
    List<Record> result = _dao.runFreeFormQuery(cypherQuery);
    List<EntityFoo> nodes = result.stream()
        .map(record -> _dao.nodeRecordToEntity(EntityFoo.class, record))
        .collect(Collectors.toList());
    assertEquals(nodes.size(), 2);
    assertEquals(nodes.get(0), entity1);
    assertEquals(nodes.get(1), entity2);

    cypherQuery = "MATCH (n {value:\"foo\"}) RETURN count(n)";
    result = _dao.runFreeFormQuery(cypherQuery);
    assertEquals(result.size(), 1);
    assertEquals(result.get(0).values().get(0).asInt(), 2);
  }

  private void createFooRelationship(FooUrn f1, FooUrn f2) throws Exception {
    _writer.addRelationship(new RelationshipFoo().setSource(f1).setDestination(f2));
  }

  private void createBarRelationship(BarUrn d1, FooUrn f1) throws Exception {
    _writer.addRelationship(new RelationshipBar().setSource(d1).setDestination(f1));
  }
}
