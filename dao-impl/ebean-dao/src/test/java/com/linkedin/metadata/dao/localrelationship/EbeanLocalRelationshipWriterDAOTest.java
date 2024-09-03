package com.linkedin.metadata.dao.localrelationship;

import com.google.common.io.Resources;
import com.linkedin.metadata.dao.EbeanLocalRelationshipWriterDAO;
import com.linkedin.metadata.dao.internal.BaseGraphWriterDAO;
import com.linkedin.metadata.dao.localrelationship.builder.BelongsToLocalRelationshipBuilder;
import com.linkedin.metadata.dao.localrelationship.builder.PairsWithLocalRelationshipBuilder;
import com.linkedin.metadata.dao.localrelationship.builder.ReportsToLocalRelationshipBuilder;
import com.linkedin.metadata.dao.localrelationship.builder.VersionOfLocalRelationshipBuilder;
import com.linkedin.metadata.dao.utils.EmbeddedMariaInstance;
import com.linkedin.metadata.dao.builder.BaseLocalRelationshipBuilder.LocalRelationshipUpdates;
import com.linkedin.testing.BarUrnArray;
import com.linkedin.testing.localrelationship.AspectFooBar;
import com.linkedin.testing.localrelationship.PairsWith;
import com.linkedin.testing.urn.BarUrn;
import com.linkedin.testing.urn.FooUrn;
import io.ebean.Ebean;
import io.ebean.EbeanServer;
import io.ebean.SqlRow;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class EbeanLocalRelationshipWriterDAOTest {
  private EbeanServer _server;
  private EbeanLocalRelationshipWriterDAO _localRelationshipWriterDAO;

  @BeforeClass
  public void init() throws IOException {
    _server = EmbeddedMariaInstance.getServer(EbeanLocalRelationshipWriterDAOTest.class.getSimpleName());
    _server.execute(Ebean.createSqlUpdate(
        Resources.toString(Resources.getResource("ebean-local-relationship-dao-create-all.sql"), StandardCharsets.UTF_8)));
    _localRelationshipWriterDAO = new EbeanLocalRelationshipWriterDAO(_server);
  }

  @Test
  public void testAddRelationshipWithRemoveAllEdgesToDestination() throws URISyntaxException {
    _server.execute(Ebean.createSqlUpdate(insertRelationships("metadata_relationship_belongsto", "urn:li:bar:000",
        "bar", "urn:li:foo:123", "foo")));

    AspectFooBar aspectFooBar = new AspectFooBar().setBars(new BarUrnArray(
        BarUrn.createFromString("urn:li:bar:123"),
        BarUrn.createFromString("urn:li:bar:456"),
        BarUrn.createFromString("urn:li:bar:789")));

    List<LocalRelationshipUpdates> updates = new BelongsToLocalRelationshipBuilder(AspectFooBar.class)
        .buildRelationships(FooUrn.createFromString("urn:li:foo:123"), aspectFooBar);

    // Before processing
    List<SqlRow> before = _server.createSqlQuery("select * from metadata_relationship_belongsto where source='urn:li:bar:000'").findList();
    assertEquals(before.size(), 1);

    _localRelationshipWriterDAO.processLocalRelationshipUpdates(FooUrn.createFromString("urn:li:foo:123"), updates, false);

    // After processing verification
    List<SqlRow> all = _server.createSqlQuery("select * from metadata_relationship_belongsto").findList();
    assertEquals(all.size(), 4); // Total number of edges is 4

    List<SqlRow> softDeleted = _server.createSqlQuery("select * from metadata_relationship_belongsto where deleted_ts IS NOT NULL").findList();
    assertEquals(softDeleted.size(), 1); // 1 soft deleted edge

    List<SqlRow> newEdges = _server.createSqlQuery(
        "select * from metadata_relationship_belongsto where destination='urn:li:foo:123' and deleted_ts IS NULL").findList();

    assertEquals(newEdges.size(), 3); // 3 new edges added.

    // Clean up
    _server.execute(Ebean.createSqlUpdate("truncate metadata_relationship_belongsto"));
  }

  @Test
  public void testAddRelationshipWithRemoveNone() throws URISyntaxException {
    _server.execute(Ebean.createSqlUpdate(insertRelationships("metadata_relationship_reportsto", "urn:li:bar:000",
        "bar", "urn:li:foo:123", "foo")));

    AspectFooBar aspectFooBar = new AspectFooBar().setBars(new BarUrnArray(
        BarUrn.createFromString("urn:li:bar:123"),
        BarUrn.createFromString("urn:li:bar:456"),
        BarUrn.createFromString("urn:li:bar:789")));

    List<LocalRelationshipUpdates> updates = new ReportsToLocalRelationshipBuilder(AspectFooBar.class)
        .buildRelationships(FooUrn.createFromString("urn:li:foo:123"), aspectFooBar);

    // Before processing
    List<SqlRow> before = _server.createSqlQuery("select * from metadata_relationship_reportsto where source='urn:li:bar:000'").findList();
    assertEquals(before.size(), 1);

    _localRelationshipWriterDAO.processLocalRelationshipUpdates(FooUrn.createFromString("urn:li:foo:123"), updates, false);

    // After processing verification
    List<SqlRow> after = _server.createSqlQuery("select * from metadata_relationship_reportsto where destination='urn:li:foo:123'").findList();
    assertEquals(after.size(), 4);
    List<SqlRow> edges = _server.createSqlQuery("select * from metadata_relationship_reportsto where source='urn:li:bar:000'").findList();
    assertEquals(edges.size(), 1);

    // Clean up
    _server.execute(Ebean.createSqlUpdate("truncate metadata_relationship_reportsto"));
  }

  @Test
  public void testAddRelationshipWithRemoveNoneInTestMode() throws URISyntaxException {
    _server.execute(Ebean.createSqlUpdate(
        insertRelationships("metadata_relationship_reportsto_test", "urn:li:bar:000", "bar", "urn:li:foo:123", "foo")));

    AspectFooBar aspectFooBar = new AspectFooBar().setBars(
        new BarUrnArray(BarUrn.createFromString("urn:li:bar:123"), BarUrn.createFromString("urn:li:bar:456"),
            BarUrn.createFromString("urn:li:bar:789")));

    List<LocalRelationshipUpdates> updates =
        new ReportsToLocalRelationshipBuilder(AspectFooBar.class).buildRelationships(
            FooUrn.createFromString("urn:li:foo:123"), aspectFooBar);

    // Before processing
    List<SqlRow> beforeTest =
        _server.createSqlQuery("select * from metadata_relationship_reportsto_test where source='urn:li:bar:000'")
            .findList();
    assertEquals(beforeTest.size(), 1);

    _localRelationshipWriterDAO.processLocalRelationshipUpdates(FooUrn.createFromString("urn:li:foo:123"), updates,
        true);

    // After processing verification
    List<SqlRow> afterTest =
        _server.createSqlQuery("select * from metadata_relationship_reportsto_test where destination='urn:li:foo:123'")
            .findList();
    assertEquals(afterTest.size(), 4);
    List<SqlRow> edgesTest =
        _server.createSqlQuery("select * from metadata_relationship_reportsto_test where source='urn:li:bar:000'")
            .findList();
    assertEquals(edgesTest.size(), 1);

    // Clean up
    _server.execute(Ebean.createSqlUpdate("truncate metadata_relationship_reportsto_test"));
  }

  @Test
  public void testAddRelationshipWithRemoveAllEdgesFromSourceToDestination() throws URISyntaxException {
    _server.execute(Ebean.createSqlUpdate(insertRelationships("metadata_relationship_pairswith", "urn:li:bar:123",
        "bar", "urn:li:foo:123", "foo")));

    _server.execute(Ebean.createSqlUpdate(insertRelationships("metadata_relationship_pairswith", "urn:li:bar:123",
        "bar", "urn:li:foo:123", "foo")));

    _server.execute(Ebean.createSqlUpdate(insertRelationships("metadata_relationship_pairswith", "urn:li:bar:000",
        "bar", "urn:li:foo:123", "foo")));

    AspectFooBar aspectFooBar = new AspectFooBar().setBars(new BarUrnArray(BarUrn.createFromString("urn:li:bar:123")));

    List<LocalRelationshipUpdates> updates = new PairsWithLocalRelationshipBuilder(AspectFooBar.class)
        .buildRelationships(FooUrn.createFromString("urn:li:foo:123"), aspectFooBar);

    // Before processing
    List<SqlRow> before = _server.createSqlQuery("select * from metadata_relationship_pairswith").findList();
    assertEquals(before.size(), 3);

    _localRelationshipWriterDAO.processLocalRelationshipUpdates(FooUrn.createFromString("urn:li:foo:123"), updates, false);

    // After processing verification
    List<SqlRow> all = _server.createSqlQuery("select * from metadata_relationship_pairswith").findList();
    assertEquals(all.size(), 4); // Total number of edges is 4

    List<SqlRow> softDeleted = _server.createSqlQuery("select * from metadata_relationship_pairswith where deleted_ts IS NOT NULL").findList();
    assertEquals(softDeleted.size(), 2); // 2 edges are soft-deleted.

    List<SqlRow> oldEdge = _server.createSqlQuery(
        "select * from metadata_relationship_pairswith where source='urn:li:bar:000' and deleted_ts IS NULL").findList();
    assertEquals(oldEdge.size(), 1); // 1 old edge untouched.

    // Clean up
    _server.execute(Ebean.createSqlUpdate("truncate metadata_relationship_pairswith"));
  }

  @Test
  public void testAddRelationshipWithRemoveAllEdgesFromSource() throws URISyntaxException {
    _server.execute(Ebean.createSqlUpdate(insertRelationships("metadata_relationship_versionof", "urn:li:bar:123",
        "bar", "urn:li:foo:123", "foo")));

    _server.execute(Ebean.createSqlUpdate(insertRelationships("metadata_relationship_versionof", "urn:li:bar:123",
        "bar", "urn:li:foo:000", "foo")));

    _server.execute(Ebean.createSqlUpdate(insertRelationships("metadata_relationship_versionof", "urn:li:bar:000",
        "bar", "urn:li:foo:123", "foo")));

    AspectFooBar aspectFooBar = new AspectFooBar().setBars(new BarUrnArray(BarUrn.createFromString("urn:li:bar:123")));

    List<LocalRelationshipUpdates> updates = new VersionOfLocalRelationshipBuilder(AspectFooBar.class)
        .buildRelationships(FooUrn.createFromString("urn:li:foo:123"), aspectFooBar);

    // Before processing
    List<SqlRow> before = _server.createSqlQuery("select * from metadata_relationship_versionof").findList();
    assertEquals(before.size(), 3);

    _localRelationshipWriterDAO.processLocalRelationshipUpdates(FooUrn.createFromString("urn:li:foo:123"), updates, false);

    // After processing verification
    List<SqlRow> all = _server.createSqlQuery("select * from metadata_relationship_versionof").findList();
    assertEquals(all.size(), 4); // Total number of edges

    List<SqlRow> softDeleted = _server.createSqlQuery("select * from metadata_relationship_versionof where deleted_ts IS NOT NULL").findList();
    assertEquals(softDeleted.size(), 2); // 2 out of 4 edges are soft-deleted

    List<SqlRow> newEdge = _server.createSqlQuery(
        "select * from metadata_relationship_versionof where source='urn:li:bar:123' and deleted_ts IS NULL").findList();
    assertEquals(newEdge.size(), 1); // Newly insert 1 edge

    List<SqlRow> oldEdge = _server.createSqlQuery(
        "select * from metadata_relationship_versionof where source='urn:li:bar:000' and deleted_ts IS NULL").findList();
    assertEquals(oldEdge.size(), 1);

    // Clean up
    _server.execute(Ebean.createSqlUpdate("truncate metadata_relationship_versionof"));
  }


  @Test
  public void testClearRelationshipsByEntityUrn() throws URISyntaxException {
    _server.execute(Ebean.createSqlUpdate(insertRelationships("metadata_relationship_pairswith", "urn:li:bar:123",
        "bar", "urn:li:foo:123", "foo")));

    _server.execute(Ebean.createSqlUpdate(insertRelationships("metadata_relationship_pairswith", "urn:li:bar:123",
        "bar", "urn:li:foo:456", "foo")));

    BarUrn barUrn = BarUrn.createFromString("urn:li:bar:123");
    FooUrn fooUrn = FooUrn.createFromString("urn:li:foo:123");

    // Before processing
    List<SqlRow> before = _server.createSqlQuery("select * from metadata_relationship_pairswith where deleted_ts is null").findList();
    assertEquals(before.size(), 2);

    _localRelationshipWriterDAO.clearRelationshipsByEntity(barUrn, PairsWith.class,
        BaseGraphWriterDAO.RemovalOption.REMOVE_ALL_EDGES_FROM_SOURCE, false);

    // After processing verification
    List<SqlRow> all = _server.createSqlQuery("select * from metadata_relationship_pairswith where deleted_ts is null").findList();
    assertEquals(all.size(), 0); // Total number of edges is 0


    _server.execute(Ebean.createSqlUpdate(insertRelationships("metadata_relationship_pairswith", "urn:li:bar:123",
        "bar", "urn:li:foo:123", "foo")));

    _server.execute(Ebean.createSqlUpdate(insertRelationships("metadata_relationship_pairswith", "urn:li:bar:123",
        "bar", "urn:li:foo:456", "foo")));

    _localRelationshipWriterDAO.clearRelationshipsByEntity(fooUrn, PairsWith.class,
        BaseGraphWriterDAO.RemovalOption.REMOVE_ALL_EDGES_TO_DESTINATION, false);

    // After processing verification
    all = _server.createSqlQuery("select * from metadata_relationship_pairswith where deleted_ts is null").findList();
    assertEquals(all.size(), 1); // Total number of edges is 1

    // Clean up
    _server.execute(Ebean.createSqlUpdate("truncate metadata_relationship_pairswith"));
  }

  private String insertRelationships(String table, String sourceUrn, String sourceType, String destinationUrn, String destinationType) {
    String insertTemplate = "INSERT INTO %s (metadata, source, source_type, destination, destination_type, lastmodifiedon, lastmodifiedby)"
        + " VALUES ('{\"metadata\": true}', '%s', '%s', '%s', '%s', '1970-01-01 00:00:01', 'unknown')";
    return String.format(insertTemplate, table, sourceUrn, sourceType, destinationUrn, destinationType);
  }
}
