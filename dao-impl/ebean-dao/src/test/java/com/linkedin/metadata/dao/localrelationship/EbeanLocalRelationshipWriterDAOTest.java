package com.linkedin.metadata.dao.localrelationship;

import com.google.common.io.Resources;
import com.linkedin.metadata.dao.EbeanLocalRelationshipWriterDAO;
import com.linkedin.metadata.dao.localrelationship.builder.BelongsToLocalRelationshipBuilder;
import com.linkedin.metadata.dao.localrelationship.builder.PairsWithLocalRelationshipBuilder;
import com.linkedin.metadata.dao.localrelationship.builder.ReportsToLocalRelationshipBuilder;
import com.linkedin.metadata.dao.localrelationship.builder.VersionOfLocalRelationshipBuilder;
import com.linkedin.metadata.dao.utils.EmbeddedMariaInstance;
import com.linkedin.metadata.dao.builder.BaseLocalRelationshipBuilder;
import com.linkedin.testing.BarUrnArray;
import com.linkedin.testing.localrelationship.AspectFooBar;
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
    _server = EmbeddedMariaInstance.getServer();
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

    List<BaseLocalRelationshipBuilder<AspectFooBar>.LocalRelationshipUpdates> updates = new BelongsToLocalRelationshipBuilder(AspectFooBar.class)
        .buildRelationships(FooUrn.createFromString("urn:li:foo:123"), aspectFooBar);

    // Before processing
    List<SqlRow> before = _server.createSqlQuery("select * from metadata_relationship_belongsto where source='urn:li:bar:000'").findList();
    assertEquals(before.size(), 1);

    _localRelationshipWriterDAO.processLocalRelationshipUpdates(updates);

    // After processing verification
    List<SqlRow> after = _server.createSqlQuery("select * from metadata_relationship_belongsto where destination='urn:li:foo:123'").findList();
    assertEquals(after.size(), 3);
    List<SqlRow> edges = _server.createSqlQuery("select * from metadata_relationship_belongsto where source='urn:li:bar:000'").findList();
    assertEquals(edges.size(), 0);

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

    List<BaseLocalRelationshipBuilder<AspectFooBar>.LocalRelationshipUpdates> updates = new ReportsToLocalRelationshipBuilder(AspectFooBar.class)
        .buildRelationships(FooUrn.createFromString("urn:li:foo:123"), aspectFooBar);

    // Before processing
    List<SqlRow> before = _server.createSqlQuery("select * from metadata_relationship_reportsto where source='urn:li:bar:000'").findList();
    assertEquals(before.size(), 1);

    _localRelationshipWriterDAO.processLocalRelationshipUpdates(updates);

    // After processing verification
    List<SqlRow> after = _server.createSqlQuery("select * from metadata_relationship_reportsto where destination='urn:li:foo:123'").findList();
    assertEquals(after.size(), 4);
    List<SqlRow> edges = _server.createSqlQuery("select * from metadata_relationship_reportsto where source='urn:li:bar:000'").findList();
    assertEquals(edges.size(), 1);

    // Clean up
    _server.execute(Ebean.createSqlUpdate("truncate metadata_relationship_reportsto"));
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

    List<BaseLocalRelationshipBuilder<AspectFooBar>.LocalRelationshipUpdates> updates = new PairsWithLocalRelationshipBuilder(AspectFooBar.class)
        .buildRelationships(FooUrn.createFromString("urn:li:foo:123"), aspectFooBar);

    // Before processing
    List<SqlRow> before = _server.createSqlQuery("select * from metadata_relationship_pairswith").findList();
    assertEquals(before.size(), 3);

    _localRelationshipWriterDAO.processLocalRelationshipUpdates(updates);

    // After processing verification
    List<SqlRow> after = _server.createSqlQuery("select * from metadata_relationship_pairswith where destination='urn:li:foo:123'").findList();
    assertEquals(after.size(), 2);
    List<SqlRow> edges = _server.createSqlQuery("select * from metadata_relationship_pairswith where source='urn:li:bar:000'").findList();
    assertEquals(edges.size(), 1);

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

    List<BaseLocalRelationshipBuilder<AspectFooBar>.LocalRelationshipUpdates> updates = new VersionOfLocalRelationshipBuilder(AspectFooBar.class)
        .buildRelationships(FooUrn.createFromString("urn:li:foo:123"), aspectFooBar);

    // Before processing
    List<SqlRow> before = _server.createSqlQuery("select * from metadata_relationship_versionof").findList();
    assertEquals(before.size(), 3);

    _localRelationshipWriterDAO.processLocalRelationshipUpdates(updates);

    // After processing verification
    List<SqlRow> after = _server.createSqlQuery("select * from metadata_relationship_versionof").findList();
    assertEquals(after.size(), 2);
    List<SqlRow> edges = _server.createSqlQuery("select * from metadata_relationship_versionof where source='urn:li:bar:123'").findList();
    assertEquals(edges.size(), 1);
    edges = _server.createSqlQuery("select * from metadata_relationship_versionof where source='urn:li:bar:000'").findList();
    assertEquals(edges.size(), 1);

    // Clean up
    _server.execute(Ebean.createSqlUpdate("truncate metadata_relationship_versionof"));
  }

  private String insertRelationships(String table, String sourceUrn, String sourceType, String destinationUrn, String destinationType) {
    String insertTemplate = "INSERT INTO %s (metadata, source, source_type, destination, destination_type, lastmodifiedon, lastmodifiedby)"
        + " VALUES ('{\"metadata\": true}', '%s', '%s', '%s', '%s', '1970-01-01 00:00:01', 'unknown')";
    return String.format(insertTemplate, table, sourceUrn, sourceType, destinationUrn, destinationType);
  }
}
