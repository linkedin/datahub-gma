package com.linkedin.metadata.dao.localrelationship;

import com.google.common.io.Resources;
import com.linkedin.metadata.dao.EbeanLocalRelationshipWriterDAO;
import com.linkedin.metadata.dao.utils.MysqlDevInstance;
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
    _server = MysqlDevInstance.getServer();
    _server.execute(Ebean.createSqlUpdate(
        Resources.toString(Resources.getResource("metadata-schema-create-all.sql"), StandardCharsets.UTF_8)));
    _localRelationshipWriterDAO = new EbeanLocalRelationshipWriterDAO(_server);
  }

  @Test
  public void testAddRelationship() throws URISyntaxException {
    AspectFooBar aspectFooBar = new AspectFooBar().setBars(new BarUrnArray(
        BarUrn.createFromString("urn:li:bar:123"),
        BarUrn.createFromString("urn:li:bar:456"),
        BarUrn.createFromString("urn:li:bar:789")));

    List<BaseLocalRelationshipBuilder<AspectFooBar>.LocalRelationshipUpdates> updates = new BelongsToLocalRelationshipBuilder(AspectFooBar.class)
        .buildRelationships(FooUrn.createFromString("urn:li:foo:123"), aspectFooBar);

    _localRelationshipWriterDAO.processLocalRelationshipUpdates(updates);

    List<SqlRow> rows = _server.createSqlQuery("select * from metadata_relationship_belongsto where destination='urn:li:foo:123'").findList();
    assertEquals(rows.size(), 3);
    _server.execute(Ebean.createSqlUpdate("truncate metadata_relationship_belongsto"));
  }
}
