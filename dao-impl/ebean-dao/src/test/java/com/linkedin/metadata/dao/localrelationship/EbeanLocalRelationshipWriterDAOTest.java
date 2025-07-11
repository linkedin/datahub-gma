package com.linkedin.metadata.dao.localrelationship;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import com.linkedin.metadata.dao.EbeanLocalRelationshipWriterDAO;
import com.linkedin.metadata.dao.builder.BaseLocalRelationshipBuilder.LocalRelationshipUpdates;
import com.linkedin.metadata.dao.internal.BaseGraphWriterDAO;
import com.linkedin.metadata.dao.localrelationship.builder.VersionOfLocalRelationshipBuilder;
import com.linkedin.metadata.dao.utils.EmbeddedMariaInstance;
import com.linkedin.testing.AspectFoo;
import com.linkedin.testing.BarUrnArray;
import com.linkedin.testing.RelationshipV2Bar;
import com.linkedin.testing.localrelationship.AspectFooBar;
import com.linkedin.testing.localrelationship.PairsWith;
import com.linkedin.testing.localrelationship.VersionOf;
import com.linkedin.testing.urn.BarUrn;
import com.linkedin.testing.urn.FooUrn;
import io.ebean.Ebean;
import io.ebean.EbeanServer;
import io.ebean.SqlRow;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class EbeanLocalRelationshipWriterDAOTest {
  private EbeanServer _server;
  private EbeanLocalRelationshipWriterDAO _localRelationshipWriterDAO;
  private boolean _useAspectColumnForRelationshipRemoval;

  @Factory(dataProvider = "inputList")
  public EbeanLocalRelationshipWriterDAOTest(boolean useAspectColumnForRelationshipRemoval) {
    _useAspectColumnForRelationshipRemoval = useAspectColumnForRelationshipRemoval;
  }

  @DataProvider
  public static Object[][] inputList() {
    return new Object[][]{
        {true},
        {false}
    };
  }
  @BeforeClass
  public void init() throws IOException {
    _server = EmbeddedMariaInstance.getServer(EbeanLocalRelationshipWriterDAOTest.class.getSimpleName());
    _server.execute(Ebean.createSqlUpdate(
        Resources.toString(Resources.getResource("ebean-local-relationship-dao-create-all.sql"), StandardCharsets.UTF_8)));
    _localRelationshipWriterDAO = new EbeanLocalRelationshipWriterDAO(_server);
  }

  @Test
  public void testAddRelationshipWithRemoveAllEdgesFromSource() throws URISyntaxException {
    _localRelationshipWriterDAO.setUseAspectColumnForRelationshipRemoval(_useAspectColumnForRelationshipRemoval);
    // Test cases for Relationship Model V1
    // set 3 existing relationships
    // 1) "urn:li:bar:123" -> "urn:li:foo:123"
    // 2) "urn:li:bar:123" -> "urn:li:foo:000"
    // 3) "urn:li:bar:000" -> "urn:li:foo:123"
    _server.execute(Ebean.createSqlUpdate(insertRelationships("metadata_relationship_versionof", "urn:li:bar:123",
        "bar", "urn:li:foo:123", "foo", AspectFooBar.class.getCanonicalName())));

    _server.execute(Ebean.createSqlUpdate(insertRelationships("metadata_relationship_versionof", "urn:li:bar:123",
        "bar", "urn:li:foo:000", "foo", AspectFooBar.class.getCanonicalName())));

    _server.execute(Ebean.createSqlUpdate(insertRelationships("metadata_relationship_versionof", "urn:li:bar:000",
        "bar", "urn:li:foo:123", "foo", AspectFooBar.class.getCanonicalName())));

    // mock a new relationship update
    // 1) "urn:li:bar:123" -> "urn:li:foo:123"
    AspectFooBar aspectFooBar = new AspectFooBar().setBars(new BarUrnArray(BarUrn.createFromString("urn:li:bar:123")));

    List<LocalRelationshipUpdates> updates = new VersionOfLocalRelationshipBuilder(AspectFooBar.class)
        .buildRelationships(FooUrn.createFromString("urn:li:foo:123"), aspectFooBar);

    // Before processing
    List<SqlRow> before = _server.createSqlQuery("select * from metadata_relationship_versionof").findList();
    assertEquals(before.size(), 3);

    _localRelationshipWriterDAO.processLocalRelationshipUpdates(BarUrn.createFromString("urn:li:bar:123"), AspectFooBar.class, updates, false);

    // After processing verification
    // now the relationship table should have the following relationships:
    // 1) "urn:li:bar:123" -> "urn:li:foo:123" (soft-deleted)
    // 2) "urn:li:bar:123" -> "urn:li:foo:000" (soft-deleted)
    // 3) "urn:li:bar:000" -> "urn:li:foo:123"
    // 4) "urn:li:bar:123" -> "urn:li:foo:123" (newly inserted)
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

    // Test cases for Relationship Model V2
    // set 3 existing relationships
    // 1) "urn:li:foo:1" -> "urn:li:bar:1"
    // 2) "urn:li:foo:1" -> "urn:li:bar:2"
    // 3) "urn:li:foo:2" -> "urn:li:bar:2"
    _server.execute(Ebean.createSqlUpdate(insertRelationships("metadata_relationship_relationshipv2bar",
        "urn:li:foo:1", "foo",
        "urn:li:bar:1", "bar", AspectFooBar.class.getCanonicalName())));

    _server.execute(Ebean.createSqlUpdate(insertRelationships("metadata_relationship_relationshipv2bar",
        "urn:li:foo:1", "foo",
        "urn:li:bar:2", "bar", AspectFooBar.class.getCanonicalName())));

    _server.execute(Ebean.createSqlUpdate(insertRelationships("metadata_relationship_relationshipv2bar",
        "urn:li:foo:2", "foo",
        "urn:li:bar:2", "bar", AspectFooBar.class.getCanonicalName())));

    // mock 3 new relationships
    // 1) "urn:li:foo:1" -> "urn:li:bar:1"
    // 2) "urn:li:foo:1" -> "urn:li:bar:22"
    // 3) "urn:li:foo:1" -> "urn:li:bar:23"
    List<RelationshipV2Bar> relationships = new ArrayList<>();
    RelationshipV2Bar relationship21 = new RelationshipV2Bar().setDestination(
        RelationshipV2Bar.Destination.createWithDestinationBar(new BarUrn(1)));
    RelationshipV2Bar relationship22 = new RelationshipV2Bar().setDestination(
        RelationshipV2Bar.Destination.createWithDestinationBar(new BarUrn(22)));
    RelationshipV2Bar relationship23 = new RelationshipV2Bar().setDestination(
        RelationshipV2Bar.Destination.createWithDestinationBar(new BarUrn(23)));
    relationships.add(relationship21);
    relationships.add(relationship22);
    relationships.add(relationship23);

    LocalRelationshipUpdates localRelationshipUpdates2 = new LocalRelationshipUpdates(relationships,
        RelationshipV2Bar.class,
        BaseGraphWriterDAO.RemovalOption.REMOVE_ALL_EDGES_FROM_SOURCE);

    List<LocalRelationshipUpdates> updates2 = new ArrayList<>();
    updates2.add(localRelationshipUpdates2);

    // Before processing new relationships, there should be 3 existing relationships
    List<SqlRow> before2 = _server.createSqlQuery("select * from metadata_relationship_relationshipv2bar").findList();
    assertEquals(before2.size(), 3);

    // process the 3 new relationships
    // then the relationship table should have the following records:
    // 1) "urn:li:foo:1" -> "urn:li:bar:1" (soft-deleted)
    // 2) "urn:li:foo:1" -> "urn:li:bar:2" (soft-deleted)
    // 3) "urn:li:foo:2" -> "urn:li:bar:2"
    // 4) "urn:li:foo:1" -> "urn:li:bar:1" (newly inserted)
    // 5) "urn:li:foo:1" -> "urn:li:bar:22" (newly inserted)
    // 6) "urn:li:foo:1" -> "urn:li:bar:23" (newly inserted)
    _localRelationshipWriterDAO.processLocalRelationshipUpdates(FooUrn.createFromString("urn:li:foo:1"), AspectFooBar.class, updates2, false);

    // After processing verification
    List<SqlRow> all2 = _server.createSqlQuery("select * from metadata_relationship_relationshipv2bar").findList();
    assertEquals(all2.size(), 6); // Total number of edges

    List<SqlRow> softDeleted2 = _server.createSqlQuery("select * from metadata_relationship_relationshipv2bar where deleted_ts IS NOT NULL").findList();
    assertEquals(softDeleted2.size(), 2); // 2 edges are soft-deleted

    List<SqlRow> newEdge2 = _server.createSqlQuery(
        "select * from metadata_relationship_relationshipv2bar where source='urn:li:foo:1' and deleted_ts IS NULL").findList();
    assertEquals(newEdge2.size(), 3); // newly insert 3 edge

    List<SqlRow> oldEdge2 = _server.createSqlQuery(
        "select * from metadata_relationship_relationshipv2bar where source='urn:li:foo:2' and deleted_ts IS NULL").findList();
    assertEquals(oldEdge2.size(), 1); // untouched record

    // Clean up
    _server.execute(Ebean.createSqlUpdate("truncate metadata_relationship_relationshipv2bar"));
  }


  @Test
  public void testClearRelationshipsByEntityUrnSameAspect() throws URISyntaxException {
    _localRelationshipWriterDAO.setUseAspectColumnForRelationshipRemoval(_useAspectColumnForRelationshipRemoval);

    _server.execute(Ebean.createSqlUpdate(insertRelationships("metadata_relationship_pairswith", "urn:li:bar:123",
        "bar", "urn:li:foo:123", "foo", AspectFooBar.class.getCanonicalName())));

    _server.execute(Ebean.createSqlUpdate(insertRelationships("metadata_relationship_pairswith", "urn:li:bar:123",
        "bar", "urn:li:foo:456", "foo", AspectFooBar.class.getCanonicalName())));

    BarUrn barUrn = BarUrn.createFromString("urn:li:bar:123");

    // Before processing
    List<SqlRow> before = _server.createSqlQuery("select * from metadata_relationship_pairswith where deleted_ts is null").findList();
    assertEquals(before.size(), 2);

    _localRelationshipWriterDAO.clearRelationshipsByEntity(barUrn, AspectFooBar.class, PairsWith.class, false);

    // After processing verification
    List<SqlRow> all = _server.createSqlQuery("select * from metadata_relationship_pairswith where deleted_ts is null").findList();
    assertEquals(all.size(), 0); // Total number of edges is 0

    // Clean up
    _server.execute(Ebean.createSqlUpdate("truncate metadata_relationship_pairswith"));
  }

  @Test
  public void testClearRelationshipsByEntityUrnDifferentAspect() throws URISyntaxException {
    if (!_useAspectColumnForRelationshipRemoval) {
      return; // this test doesn't apply to this case
    }
    _localRelationshipWriterDAO.setUseAspectColumnForRelationshipRemoval(_useAspectColumnForRelationshipRemoval);

    _server.execute(Ebean.createSqlUpdate(insertRelationships("metadata_relationship_pairswith", "urn:li:bar:123",
        "bar", "urn:li:foo:123", "foo", AspectFooBar.class.getCanonicalName())));

    _server.execute(Ebean.createSqlUpdate(insertRelationships("metadata_relationship_pairswith", "urn:li:bar:123",
        "bar", "urn:li:foo:456", "foo", AspectFoo.class.getCanonicalName())));

    BarUrn barUrn = BarUrn.createFromString("urn:li:bar:123");

    // Before processing
    List<SqlRow> before = _server.createSqlQuery("select * from metadata_relationship_pairswith where deleted_ts is null").findList();
    assertEquals(before.size(), 2);

    _localRelationshipWriterDAO.clearRelationshipsByEntity(barUrn, AspectFooBar.class, PairsWith.class, false);

    // After processing verification - only the first relationship with foo123 should have been deleted
    List<SqlRow> all = _server.createSqlQuery("select * from metadata_relationship_pairswith where deleted_ts is null").findList();
    assertEquals(all.size(), 1); // Total number of edges is 1
    // Clean up
    _server.execute(Ebean.createSqlUpdate("truncate metadata_relationship_pairswith"));
  }

  @Test
  public void testClearRelationshipsByEntityUrnWithBatching() throws URISyntaxException {
    // Insert a large number of relationships to trigger batch processing
    for (int i = 0; i < 10001; i++) {
      _server.execute(Ebean.createSqlUpdate(insertRelationships("metadata_relationship_pairswith", "urn:li:bar:123",
          "bar", "urn:li:foo:" + i, "foo", AspectFoo.class.getCanonicalName())));
    }

    BarUrn barUrn = BarUrn.createFromString("urn:li:bar:123");
    // Before processing
    List<SqlRow> before = _server.createSqlQuery("select * from metadata_relationship_pairswith where deleted_ts is null").findList();
    assertEquals(before.size(), 10001);

    _localRelationshipWriterDAO.clearRelationshipsByEntity(barUrn, AspectFoo.class, PairsWith.class, false);

    // After processing verification
    List<SqlRow> all = _server.createSqlQuery("select * from metadata_relationship_pairswith where deleted_ts is null").findList();
    assertEquals(all.size(), 0); // Total number of edges is 0
    assertEquals(_localRelationshipWriterDAO.getBatchCount(), 2); //expect 2 batches
    // Clean up
    _server.execute(Ebean.createSqlUpdate("truncate metadata_relationship_pairswith"));
  }

  @Test
  public void testRemoveRelationshipsSameAspect() throws URISyntaxException {
    _localRelationshipWriterDAO.setUseAspectColumnForRelationshipRemoval(_useAspectColumnForRelationshipRemoval);
    BarUrn barUrn = BarUrn.createFromString("urn:li:bar:123");
    FooUrn fooUrn123 = FooUrn.createFromString("urn:li:foo:123");
    FooUrn fooUrn456 = FooUrn.createFromString("urn:li:foo:456");
    _server.execute(Ebean.createSqlUpdate(insertRelationships("metadata_relationship_pairswith", barUrn.toString(),
        "bar", fooUrn123.toString(), "foo", AspectFooBar.class.getCanonicalName())));

    _server.execute(Ebean.createSqlUpdate(insertRelationships("metadata_relationship_pairswith", barUrn.toString(),
        "bar", fooUrn456.toString(), "foo", AspectFooBar.class.getCanonicalName())));

    // Before processing
    List<SqlRow> before = _server.createSqlQuery("select * from metadata_relationship_pairswith where deleted_ts is null").findList();
    assertEquals(before.size(), 2);

    PairsWith pairsWith = new PairsWith().setSource(barUrn).setDestination(fooUrn123);
    _localRelationshipWriterDAO.removeRelationships(barUrn, AspectFooBar.class, Collections.singletonList(pairsWith));

    // After processing verification
    List<SqlRow> all = _server.createSqlQuery("select * from metadata_relationship_pairswith where deleted_ts is null").findList();
    assertEquals(all.size(), 0); // Total number of edges is 0

    // Clean up
    _server.execute(Ebean.createSqlUpdate("truncate metadata_relationship_pairswith"));
  }

  @Test
  public void testRemoveRelationshipsDifferentAspect() throws URISyntaxException {
    if (!_useAspectColumnForRelationshipRemoval) {
      return; // this test doesn't apply to this case
    }
    _localRelationshipWriterDAO.setUseAspectColumnForRelationshipRemoval(_useAspectColumnForRelationshipRemoval);

    BarUrn barUrn = BarUrn.createFromString("urn:li:bar:123");
    FooUrn fooUrn123 = FooUrn.createFromString("urn:li:foo:123");
    FooUrn fooUrn456 = FooUrn.createFromString("urn:li:foo:456");
    _server.execute(Ebean.createSqlUpdate(insertRelationships("metadata_relationship_pairswith", barUrn.toString(),
        "bar", fooUrn123.toString(), "foo", AspectFooBar.class.getCanonicalName())));

    _server.execute(Ebean.createSqlUpdate(insertRelationships("metadata_relationship_pairswith", barUrn.toString(),
        "bar", fooUrn456.toString(), "foo", AspectFoo.class.getCanonicalName())));

    // Before processing
    List<SqlRow> before = _server.createSqlQuery("select * from metadata_relationship_pairswith where deleted_ts is null").findList();
    assertEquals(before.size(), 2);

    PairsWith pairsWith = new PairsWith().setSource(barUrn).setDestination(fooUrn123);
    _localRelationshipWriterDAO.removeRelationships(barUrn, AspectFooBar.class, Collections.singletonList(pairsWith));

    // After processing verification - only the first relationship with foo123 should have been deleted.
    List<SqlRow> all = _server.createSqlQuery("select * from metadata_relationship_pairswith where deleted_ts is null").findList();
    assertEquals(all.size(), 1); // Total number of edges is 1
    assertEquals(all.get(0).getString("source"), barUrn.toString());
    assertEquals(all.get(0).getString("destination"), fooUrn456.toString());

    // Clean up
    _server.execute(Ebean.createSqlUpdate("truncate metadata_relationship_pairswith"));
  }

  @Test
  public void testAddRelationships() throws URISyntaxException, ReflectiveOperationException {
    _localRelationshipWriterDAO.setUseAspectColumnForRelationshipRemoval(_useAspectColumnForRelationshipRemoval);

    BarUrn barUrn = BarUrn.createFromString("urn:li:bar:123");
    FooUrn fooUrn123 = FooUrn.createFromString("urn:li:foo:123");
    FooUrn fooUrn456 = FooUrn.createFromString("urn:li:foo:456");
    FooUrn fooUrn789 = FooUrn.createFromString("urn:li:foo:789");
    PairsWith pairsWith1 = new PairsWith().setSource(barUrn).setDestination(fooUrn123);
    PairsWith pairsWith2 = new PairsWith().setSource(barUrn).setDestination(fooUrn456);
    PairsWith pairsWith3 = new PairsWith().setSource(barUrn).setDestination(fooUrn789);

    List<PairsWith> relationshipsToInsert = ImmutableList.of(pairsWith1, pairsWith2, pairsWith3);

    // set INSERT_BATCH_SIZE from 1000 to 2 for testing purposes
    Field field = _localRelationshipWriterDAO.getClass().getDeclaredField("INSERT_BATCH_SIZE");
    field.setAccessible(true); // ignore private keyword
    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL); // remove the 'final' modifier
    field.set(null, 2); // use null bc of static context

    _localRelationshipWriterDAO.addRelationships(barUrn, AspectFooBar.class, relationshipsToInsert, false);

    // After processing verification - all 3 relationships should be ingested
    List<SqlRow> all = _server.createSqlQuery("select * from metadata_relationship_pairswith where deleted_ts is null").findList();
    assertEquals(all.size(), 3); // Total number of edges is 1
    assertEquals(all.get(0).getString("source"), barUrn.toString());
    assertEquals(all.get(0).getString("destination"), fooUrn123.toString());
    assertEquals(all.get(1).getString("source"), barUrn.toString());
    assertEquals(all.get(1).getString("destination"), fooUrn456.toString());
    assertEquals(all.get(2).getString("source"), barUrn.toString());
    assertEquals(all.get(2).getString("destination"), fooUrn789.toString());

    // Clean up
    _server.execute(Ebean.createSqlUpdate("truncate metadata_relationship_pairswith"));
  }

  @Test
  public void testConcurrentAddRelationships() throws Exception {
    if (!_useAspectColumnForRelationshipRemoval) {
      return;
    }
    _localRelationshipWriterDAO.setUseAspectColumnForRelationshipRemoval(_useAspectColumnForRelationshipRemoval);

    BarUrn barUrn = BarUrn.createFromString("urn:li:bar:123");
    final int numThreads = 20;
    final int relationshipsPerThread = 2;
    final ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    final CountDownLatch latch = new CountDownLatch(numThreads);

    // set INSERT_BATCH_SIZE from 1000 to 2 for testing purposes
    Field field = _localRelationshipWriterDAO.getClass().getDeclaredField("INSERT_BATCH_SIZE");
    field.setAccessible(true); // ignore private keyword
    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL); // remove the 'final' modifier
    field.set(null, 2); // use null bc of static context

    for (int i = 0; i < numThreads; i++) {
      final int threadId = i;
      executor.submit(() -> {
        try {
          List<VersionOf> relationships = new ArrayList<>();
          for (int j = 0; j < relationshipsPerThread; j++) {
            FooUrn destination = FooUrn.createFromString("urn:li:foo:" + threadId + "00000" + j);
            relationships.add(new VersionOf().setSource(barUrn).setDestination(destination));
          }

          _localRelationshipWriterDAO.addRelationships(barUrn, AspectFooBar.class, relationships, false);
        } catch (Exception e) {
          e.printStackTrace(); // helpful 4 debugging failures
        } finally {
          latch.countDown();
        }
      });
    }

    latch.await(); // wait for all threads to finish
    executor.shutdown();

    // Verify all relationships were inserted
    List<SqlRow> all = _server.createSqlQuery("select * from metadata_relationship_versionof where deleted_ts is null").findList();
    int expected = numThreads * relationshipsPerThread;
    assertEquals(expected, all.size());

    // Verify uniqueness of destination URNs
    Set<String> uniqueDestinations = all.stream()
        .map(row -> row.getString("destination"))
        .collect(Collectors.toSet());

    assertEquals(expected, uniqueDestinations.size());

    // Clean up
    _server.execute(Ebean.createSqlUpdate("truncate metadata_relationship_versionof"));
  }

  @Test
  public void testRemoveRelationshipsSameAspectDifferentNamespace() throws URISyntaxException {
    if (!_useAspectColumnForRelationshipRemoval) {
      return; // this test doesn't apply to this case
    }
    _localRelationshipWriterDAO.setUseAspectColumnForRelationshipRemoval(_useAspectColumnForRelationshipRemoval);

    BarUrn barUrn = BarUrn.createFromString("urn:li:bar:123");
    FooUrn fooUrn123 = FooUrn.createFromString("urn:li:foo:123");
    FooUrn fooUrn456 = FooUrn.createFromString("urn:li:foo:456");
    _server.execute(Ebean.createSqlUpdate(insertRelationships("metadata_relationship_pairswith", barUrn.toString(),
        "bar", fooUrn123.toString(), "foo", AspectFooBar.class.getCanonicalName())));

    _server.execute(Ebean.createSqlUpdate(insertRelationships("metadata_relationship_pairswith", barUrn.toString(),
        "bar", fooUrn456.toString(), "foo", "pegasus." + AspectFooBar.class.getCanonicalName())));

    // Before processing
    List<SqlRow> before = _server.createSqlQuery("select * from metadata_relationship_pairswith where deleted_ts is null").findList();
    assertEquals(before.size(), 2);

    PairsWith pairsWith = new PairsWith().setSource(barUrn).setDestination(fooUrn123);
    _localRelationshipWriterDAO.removeRelationships(barUrn, AspectFooBar.class, Collections.singletonList(pairsWith));

    // After processing verification - both relationships should have been deleted.
    List<SqlRow> all = _server.createSqlQuery("select * from metadata_relationship_pairswith where deleted_ts is null").findList();
    assertEquals(all.size(), 0); // Total number of edges is 0

    // Clean up
    _server.execute(Ebean.createSqlUpdate("truncate metadata_relationship_pairswith"));
  }

  private String insertRelationships(String table, String sourceUrn, String sourceType, String destinationUrn, String destinationType, String aspect) {
    String insertWithAspectTemplate = "INSERT INTO %s (metadata, source, source_type, destination, destination_type, lastmodifiedon, lastmodifiedby, aspect)"
        + " VALUES ('{\"metadata\": true}', '%s', '%s', '%s', '%s', CURRENT_TIMESTAMP, 'unknown', '%s')";
    String insertTemplate = "INSERT INTO %s (metadata, source, source_type, destination, destination_type, lastmodifiedon, lastmodifiedby)"
        + " VALUES ('{\"metadata\": true}', '%s', '%s', '%s', '%s', CURRENT_TIMESTAMP, 'unknown')";
    if (_useAspectColumnForRelationshipRemoval) {
      return String.format(insertWithAspectTemplate, table, sourceUrn, sourceType, destinationUrn, destinationType, aspect);
    }
    return String.format(insertTemplate, table, sourceUrn, sourceType, destinationUrn, destinationType);
  }
}
