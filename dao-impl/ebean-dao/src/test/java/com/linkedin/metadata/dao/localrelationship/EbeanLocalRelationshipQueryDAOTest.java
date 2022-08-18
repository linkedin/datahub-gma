package com.linkedin.metadata.dao.localrelationship;

import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import com.linkedin.common.AuditStamp;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.EbeanLocalAccess;
import com.linkedin.metadata.dao.EbeanLocalRelationshipQueryDAO;
import com.linkedin.metadata.dao.EbeanLocalRelationshipWriterDAO;
import com.linkedin.metadata.dao.IEbeanLocalAccess;
import com.linkedin.metadata.dao.utils.MysqlDevInstance;
import com.linkedin.metadata.query.Condition;
import com.linkedin.metadata.query.Criterion;
import com.linkedin.metadata.query.CriterionArray;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.RelationshipDirection;
import com.linkedin.metadata.query.RelationshipFilter;
import com.linkedin.testing.AspectBar;
import com.linkedin.testing.AspectFoo;
import com.linkedin.testing.BarSnapshot;
import com.linkedin.testing.EntityAspectUnion;
import com.linkedin.testing.EntityAspectUnionArray;
import com.linkedin.testing.FooSnapshot;
import com.linkedin.testing.localrelationship.BelongsTo;
import com.linkedin.testing.localrelationship.PairsWith;
import com.linkedin.testing.localrelationship.ReportsTo;
import com.linkedin.testing.urn.BarUrn;
import com.linkedin.testing.urn.FooUrn;
import io.ebean.Ebean;
import io.ebean.EbeanServer;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class EbeanLocalRelationshipQueryDAOTest {
  private EbeanServer _server;
  private EbeanLocalRelationshipWriterDAO _localRelationshipWriterDAO;
  private EbeanLocalRelationshipQueryDAO _localRelationshipQueryDAO;
  private IEbeanLocalAccess<FooUrn> _fooUrnEBeanLocalAccess;
  private IEbeanLocalAccess<BarUrn> _barUrnEBeanLocalAccess;

  @BeforeClass
  public void init() {
    _server = MysqlDevInstance.getServer();
    _localRelationshipWriterDAO = new EbeanLocalRelationshipWriterDAO(_server);
    _localRelationshipQueryDAO = new EbeanLocalRelationshipQueryDAO(_server);
    _fooUrnEBeanLocalAccess = new EbeanLocalAccess<>(_server, MysqlDevInstance.SERVER_CONFIG, FooUrn.class);
    _barUrnEBeanLocalAccess = new EbeanLocalAccess<>(_server, MysqlDevInstance.SERVER_CONFIG, BarUrn.class);
  }

  @BeforeMethod
  public void recreateTables() throws IOException {
    _server.execute(Ebean.createSqlUpdate(
        Resources.toString(Resources.getResource("ebean-local-relationship-dao-create-all.sql"), StandardCharsets.UTF_8)));
  }

  @Test
  public void testFindOneEntity() throws URISyntaxException {
    _fooUrnEBeanLocalAccess.add(new FooUrn(1), new AspectFoo().setValue("foo"), AspectFoo.class, new AuditStamp());
    Criterion filterCrtierion = new Criterion().setField("i_aspectfoo$value").setValue("foo").setCondition(Condition.EQUAL);
    Filter filter = new Filter().setCriteria(new CriterionArray(filterCrtierion));
    List<FooSnapshot> fooSnapshotList = _localRelationshipQueryDAO.findEntities(FooSnapshot.class, filter, 0, 10);

    assertEquals(fooSnapshotList.size(), 1);
    assertEquals(fooSnapshotList.get(0).getAspects().size(), 1);
    assertEquals(fooSnapshotList.get(0).getAspects().get(0).getAspectFoo(), new AspectFoo().setValue("foo"));
  }

  @Test
  public void testFindOneEntityTwoAspects() throws URISyntaxException {
    _fooUrnEBeanLocalAccess.add(new FooUrn(1), new AspectFoo().setValue("foo"), AspectFoo.class, new AuditStamp());
    _fooUrnEBeanLocalAccess.add(new FooUrn(1), new AspectBar().setValue("bar"), AspectBar.class, new AuditStamp());
    Criterion filterCrtierion = new Criterion().setField("i_aspectfoo$value").setValue("foo").setCondition(Condition.EQUAL);
    Filter filter = new Filter().setCriteria(new CriterionArray(filterCrtierion));
    List<FooSnapshot> fooSnapshotList = _localRelationshipQueryDAO.findEntities(FooSnapshot.class, filter, 0, 10);

    assertEquals(fooSnapshotList.size(), 1);
    assertEquals(fooSnapshotList.get(0).getAspects().size(), 2);
    EntityAspectUnion fooAspectUnion = new EntityAspectUnion();
    fooAspectUnion.setAspectFoo(new AspectFoo().setValue("foo"));
    EntityAspectUnion barAspectUnion = new EntityAspectUnion();
    barAspectUnion.setAspectBar(new AspectBar().setValue("bar"));

    EntityAspectUnionArray expected = new EntityAspectUnionArray(fooAspectUnion, barAspectUnion);

    assertEquals(fooSnapshotList.get(0).getAspects(), expected);
  }

  @Test
  public void testFindOneRelationship() throws Exception {
    FooUrn alice = new FooUrn(1);
    FooUrn bob = new FooUrn(2);
    FooUrn jack = new FooUrn(3);

    // Add Alice, Bob and Jack into entity tables.
    _fooUrnEBeanLocalAccess.add(alice, new AspectFoo().setValue("Alice"), AspectFoo.class, new AuditStamp());
    _fooUrnEBeanLocalAccess.add(bob, new AspectFoo().setValue("Bob"), AspectFoo.class, new AuditStamp());
    _fooUrnEBeanLocalAccess.add(jack, new AspectFoo().setValue("Jack"), AspectFoo.class, new AuditStamp());

    // Add Bob reports-to ALice relationship
    ReportsTo bobReportsToAlice = new ReportsTo().setSource(bob).setDestination(alice);
    _localRelationshipWriterDAO.addRelationship(bobReportsToAlice);

    // Add Jack reports-to ALice relationship
    ReportsTo jackReportsToAlice = new ReportsTo().setSource(jack).setDestination(alice);
    _localRelationshipWriterDAO.addRelationship(jackReportsToAlice);

    // Find all reports-to relationship for Alice.
    Criterion filterCriterion = new Criterion().setField("i_aspectfoo$value").setValue("Alice").setCondition(Condition.EQUAL);
    Filter filter = new Filter().setCriteria(new CriterionArray(filterCriterion));
    List<ReportsTo> reportsToAlice = _localRelationshipQueryDAO.findRelationships(FooSnapshot.class, new Filter().setCriteria(new CriterionArray()),
        FooSnapshot.class, filter, ReportsTo.class, new Filter().setCriteria(new CriterionArray()), 0, 10);

    // Asserts
    assertEquals(reportsToAlice.size(), 2);
    Set<FooUrn> actual = reportsToAlice.stream().map(reportsTo -> makeFooUrn(reportsTo.getSource().toString())).collect(Collectors.toSet());
    Set<FooUrn> expected = ImmutableSet.of(jack, bob);
    assertEquals(actual, expected);
  }

  @Test
  public void testFindEntitiesOneHopAwayIncomingDirection() throws Exception {
    FooUrn alice = new FooUrn(1);
    FooUrn bob = new FooUrn(2);
    FooUrn jack = new FooUrn(3);

    // Add Alice, Bob and Jack into entity tables.
    _fooUrnEBeanLocalAccess.add(alice, new AspectFoo().setValue("Alice"), AspectFoo.class, new AuditStamp());
    _fooUrnEBeanLocalAccess.add(bob, new AspectFoo().setValue("Bob"), AspectFoo.class, new AuditStamp());
    _fooUrnEBeanLocalAccess.add(jack, new AspectFoo().setValue("Jack"), AspectFoo.class, new AuditStamp());

    // Add Bob reports-to ALice relationship
    ReportsTo bobReportsToAlice = new ReportsTo().setSource(bob).setDestination(alice);
    _localRelationshipWriterDAO.addRelationship(bobReportsToAlice);

    // Add Jack reports-to ALice relationship
    ReportsTo jackReportsToAlice = new ReportsTo().setSource(jack).setDestination(alice);
    _localRelationshipWriterDAO.addRelationship(jackReportsToAlice);

    // Find all Alice's direct reports.
    Criterion filterCriterion = new Criterion().setField("i_aspectfoo$value").setValue("Alice").setCondition(Condition.EQUAL);
    Filter filter = new Filter().setCriteria(new CriterionArray(filterCriterion));
    List<RecordTemplate> aliceDirectReports = _localRelationshipQueryDAO.findEntities(FooSnapshot.class, new Filter().setCriteria(new CriterionArray()),
        FooSnapshot.class, filter, ReportsTo.class, new RelationshipFilter().setDirection(RelationshipDirection.INCOMING).setCriteria(new CriterionArray()),
        1, 1, 0, 10);

    // Asserts Alice has two direct reports
    assertEquals(aliceDirectReports.size(), 2);

    Set<FooUrn> actual = aliceDirectReports.stream().map(result -> {
      FooSnapshot person = (FooSnapshot) result;
      return makeFooUrn(person.data().get("urn").toString());
    }).collect(Collectors.toSet());

    // Asserts Alice's direct reports are Jack and Bob.
    Set<FooUrn> expected = ImmutableSet.of(jack, bob);
    assertEquals(actual, expected);
  }

  @Test
  public void testFindEntitiesOneHopAwayOutgoingDirection() throws Exception {
    FooUrn alice = new FooUrn(1);
    FooUrn bob = new FooUrn(2);
    BarUrn stanford = new BarUrn(1);
    BarUrn mit = new BarUrn(2);

    // Add Alice and Bob into entity tables.
    _fooUrnEBeanLocalAccess.add(alice, new AspectFoo().setValue("Alice"), AspectFoo.class, new AuditStamp());
    _fooUrnEBeanLocalAccess.add(bob, new AspectFoo().setValue("Bob"), AspectFoo.class, new AuditStamp());

    // Add Stanford and MIT into entity tables.
    _barUrnEBeanLocalAccess.add(stanford, new AspectFoo().setValue("Stanford"), AspectFoo.class, new AuditStamp());
    _barUrnEBeanLocalAccess.add(mit, new AspectFoo().setValue("MIT"), AspectFoo.class, new AuditStamp());

    // Add Alice belongs to MIT and Stanford.
    BelongsTo aliceBelongsToMit = new BelongsTo().setSource(alice).setDestination(mit);
    BelongsTo aliceBelongsToStanford = new BelongsTo().setSource(alice).setDestination(stanford);
    _localRelationshipWriterDAO.addRelationship(aliceBelongsToMit);
    _localRelationshipWriterDAO.addRelationship(aliceBelongsToStanford);

    // Add Bob belongs to Stanford.
    BelongsTo bobBelongsToStandford = new BelongsTo().setSource(bob).setDestination(stanford);
    _localRelationshipWriterDAO.addRelationship(bobBelongsToStandford);

    // Alice filter
    Criterion aliceFilterCriterion = new Criterion().setField("i_aspectfoo$value").setValue("Alice").setCondition(Condition.EQUAL);
    Filter aliceFilter = new Filter().setCriteria(new CriterionArray(aliceFilterCriterion));

    // Find all the schools Alice has attended.
    List<RecordTemplate> schoolsAliceAttends = _localRelationshipQueryDAO.findEntities(FooSnapshot.class, aliceFilter,
        BarSnapshot.class, new Filter().setCriteria(new CriterionArray()), BelongsTo.class,
        new RelationshipFilter().setDirection(RelationshipDirection.OUTGOING).setCriteria(new CriterionArray()), 1, 1, 0, 10);

    // Asserts Alice attends two schools
    assertEquals(schoolsAliceAttends.size(), 2);

    Set<BarUrn> actual = schoolsAliceAttends.stream().map(result -> {
      BarSnapshot school = (BarSnapshot) result;
      return makeBarUrn(school.data().get("urn").toString());
    }).collect(Collectors.toSet());

    // Asserts Alice attends Stanford and MIT
    Set<BarUrn> expected = ImmutableSet.of(stanford, mit);
    assertEquals(actual, expected);
  }

  @Test
  public void testFindEntitiesOneHopAwayUndirected() throws Exception {
    FooUrn alice = new FooUrn(1);
    FooUrn bob = new FooUrn(2);
    FooUrn jack = new FooUrn(3);
    FooUrn john = new FooUrn(4);

    // Add Alice, Bob, Jack and John into entity tables.
    _fooUrnEBeanLocalAccess.add(alice, new AspectFoo().setValue("Alice"), AspectFoo.class, new AuditStamp());
    _fooUrnEBeanLocalAccess.add(bob, new AspectFoo().setValue("Bob"), AspectFoo.class, new AuditStamp());
    _fooUrnEBeanLocalAccess.add(jack, new AspectFoo().setValue("Jack"), AspectFoo.class, new AuditStamp());
    _fooUrnEBeanLocalAccess.add(john, new AspectFoo().setValue("John"), AspectFoo.class, new AuditStamp());

    _fooUrnEBeanLocalAccess.add(alice, new AspectBar().setValue("32"), AspectBar.class, new AuditStamp()); // Alice 32 years old
    _fooUrnEBeanLocalAccess.add(bob, new AspectBar().setValue("52"), AspectBar.class, new AuditStamp()); // Bob 52 years old
    _fooUrnEBeanLocalAccess.add(jack, new AspectBar().setValue("16"), AspectBar.class, new AuditStamp()); // Jack 16 years old
    _fooUrnEBeanLocalAccess.add(john, new AspectBar().setValue("42"), AspectBar.class, new AuditStamp()); // John 42 years old

    // Add Alice pair-with Jack relationships. Alice --> Jack.
    PairsWith alicePairsWithJack = new PairsWith().setSource(alice).setDestination(jack);
    _localRelationshipWriterDAO.addRelationship(alicePairsWithJack);

    // Add Bob pair-with Alice relationships. Bob --> Alice.
    PairsWith bobPairsWithAlice = new PairsWith().setSource(bob).setDestination(alice);
    _localRelationshipWriterDAO.addRelationship(bobPairsWithAlice);

    // Add Alice pair-with John relationships. Alice --> John.
    PairsWith alicePairsWithJohn = new PairsWith().setSource(alice).setDestination(john);
    _localRelationshipWriterDAO.addRelationship(alicePairsWithJohn);

    // Alice filter
    Criterion aliceFilterCriterion = new Criterion().setField("i_aspectfoo$value").setValue("Alice").setCondition(Condition.EQUAL);
    Filter aliceFilter = new Filter().setCriteria(new CriterionArray(aliceFilterCriterion));

    // Age filter
    Criterion ageFilterCriterion = new Criterion().setField("i_aspectbar$value").setValue("30").setCondition(Condition.GREATER_THAN);
    Filter ageFilter = new Filter().setCriteria(new CriterionArray(ageFilterCriterion));

    // Find all the persons that are paired with Alice and also more than 30 years old.
    List<RecordTemplate> personsPairedWithAlice = _localRelationshipQueryDAO.findEntities(FooSnapshot.class, aliceFilter,
        FooSnapshot.class, ageFilter, PairsWith.class,
        new RelationshipFilter().setDirection(RelationshipDirection.UNDIRECTED).setCriteria(new CriterionArray()), 1, 1, 0, 10);

    // Asserts Alice pairs with two persons
    assertEquals(personsPairedWithAlice.size(), 2);

    Set<FooUrn> actual = personsPairedWithAlice.stream().map(result -> {
      FooSnapshot school = (FooSnapshot) result;
      return makeFooUrn(school.data().get("urn").toString());
    }).collect(Collectors.toSet());

    // Asserts Alice paired with Bob and John
    Set<FooUrn> expected = ImmutableSet.of(bob, john);
    assertEquals(actual, expected);
  }

  private FooUrn makeFooUrn(String urn) {
    try {
      return FooUrn.createFromString(urn);
    } catch (URISyntaxException e) {
      throw new RuntimeException(String.format("Invalid urn %s", urn));
    }
  }

  private BarUrn makeBarUrn(String urn) {
    try {
      return BarUrn.createFromString(urn);
    } catch (URISyntaxException e) {
      throw new RuntimeException(String.format("Invalid urn %s", urn));
    }
  }
}
