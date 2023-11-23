package com.linkedin.metadata.dao.localrelationship;

import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import com.linkedin.avro2pegasus.events.UUID;
import com.linkedin.common.AuditStamp;
import com.linkedin.data.ByteString;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.dao.EbeanLocalAccess;
import com.linkedin.metadata.dao.EbeanLocalRelationshipQueryDAO;
import com.linkedin.metadata.dao.EbeanLocalRelationshipWriterDAO;
import com.linkedin.metadata.dao.IEbeanLocalAccess;
import com.linkedin.metadata.dao.scsi.EmptyPathExtractor;
import com.linkedin.metadata.dao.utils.EmbeddedMariaInstance;
import com.linkedin.metadata.query.AspectField;
import com.linkedin.metadata.query.Condition;
import com.linkedin.metadata.query.LocalRelationshipCriterion;
import com.linkedin.metadata.query.LocalRelationshipCriterionArray;
import com.linkedin.metadata.query.LocalRelationshipFilter;
import com.linkedin.metadata.query.LocalRelationshipValue;
import com.linkedin.metadata.query.RelationshipDirection;
import com.linkedin.metadata.query.RelationshipField;
import com.linkedin.metadata.query.UrnField;
import com.linkedin.testing.AspectBar;
import com.linkedin.testing.AspectFoo;
import com.linkedin.testing.BarSnapshot;
import com.linkedin.testing.EntityAspectUnion;
import com.linkedin.testing.EntityAspectUnionArray;
import com.linkedin.testing.FooSnapshot;
import com.linkedin.testing.localrelationship.BelongsTo;
import com.linkedin.testing.localrelationship.ConsumeFrom;
import com.linkedin.testing.localrelationship.EnvorinmentTyep;
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

import static com.linkedin.testing.TestUtils.*;
import static org.testng.Assert.*;


public class EbeanLocalRelationshipQueryDAOTest {
  private EbeanServer _server;
  private EbeanLocalRelationshipWriterDAO _localRelationshipWriterDAO;
  private EbeanLocalRelationshipQueryDAO _localRelationshipQueryDAO;
  private IEbeanLocalAccess<FooUrn> _fooUrnEBeanLocalAccess;
  private IEbeanLocalAccess<BarUrn> _barUrnEBeanLocalAccess;
  private static final byte[] UUID = {0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1};

  @BeforeClass
  public void init() {
    _server = EmbeddedMariaInstance.getServer(EbeanLocalRelationshipQueryDAOTest.class.getSimpleName());
    _localRelationshipWriterDAO = new EbeanLocalRelationshipWriterDAO(_server);
    _localRelationshipQueryDAO = new EbeanLocalRelationshipQueryDAO(_server);
    _fooUrnEBeanLocalAccess = new EbeanLocalAccess<>(_server, EmbeddedMariaInstance.SERVER_CONFIG_MAP.get(_server.getName()),
        FooUrn.class, new EmptyPathExtractor<>());
    _barUrnEBeanLocalAccess = new EbeanLocalAccess<>(_server, EmbeddedMariaInstance.SERVER_CONFIG_MAP.get(_server.getName()),
        BarUrn.class, new EmptyPathExtractor<>());
  }

  @BeforeMethod
  public void recreateTables() throws IOException {
    _server.execute(Ebean.createSqlUpdate(
        Resources.toString(Resources.getResource("ebean-local-relationship-dao-create-all.sql"), StandardCharsets.UTF_8)));
  }

  @Test
  public void testFindOneEntity() throws URISyntaxException {
    // Ingest data
    _fooUrnEBeanLocalAccess.add(new FooUrn(1), new AspectFoo().setValue("foo"), AspectFoo.class, new AuditStamp(), new UUID(ByteString.copy(UUID)));

    // Prepare filter
    AspectField aspectField = new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value");
    LocalRelationshipCriterion.Field field = new LocalRelationshipCriterion.Field();
    field.setAspectField(aspectField);

    LocalRelationshipCriterion filterCriterion = new LocalRelationshipCriterion()
        .setField(field)
        .setValue(LocalRelationshipValue.create("foo"))
        .setCondition(Condition.EQUAL);

    LocalRelationshipFilter filter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));
    List<FooSnapshot> fooSnapshotList = _localRelationshipQueryDAO.findEntities(FooSnapshot.class, filter, 0, 10);

    assertEquals(fooSnapshotList.size(), 1);
    assertEquals(fooSnapshotList.get(0).getAspects().size(), 1);
    assertEquals(fooSnapshotList.get(0).getAspects().get(0).getAspectFoo(), new AspectFoo().setValue("foo"));
  }

  @Test
  public void testFindOneEntityTwoAspects() throws URISyntaxException {
    // Ingest data
    _fooUrnEBeanLocalAccess.add(new FooUrn(1), new AspectFoo().setValue("foo"), AspectFoo.class, new AuditStamp(), new UUID(ByteString.copy(UUID)));
    _fooUrnEBeanLocalAccess.add(new FooUrn(1), new AspectBar().setValue("bar"), AspectBar.class, new AuditStamp(), new UUID(ByteString.copy(UUID)));

    // Prepare filter
    AspectField aspectField = new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value");
    LocalRelationshipCriterion.Field field = new LocalRelationshipCriterion.Field();
    field.setAspectField(aspectField);

    LocalRelationshipCriterion filterCriterion = new LocalRelationshipCriterion()
        .setField(field)
        .setValue(LocalRelationshipValue.create("foo"))
        .setCondition(Condition.EQUAL);

    LocalRelationshipFilter filter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));

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
    _fooUrnEBeanLocalAccess.add(alice, new AspectFoo().setValue("Alice"), AspectFoo.class, new AuditStamp(), new UUID(ByteString.copy(UUID)));
    _fooUrnEBeanLocalAccess.add(bob, new AspectFoo().setValue("Bob"), AspectFoo.class, new AuditStamp(), new UUID(ByteString.copy(UUID)));
    _fooUrnEBeanLocalAccess.add(jack, new AspectFoo().setValue("Jack"), AspectFoo.class, new AuditStamp(), new UUID(ByteString.copy(UUID)));

    // Add Bob reports-to ALice relationship
    ReportsTo bobReportsToAlice = new ReportsTo().setSource(bob).setDestination(alice);
    _localRelationshipWriterDAO.addRelationship(bobReportsToAlice);

    // Add Jack reports-to ALice relationship
    ReportsTo jackReportsToAlice = new ReportsTo().setSource(jack).setDestination(alice);
    _localRelationshipWriterDAO.addRelationship(jackReportsToAlice);

    // Find all reports-to relationship for Alice.
    AspectField aspectField = new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value");
    LocalRelationshipCriterion.Field field = new LocalRelationshipCriterion.Field();
    field.setAspectField(aspectField);

    LocalRelationshipCriterion filterCriterion = new LocalRelationshipCriterion()
        .setField(field)
        .setValue(LocalRelationshipValue.create("Alice"))
        .setCondition(Condition.EQUAL);

    LocalRelationshipFilter filter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));
    List<ReportsTo> reportsToAlice = _localRelationshipQueryDAO.findRelationships(FooSnapshot.class,
        new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()), FooSnapshot.class, filter,
        ReportsTo.class, new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()), 0, 10);

    // Asserts
    assertEquals(reportsToAlice.size(), 2);
    Set<FooUrn> actual = reportsToAlice.stream().map(reportsTo -> makeFooUrn(reportsTo.getSource().toString())).collect(Collectors.toSet());
    Set<FooUrn> expected = ImmutableSet.of(jack, bob);
    assertEquals(actual, expected);
  }

  @Test
  public void testFindOneRelationshipWithFilter() throws Exception {
    FooUrn kafka = new FooUrn(1);
    FooUrn hdfs = new FooUrn(2);
    FooUrn restli = new FooUrn(3);

    BarUrn spark = new BarUrn(1);
    BarUrn samza = new BarUrn(2);

    // Add Kafka_Topic, HDFS_Dataset and Restli_Service into entity tables.
    _fooUrnEBeanLocalAccess.add(kafka, new AspectFoo().setValue("Kafka_Topic"), AspectFoo.class, new AuditStamp(), new UUID(ByteString.copy(UUID)));
    _fooUrnEBeanLocalAccess.add(hdfs, new AspectFoo().setValue("HDFS_Dataset"), AspectFoo.class, new AuditStamp(), new UUID(ByteString.copy(UUID)));
    _fooUrnEBeanLocalAccess.add(restli, new AspectFoo().setValue("Restli_Service"), AspectFoo.class, new AuditStamp(), new UUID(ByteString.copy(UUID)));

    // Add Spark and Samza into entity tables.
    _barUrnEBeanLocalAccess.add(spark, new AspectFoo().setValue("Spark"), AspectFoo.class, new AuditStamp(), new UUID(ByteString.copy(UUID)));
    _barUrnEBeanLocalAccess.add(samza, new AspectFoo().setValue("Samza"), AspectFoo.class, new AuditStamp(), new UUID(ByteString.copy(UUID)));

    // Add Spark consume-from hdfs relationship
    ConsumeFrom sparkConsumeFromHdfs = new ConsumeFrom().setSource(spark).setDestination(hdfs).setEnvironment(EnvorinmentTyep.OFFLINE);
    _localRelationshipWriterDAO.addRelationship(sparkConsumeFromHdfs);

    // Add Samza consume-from kafka relationship
    ConsumeFrom samzaConsumeFromKafka = new ConsumeFrom().setSource(samza).setDestination(kafka).setEnvironment(EnvorinmentTyep.NEARLINE);
    _localRelationshipWriterDAO.addRelationship(samzaConsumeFromKafka);

    // Add Samza consume-from restli relationship
    ConsumeFrom samzaConsumeFromRestli = new ConsumeFrom().setSource(samza).setDestination(restli).setEnvironment(EnvorinmentTyep.ONLINE);
    _localRelationshipWriterDAO.addRelationship(samzaConsumeFromRestli);

    // Find all consume-from relationship for Samza.
    LocalRelationshipCriterion.Field urnField = new LocalRelationshipCriterion.Field();
    urnField.setUrnField(new UrnField());

    LocalRelationshipCriterion filterUrnCriterion = new LocalRelationshipCriterion()
        .setField(urnField)
        .setValue(LocalRelationshipValue.create("urn:li:bar:2")) // 2 is Samza as defined at very beginning.
        .setCondition(Condition.EQUAL);

    LocalRelationshipFilter filterUrn = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterUrnCriterion));
    List<ConsumeFrom> consumeFromSamza = _localRelationshipQueryDAO.findRelationships(
        BarSnapshot.class,
        filterUrn,
        FooSnapshot.class,
        new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()),
        ConsumeFrom.class,
        new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()),
        0, 10);

    // Assert
    assertEquals(consumeFromSamza.size(), 2); // Because Samza consume from 1. kafka and 2. restli

    // Find all consume-from relationship for Samza which happens in NEARLINE.
    LocalRelationshipCriterion.Field relationshipField = new LocalRelationshipCriterion.Field();
    relationshipField.setRelationshipField(new RelationshipField().setPath("/environment"));

    LocalRelationshipCriterion filterRelationshipCriterion = new LocalRelationshipCriterion()
        .setField(relationshipField)
        .setValue(LocalRelationshipValue.create("NEARLINE"))
        .setCondition(Condition.EQUAL);

    LocalRelationshipFilter filterRelationship = new LocalRelationshipFilter().setCriteria(
        new LocalRelationshipCriterionArray(filterRelationshipCriterion));

    List<ConsumeFrom> consumeFromSamzaInNearline = _localRelationshipQueryDAO.findRelationships(
        BarSnapshot.class,
        filterUrn,
        FooSnapshot.class,
        new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()),
        ConsumeFrom.class,
        filterRelationship,
        0, 10);

    // Assert
    assertEquals(consumeFromSamzaInNearline.size(), 1); // Because Samza only consumes kafka in NEARLINE.
  }

  @Test
  public void testFindEntitiesOneHopAwayIncomingDirection() throws Exception {
    FooUrn alice = new FooUrn(1);
    FooUrn bob = new FooUrn(2);
    FooUrn jack = new FooUrn(3);

    // Add Alice, Bob and Jack into entity tables.
    _fooUrnEBeanLocalAccess.add(alice, new AspectFoo().setValue("Alice"), AspectFoo.class, new AuditStamp(), new UUID(ByteString.copy(UUID)));
    _fooUrnEBeanLocalAccess.add(bob, new AspectFoo().setValue("Bob"), AspectFoo.class, new AuditStamp(), new UUID(ByteString.copy(UUID)));
    _fooUrnEBeanLocalAccess.add(jack, new AspectFoo().setValue("Jack"), AspectFoo.class, new AuditStamp(), new UUID(ByteString.copy(UUID)));

    // Add Bob reports-to ALice relationship
    ReportsTo bobReportsToAlice = new ReportsTo().setSource(bob).setDestination(alice);
    _localRelationshipWriterDAO.addRelationship(bobReportsToAlice);

    // Add Jack reports-to ALice relationship
    ReportsTo jackReportsToAlice = new ReportsTo().setSource(jack).setDestination(alice);
    _localRelationshipWriterDAO.addRelationship(jackReportsToAlice);

    // Find all Alice's direct reports.
    AspectField aspectField = new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value");
    LocalRelationshipCriterion.Field field = new LocalRelationshipCriterion.Field();
    field.setAspectField(aspectField);

    LocalRelationshipCriterion filterCriterion = new LocalRelationshipCriterion()
        .setField(field)
        .setValue(LocalRelationshipValue.create("Alice"))
        .setCondition(Condition.EQUAL);

    LocalRelationshipFilter filter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));
    List<RecordTemplate> aliceDirectReports = _localRelationshipQueryDAO.findEntities(
        FooSnapshot.class,
        new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()),
        FooSnapshot.class,
        filter,
        ReportsTo.class,
        new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.INCOMING),
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
    _fooUrnEBeanLocalAccess.add(alice, new AspectFoo().setValue("Alice"), AspectFoo.class, new AuditStamp(), new UUID(ByteString.copy(UUID)));
    _fooUrnEBeanLocalAccess.add(bob, new AspectFoo().setValue("Bob"), AspectFoo.class, new AuditStamp(), new UUID(ByteString.copy(UUID)));

    // Add Stanford and MIT into entity tables.
    _barUrnEBeanLocalAccess.add(stanford, new AspectFoo().setValue("Stanford"), AspectFoo.class, new AuditStamp(), new UUID(ByteString.copy(UUID)));
    _barUrnEBeanLocalAccess.add(mit, new AspectFoo().setValue("MIT"), AspectFoo.class, new AuditStamp(), new UUID(ByteString.copy(UUID)));

    // Add Alice belongs to MIT and Stanford.
    BelongsTo aliceBelongsToMit = new BelongsTo().setSource(alice).setDestination(mit);
    BelongsTo aliceBelongsToStanford = new BelongsTo().setSource(alice).setDestination(stanford);
    _localRelationshipWriterDAO.addRelationship(aliceBelongsToMit);
    _localRelationshipWriterDAO.addRelationship(aliceBelongsToStanford);

    // Add Bob belongs to Stanford.
    BelongsTo bobBelongsToStandford = new BelongsTo().setSource(bob).setDestination(stanford);
    _localRelationshipWriterDAO.addRelationship(bobBelongsToStandford);

    // Alice filter
    AspectField aspectField = new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value");
    LocalRelationshipCriterion.Field field = new LocalRelationshipCriterion.Field();
    field.setAspectField(aspectField);

    LocalRelationshipCriterion filterCriterion = new LocalRelationshipCriterion()
        .setField(field)
        .setValue(LocalRelationshipValue.create("Alice"))
        .setCondition(Condition.EQUAL);

    LocalRelationshipFilter aliceFilter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));

    // Find all the schools Alice has attended.
    List<RecordTemplate> schoolsAliceAttends = _localRelationshipQueryDAO.findEntities(
        FooSnapshot.class,
        aliceFilter,
        BarSnapshot.class,
        new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()),
        BelongsTo.class,
        new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.OUTGOING),
        1, 1, 0, 10);

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
    _fooUrnEBeanLocalAccess.add(alice, new AspectFoo().setValue("Alice"), AspectFoo.class, new AuditStamp(), new UUID(ByteString.copy(UUID)));
    _fooUrnEBeanLocalAccess.add(bob, new AspectFoo().setValue("Bob"), AspectFoo.class, new AuditStamp(), new UUID(ByteString.copy(UUID)));
    _fooUrnEBeanLocalAccess.add(jack, new AspectFoo().setValue("Jack"), AspectFoo.class, new AuditStamp(), new UUID(ByteString.copy(UUID)));
    _fooUrnEBeanLocalAccess.add(john, new AspectFoo().setValue("John"), AspectFoo.class, new AuditStamp(), new UUID(ByteString.copy(UUID)));

    _fooUrnEBeanLocalAccess.add(alice, new AspectBar().setValue("32"), AspectBar.class, new AuditStamp(),
        new UUID(ByteString.copy(UUID))); // Alice 32 years old

    _fooUrnEBeanLocalAccess.add(bob, new AspectBar().setValue("52"), AspectBar.class, new AuditStamp(),
        new UUID(ByteString.copy(UUID))); // Bob 52 years old

    _fooUrnEBeanLocalAccess.add(jack, new AspectBar().setValue("16"), AspectBar.class, new AuditStamp(),
        new UUID(ByteString.copy(UUID))); // Jack 16 years old

    _fooUrnEBeanLocalAccess.add(john, new AspectBar().setValue("42"), AspectBar.class, new AuditStamp(),
        new UUID(ByteString.copy(UUID))); // John 42 years old

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
    AspectField aspectField = new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value");
    LocalRelationshipCriterion.Field field = new LocalRelationshipCriterion.Field();
    field.setAspectField(aspectField);

    LocalRelationshipCriterion filterCriterion = new LocalRelationshipCriterion()
        .setField(field)
        .setValue(LocalRelationshipValue.create("Alice"))
        .setCondition(Condition.EQUAL);

    LocalRelationshipFilter aliceFilter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));

    // Age filter
    AspectField aspectField2 = new AspectField().setAspect(AspectBar.class.getCanonicalName()).setPath("/value");
    LocalRelationshipCriterion.Field field2 = new LocalRelationshipCriterion.Field();
    field2.setAspectField(aspectField2);

    LocalRelationshipCriterion filterCriterion2 = new LocalRelationshipCriterion()
        .setField(field2)
        .setValue(LocalRelationshipValue.create("30"))
        .setCondition(Condition.GREATER_THAN);

    LocalRelationshipFilter ageFilter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion2));


    // Find all the persons that are paired with Alice and also more than 30 years old.
    List<RecordTemplate> personsPairedWithAlice = _localRelationshipQueryDAO.findEntities(
        FooSnapshot.class,
        aliceFilter,
        FooSnapshot.class,
        ageFilter,
        PairsWith.class,
        new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
        1, 1, 0, 10);

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

  @Test
  public void testFindOneEntityWithInCondition() throws URISyntaxException {
    // Ingest data
    _fooUrnEBeanLocalAccess.add(new FooUrn(1), new AspectFoo().setValue("foo"), AspectFoo.class, new AuditStamp(), new UUID(ByteString.copy(UUID)));

    // Prepare filter
    AspectField aspectField = new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value");
    LocalRelationshipCriterion.Field field = new LocalRelationshipCriterion.Field();
    field.setAspectField(aspectField);

    LocalRelationshipCriterion filterCriterion = new LocalRelationshipCriterion()
        .setField(field)
        .setValue(LocalRelationshipValue.create(new StringArray("foo")))
        .setCondition(Condition.IN);

    LocalRelationshipFilter filter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));
    List<FooSnapshot> fooSnapshotList = _localRelationshipQueryDAO.findEntities(FooSnapshot.class, filter, 0, 10);

    assertEquals(fooSnapshotList.size(), 1);
    assertEquals(fooSnapshotList.get(0).getAspects().size(), 1);
    assertEquals(fooSnapshotList.get(0).getAspects().get(0).getAspectFoo(), new AspectFoo().setValue("foo"));
  }

  @Test
  public void testFindNoEntityWithInCondition() throws URISyntaxException {
    // Ingest data
    _fooUrnEBeanLocalAccess.add(new FooUrn(1), new AspectFoo().setValue("foo"), AspectFoo.class, new AuditStamp(), new UUID(ByteString.copy(UUID)));

    // Prepare filter
    AspectField aspectField = new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value");
    LocalRelationshipCriterion.Field field = new LocalRelationshipCriterion.Field();
    field.setAspectField(aspectField);

    LocalRelationshipCriterion filterCriterion = new LocalRelationshipCriterion()
        .setField(field)
        .setValue(LocalRelationshipValue.create(new StringArray("bar")))
        .setCondition(Condition.IN);

    LocalRelationshipFilter filter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));
    List<FooSnapshot> fooSnapshotList = _localRelationshipQueryDAO.findEntities(FooSnapshot.class, filter, 0, 10);

    assertEquals(fooSnapshotList.size(), 0);
  }

  @Test
  public void testFindEntitiesWithEmptyRelationshipFilter() throws URISyntaxException {
    // Ingest data
    _fooUrnEBeanLocalAccess.add(new FooUrn(1), new AspectFoo().setValue("foo"), AspectFoo.class, new AuditStamp(), new UUID(ByteString.copy(UUID)));

    // Create empty filter
    LocalRelationshipFilter emptyFilter = new LocalRelationshipFilter();

    try {
      _localRelationshipQueryDAO.findEntities(FooSnapshot.class, emptyFilter, FooSnapshot.class, emptyFilter, PairsWith.class, emptyFilter, 1, 1, 0, 10);
    } catch (Exception ex) {
      assertTrue(ex instanceof IllegalArgumentException);
      assertEquals(ex.getMessage(), "Relationship direction cannot be null or UNKNOWN.");
    }
  }
}
