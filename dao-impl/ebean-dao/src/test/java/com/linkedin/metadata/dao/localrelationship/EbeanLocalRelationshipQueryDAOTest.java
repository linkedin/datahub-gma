package com.linkedin.metadata.dao.localrelationship;

import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import com.linkedin.common.AuditStamp;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.dao.EBeanDAOConfig;
import com.linkedin.metadata.dao.EbeanLocalAccess;
import com.linkedin.metadata.dao.EbeanLocalRelationshipQueryDAO;
import com.linkedin.metadata.dao.EbeanLocalRelationshipWriterDAO;
import com.linkedin.metadata.dao.IEbeanLocalAccess;
import com.linkedin.metadata.dao.internal.BaseGraphWriterDAO;
import com.linkedin.metadata.dao.urnpath.EmptyPathExtractor;
import com.linkedin.metadata.dao.utils.EBeanDAOUtils;
import com.linkedin.metadata.dao.utils.EmbeddedMariaInstance;
import com.linkedin.metadata.dao.utils.SQLSchemaUtils;
import com.linkedin.metadata.dao.utils.SQLStatementUtils;
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
import com.linkedin.testing.localrelationship.EnvorinmentType;
import com.linkedin.testing.localrelationship.OwnedBy;
import com.linkedin.testing.localrelationship.PairsWith;
import com.linkedin.testing.localrelationship.ReportsTo;
import com.linkedin.testing.urn.BarUrn;
import com.linkedin.testing.urn.FooUrn;
import io.ebean.Ebean;
import io.ebean.EbeanServer;
import io.ebean.SqlUpdate;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import static com.linkedin.testing.TestUtils.*;
import static org.testng.Assert.*;


public class EbeanLocalRelationshipQueryDAOTest {
  public static final String FOO_ENTITY_URN = "urn:li:foo";
  public static final String BAR_ENTITY_URN = "urn:li:bar";
  public static final String CREW_ENTITY_URN = "urn:li:crew";
  private EbeanServer _server;
  private EbeanLocalRelationshipWriterDAO _localRelationshipWriterDAO;
  private EbeanLocalRelationshipQueryDAO _localRelationshipQueryDAO;
  private IEbeanLocalAccess<FooUrn> _fooUrnEBeanLocalAccess;
  private IEbeanLocalAccess<BarUrn> _barUrnEBeanLocalAccess;
  private final EBeanDAOConfig _eBeanDAOConfig = new EBeanDAOConfig();

  @Factory(dataProvider = "inputList")
  public EbeanLocalRelationshipQueryDAOTest(boolean nonDollarVirtualColumnsEnabled) {
    _eBeanDAOConfig.setNonDollarVirtualColumnsEnabled(nonDollarVirtualColumnsEnabled);
  }

  @DataProvider(name = "inputList")
  public static Object[][] inputList() {
    return new Object[][] {
        { true },
        { false }
    };
  }

  @BeforeClass
  public void init() {
    _server = EmbeddedMariaInstance.getServer(EbeanLocalRelationshipQueryDAOTest.class.getSimpleName());
    _localRelationshipWriterDAO = new EbeanLocalRelationshipWriterDAO(_server);
    _localRelationshipQueryDAO = new EbeanLocalRelationshipQueryDAO(_server, _eBeanDAOConfig);
    _fooUrnEBeanLocalAccess = new EbeanLocalAccess<>(_server, EmbeddedMariaInstance.SERVER_CONFIG_MAP.get(_server.getName()),
        FooUrn.class, new EmptyPathExtractor<>(), _eBeanDAOConfig.isNonDollarVirtualColumnsEnabled());
    _barUrnEBeanLocalAccess = new EbeanLocalAccess<>(_server, EmbeddedMariaInstance.SERVER_CONFIG_MAP.get(_server.getName()),
        BarUrn.class, new EmptyPathExtractor<>(), _eBeanDAOConfig.isNonDollarVirtualColumnsEnabled());
  }

  @BeforeMethod
  public void recreateTables() throws IOException {
    if (!_eBeanDAOConfig.isNonDollarVirtualColumnsEnabled()) {
      _server.execute(Ebean.createSqlUpdate(
          Resources.toString(Resources.getResource("ebean-local-relationship-dao-create-all.sql"), StandardCharsets.UTF_8)));
    } else {
      _server.execute(Ebean.createSqlUpdate(
          Resources.toString(Resources.getResource("ebean-local-relationship-create-all-with-non-dollar-virtual-column-names.sql"), StandardCharsets.UTF_8)));
    }
  }

  @Test
  public void testFindOneEntity() throws URISyntaxException {
    // Ingest data
    _fooUrnEBeanLocalAccess.add(new FooUrn(1), new AspectFoo().setValue("foo"), AspectFoo.class, new AuditStamp(), null);

    // Prepare filter
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("foo"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));

    LocalRelationshipFilter filter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));
    List<FooSnapshot> fooSnapshotList = _localRelationshipQueryDAO.findEntities(FooSnapshot.class, filter, 0, 10);

    assertEquals(fooSnapshotList.size(), 1);
    assertEquals(fooSnapshotList.get(0).getAspects().size(), 1);
    assertEquals(fooSnapshotList.get(0).getAspects().get(0).getAspectFoo(), new AspectFoo().setValue("foo"));
  }

  @Test
  public void testFindOneEntityTwoAspects() throws URISyntaxException {
    // Ingest data
    _fooUrnEBeanLocalAccess.add(new FooUrn(1), new AspectFoo().setValue("foo"), AspectFoo.class, new AuditStamp(), null);
    _fooUrnEBeanLocalAccess.add(new FooUrn(1), new AspectBar().setValue("bar"), AspectBar.class, new AuditStamp(), null);

    // Prepare filter
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("foo"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));

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
    _fooUrnEBeanLocalAccess.add(alice, new AspectFoo().setValue("Alice"), AspectFoo.class, new AuditStamp(), null);
    _fooUrnEBeanLocalAccess.add(bob, new AspectFoo().setValue("Bob"), AspectFoo.class, new AuditStamp(), null);
    _fooUrnEBeanLocalAccess.add(jack, new AspectFoo().setValue("Jack"), AspectFoo.class, new AuditStamp(), null);

    // Add Bob reports-to ALice relationship
    ReportsTo bobReportsToAlice = new ReportsTo().setSource(bob).setDestination(alice);
    _localRelationshipWriterDAO.addRelationship(bobReportsToAlice);

    // Add Jack reports-to ALice relationship
    ReportsTo jackReportsToAlice = new ReportsTo().setSource(jack).setDestination(alice);
    _localRelationshipWriterDAO.addRelationship(jackReportsToAlice);

    // Find all reports-to relationship for Alice.
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Alice"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));

    LocalRelationshipFilter filter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));
    List<ReportsTo> reportsToAlice = _localRelationshipQueryDAO.findRelationships(FooSnapshot.class,
        new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()), FooSnapshot.class, filter,
        ReportsTo.class, new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()), 0, 10);

    // Asserts
    assertEquals(reportsToAlice.size(), 2);
    Set<FooUrn> actual = reportsToAlice.stream().map(reportsTo -> makeFooUrn(reportsTo.getSource().toString())).collect(Collectors.toSet());
    Set<FooUrn> expected = ImmutableSet.of(jack, bob);
    assertEquals(actual, expected);

    // Soft (set delete_ts = now()) Delete Jack reports-to ALice relationship
    SqlUpdate deletionSQL = _server.createSqlUpdate(
        SQLStatementUtils.deleteLocaRelationshipSQL(SQLSchemaUtils.getRelationshipTableName(jackReportsToAlice),
            BaseGraphWriterDAO.RemovalOption.REMOVE_ALL_EDGES_FROM_SOURCE));
    deletionSQL.setParameter("source", jack.toString());
    deletionSQL.execute();

    reportsToAlice = _localRelationshipQueryDAO.findRelationships(FooSnapshot.class,
        new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()), FooSnapshot.class, filter,
        ReportsTo.class, new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()), 0, 10);

    // Expect: only bob reports to Alice
    assertEquals(reportsToAlice.size(), 1);
    actual = reportsToAlice.stream()
        .map(reportsTo -> makeFooUrn(reportsTo.getSource().toString()))
        .collect(Collectors.toSet());
    expected = ImmutableSet.of(bob);
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
    _fooUrnEBeanLocalAccess.add(kafka, new AspectFoo().setValue("Kafka_Topic"), AspectFoo.class, new AuditStamp(), null);
    _fooUrnEBeanLocalAccess.add(hdfs, new AspectFoo().setValue("HDFS_Dataset"), AspectFoo.class, new AuditStamp(), null);
    _fooUrnEBeanLocalAccess.add(restli, new AspectFoo().setValue("Restli_Service"), AspectFoo.class, new AuditStamp(),
        null);

    // Add Spark and Samza into entity tables.
    _barUrnEBeanLocalAccess.add(spark, new AspectFoo().setValue("Spark"), AspectFoo.class, new AuditStamp(), null);
    _barUrnEBeanLocalAccess.add(samza, new AspectFoo().setValue("Samza"), AspectFoo.class, new AuditStamp(), null);

    // Add Spark consume-from hdfs relationship
    ConsumeFrom sparkConsumeFromHdfs = new ConsumeFrom().setSource(spark).setDestination(hdfs).setEnvironment(EnvorinmentType.OFFLINE);
    _localRelationshipWriterDAO.addRelationship(sparkConsumeFromHdfs);

    // Add Samza consume-from kafka relationship
    ConsumeFrom samzaConsumeFromKafka = new ConsumeFrom().setSource(samza).setDestination(kafka).setEnvironment(EnvorinmentType.NEARLINE);
    _localRelationshipWriterDAO.addRelationship(samzaConsumeFromKafka);

    // Add Samza consume-from restli relationship
    ConsumeFrom samzaConsumeFromRestli = new ConsumeFrom().setSource(samza).setDestination(restli).setEnvironment(EnvorinmentType.ONLINE);
    _localRelationshipWriterDAO.addRelationship(samzaConsumeFromRestli);

    // Find all consume-from relationship for Samza.
    LocalRelationshipCriterion filterUrnCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(
        LocalRelationshipValue.create("urn:li:bar:2"), // 2 is Samza as defined at very beginning.
        Condition.EQUAL,
        new UrnField());

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
    LocalRelationshipCriterion filterRelationshipCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("NEARLINE"),
        Condition.EQUAL,
        new RelationshipField().setPath("/environment"));

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
  public void testFindOneRelationshipWithEntityUrn() throws Exception {
    FooUrn alice = new FooUrn(1);
    FooUrn bob = new FooUrn(2);
    FooUrn jack = new FooUrn(3);

    // Add Alice, Bob and Jack into entity tables.
    _fooUrnEBeanLocalAccess.add(alice, new AspectFoo().setValue("Alice"), AspectFoo.class, new AuditStamp(), null);
    _fooUrnEBeanLocalAccess.add(bob, new AspectFoo().setValue("Bob"), AspectFoo.class, new AuditStamp(), null);
    _fooUrnEBeanLocalAccess.add(jack, new AspectFoo().setValue("Jack"), AspectFoo.class, new AuditStamp(), null);

    // Add Bob reports-to ALice relationship
    ReportsTo bobReportsToAlice = new ReportsTo().setSource(bob).setDestination(alice);
    _localRelationshipWriterDAO.addRelationship(bobReportsToAlice);

    // Add Jack reports-to ALice relationship
    ReportsTo jackReportsToAlice = new ReportsTo().setSource(jack).setDestination(alice);
    _localRelationshipWriterDAO.addRelationship(jackReportsToAlice);

    // Find all reports-to relationship for Alice.
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Alice"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
    LocalRelationshipFilter destFilter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));

    List<ReportsTo> reportsToAlice = _localRelationshipQueryDAO.findRelationships(
        null, null,
        FOO_ENTITY_URN, destFilter,
        ReportsTo.class, new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()),
        -1, -1);

    // Asserts
    assertEquals(reportsToAlice.size(), 2);
    Set<FooUrn> actual = reportsToAlice.stream().map(reportsTo -> makeFooUrn(reportsTo.getSource().toString())).collect(Collectors.toSet());
    Set<FooUrn> expected = ImmutableSet.of(jack, bob);
    assertEquals(actual, expected);

    // Soft (set delete_ts = now()) Delete Jack reports-to ALice relationship
    SqlUpdate deletionSQL = _server.createSqlUpdate(
        SQLStatementUtils.deleteLocaRelationshipSQL(SQLSchemaUtils.getRelationshipTableName(jackReportsToAlice),
            BaseGraphWriterDAO.RemovalOption.REMOVE_ALL_EDGES_FROM_SOURCE));
    deletionSQL.setParameter("source", jack.toString());
    deletionSQL.execute();

    reportsToAlice = _localRelationshipQueryDAO.findRelationships(
        null, null,
        FOO_ENTITY_URN, destFilter,
        ReportsTo.class, new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()),
        -1, -1);

    // Expect: only bob reports to Alice
    assertEquals(reportsToAlice.size(), 1);
    actual = reportsToAlice.stream()
        .map(reportsTo -> makeFooUrn(reportsTo.getSource().toString()))
        .collect(Collectors.toSet());
    expected = ImmutableSet.of(bob);
    assertEquals(actual, expected);
  }

  @Test
  public void testFindOneRelationshipWithFilterWithEntityUrn() throws Exception {
    FooUrn kafka = new FooUrn(1);
    FooUrn hdfs = new FooUrn(2);
    FooUrn restli = new FooUrn(3);

    BarUrn spark = new BarUrn(1);
    BarUrn samza = new BarUrn(2);

    // Add Kafka_Topic, HDFS_Dataset and Restli_Service into entity tables.
    _fooUrnEBeanLocalAccess.add(kafka, new AspectFoo().setValue("Kafka_Topic"), AspectFoo.class, new AuditStamp(), null);
    _fooUrnEBeanLocalAccess.add(hdfs, new AspectFoo().setValue("HDFS_Dataset"), AspectFoo.class, new AuditStamp(), null);
    _fooUrnEBeanLocalAccess.add(restli, new AspectFoo().setValue("Restli_Service"), AspectFoo.class, new AuditStamp(),
        null);

    // Add Spark and Samza into entity tables.
    _barUrnEBeanLocalAccess.add(spark, new AspectFoo().setValue("Spark"), AspectFoo.class, new AuditStamp(), null);
    _barUrnEBeanLocalAccess.add(samza, new AspectFoo().setValue("Samza"), AspectFoo.class, new AuditStamp(), null);

    // Add Spark consume-from hdfs relationship
    ConsumeFrom sparkConsumeFromHdfs = new ConsumeFrom().setSource(spark).setDestination(hdfs).setEnvironment(EnvorinmentType.OFFLINE);
    _localRelationshipWriterDAO.addRelationship(sparkConsumeFromHdfs);

    // Add Samza consume-from kafka relationship
    ConsumeFrom samzaConsumeFromKafka = new ConsumeFrom().setSource(samza).setDestination(kafka).setEnvironment(EnvorinmentType.NEARLINE);
    _localRelationshipWriterDAO.addRelationship(samzaConsumeFromKafka);

    // Add Samza consume-from restli relationship
    ConsumeFrom samzaConsumeFromRestli = new ConsumeFrom().setSource(samza).setDestination(restli).setEnvironment(EnvorinmentType.ONLINE);
    _localRelationshipWriterDAO.addRelationship(samzaConsumeFromRestli);

    // Find all consume-from relationship for Samza.
    LocalRelationshipCriterion filterUrnCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(
        LocalRelationshipValue.create("urn:li:bar:2"), // 2 is Samza as defined at very beginning.
        Condition.EQUAL,
        new UrnField());
    LocalRelationshipFilter filterUrn = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterUrnCriterion));

    List<ConsumeFrom> consumeFromSamza = _localRelationshipQueryDAO.findRelationships(
        BAR_ENTITY_URN, filterUrn,
        FOO_ENTITY_URN, null,
        ConsumeFrom.class,
        new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()),
        -1, -1);

    assertEquals(consumeFromSamza.size(), 2); // Because Samza consumes from 1. kafka and 2. restli

    // Find all consume-from relationship for Samza which happens in NEARLINE.
    LocalRelationshipCriterion filterRelationshipCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("NEARLINE"),
        Condition.EQUAL,
        new RelationshipField().setPath("/environment"));

    LocalRelationshipFilter filterRelationship = new LocalRelationshipFilter().setCriteria(
        new LocalRelationshipCriterionArray(filterRelationshipCriterion));

    List<ConsumeFrom> consumeFromSamzaInNearline = _localRelationshipQueryDAO.findRelationships(
        BAR_ENTITY_URN, filterUrn,
        FOO_ENTITY_URN, null,
        ConsumeFrom.class,
        filterRelationship,
        -1, -1);

    // Assert
    assertEquals(consumeFromSamzaInNearline.size(), 1); // Because Samza only consumes kafka in NEARLINE.
  }

  @Test
  public void testFindOneRelationshipForCrewUsage() throws Exception {
    FooUrn kafka = new FooUrn(1);
    FooUrn hdfs = new FooUrn(2);
    FooUrn restli = new FooUrn(3);

    BarUrn spark = new BarUrn(1);
    BarUrn samza = new BarUrn(2);

    // Add Kafka_Topic, HDFS_Dataset and Restli_Service into entity tables.
    _fooUrnEBeanLocalAccess.add(kafka, new AspectFoo().setValue("Kafka_Topic"), AspectFoo.class, new AuditStamp(), null);
    _fooUrnEBeanLocalAccess.add(hdfs, new AspectFoo().setValue("HDFS_Dataset"), AspectFoo.class, new AuditStamp(), null);
    _fooUrnEBeanLocalAccess.add(restli, new AspectFoo().setValue("Restli_Service"), AspectFoo.class, new AuditStamp(),
        null);

    // Add Spark and Samza into entity tables.
    _barUrnEBeanLocalAccess.add(spark, new AspectFoo().setValue("Spark"), AspectFoo.class, new AuditStamp(), null);
    _barUrnEBeanLocalAccess.add(samza, new AspectFoo().setValue("Samza"), AspectFoo.class, new AuditStamp(), null);

    // crew1 is a non-mg entity
    FooUrn crew1 = new FooUrn(4);
    FooUrn crew2 = new FooUrn(5);

    // add kafka owned by crew1
    OwnedBy kafkaOwnedByCrew1 = new OwnedBy().setSource(kafka).setDestination(crew1);
    _localRelationshipWriterDAO.addRelationship(kafkaOwnedByCrew1);

    // add hdfs owned by crew1
    OwnedBy hdfsOwnedByCrew1 = new OwnedBy().setSource(hdfs).setDestination(crew1);
    _localRelationshipWriterDAO.addRelationship(hdfsOwnedByCrew1);

    // add restli owned by crew1
    OwnedBy restliOwnedByCrew1 = new OwnedBy().setSource(restli).setDestination(crew1);
    _localRelationshipWriterDAO.addRelationship(restliOwnedByCrew1);

    // add spark owned by crew2
    OwnedBy sparkOwnedByCrew2 = new OwnedBy().setSource(spark).setDestination(crew2);
    _localRelationshipWriterDAO.addRelationship(sparkOwnedByCrew2);

    // add samza owned by crew2
    OwnedBy samzaOwnedByCrew2 = new OwnedBy().setSource(samza).setDestination(crew2);
    _localRelationshipWriterDAO.addRelationship(samzaOwnedByCrew2);

    // Find all owned-by relationship for crew1.
    LocalRelationshipCriterion filterUrnCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(
        LocalRelationshipValue.create("urn:li:foo:4"), // 4 is crew1 as defined at very beginning.
        Condition.EQUAL,
        new UrnField().setName("destination"));
    LocalRelationshipFilter filterUrn = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterUrnCriterion));

    // test owned by of crew1 can be found
    List<OwnedBy> ownedByCrew1 = _localRelationshipQueryDAO.findRelationships(null, null,
        CREW_ENTITY_URN, filterUrn,
        OwnedBy.class, new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()),
        -1, -1);

    assertEquals(ownedByCrew1.size(), 3);

    // Find all owned-by relationship for crew2.
    LocalRelationshipCriterion filterUrnCriterion2 = EBeanDAOUtils.buildRelationshipFieldCriterion(
        LocalRelationshipValue.create("urn:li:foo:5"), // 5 is crew2 as defined at very beginning.
        Condition.EQUAL,
        new UrnField().setName("destination"));
    LocalRelationshipFilter filterUrn2 = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterUrnCriterion2));

    // test owned by of crew2 can be found
    List<OwnedBy> ownedByCrew2 = _localRelationshipQueryDAO.findRelationships(null, null,
        CREW_ENTITY_URN, filterUrn2,
        OwnedBy.class, new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()),
        -1, -1);

    assertEquals(ownedByCrew2.size(), 2);
  }

  @Test
  public void testFindOneRelationshipWithFilterOnSourceEntityForCrewUsage() throws Exception {
    FooUrn kafka = new FooUrn(1);
    FooUrn hdfs = new FooUrn(2);
    FooUrn restli = new FooUrn(3);

    // Add Kafka_Topic, HDFS_Dataset and Restli_Service into entity tables.
    _fooUrnEBeanLocalAccess.add(kafka, new AspectFoo().setValue("Kafka_Topic"), AspectFoo.class, new AuditStamp(), null);
    _fooUrnEBeanLocalAccess.add(hdfs, new AspectFoo().setValue("HDFS_Dataset"), AspectFoo.class, new AuditStamp(), null);
    _fooUrnEBeanLocalAccess.add(restli, new AspectFoo().setValue("Restli_Service"), AspectFoo.class, new AuditStamp(),
        null);

    // crew is a non-mg entity
    FooUrn crew = new FooUrn(4);

    // add kafka owned by crew
    OwnedBy kafkaOwnedByCrew = new OwnedBy().setSource(kafka).setDestination(crew);
    _localRelationshipWriterDAO.addRelationship(kafkaOwnedByCrew);

    // add hdfs owned by crew
    OwnedBy hdfsOwnedByCrew = new OwnedBy().setSource(hdfs).setDestination(crew);
    _localRelationshipWriterDAO.addRelationship(hdfsOwnedByCrew);

    // add restli owned by crew
    OwnedBy restliOwnedByCrew = new OwnedBy().setSource(restli).setDestination(crew);
    _localRelationshipWriterDAO.addRelationship(restliOwnedByCrew);

    // Find all owned-by relationship for crew.
    LocalRelationshipCriterion filterUrnCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(
        LocalRelationshipValue.create("urn:li:foo:4"), // 4 is crew as defined at very beginning.
        Condition.EQUAL,
        new UrnField().setName("destination"));
    LocalRelationshipFilter filterUrn = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterUrnCriterion));

    LocalRelationshipCriterion filterUrnCriterion1 = EBeanDAOUtils.buildRelationshipFieldCriterion(
        LocalRelationshipValue.create("urn:li:foo:1"), // 1 is kafka as defined at very beginning.
        Condition.EQUAL,
        new UrnField());
    LocalRelationshipFilter filterUrn1 = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterUrnCriterion1));


    // test owned by of crew can be filtered by source entity, e.g. only include kafka
    List<OwnedBy> ownedByCrew1 = _localRelationshipQueryDAO.findRelationships(
        FOO_ENTITY_URN, filterUrn1,
        CREW_ENTITY_URN, filterUrn,
        OwnedBy.class, new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()),
        -1, -1);

    assertEquals(ownedByCrew1.size(), 1);
  }

  @Test
  void testFindRelationshipsWithEntityUrnOffsetAndCount() throws Exception {
    FooUrn alice = new FooUrn(1);
    FooUrn bob = new FooUrn(2);
    FooUrn jack = new FooUrn(3);
    FooUrn lisa = new FooUrn(4);
    FooUrn rose = new FooUrn(5);
    FooUrn jenny = new FooUrn(6);

    // Add Alice, Bob, Jack, Lisa, Rose, and Jenny into entity tables.
    _fooUrnEBeanLocalAccess.add(alice, new AspectFoo().setValue("Alice"), AspectFoo.class, new AuditStamp(), null);
    _fooUrnEBeanLocalAccess.add(bob, new AspectFoo().setValue("Bob"), AspectFoo.class, new AuditStamp(), null);
    _fooUrnEBeanLocalAccess.add(jack, new AspectFoo().setValue("Jack"), AspectFoo.class, new AuditStamp(), null);
    _fooUrnEBeanLocalAccess.add(lisa, new AspectFoo().setValue("Lisa"), AspectFoo.class, new AuditStamp(), null);
    _fooUrnEBeanLocalAccess.add(rose, new AspectFoo().setValue("Rose"), AspectFoo.class, new AuditStamp(), null);
    _fooUrnEBeanLocalAccess.add(jenny, new AspectFoo().setValue("Jenny"), AspectFoo.class, new AuditStamp(), null);

    // Add Bob reports-to ALice relationship
    ReportsTo bobReportsToAlice = new ReportsTo().setSource(bob).setDestination(alice);
    _localRelationshipWriterDAO.addRelationship(bobReportsToAlice);

    // Add Jack reports-to ALice relationship
    ReportsTo jackReportsToAlice = new ReportsTo().setSource(jack).setDestination(alice);
    _localRelationshipWriterDAO.addRelationship(jackReportsToAlice);

    // Add Lisa reports-to ALice relationship
    ReportsTo lisaReportsToAlice = new ReportsTo().setSource(lisa).setDestination(alice);
    _localRelationshipWriterDAO.addRelationship(lisaReportsToAlice);

    // Add Rose reports-to ALice relationship
    ReportsTo roseReportsToAlice = new ReportsTo().setSource(rose).setDestination(alice);
    _localRelationshipWriterDAO.addRelationship(roseReportsToAlice);

    // Add Jenny reports-to ALice relationship
    ReportsTo jennyReportsToAlice = new ReportsTo().setSource(jenny).setDestination(alice);
    _localRelationshipWriterDAO.addRelationship(jennyReportsToAlice);

    // Find all reports-to relationship for Alice.
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Alice"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
    LocalRelationshipFilter filter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));

    List<ReportsTo> reportsToAlice = _localRelationshipQueryDAO.findRelationships(
        null, null, FOO_ENTITY_URN, filter,
        ReportsTo.class, new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()),
        -1, 3);

    // Asserts only 3 reports-to relationships are returned
    assertEquals(reportsToAlice.size(), 3);

    reportsToAlice = _localRelationshipQueryDAO.findRelationships(
        null, null, FOO_ENTITY_URN, filter,
        ReportsTo.class, new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()),
        2, -1);

    // Asserts 3 returns, and the content starts from the 3rd report (Lisa)
    assertEquals(reportsToAlice.size(), 3);
    Set<FooUrn> actual = reportsToAlice.stream().map(reportsTo -> makeFooUrn(reportsTo.getSource().toString())).collect(Collectors.toSet());
    Set<FooUrn> expected = ImmutableSet.of(lisa, rose, jenny);
    assertEquals(actual, expected);
  }

  @Test
  public void testIsMgEntityUrn() throws Exception {
    // add foo to EBeanLocalAccess to create table
    FooUrn fooUrn = new FooUrn(1);
    _fooUrnEBeanLocalAccess.add(fooUrn, new AspectFoo().setValue("Alice"), AspectFoo.class, new AuditStamp(), null);

    // EbeanLocalRelationshipQueryDAOTest does not have the same package as EbeanLocalRelationshipQueryDAO (cant access protected method directly).
    Method isMgEntityUrnMethod = EbeanLocalRelationshipQueryDAO.class.getDeclaredMethod("isMgEntityUrn", String.class);
    isMgEntityUrnMethod.setAccessible(true);

    // assert foo is an MG entity (has metadata_entity_foo table in db)
    assertTrue((Boolean) isMgEntityUrnMethod.invoke(_localRelationshipQueryDAO, FOO_ENTITY_URN));

    // assert bar is not an MG entity (does not have metadata_entity_bar table in db)
    assertFalse((Boolean) isMgEntityUrnMethod.invoke(_localRelationshipQueryDAO, BAR_ENTITY_URN));
  }

  @Test
  public void testFindEntitiesOneHopAwayIncomingDirection() throws Exception {
    FooUrn alice = new FooUrn(1);
    FooUrn bob = new FooUrn(2);
    FooUrn jack = new FooUrn(3);

    // Add Alice, Bob and Jack into entity tables.
    _fooUrnEBeanLocalAccess.add(alice, new AspectFoo().setValue("Alice"), AspectFoo.class, new AuditStamp(), null);
    _fooUrnEBeanLocalAccess.add(bob, new AspectFoo().setValue("Bob"), AspectFoo.class, new AuditStamp(), null);
    _fooUrnEBeanLocalAccess.add(jack, new AspectFoo().setValue("Jack"), AspectFoo.class, new AuditStamp(), null);

    // Add Bob reports-to ALice relationship
    ReportsTo bobReportsToAlice = new ReportsTo().setSource(bob).setDestination(alice);
    _localRelationshipWriterDAO.addRelationship(bobReportsToAlice);

    // Add Jack reports-to ALice relationship
    ReportsTo jackReportsToAlice = new ReportsTo().setSource(jack).setDestination(alice);
    _localRelationshipWriterDAO.addRelationship(jackReportsToAlice);

    // Find all Alice's direct reports.
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Alice"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));

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
    _fooUrnEBeanLocalAccess.add(alice, new AspectFoo().setValue("Alice"), AspectFoo.class, new AuditStamp(), null);
    _fooUrnEBeanLocalAccess.add(bob, new AspectFoo().setValue("Bob"), AspectFoo.class, new AuditStamp(), null);

    // Add Stanford and MIT into entity tables.
    _barUrnEBeanLocalAccess.add(stanford, new AspectFoo().setValue("Stanford"), AspectFoo.class, new AuditStamp(), null);
    _barUrnEBeanLocalAccess.add(mit, new AspectFoo().setValue("MIT"), AspectFoo.class, new AuditStamp(), null);

    // Add Alice belongs to MIT and Stanford.
    BelongsTo aliceBelongsToMit = new BelongsTo().setSource(alice).setDestination(mit);
    BelongsTo aliceBelongsToStanford = new BelongsTo().setSource(alice).setDestination(stanford);
    _localRelationshipWriterDAO.addRelationship(aliceBelongsToMit);
    _localRelationshipWriterDAO.addRelationship(aliceBelongsToStanford);

    // Add Bob belongs to Stanford.
    BelongsTo bobBelongsToStandford = new BelongsTo().setSource(bob).setDestination(stanford);
    _localRelationshipWriterDAO.addRelationship(bobBelongsToStandford);

    // Alice filter
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Alice"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));

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
    _fooUrnEBeanLocalAccess.add(alice, new AspectFoo().setValue("Alice"), AspectFoo.class, new AuditStamp(), null);
    _fooUrnEBeanLocalAccess.add(bob, new AspectFoo().setValue("Bob"), AspectFoo.class, new AuditStamp(), null);
    _fooUrnEBeanLocalAccess.add(jack, new AspectFoo().setValue("Jack"), AspectFoo.class, new AuditStamp(), null);
    _fooUrnEBeanLocalAccess.add(john, new AspectFoo().setValue("John"), AspectFoo.class, new AuditStamp(), null);

    _fooUrnEBeanLocalAccess.add(alice, new AspectBar().setValue("32"), AspectBar.class, new AuditStamp(), null); // Alice 32 years old

    _fooUrnEBeanLocalAccess.add(bob, new AspectBar().setValue("52"), AspectBar.class, new AuditStamp(), null); // Bob 52 years old

    _fooUrnEBeanLocalAccess.add(jack, new AspectBar().setValue("16"), AspectBar.class, new AuditStamp(), null); // Jack 16 years old

    _fooUrnEBeanLocalAccess.add(john, new AspectBar().setValue("42"), AspectBar.class, new AuditStamp(), null); // John 42 years old

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
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Alice"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));

    LocalRelationshipFilter aliceFilter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));

    // Age filter
    LocalRelationshipCriterion filterCriterion2 = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("30"),
        Condition.GREATER_THAN,
        new AspectField().setAspect(AspectBar.class.getCanonicalName()).setPath("/value"));

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
    _fooUrnEBeanLocalAccess.add(new FooUrn(1), new AspectFoo().setValue("foo"), AspectFoo.class, new AuditStamp(), null);

    // Prepare filter
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create(new StringArray("foo")),
        Condition.IN,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));

    LocalRelationshipFilter filter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));
    List<FooSnapshot> fooSnapshotList = _localRelationshipQueryDAO.findEntities(FooSnapshot.class, filter, 0, 10);

    assertEquals(fooSnapshotList.size(), 1);
    assertEquals(fooSnapshotList.get(0).getAspects().size(), 1);
    assertEquals(fooSnapshotList.get(0).getAspects().get(0).getAspectFoo(), new AspectFoo().setValue("foo"));
  }

  @Test
  public void testFindNoEntityWithInCondition() throws URISyntaxException {
    // Ingest data
    _fooUrnEBeanLocalAccess.add(new FooUrn(1), new AspectFoo().setValue("foo"), AspectFoo.class, new AuditStamp(), null);

    // Prepare filter
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create(new StringArray("bar")),
        Condition.IN,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));

    LocalRelationshipFilter filter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));
    List<FooSnapshot> fooSnapshotList = _localRelationshipQueryDAO.findEntities(FooSnapshot.class, filter, 0, 10);

    assertEquals(fooSnapshotList.size(), 0);
  }

  @Test
  public void testFindEntitiesWithEmptyRelationshipFilter() throws URISyntaxException {
    // Ingest data
    _fooUrnEBeanLocalAccess.add(new FooUrn(1), new AspectFoo().setValue("foo"), AspectFoo.class, new AuditStamp(), null);

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
