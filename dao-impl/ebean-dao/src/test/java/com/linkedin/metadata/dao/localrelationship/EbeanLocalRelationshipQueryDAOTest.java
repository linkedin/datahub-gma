package com.linkedin.metadata.dao.localrelationship;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.dao.EBeanDAOConfig;
import com.linkedin.metadata.dao.EbeanLocalAccess;
import com.linkedin.metadata.dao.EbeanLocalDAO;
import com.linkedin.metadata.dao.EbeanLocalRelationshipQueryDAO;
import com.linkedin.metadata.dao.EbeanLocalRelationshipWriterDAO;
import com.linkedin.metadata.dao.IEbeanLocalAccess;
import com.linkedin.metadata.dao.urnpath.EmptyPathExtractor;
import com.linkedin.metadata.dao.utils.EBeanDAOUtils;
import com.linkedin.metadata.dao.utils.EmbeddedMariaInstance;
import com.linkedin.metadata.dao.utils.RelationshipLookUpContext;
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
import com.linkedin.testing.localrelationship.AssetRelationship;
import com.linkedin.testing.localrelationship.BelongsTo;
import com.linkedin.testing.localrelationship.BelongsToV2;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.naming.OperationNotSupportedException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import static com.linkedin.metadata.dao.EbeanLocalRelationshipQueryDAO.*;
import static com.linkedin.testing.TestUtils.*;
import static org.testng.Assert.*;


public class EbeanLocalRelationshipQueryDAOTest {
  public Urn fooEntityUrn;
  public Urn barEntityUrn;
  public Urn crewEntityUrn;

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
  public void init() throws URISyntaxException {
    _server = EmbeddedMariaInstance.getServer(EbeanLocalRelationshipQueryDAOTest.class.getSimpleName());
    _localRelationshipWriterDAO = new EbeanLocalRelationshipWriterDAO(_server);
    _fooUrnEBeanLocalAccess = new EbeanLocalAccess<>(_server, EmbeddedMariaInstance.SERVER_CONFIG_MAP.get(_server.getName()),
        FooUrn.class, new EmptyPathExtractor<>(), _eBeanDAOConfig.isNonDollarVirtualColumnsEnabled());
    _barUrnEBeanLocalAccess = new EbeanLocalAccess<>(_server, EmbeddedMariaInstance.SERVER_CONFIG_MAP.get(_server.getName()),
        BarUrn.class, new EmptyPathExtractor<>(), _eBeanDAOConfig.isNonDollarVirtualColumnsEnabled());
    _localRelationshipQueryDAO = new EbeanLocalRelationshipQueryDAO(_server, _eBeanDAOConfig);

    fooEntityUrn = new Urn("urn:li:foo");
    barEntityUrn = new Urn("urn:li:bar");
    crewEntityUrn = new Urn("urn:li:crew");
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

    // also reset the schema mode to NEW_SCHEMA_ONLY
    _localRelationshipQueryDAO.setSchemaConfig(EbeanLocalDAO.SchemaConfig.NEW_SCHEMA_ONLY);
  }

  @Test
  public void testFindOneEntity() throws URISyntaxException, OperationNotSupportedException {
    // Ingest data
    _fooUrnEBeanLocalAccess.add(new FooUrn(1), new AspectFoo().setValue("foo"), AspectFoo.class, new AuditStamp(), null, false);

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
  public void testFindOneEntityTwoAspects() throws URISyntaxException, OperationNotSupportedException {
    // Ingest data
    _fooUrnEBeanLocalAccess.add(new FooUrn(1), new AspectFoo().setValue("foo"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(new FooUrn(1), new AspectBar().setValue("bar"), AspectBar.class, new AuditStamp(), null, false);

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

  @DataProvider(name = "schemaConfig")
  public static Object[][] schemaConfig() {
    return new Object[][] {
        { EbeanLocalDAO.SchemaConfig.NEW_SCHEMA_ONLY },
        { EbeanLocalDAO.SchemaConfig.OLD_SCHEMA_ONLY }
    };
  }

  @Test(dataProvider = "schemaConfig")
  public void testFindOneRelationship(EbeanLocalDAO.SchemaConfig schemaConfig) throws Exception {
    FooUrn alice = new FooUrn(1);
    FooUrn bob = new FooUrn(2);
    FooUrn jack = new FooUrn(3);

    // Add Alice, Bob and Jack into entity tables.
    if (schemaConfig == EbeanLocalDAO.SchemaConfig.NEW_SCHEMA_ONLY) {
      _fooUrnEBeanLocalAccess.add(alice, new AspectFoo().setValue("Alice"), AspectFoo.class, new AuditStamp(), null, false);
      _fooUrnEBeanLocalAccess.add(bob, new AspectFoo().setValue("Bob"), AspectFoo.class, new AuditStamp(), null, false);
      _fooUrnEBeanLocalAccess.add(jack, new AspectFoo().setValue("Jack"), AspectFoo.class, new AuditStamp(), null, false);
    }

    // Add Bob reports-to ALice relationship
    ReportsTo bobReportsToAlice = new ReportsTo().setSource(bob).setDestination(alice);
    _localRelationshipWriterDAO.addRelationships(bob, AspectFoo.class, Collections.singletonList(bobReportsToAlice), false);

    // Add Jack reports-to ALice relationship
    ReportsTo jackReportsToAlice = new ReportsTo().setSource(jack).setDestination(alice);
    _localRelationshipWriterDAO.addRelationships(jack, AspectFoo.class, Collections.singletonList(jackReportsToAlice), false);

    // Find all reports-to relationship for Alice.
    LocalRelationshipFilter filter;
    if (schemaConfig == EbeanLocalDAO.SchemaConfig.OLD_SCHEMA_ONLY) {
      // old schema does not support non-urn field filters
      LocalRelationshipCriterion oldSchemaFilterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create(alice.toString()),
          Condition.EQUAL,
          new UrnField());
      filter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(oldSchemaFilterCriterion));
    } else {
      LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Alice"),
          Condition.EQUAL,
          new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
      filter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));
    }

    _localRelationshipQueryDAO.setSchemaConfig(schemaConfig);
    List<ReportsTo> reportsToAlice = _localRelationshipQueryDAO.findRelationships(FooSnapshot.class,
        new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()), FooSnapshot.class, filter,
        ReportsTo.class, new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.OUTGOING),
        0, 10);

    // Asserts
    assertEquals(reportsToAlice.size(), 2);
    Set<FooUrn> actual = reportsToAlice.stream().map(reportsTo -> makeFooUrn(reportsTo.getSource().toString())).collect(Collectors.toSet());
    Set<FooUrn> expected = ImmutableSet.of(jack, bob);
    assertEquals(actual, expected);

    // Soft (set delete_ts = now()) Delete Jack reports-to ALice relationship
    SqlUpdate deletionSQL = _server.createSqlUpdate(
        SQLStatementUtils.deleteLocalRelationshipSQL(SQLSchemaUtils.getRelationshipTableName(jackReportsToAlice), false));
    deletionSQL.setParameter("source", jack.toString());
    deletionSQL.execute();

    reportsToAlice = _localRelationshipQueryDAO.findRelationships(FooSnapshot.class,
        new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()), FooSnapshot.class, filter,
        ReportsTo.class, new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.OUTGOING),
        0, 10);

    // Expect: only bob reports to Alice
    assertEquals(reportsToAlice.size(), 1);
    actual = reportsToAlice.stream()
        .map(reportsTo -> makeFooUrn(reportsTo.getSource().toString()))
        .collect(Collectors.toSet());
    expected = ImmutableSet.of(bob);
    assertEquals(actual, expected);
  }

  @Test(dataProvider = "schemaConfig")
  public void testFindOneRelationshipWithFilter(EbeanLocalDAO.SchemaConfig schemaConfig) throws Exception {
    FooUrn kafka = new FooUrn(1);
    FooUrn hdfs = new FooUrn(2);
    FooUrn restli = new FooUrn(3);

    BarUrn spark = new BarUrn(1);
    BarUrn samza = new BarUrn(2);

    // Add Kafka_Topic, HDFS_Dataset and Restli_Service into entity tables.
    if (schemaConfig == EbeanLocalDAO.SchemaConfig.NEW_SCHEMA_ONLY) {
      _fooUrnEBeanLocalAccess.add(kafka, new AspectFoo().setValue("Kafka_Topic"), AspectFoo.class, new AuditStamp(),
          null, false);
      _fooUrnEBeanLocalAccess.add(hdfs, new AspectFoo().setValue("HDFS_Dataset"), AspectFoo.class, new AuditStamp(),
          null, false);
      _fooUrnEBeanLocalAccess.add(restli, new AspectFoo().setValue("Restli_Service"), AspectFoo.class, new AuditStamp(),
          null, false);

      // Add Spark and Samza into entity tables.
      _barUrnEBeanLocalAccess.add(spark, new AspectFoo().setValue("Spark"), AspectFoo.class, new AuditStamp(), null, false);
      _barUrnEBeanLocalAccess.add(samza, new AspectFoo().setValue("Samza"), AspectFoo.class, new AuditStamp(), null, false);
    }

    // Add Spark consume-from hdfs relationship
    ConsumeFrom sparkConsumeFromHdfs = new ConsumeFrom().setSource(spark).setDestination(hdfs).setEnvironment(EnvorinmentType.OFFLINE);
    _localRelationshipWriterDAO.addRelationships(spark, AspectFoo.class, Collections.singletonList(sparkConsumeFromHdfs), false);

    // Add Samza consume-from kafka and Samza consume-from restli relationships
    ConsumeFrom samzaConsumeFromKafka = new ConsumeFrom().setSource(samza).setDestination(kafka).setEnvironment(EnvorinmentType.NEARLINE);
    ConsumeFrom samzaConsumeFromRestli = new ConsumeFrom().setSource(samza).setDestination(restli).setEnvironment(EnvorinmentType.ONLINE);

    _localRelationshipWriterDAO.addRelationships(samza, AspectFoo.class, ImmutableList.of(samzaConsumeFromKafka, samzaConsumeFromRestli), false);

    // Find all consume-from relationship for Samza.
    LocalRelationshipCriterion filterUrnCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(
        LocalRelationshipValue.create(samza.toString()),
        Condition.EQUAL,
        new UrnField());

    LocalRelationshipFilter filterUrn = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterUrnCriterion));

    _localRelationshipQueryDAO.setSchemaConfig(schemaConfig);
    List<ConsumeFrom> consumeFromSamza = _localRelationshipQueryDAO.findRelationships(
        BarSnapshot.class,
        filterUrn,
        FooSnapshot.class,
        new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()),
        ConsumeFrom.class,
        new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.OUTGOING),
        0, 10);

    // Assert
    assertEquals(consumeFromSamza.size(), 2); // Because Samza consume from 1. kafka and 2. restli

    // Find all consume-from relationship for Samza which happens in NEARLINE. Not supported in old schema mode.
    if (schemaConfig != EbeanLocalDAO.SchemaConfig.OLD_SCHEMA_ONLY) {
      LocalRelationshipCriterion filterRelationshipCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("NEARLINE"),
          Condition.EQUAL,
          new RelationshipField().setPath("/environment"));

      LocalRelationshipFilter filterRelationship = new LocalRelationshipFilter().setCriteria(
          new LocalRelationshipCriterionArray(filterRelationshipCriterion)).setDirection(RelationshipDirection.OUTGOING);

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
  }

  @Test(dataProvider = "schemaConfig")
  public void testFindOneRelationshipWithEntityUrn(EbeanLocalDAO.SchemaConfig schemaConfig) throws Exception {
    FooUrn alice = new FooUrn(1);
    FooUrn bob = new FooUrn(2);
    FooUrn jack = new FooUrn(3);

    // Add Alice, Bob and Jack into entity tables.
    if (schemaConfig == EbeanLocalDAO.SchemaConfig.NEW_SCHEMA_ONLY) {
      _fooUrnEBeanLocalAccess.add(alice, new AspectFoo().setValue("Alice"), AspectFoo.class, new AuditStamp(), null, false);
      _fooUrnEBeanLocalAccess.add(bob, new AspectFoo().setValue("Bob"), AspectFoo.class, new AuditStamp(), null, false);
      _fooUrnEBeanLocalAccess.add(jack, new AspectFoo().setValue("Jack"), AspectFoo.class, new AuditStamp(), null, false);
    }

    // Add Bob reports-to ALice relationship
    ReportsTo bobReportsToAlice = new ReportsTo().setSource(bob).setDestination(alice);
    _localRelationshipWriterDAO.addRelationships(bob, AspectFoo.class, Collections.singletonList(bobReportsToAlice), false);

    // Add Jack reports-to ALice relationship
    ReportsTo jackReportsToAlice = new ReportsTo().setSource(jack).setDestination(alice);
    _localRelationshipWriterDAO.addRelationships(jack, AspectFoo.class, Collections.singletonList(jackReportsToAlice), false);

    // Find all reports-to relationship for Alice.
    LocalRelationshipFilter destFilter;
    if (schemaConfig == EbeanLocalDAO.SchemaConfig.OLD_SCHEMA_ONLY) {
      // old schema does not support non-urn field filters
      LocalRelationshipCriterion oldSchemaFilterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create(alice.toString()),
          Condition.EQUAL,
          new UrnField());
      destFilter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(oldSchemaFilterCriterion));
    } else {
      LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Alice"),
          Condition.EQUAL,
          new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
      destFilter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));
    }

    _localRelationshipQueryDAO.setSchemaConfig(schemaConfig);

    List<ReportsTo> reportsToAlice = _localRelationshipQueryDAO.findRelationshipsV2(
        null, null, "foo", destFilter,
        ReportsTo.class, new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
        -1, -1, new RelationshipLookUpContext());

    // Asserts
    assertEquals(reportsToAlice.size(), 2);
    Set<FooUrn> actual = reportsToAlice.stream().map(reportsTo -> makeFooUrn(reportsTo.getSource().toString())).collect(Collectors.toSet());
    Set<FooUrn> expected = ImmutableSet.of(jack, bob);
    assertEquals(actual, expected);

    // Soft (set delete_ts = now()) Delete Jack reports-to ALice relationship
    SqlUpdate deletionSQL = _server.createSqlUpdate(
        SQLStatementUtils.deleteLocalRelationshipSQL(SQLSchemaUtils.getRelationshipTableName(jackReportsToAlice), false));
    deletionSQL.setParameter("source", jack.toString());
    deletionSQL.execute();

    reportsToAlice = _localRelationshipQueryDAO.findRelationshipsV2(
        null, null, "foo", destFilter,
        ReportsTo.class, new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
        -1, -1, new RelationshipLookUpContext());

    // Expect: only bob reports to Alice
    assertEquals(reportsToAlice.size(), 1);
    actual = reportsToAlice.stream()
        .map(reportsTo -> makeFooUrn(reportsTo.getSource().toString()))
        .collect(Collectors.toSet());
    expected = ImmutableSet.of(bob);
    assertEquals(actual, expected);
  }

  @Test(dataProvider = "schemaConfig")
  public void testFindOneRelationshipWithFilterWithEntityUrn(EbeanLocalDAO.SchemaConfig schemaConfig) throws Exception {
    FooUrn kafka = new FooUrn(1);
    FooUrn hdfs = new FooUrn(2);
    FooUrn restli = new FooUrn(3);

    BarUrn spark = new BarUrn(1);
    BarUrn samza = new BarUrn(2);

    // Add Kafka_Topic, HDFS_Dataset and Restli_Service into entity tables.
    if (schemaConfig == EbeanLocalDAO.SchemaConfig.NEW_SCHEMA_ONLY) {
      _fooUrnEBeanLocalAccess.add(kafka, new AspectFoo().setValue("Kafka_Topic"), AspectFoo.class, new AuditStamp(),
          null, false);
      _fooUrnEBeanLocalAccess.add(hdfs, new AspectFoo().setValue("HDFS_Dataset"), AspectFoo.class, new AuditStamp(),
          null, false);
      _fooUrnEBeanLocalAccess.add(restli, new AspectFoo().setValue("Restli_Service"), AspectFoo.class, new AuditStamp(),
          null, false);

      // Add Spark and Samza into entity tables.
      _barUrnEBeanLocalAccess.add(spark, new AspectFoo().setValue("Spark"), AspectFoo.class, new AuditStamp(), null, false);
      _barUrnEBeanLocalAccess.add(samza, new AspectFoo().setValue("Samza"), AspectFoo.class, new AuditStamp(), null, false);
    }

    // Add Spark consume-from hdfs relationship
    ConsumeFrom sparkConsumeFromHdfs = new ConsumeFrom().setSource(spark).setDestination(hdfs).setEnvironment(EnvorinmentType.OFFLINE);
    _localRelationshipWriterDAO.addRelationships(spark, AspectFoo.class, Collections.singletonList(sparkConsumeFromHdfs), false);

    // Add Samza consume-from kafka and Samza consume-from restli relationships
    ConsumeFrom samzaConsumeFromKafka = new ConsumeFrom().setSource(samza).setDestination(kafka).setEnvironment(EnvorinmentType.NEARLINE);
    ConsumeFrom samzaConsumeFromRestli = new ConsumeFrom().setSource(samza).setDestination(restli).setEnvironment(EnvorinmentType.ONLINE);

    _localRelationshipWriterDAO.addRelationships(samza, AspectFoo.class, ImmutableList.of(samzaConsumeFromRestli, samzaConsumeFromKafka), false);

    // Find all consume-from relationship for Samza.
    LocalRelationshipCriterion filterUrnCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(
        LocalRelationshipValue.create("urn:li:bar:2"), // 2 is Samza as defined at very beginning.
        Condition.EQUAL,
        new UrnField());
    LocalRelationshipFilter filterUrn = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterUrnCriterion));
    _localRelationshipQueryDAO.setSchemaConfig(schemaConfig);

    List<ConsumeFrom> consumeFromSamza = _localRelationshipQueryDAO.findRelationshipsV2("bar", filterUrn, "foo", null,
        ConsumeFrom.class,
        new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
        -1, -1, new RelationshipLookUpContext());

    assertEquals(consumeFromSamza.size(), 2); // Because Samza consumes from 1. kafka and 2. restli

    // Find all consume-from relationship for Samza which happens in NEARLINE. Not supported in OLD_SCHEMA_ONLY mode.
    if (schemaConfig != EbeanLocalDAO.SchemaConfig.OLD_SCHEMA_ONLY) {
      LocalRelationshipCriterion filterRelationshipCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(
          LocalRelationshipValue.create("NEARLINE"), Condition.EQUAL, new RelationshipField().setPath("/environment"));

      LocalRelationshipFilter filterRelationship = new LocalRelationshipFilter().setCriteria(
          new LocalRelationshipCriterionArray(filterRelationshipCriterion)).setDirection(RelationshipDirection.OUTGOING);

      List<ConsumeFrom> consumeFromSamzaInNearline = _localRelationshipQueryDAO.findRelationshipsV2("bar", filterUrn, "foo", null,
          ConsumeFrom.class,
          filterRelationship,
          -1, -1, new RelationshipLookUpContext());

      // Assert
      assertEquals(consumeFromSamzaInNearline.size(), 1); // Because Samza only consumes kafka in NEARLINE.
    }
  }

  @Test(dataProvider = "schemaConfig")
  public void testFindOneRelationshipForCrewUsage(EbeanLocalDAO.SchemaConfig schemaConfig) throws Exception {
    FooUrn kafka = new FooUrn(1);
    FooUrn hdfs = new FooUrn(2);
    FooUrn restli = new FooUrn(3);

    BarUrn spark = new BarUrn(1);
    BarUrn samza = new BarUrn(2);

    // Add Kafka_Topic, HDFS_Dataset and Restli_Service into entity tables.
    if (schemaConfig == EbeanLocalDAO.SchemaConfig.NEW_SCHEMA_ONLY) {
      _fooUrnEBeanLocalAccess.add(kafka, new AspectFoo().setValue("Kafka_Topic"), AspectFoo.class, new AuditStamp(),
          null, false);
      _fooUrnEBeanLocalAccess.add(hdfs, new AspectFoo().setValue("HDFS_Dataset"), AspectFoo.class, new AuditStamp(),
          null, false);
      _fooUrnEBeanLocalAccess.add(restli, new AspectFoo().setValue("Restli_Service"), AspectFoo.class, new AuditStamp(),
          null, false);

      // Add Spark and Samza into entity tables.
      _barUrnEBeanLocalAccess.add(spark, new AspectFoo().setValue("Spark"), AspectFoo.class, new AuditStamp(), null, false);
      _barUrnEBeanLocalAccess.add(samza, new AspectFoo().setValue("Samza"), AspectFoo.class, new AuditStamp(), null, false);
    }

    // crew1 is a non-mg entity
    FooUrn crew1 = new FooUrn(4);
    FooUrn crew2 = new FooUrn(5);

    // add kafka owned by crew1
    OwnedBy kafkaOwnedByCrew1 = new OwnedBy().setSource(kafka).setDestination(crew1);
    _localRelationshipWriterDAO.addRelationships(kafka, AspectFoo.class, Collections.singletonList(kafkaOwnedByCrew1), false);

    // add hdfs owned by crew1
    OwnedBy hdfsOwnedByCrew1 = new OwnedBy().setSource(hdfs).setDestination(crew1);
    _localRelationshipWriterDAO.addRelationships(hdfs, AspectFoo.class, Collections.singletonList(hdfsOwnedByCrew1), false);

    // add restli owned by crew1
    OwnedBy restliOwnedByCrew1 = new OwnedBy().setSource(restli).setDestination(crew1);
    _localRelationshipWriterDAO.addRelationships(restli, AspectFoo.class, Collections.singletonList(restliOwnedByCrew1), false);

    // add spark owned by crew2
    OwnedBy sparkOwnedByCrew2 = new OwnedBy().setSource(spark).setDestination(crew2);
    _localRelationshipWriterDAO.addRelationships(spark, AspectFoo.class, Collections.singletonList(sparkOwnedByCrew2), false);

    // add samza owned by crew2
    OwnedBy samzaOwnedByCrew2 = new OwnedBy().setSource(samza).setDestination(crew2);
    _localRelationshipWriterDAO.addRelationships(samza, AspectFoo.class, Collections.singletonList(samzaOwnedByCrew2), false);

    // Find all owned-by relationship for crew1.
    LocalRelationshipCriterion filterUrnCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(
        LocalRelationshipValue.create("urn:li:foo:4"), // 4 is crew1 as defined at very beginning.
        Condition.EQUAL,
        new UrnField().setName("destination"));
    LocalRelationshipFilter filterUrn = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterUrnCriterion));

    _localRelationshipQueryDAO.setSchemaConfig(schemaConfig);

    // test owned by of crew1 can be found
    List<OwnedBy> ownedByCrew1 = _localRelationshipQueryDAO.findRelationshipsV2(null, null, "crew", filterUrn,
        OwnedBy.class, new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
        -1, -1, new RelationshipLookUpContext());

    assertEquals(ownedByCrew1.size(), 3);

    // Find all owned-by relationship for crew2.
    LocalRelationshipCriterion filterUrnCriterion2 = EBeanDAOUtils.buildRelationshipFieldCriterion(
        LocalRelationshipValue.create("urn:li:foo:5"), // 5 is crew2 as defined at very beginning.
        Condition.EQUAL,
        new UrnField().setName("destination"));
    LocalRelationshipFilter filterUrn2 = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterUrnCriterion2));

    // test owned by of crew2 can be found
    List<OwnedBy> ownedByCrew2 = _localRelationshipQueryDAO.findRelationshipsV2(null, null, "crew", filterUrn2,
        OwnedBy.class, new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
        -1, -1, new RelationshipLookUpContext());

    assertEquals(ownedByCrew2.size(), 2);
  }

  @Test(dataProvider = "schemaConfig")
  public void testFindOneRelationshipWithFilterOnSourceEntityForCrewUsage(EbeanLocalDAO.SchemaConfig schemaConfig) throws Exception {
    FooUrn kafka = new FooUrn(1);
    FooUrn hdfs = new FooUrn(2);
    FooUrn restli = new FooUrn(3);

    // Add Kafka_Topic, HDFS_Dataset and Restli_Service into entity tables.
    if (schemaConfig == EbeanLocalDAO.SchemaConfig.NEW_SCHEMA_ONLY) {
      _fooUrnEBeanLocalAccess.add(kafka, new AspectFoo().setValue("Kafka_Topic"), AspectFoo.class, new AuditStamp(),
          null, false);
      _fooUrnEBeanLocalAccess.add(hdfs, new AspectFoo().setValue("HDFS_Dataset"), AspectFoo.class, new AuditStamp(),
          null, false);
      _fooUrnEBeanLocalAccess.add(restli, new AspectFoo().setValue("Restli_Service"), AspectFoo.class, new AuditStamp(),
          null, false);
    }

    // crew is a non-mg entity
    FooUrn crew = new FooUrn(4);

    // add kafka owned by crew
    OwnedBy kafkaOwnedByCrew = new OwnedBy().setSource(kafka).setDestination(crew);
    _localRelationshipWriterDAO.addRelationships(kafka, AspectFoo.class, Collections.singletonList(kafkaOwnedByCrew), false);

    // add hdfs owned by crew
    OwnedBy hdfsOwnedByCrew = new OwnedBy().setSource(hdfs).setDestination(crew);
    _localRelationshipWriterDAO.addRelationships(hdfs, AspectFoo.class, Collections.singletonList(hdfsOwnedByCrew), false);

    // add restli owned by crew
    OwnedBy restliOwnedByCrew = new OwnedBy().setSource(restli).setDestination(crew);
    _localRelationshipWriterDAO.addRelationships(restli, AspectFoo.class, Collections.singletonList(restliOwnedByCrew), false);

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

    _localRelationshipQueryDAO.setSchemaConfig(schemaConfig);

    // test owned by of crew can be filtered by source entity, e.g. only include kafka
    List<OwnedBy> ownedByCrew1 = _localRelationshipQueryDAO.findRelationshipsV2("foo", filterUrn1, "crew", filterUrn,
        OwnedBy.class, new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
        -1, -1, new RelationshipLookUpContext());

    assertEquals(ownedByCrew1.size(), 1);
  }

  @Test(dataProvider = "schemaConfig")
  public void testFindOneRelationshipWithNonUrnFilterOnSourceEntityForCrewUsage(EbeanLocalDAO.SchemaConfig schemaConfig) throws Exception {
    // Find all owned-by relationship for crew.
    LocalRelationshipCriterion filterUrnCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(
        LocalRelationshipValue.create("urn:li:foo:4"), // 4 is crew as defined at very beginning.
        Condition.EQUAL,
        new AspectField());
    LocalRelationshipFilter filterUrn = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterUrnCriterion));

    LocalRelationshipCriterion filterUrnCriterion1 = EBeanDAOUtils.buildRelationshipFieldCriterion(
        LocalRelationshipValue.create("urn:li:foo:1"), // 1 is kafka as defined at very beginning.
        Condition.EQUAL,
        new UrnField());
    LocalRelationshipFilter filterUrn1 = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterUrnCriterion1));

    _localRelationshipQueryDAO.setSchemaConfig(schemaConfig);

    // non-mg entity cannot be filtered by non-urn filter. This will throw an exception.
    assertThrows(IllegalArgumentException.class, () -> {
      _localRelationshipQueryDAO.findRelationshipsV2("foo", filterUrn1, "crew", filterUrn,
          OwnedBy.class, new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
          -1, -1, new RelationshipLookUpContext());
    });
  }

  @Test(dataProvider = "schemaConfig")
  void testFindRelationshipsWithEntityUrnOffsetAndCount(EbeanLocalDAO.SchemaConfig schemaConfig) throws Exception {
    FooUrn alice = new FooUrn(1);
    FooUrn bob = new FooUrn(2);
    FooUrn jack = new FooUrn(3);
    FooUrn lisa = new FooUrn(4);
    FooUrn rose = new FooUrn(5);
    FooUrn jenny = new FooUrn(6);

    // Add Alice, Bob, Jack, Lisa, Rose, and Jenny into entity tables.
    if (schemaConfig == EbeanLocalDAO.SchemaConfig.NEW_SCHEMA_ONLY) {
      _fooUrnEBeanLocalAccess.add(alice, new AspectFoo().setValue("Alice"), AspectFoo.class, new AuditStamp(), null, false);
      _fooUrnEBeanLocalAccess.add(bob, new AspectFoo().setValue("Bob"), AspectFoo.class, new AuditStamp(), null, false);
      _fooUrnEBeanLocalAccess.add(jack, new AspectFoo().setValue("Jack"), AspectFoo.class, new AuditStamp(), null, false);
      _fooUrnEBeanLocalAccess.add(lisa, new AspectFoo().setValue("Lisa"), AspectFoo.class, new AuditStamp(), null, false);
      _fooUrnEBeanLocalAccess.add(rose, new AspectFoo().setValue("Rose"), AspectFoo.class, new AuditStamp(), null, false);
      _fooUrnEBeanLocalAccess.add(jenny, new AspectFoo().setValue("Jenny"), AspectFoo.class, new AuditStamp(), null, false);
    }

    // Add Bob reports-to ALice relationship
    ReportsTo bobReportsToAlice = new ReportsTo().setSource(bob).setDestination(alice);
    _localRelationshipWriterDAO.addRelationships(bob, AspectFoo.class, Collections.singletonList(bobReportsToAlice), false);

    // Add Jack reports-to ALice relationship
    ReportsTo jackReportsToAlice = new ReportsTo().setSource(jack).setDestination(alice);
    _localRelationshipWriterDAO.addRelationships(jack, AspectFoo.class, Collections.singletonList(jackReportsToAlice), false);

    // Add Lisa reports-to ALice relationship
    ReportsTo lisaReportsToAlice = new ReportsTo().setSource(lisa).setDestination(alice);
    _localRelationshipWriterDAO.addRelationships(lisa, AspectFoo.class, Collections.singletonList(lisaReportsToAlice), false);

    // Add Rose reports-to ALice relationship
    ReportsTo roseReportsToAlice = new ReportsTo().setSource(rose).setDestination(alice);
    _localRelationshipWriterDAO.addRelationships(rose, AspectFoo.class, Collections.singletonList(roseReportsToAlice), false);

    // Add Jenny reports-to ALice relationship
    ReportsTo jennyReportsToAlice = new ReportsTo().setSource(jenny).setDestination(alice);
    _localRelationshipWriterDAO.addRelationships(jenny, AspectFoo.class, Collections.singletonList(jennyReportsToAlice), false);

    // Find all reports-to relationship for Alice.
    LocalRelationshipFilter filter;
    if (schemaConfig == EbeanLocalDAO.SchemaConfig.OLD_SCHEMA_ONLY) {
      // old schema does not support non-urn field filters
      LocalRelationshipCriterion oldSchemaFilterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create(alice.toString()),
          Condition.EQUAL,
          new UrnField());
      filter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(oldSchemaFilterCriterion));
    } else {
      LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Alice"),
          Condition.EQUAL,
          new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
      filter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));
    }

    _localRelationshipQueryDAO.setSchemaConfig(schemaConfig);

    List<ReportsTo> reportsToAlice = _localRelationshipQueryDAO.findRelationshipsV2(
        null, null, "foo", filter,
        ReportsTo.class, new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
        -1, 3, new RelationshipLookUpContext());

    // Asserts only 3 reports-to relationships are returned
    assertEquals(reportsToAlice.size(), 3);

    reportsToAlice = _localRelationshipQueryDAO.findRelationshipsV2(
        null, null, "foo", filter,
        ReportsTo.class, new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
        2, 10, new RelationshipLookUpContext());

    // Asserts 3 returns, and the content starts from the 3rd report (Lisa)
    assertEquals(reportsToAlice.size(), 3);
    Set<FooUrn> actual = reportsToAlice.stream().map(reportsTo -> makeFooUrn(reportsTo.getSource().toString())).collect(Collectors.toSet());
    Set<FooUrn> expected = ImmutableSet.of(lisa, rose, jenny);
    assertEquals(actual, expected);

    reportsToAlice = _localRelationshipQueryDAO.findRelationshipsV2(
        null, null, "foo", filter,
        ReportsTo.class, new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
        2, -1, new RelationshipLookUpContext());

    // Asserts 5 returns, because offset cannot be applied when count isn't specified.
    assertEquals(reportsToAlice.size(), 5);
    actual = reportsToAlice.stream().map(reportsTo -> makeFooUrn(reportsTo.getSource().toString())).collect(Collectors.toSet());
    expected = ImmutableSet.of(bob, jack, lisa, rose, jenny);
    assertEquals(actual, expected);
  }

  @Test(dataProvider = "schemaConfig")
  public void testFindRelationshipsV3WithRelationshipV1(EbeanLocalDAO.SchemaConfig schemaConfig) throws URISyntaxException {
    FooUrn alice = new FooUrn(1);
    FooUrn bob = new FooUrn(2);

    // Add Alice, Bob and Jack into entity tables.
    if (schemaConfig == EbeanLocalDAO.SchemaConfig.NEW_SCHEMA_ONLY) {
      _fooUrnEBeanLocalAccess.add(alice, new AspectFoo().setValue("Alice"), AspectFoo.class, new AuditStamp(), null, false);
      _fooUrnEBeanLocalAccess.add(bob, new AspectFoo().setValue("Bob"), AspectFoo.class, new AuditStamp(), null, false);
    }

    // Add Bob reports-to ALice relationship
    ReportsTo bobReportsToAlice = new ReportsTo().setSource(bob).setDestination(alice);
    _localRelationshipWriterDAO.addRelationships(bob, AspectFoo.class, Collections.singletonList(bobReportsToAlice), false);

    // Find all reports-to relationship for Alice.
    LocalRelationshipFilter destFilter;
    if (schemaConfig == EbeanLocalDAO.SchemaConfig.OLD_SCHEMA_ONLY) {
      // old schema does not support non-urn field filters
      LocalRelationshipCriterion oldSchemaFilterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create(alice.toString()),
          Condition.EQUAL,
          new UrnField());
      destFilter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(oldSchemaFilterCriterion));
    } else {
      LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Alice"),
          Condition.EQUAL,
          new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
      destFilter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));
    }

    _localRelationshipQueryDAO.setSchemaConfig(schemaConfig);

    Map<String, Object> wrapOptions = new HashMap<>();
    wrapOptions.put(RELATIONSHIP_RETURN_TYPE, MG_INTERNAL_ASSET_RELATIONSHIP_TYPE);

    List<AssetRelationship> reportsToAlice = _localRelationshipQueryDAO.findRelationshipsV3(
        null, null, "foo", destFilter,
        ReportsTo.class, new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
        AssetRelationship.class, wrapOptions,
        -1, -1, new RelationshipLookUpContext());

    AssetRelationship expected = reportsToAlice.get(0);
    assertEquals(expected.getSource(), "urn:li:foo:2");

    ReportsTo expectedReportsTo = expected.getRelatedTo().getReportsTo();

    assertNotNull(expectedReportsTo);
    assertEquals(expectedReportsTo.getSource().toString(), "urn:li:foo:2");
    assertEquals(expectedReportsTo.getDestination().toString(), "urn:li:foo:1");
  }

  @Test(dataProvider = "schemaConfig")
  public void testFindRelationshipsV3WithRelationshipV2(EbeanLocalDAO.SchemaConfig schemaConfig) throws URISyntaxException {
    FooUrn owner = new FooUrn(1);
    FooUrn car = new FooUrn(2);

    // Add car and owner into entity tables.
    if (schemaConfig == EbeanLocalDAO.SchemaConfig.NEW_SCHEMA_ONLY) {
      _fooUrnEBeanLocalAccess.add(car, new AspectFoo().setValue("Car"), AspectFoo.class, new AuditStamp(), null, false);
      _fooUrnEBeanLocalAccess.add(owner, new AspectFoo().setValue("Owner"), AspectFoo.class, new AuditStamp(), null, false);
    }

    // Add car belongs-to owner relationship
    BelongsToV2 carBelongsToOwner = new BelongsToV2();
    carBelongsToOwner.setDestination(BelongsToV2.Destination.create(owner.toString()));
    _localRelationshipWriterDAO.addRelationships(car, AspectFoo.class, Collections.singletonList(carBelongsToOwner), false);

    // Find all belongs-to relationship for owner.
    LocalRelationshipFilter destFilter;
    if (schemaConfig == EbeanLocalDAO.SchemaConfig.OLD_SCHEMA_ONLY) {
      // old schema does not support non-urn field filters
      LocalRelationshipCriterion oldSchemaFilterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create(owner.toString()),
          Condition.EQUAL,
          new UrnField());
      destFilter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(oldSchemaFilterCriterion));
    } else {
      LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Owner"),
          Condition.EQUAL,
          new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
      destFilter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));
    }

    _localRelationshipQueryDAO.setSchemaConfig(schemaConfig);

    Map<String, Object> wrapOptions = new HashMap<>();
    wrapOptions.put(RELATIONSHIP_RETURN_TYPE, MG_INTERNAL_ASSET_RELATIONSHIP_TYPE);

    List<AssetRelationship> belongsToOwner = _localRelationshipQueryDAO.findRelationshipsV3(
        null, null, "foo", destFilter,
        BelongsToV2.class, new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
        AssetRelationship.class, wrapOptions,
        -1, -1, new RelationshipLookUpContext());

    AssetRelationship expected = belongsToOwner.get(0);
    assertEquals(expected.getSource(), "urn:li:foo:2");

    BelongsToV2 expectedBelongsToV2 = expected.getRelatedTo().getBelongsToV2();
    assertEquals(expectedBelongsToV2.getDestination().getString(), owner.toString());
  }

  @Test(dataProvider = "schemaConfig")
  public void testFindRelationshipsV3WithRelationshipV2WithHistory(EbeanLocalDAO.SchemaConfig schemaConfig) throws URISyntaxException {
    FooUrn owner = new FooUrn(1);
    FooUrn car = new FooUrn(2);

    // Add car and owner into entity tables.
    if (schemaConfig == EbeanLocalDAO.SchemaConfig.NEW_SCHEMA_ONLY) {
      _fooUrnEBeanLocalAccess.add(car, new AspectFoo().setValue("Car"), AspectFoo.class, new AuditStamp(), null, false);
      _fooUrnEBeanLocalAccess.add(owner, new AspectFoo().setValue("Owner"), AspectFoo.class, new AuditStamp(), null, false);
    }

    // Add car belongs-to owner relationship
    BelongsToV2 carBelongsToOwner = new BelongsToV2();
    carBelongsToOwner.setDestination(BelongsToV2.Destination.create(owner.toString()));
    _localRelationshipWriterDAO.addRelationships(car, AspectFoo.class, Collections.singletonList(carBelongsToOwner), false);

    // IMPORTANT: remove the relationship so that we can test history.
    _localRelationshipWriterDAO.removeRelationships(car, AspectFoo.class, Collections.singletonList(carBelongsToOwner));

    // Find all belongs-to relationship for owner.
    LocalRelationshipFilter destFilter;
    if (schemaConfig == EbeanLocalDAO.SchemaConfig.OLD_SCHEMA_ONLY) {
      // old schema does not support non-urn field filters
      LocalRelationshipCriterion oldSchemaFilterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create(owner.toString()),
          Condition.EQUAL,
          new UrnField());
      destFilter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(oldSchemaFilterCriterion));
    } else {
      LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Owner"),
          Condition.EQUAL,
          new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
      destFilter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));
    }

    _localRelationshipQueryDAO.setSchemaConfig(schemaConfig);

    Map<String, Object> wrapOptions = new HashMap<>();
    wrapOptions.put(RELATIONSHIP_RETURN_TYPE, MG_INTERNAL_ASSET_RELATIONSHIP_TYPE);

    List<AssetRelationship> belongsToOwner = _localRelationshipQueryDAO.findRelationshipsV3(
        null, null, "foo", destFilter,
        BelongsToV2.class, new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
        AssetRelationship.class, wrapOptions,
        -1, -1, new RelationshipLookUpContext(true));

    AssetRelationship expected = belongsToOwner.get(0);
    assertEquals(expected.getSource(), "urn:li:foo:2");

    BelongsToV2 expectedBelongsToV2 = expected.getRelatedTo().getBelongsToV2();
    assertEquals(expectedBelongsToV2.getDestination().getString(), owner.toString());
  }

  @Test
  public void testIsMgEntityType() throws Exception {
    // EbeanLocalRelationshipQueryDAOTest does not have the same package as EbeanLocalRelationshipQueryDAO (cant access protected method directly).
    Method isMgEntityTypeMethod = EbeanLocalRelationshipQueryDAO.class.getDeclaredMethod("isMgEntityType", String.class);
    isMgEntityTypeMethod.setAccessible(true);

    // assert foo is an MG entity (has metadata_entity_foo table in db)
    assertTrue((Boolean) isMgEntityTypeMethod.invoke(_localRelationshipQueryDAO, "foo"));

    // assert crew is not an MG entity (does not have metadata_entity_crew table in db)
    assertFalse((Boolean) isMgEntityTypeMethod.invoke(_localRelationshipQueryDAO, "crew"));
  }

  @Test
  public void testFindEntitiesOneHopAwayIncomingDirection() throws Exception {
    FooUrn alice = new FooUrn(1);
    FooUrn bob = new FooUrn(2);
    FooUrn jack = new FooUrn(3);

    // Add Alice, Bob and Jack into entity tables.
    _fooUrnEBeanLocalAccess.add(alice, new AspectFoo().setValue("Alice"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(bob, new AspectFoo().setValue("Bob"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(jack, new AspectFoo().setValue("Jack"), AspectFoo.class, new AuditStamp(), null, false);

    // Add Bob reports-to Alice relationship
    ReportsTo bobReportsToAlice = new ReportsTo().setSource(bob).setDestination(alice);
    _localRelationshipWriterDAO.addRelationships(bob, AspectFoo.class, Collections.singletonList(bobReportsToAlice), false);

    // Add Jack reports-to Alice relationship
    ReportsTo jackReportsToAlice = new ReportsTo().setSource(jack).setDestination(alice);
    _localRelationshipWriterDAO.addRelationships(jack, AspectFoo.class, Collections.singletonList(jackReportsToAlice), false);

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
    _fooUrnEBeanLocalAccess.add(alice, new AspectFoo().setValue("Alice"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(bob, new AspectFoo().setValue("Bob"), AspectFoo.class, new AuditStamp(), null, false);

    // Add Stanford and MIT into entity tables.
    _barUrnEBeanLocalAccess.add(stanford, new AspectFoo().setValue("Stanford"), AspectFoo.class, new AuditStamp(), null, false);
    _barUrnEBeanLocalAccess.add(mit, new AspectFoo().setValue("MIT"), AspectFoo.class, new AuditStamp(), null, false);

    // Add Alice belongs to MIT and Stanford.
    BelongsTo aliceBelongsToMit = new BelongsTo().setSource(alice).setDestination(mit);
    BelongsTo aliceBelongsToStanford = new BelongsTo().setSource(alice).setDestination(stanford);
    _localRelationshipWriterDAO.addRelationships(alice, AspectFoo.class, ImmutableList.of(aliceBelongsToStanford, aliceBelongsToMit), false);

    // Add Bob belongs to Stanford.
    BelongsTo bobBelongsToStandford = new BelongsTo().setSource(bob).setDestination(stanford);
    _localRelationshipWriterDAO.addRelationships(bob, AspectFoo.class, Collections.singletonList(bobBelongsToStandford), false);

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
    _fooUrnEBeanLocalAccess.add(alice, new AspectFoo().setValue("Alice"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(bob, new AspectFoo().setValue("Bob"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(jack, new AspectFoo().setValue("Jack"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(john, new AspectFoo().setValue("John"), AspectFoo.class, new AuditStamp(), null, false);

    _fooUrnEBeanLocalAccess.add(alice, new AspectBar().setValue("32"), AspectBar.class, new AuditStamp(), null, false); // Alice 32 years old

    _fooUrnEBeanLocalAccess.add(bob, new AspectBar().setValue("52"), AspectBar.class, new AuditStamp(), null, false); // Bob 52 years old

    _fooUrnEBeanLocalAccess.add(jack, new AspectBar().setValue("16"), AspectBar.class, new AuditStamp(), null, false); // Jack 16 years old

    _fooUrnEBeanLocalAccess.add(john, new AspectBar().setValue("42"), AspectBar.class, new AuditStamp(), null, false); // John 42 years old

    // Add Alice pair-with Jack relationships. Alice --> Jack.
    PairsWith alicePairsWithJack = new PairsWith().setSource(alice).setDestination(jack);
    _localRelationshipWriterDAO.addRelationships(alice, AspectFoo.class, Collections.singletonList(alicePairsWithJack), false);

    // Add Bob pair-with Alice relationships. Bob --> Alice.
    PairsWith bobPairsWithAlice = new PairsWith().setSource(bob).setDestination(alice);
    _localRelationshipWriterDAO.addRelationships(bob, AspectFoo.class, Collections.singletonList(bobPairsWithAlice), false);

    // Add Alice pair-with John relationships. Alice --> John.
    PairsWith alicePairsWithJohn = new PairsWith().setSource(alice).setDestination(john);
    _localRelationshipWriterDAO.addRelationships(alice, AspectFoo.class, Collections.singletonList(alicePairsWithJohn), false);

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
  public void testFindOneEntityWithInCondition() throws URISyntaxException, OperationNotSupportedException {
    // Ingest data
    _fooUrnEBeanLocalAccess.add(new FooUrn(1), new AspectFoo().setValue("foo"), AspectFoo.class, new AuditStamp(), null, false);

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
  public void testFindNoEntityWithInCondition() throws URISyntaxException, OperationNotSupportedException {
    // Ingest data
    _fooUrnEBeanLocalAccess.add(new FooUrn(1), new AspectFoo().setValue("foo"), AspectFoo.class, new AuditStamp(), null, false);

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
    _fooUrnEBeanLocalAccess.add(new FooUrn(1), new AspectFoo().setValue("foo"), AspectFoo.class, new AuditStamp(), null, false);

    // Create empty filter
    LocalRelationshipFilter emptyFilter = new LocalRelationshipFilter();

    try {
      _localRelationshipQueryDAO.findEntities(FooSnapshot.class, emptyFilter, FooSnapshot.class, emptyFilter, PairsWith.class, emptyFilter, 1, 1, 0, 10);
    } catch (Exception ex) {
      assertTrue(ex instanceof IllegalArgumentException);
      assertEquals(ex.getMessage(), "Relationship direction cannot be null or UNKNOWN.");
    }
  }

  @Test
  public void testFindEntitiesWithSingleInCondition() throws OperationNotSupportedException, URISyntaxException {
    // Added 20 FooUrn entities with aspect AspectFoo and value "foo1" to "foo20"
    for (int i = 1; i <= 20; i++) {
      _fooUrnEBeanLocalAccess.add(new FooUrn(i), new AspectFoo().setValue("foo" + i), AspectFoo.class, new AuditStamp(),
          null, false);
    }

    // Created one more FooUrn entity with aspect AspectBar and value "bar" and AspectFoo with value "foo5"
    FooUrn one = new FooUrn(21);
    _fooUrnEBeanLocalAccess.add(one, new AspectFoo().setValue("foo5"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(one, new AspectBar().setValue("bar"), AspectBar.class, new AuditStamp(), null, false);

    // Prepare the filter values for AspectFoo
    List<String> values = Arrays.asList("foo1", "foo2", "foo3", "foo4", "foo5");

    // Create a single criterion with all values in one IN clause for AspectFoo
    LocalRelationshipCriterion filterCriterion =
        EBeanDAOUtils.buildRelationshipFieldCriterion(
            LocalRelationshipValue.create(new StringArray(values)),
            Condition.IN,
            new AspectField()
                .setAspect(AspectFoo.class.getCanonicalName())
                .setPath("/value")
        );

    // Create the EQUAL criterion for AspectBar
    LocalRelationshipCriterion filterCriterion1 = EBeanDAOUtils.buildRelationshipFieldCriterion(
        LocalRelationshipValue.create("bar"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectBar.class.getCanonicalName()).setPath("/value")
    );

    LocalRelationshipFilter filter = new LocalRelationshipFilter();
    filter.setCriteria(new LocalRelationshipCriterionArray(Arrays.asList(filterCriterion, filterCriterion1)));

    // Retrieve entities (limit to 100 results for testing)
    List<FooSnapshot> fooSnapshotList = _localRelationshipQueryDAO.findEntities(FooSnapshot.class, filter, 0, 100);

    // Assertions
    assertEquals(fooSnapshotList.size(), 1); // Only one entity should match the criteria
  }


  /**
   * Same as {@link #testFindEntitiesWithSingleInCondition} but with multiple IN conditions.
   */
  @Test
  public void testFindEntitiesWithMultipleInConditions()
      throws OperationNotSupportedException, URISyntaxException, NoSuchFieldException, IllegalAccessException {
    // Added 20 FooUrn entities with aspect AspectFoo and value "foo1" to "foo20"
    for (int i = 1; i <= 20; i++) {
      _fooUrnEBeanLocalAccess.add(new FooUrn(i), new AspectFoo().setValue("foo" + i), AspectFoo.class, new AuditStamp(),
          null, false);
    }
    // Created one more FooUrn entity with aspect AspectBar and value "bar" and AspectFoo with value "foo5"
    FooUrn one = new FooUrn(21);
    _fooUrnEBeanLocalAccess.add(one, new AspectFoo().setValue("foo5"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(one, new AspectBar().setValue("bar"), AspectBar.class, new AuditStamp(), null, false);


    List<LocalRelationshipCriterion> criteriaList = new ArrayList<>();
    for (int i = 1; i <= 5; i++) {
      LocalRelationshipCriterion filterCriterion =
          EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create(new StringArray("foo" + i)),
              Condition.IN, new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
      criteriaList.add(filterCriterion);
    }
    // Create the EQUAL criterion for AspectBar
    criteriaList.add(EBeanDAOUtils.buildRelationshipFieldCriterion(
        LocalRelationshipValue.create("bar"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectBar.class.getCanonicalName()).setPath("/value")
    ));

    LocalRelationshipFilter filter = new LocalRelationshipFilter();
    filter.setCriteria(new LocalRelationshipCriterionArray(criteriaList));

    // Retrieve entities (limit to 100 results for testing)
    List<FooSnapshot> fooSnapshotList = _localRelationshipQueryDAO.findEntities(FooSnapshot.class, filter, 0, 100);

    // Assertions
    assertEquals(fooSnapshotList.size(), 1); // Only one entity should match the criteria
  }

  @Test
  public void testBuildFindRelationshipSQL() {
    String sql = _localRelationshipQueryDAO.buildFindRelationshipSQL("relationship_table_name",
        new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
        "source_table_name", null, "destination_table_name", null,
        -1, -1, new RelationshipLookUpContext());

    assertEquals(sql,
        "SELECT rt.* FROM relationship_table_name rt INNER JOIN destination_table_name dt ON dt.urn=rt.destination "
        + "INNER JOIN source_table_name st ON st.urn=rt.source WHERE rt.deleted_ts is NULL");
  }

  @Test
  public void testBuildFindRelationshipSQLWithSource() {
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Alice"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
    LocalRelationshipFilter srcFilter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));

    String sql = _localRelationshipQueryDAO.buildFindRelationshipSQL("relationship_table_name",
        new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
        "source_table_name", srcFilter, "destination_table_name", null,
        -1, -1, new RelationshipLookUpContext());

    assertEquals(sql,
        "SELECT rt.* FROM relationship_table_name rt INNER JOIN destination_table_name dt ON dt.urn=rt.destination "
            + "INNER JOIN source_table_name st ON st.urn=rt.source WHERE rt.deleted_ts is NULL AND st.i_aspectfoo"
            + (_eBeanDAOConfig.isNonDollarVirtualColumnsEnabled() ? "0" : "$") + "value='Alice'");
  }

  @Test
  public void testBuildFindRelationshipSQLWithDestination() {
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Alice"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
    LocalRelationshipFilter destFilter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));

    String sql = _localRelationshipQueryDAO.buildFindRelationshipSQL("relationship_table_name",
        new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
        "source_table_name", null, "destination_table_name", destFilter,
        -1, -1, new RelationshipLookUpContext());

    assertEquals(sql,
        "SELECT rt.* FROM relationship_table_name rt INNER JOIN destination_table_name dt ON dt.urn=rt.destination "
            + "INNER JOIN source_table_name st ON st.urn=rt.source WHERE rt.deleted_ts is NULL AND dt.i_aspectfoo"
            + (_eBeanDAOConfig.isNonDollarVirtualColumnsEnabled() ? "0" : "$") + "value='Alice'");
  }

  @Test
  public void testBuildFindRelationshipSQLWithSourceAndDestination() {
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Alice"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
    LocalRelationshipFilter srcFilter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));

    LocalRelationshipCriterion filterCriterion2 = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Bob"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
    LocalRelationshipFilter destFilter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion2));

    String sql = _localRelationshipQueryDAO.buildFindRelationshipSQL("relationship_table_name",
        new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
        "source_table_name", srcFilter, "destination_table_name", destFilter,
        -1, -1, new RelationshipLookUpContext());

    char virtualColumnDelimiter = _eBeanDAOConfig.isNonDollarVirtualColumnsEnabled() ? '0' : '$';
    assertEquals(sql,
        "SELECT rt.* FROM relationship_table_name rt INNER JOIN destination_table_name dt ON dt.urn=rt.destination "
            + "INNER JOIN source_table_name st ON st.urn=rt.source WHERE rt.deleted_ts is NULL AND (dt.i_aspectfoo"
            + virtualColumnDelimiter + "value='Bob') AND (st.i_aspectfoo" + virtualColumnDelimiter + "value='Alice')");
  }

  @Test
  public void testBuildFindRelationshipSQLWithHistory() {
    String sql = _localRelationshipQueryDAO.buildFindRelationshipSQL("relationship_table_name",
        new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
        "source_table_name", null, "destination_table_name", null,
        -1, -1, new RelationshipLookUpContext(true));

    assertEquals(sql,
        "SELECT * FROM ("
            + "SELECT rt.*, ROW_NUMBER() OVER (PARTITION BY rt.source, rt.destination ORDER BY rt.lastmodifiedon DESC) AS row_num "
            + "FROM relationship_table_name rt INNER JOIN destination_table_name dt ON dt.urn=rt.destination "
            + "INNER JOIN source_table_name st ON st.urn=rt.source ) ranked_rows WHERE row_num = 1");
  }

  @Test
  public void testBuildFindRelationshipSQLWithHistoryWithRelationshipWithSubtype() {
    String sql = _localRelationshipQueryDAO.buildFindRelationshipSQL("metadata_relationship_belongstov2",
        new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
        "source_table_name", null, "destination_table_name", null,
        -1, -1, new RelationshipLookUpContext(true));

    assertEquals(sql,
        "SELECT * FROM ("
            + "SELECT rt.*, ROW_NUMBER() OVER (PARTITION BY rt.source"
            + (", rt.metadata" + (_eBeanDAOConfig.isNonDollarVirtualColumnsEnabled() ? "0" : "$") + "type")
            + ", rt.destination ORDER BY rt.lastmodifiedon DESC) AS row_num "
            + "FROM metadata_relationship_belongstov2 rt INNER JOIN destination_table_name dt ON dt.urn=rt.destination "
            + "INNER JOIN source_table_name st ON st.urn=rt.source ) ranked_rows WHERE row_num = 1");
  }

  @Test
  public void testBuildFindRelationshipSQLWithHistoryWithSource() {
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Alice"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
    LocalRelationshipFilter srcFilter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));

    String sql = _localRelationshipQueryDAO.buildFindRelationshipSQL("relationship_table_name",
        new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
        "source_table_name", srcFilter, "destination_table_name", null,
        -1, -1, new RelationshipLookUpContext(true));

    assertEquals(sql,
        "SELECT * FROM ("
            + "SELECT rt.*, ROW_NUMBER() OVER (PARTITION BY rt.source, rt.destination ORDER BY rt.lastmodifiedon DESC) AS row_num "
            + "FROM relationship_table_name rt INNER JOIN destination_table_name dt ON dt.urn=rt.destination "
            + "INNER JOIN source_table_name st ON st.urn=rt.source  WHERE st.i_aspectfoo"
            + (_eBeanDAOConfig.isNonDollarVirtualColumnsEnabled() ? "0" : "$") + "value='Alice') ranked_rows WHERE row_num = 1");
  }

  @Test
  public void testBuildFindRelationshipSQLWithHistoryWithDestination() {
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Alice"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
    LocalRelationshipFilter destFilter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));

    String sql = _localRelationshipQueryDAO.buildFindRelationshipSQL("relationship_table_name",
        new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
        "source_table_name", null, "destination_table_name", destFilter,
        -1, -1, new RelationshipLookUpContext(true));

    assertEquals(sql,
        "SELECT * FROM ("
            + "SELECT rt.*, ROW_NUMBER() OVER (PARTITION BY rt.source, rt.destination ORDER BY rt.lastmodifiedon DESC) AS row_num "
            + "FROM relationship_table_name rt INNER JOIN destination_table_name dt ON dt.urn=rt.destination "
            + "INNER JOIN source_table_name st ON st.urn=rt.source  WHERE dt.i_aspectfoo"
            + (_eBeanDAOConfig.isNonDollarVirtualColumnsEnabled() ? "0" : "$") + "value='Alice') ranked_rows WHERE row_num = 1");
  }

  @Test
  public void testBuildFindRelationshipSQLWithHistoryWithSourceAndDestination() {
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(
            LocalRelationshipValue.create("urn:li:foo:4"),
            Condition.EQUAL,
            new UrnField());
    LocalRelationshipFilter srcFilter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));

    LocalRelationshipCriterion filterCriterion2 = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Bob"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
    LocalRelationshipFilter destFilter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion2));

    String sql = _localRelationshipQueryDAO.buildFindRelationshipSQL("relationship_table_name",
        new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
        "source_table_name", srcFilter, "destination_table_name", destFilter,
        -1, -1, new RelationshipLookUpContext(true));

    char virtualColumnDelimiter = _eBeanDAOConfig.isNonDollarVirtualColumnsEnabled() ? '0' : '$';

    assertEquals(sql,
        "SELECT * FROM ("
            + "SELECT rt.*, ROW_NUMBER() OVER (PARTITION BY rt.source, rt.destination ORDER BY rt.lastmodifiedon DESC) AS row_num "
            + "FROM relationship_table_name rt INNER JOIN destination_table_name dt ON dt.urn=rt.destination "
            + "INNER JOIN source_table_name st ON st.urn=rt.source  WHERE (dt.i_aspectfoo" + virtualColumnDelimiter
            + "value='Bob') AND (st.urn='urn:li:foo:4')) ranked_rows WHERE row_num = 1");
  }
}