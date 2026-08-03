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
import com.linkedin.metadata.dao.RelationshipKeysetCursor;
import com.linkedin.metadata.dao.RelationshipKeysetPage;
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
import javax.persistence.PersistenceException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;
import pegasus.com.linkedin.metadata.query.LogicalExpressionLocalRelationshipCriterion;
import pegasus.com.linkedin.metadata.query.LogicalExpressionLocalRelationshipCriterionArray;
import pegasus.com.linkedin.metadata.query.LogicalOperation;
import pegasus.com.linkedin.metadata.query.innerLogicalOperation.Operator;

import static com.linkedin.metadata.dao.EbeanLocalRelationshipQueryDAO.*;
import static com.linkedin.metadata.dao.utils.LogicalExpressionLocalRelationshipCriterionUtils.*;
import static com.linkedin.testing.TestUtils.*;
import static org.mockito.Mockito.*;
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

  @Test
  public void testFindEntitiesV2WithV1Filter() throws URISyntaxException, OperationNotSupportedException {
    // Ingest data
    _fooUrnEBeanLocalAccess.add(new FooUrn(1), new AspectFoo().setValue("foo"), AspectFoo.class, new AuditStamp(), null, false);

    // Prepare filter
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("foo"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));

    LocalRelationshipFilter filter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));

    assertThrows(IllegalArgumentException.class, () -> _localRelationshipQueryDAO.findEntitiesV2(FooSnapshot.class, filter, 0, 10));
  }

  @Test
  public void testFindOneEntityV2() throws URISyntaxException, OperationNotSupportedException {
    // Ingest data
    _fooUrnEBeanLocalAccess.add(new FooUrn(1), new AspectFoo().setValue("foo"), AspectFoo.class, new AuditStamp(), null, false);

    // Prepare filter
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("foo"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));

    LocalRelationshipFilter filter = new LocalRelationshipFilter().setLogicalExpressionCriteria(
        wrapCriterionAsLogicalExpression(filterCriterion));
    List<FooSnapshot> fooSnapshotList = _localRelationshipQueryDAO.findEntitiesV2(FooSnapshot.class, filter, 0, 10);

    assertEquals(fooSnapshotList.size(), 1);
    assertEquals(fooSnapshotList.get(0).getAspects().size(), 1);
    assertEquals(fooSnapshotList.get(0).getAspects().get(0).getAspectFoo(), new AspectFoo().setValue("foo"));
  }

  @Test
  public void testFindOneEntityV2WithStartWithCondition() throws URISyntaxException, OperationNotSupportedException {
    // Ingest data
    _fooUrnEBeanLocalAccess.add(new FooUrn(1), new AspectFoo().setValue("foo"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(new FooUrn(2), new AspectFoo().setValue("fooTwo"), AspectFoo.class, new AuditStamp(), null, false);

    // Prepare filter
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("foo"),
        Condition.START_WITH,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));

    LocalRelationshipFilter filter = new LocalRelationshipFilter().setLogicalExpressionCriteria(
        wrapCriterionAsLogicalExpression(filterCriterion));
    List<FooSnapshot> fooSnapshotList = _localRelationshipQueryDAO.findEntitiesV2(FooSnapshot.class, filter, 0, 10);

    assertEquals(fooSnapshotList.size(), 2);
    assertEquals(fooSnapshotList.get(0).getAspects().size(), 1);
    assertEquals(fooSnapshotList.get(0).getAspects().get(0).getAspectFoo(), new AspectFoo().setValue("foo"));

    assertEquals(fooSnapshotList.get(1).getAspects().size(), 1);
    assertEquals(fooSnapshotList.get(1).getAspects().get(0).getAspectFoo(), new AspectFoo().setValue("fooTwo"));

    // Prepare filter
    LocalRelationshipCriterion filterCriterion2 = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("fooT"),
        Condition.START_WITH,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));

    LocalRelationshipFilter filter2 = new LocalRelationshipFilter().setLogicalExpressionCriteria(
        wrapCriterionAsLogicalExpression(filterCriterion2));
    List<FooSnapshot> fooSnapshotList2 = _localRelationshipQueryDAO.findEntitiesV2(FooSnapshot.class, filter2, 0, 10);

    assertEquals(fooSnapshotList2.size(), 1);
    assertEquals(fooSnapshotList2.get(0).getAspects().size(), 1);
    assertEquals(fooSnapshotList2.get(0).getAspects().get(0).getAspectFoo(), new AspectFoo().setValue("fooTwo"));
  }

  @Test
  public void testFindOneEntityTwoAspectsV2() throws URISyntaxException, OperationNotSupportedException {
    // Ingest data
    _fooUrnEBeanLocalAccess.add(new FooUrn(1), new AspectFoo().setValue("foo"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(new FooUrn(1), new AspectBar().setValue("bar"), AspectBar.class, new AuditStamp(), null, false);

    // Prepare filter
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("foo"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));

    LocalRelationshipFilter filter = new LocalRelationshipFilter().setLogicalExpressionCriteria(
        wrapCriterionAsLogicalExpression(filterCriterion));

    List<FooSnapshot> fooSnapshotList = _localRelationshipQueryDAO.findEntitiesV2(FooSnapshot.class, filter, 0, 10);

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

  @Test(dataProvider = "schemaConfig")
  public void testFindRelationshipsV4WithNullFilter(EbeanLocalDAO.SchemaConfig schemaConfig) throws URISyntaxException {
    FooUrn owner = new FooUrn(1);
    FooUrn owner2 = new FooUrn(3);
    FooUrn car = new FooUrn(2);
    FooUrn car2 = new FooUrn(4);

    // Add car and owner into entity tables.
    if (schemaConfig == EbeanLocalDAO.SchemaConfig.NEW_SCHEMA_ONLY) {
      _fooUrnEBeanLocalAccess.add(car, new AspectFoo().setValue("Car"), AspectFoo.class, new AuditStamp(), null, false);
      _fooUrnEBeanLocalAccess.add(car2, new AspectFoo().setValue("Car2"), AspectFoo.class, new AuditStamp(), null, false);
      _fooUrnEBeanLocalAccess.add(owner, new AspectFoo().setValue("Owner"), AspectFoo.class, new AuditStamp(), null, false);
      _fooUrnEBeanLocalAccess.add(owner2, new AspectFoo().setValue("Owner2"), AspectFoo.class, new AuditStamp(), null, false);
    }

    // Add car belongs-to owner relationship
    BelongsToV2 carBelongsToOwner = new BelongsToV2();
    carBelongsToOwner.setDestination(BelongsToV2.Destination.create(owner.toString()));
    _localRelationshipWriterDAO.addRelationships(car, AspectFoo.class, Collections.singletonList(carBelongsToOwner), false);

    // Add car belongs-to owner2 relationship
    BelongsToV2 carBelongsToOwner2 = new BelongsToV2();
    carBelongsToOwner2.setDestination(BelongsToV2.Destination.create(owner2.toString()));
    _localRelationshipWriterDAO.addRelationships(car2, AspectFoo.class, Collections.singletonList(carBelongsToOwner2), false);

    _localRelationshipQueryDAO.setSchemaConfig(schemaConfig);

    Map<String, Object> wrapOptions = new HashMap<>();
    wrapOptions.put(RELATIONSHIP_RETURN_TYPE, MG_INTERNAL_ASSET_RELATIONSHIP_TYPE);

    List<AssetRelationship> belongsToOwner = _localRelationshipQueryDAO.findRelationshipsV4(
        null, null, null, null,
        BelongsToV2.class, new LocalRelationshipFilter().setDirection(RelationshipDirection.UNDIRECTED),
        AssetRelationship.class, wrapOptions,
        -1, -1, new RelationshipLookUpContext());

    assertEquals(belongsToOwner.size(), 2);
  }

  @Test(dataProvider = "schemaConfig")
  public void testFindRelationshipsV4WithOneCriterion(EbeanLocalDAO.SchemaConfig schemaConfig) throws URISyntaxException {
    FooUrn owner = new FooUrn(1);
    FooUrn owner2 = new FooUrn(3);
    FooUrn car = new FooUrn(2);
    FooUrn car2 = new FooUrn(4);

    // Add car and owner into entity tables.
    if (schemaConfig == EbeanLocalDAO.SchemaConfig.NEW_SCHEMA_ONLY) {
      _fooUrnEBeanLocalAccess.add(car, new AspectFoo().setValue("Car"), AspectFoo.class, new AuditStamp(), null, false);
      _fooUrnEBeanLocalAccess.add(car2, new AspectFoo().setValue("Car2"), AspectFoo.class, new AuditStamp(), null, false);
      _fooUrnEBeanLocalAccess.add(owner, new AspectFoo().setValue("Owner"), AspectFoo.class, new AuditStamp(), null, false);
      _fooUrnEBeanLocalAccess.add(owner2, new AspectFoo().setValue("Owner2"), AspectFoo.class, new AuditStamp(), null, false);
    }

    // Add car belongs-to owner relationship
    BelongsToV2 carBelongsToOwner = new BelongsToV2();
    carBelongsToOwner.setDestination(BelongsToV2.Destination.create(owner.toString()));
    _localRelationshipWriterDAO.addRelationships(car, AspectFoo.class, Collections.singletonList(carBelongsToOwner), false);

    // Add car belongs-to owner2 relationship
    BelongsToV2 carBelongsToOwner2 = new BelongsToV2();
    carBelongsToOwner2.setDestination(BelongsToV2.Destination.create(owner2.toString()));
    _localRelationshipWriterDAO.addRelationships(car2, AspectFoo.class, Collections.singletonList(carBelongsToOwner2), false);

    // Find all belongs-to relationship for owner.
    LocalRelationshipFilter destFilter;
    if (schemaConfig == EbeanLocalDAO.SchemaConfig.OLD_SCHEMA_ONLY) {
      // old schema does not support non-urn field filters, and only support one urn filter.
      LocalRelationshipCriterion oldSchemaFilterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create(owner.toString()),
          Condition.EQUAL,
          new UrnField());
      LogicalExpressionLocalRelationshipCriterion logicalExpressionCriterion = wrapCriterionAsLogicalExpression(oldSchemaFilterCriterion);

      destFilter = new LocalRelationshipFilter().setLogicalExpressionCriteria(logicalExpressionCriterion);
    } else {
      LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Owner"),
          Condition.EQUAL,
          new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
      LogicalExpressionLocalRelationshipCriterion logicalExpressionCriterion = wrapCriterionAsLogicalExpression(filterCriterion);

      destFilter = new LocalRelationshipFilter().setLogicalExpressionCriteria(logicalExpressionCriterion);
    }

    _localRelationshipQueryDAO.setSchemaConfig(schemaConfig);

    Map<String, Object> wrapOptions = new HashMap<>();
    wrapOptions.put(RELATIONSHIP_RETURN_TYPE, MG_INTERNAL_ASSET_RELATIONSHIP_TYPE);

    List<AssetRelationship> belongsToOwner = _localRelationshipQueryDAO.findRelationshipsV4(
        null, null, "foo", destFilter,
        BelongsToV2.class, new LocalRelationshipFilter().setLogicalExpressionCriteria(
            new LogicalExpressionLocalRelationshipCriterion()).setDirection(RelationshipDirection.UNDIRECTED),
        AssetRelationship.class, wrapOptions,
        -1, -1, new RelationshipLookUpContext());

    assertEquals(belongsToOwner.size(), 1);

    AssetRelationship actual1 = belongsToOwner.get(0);
    assertEquals(actual1.getSource(), "urn:li:foo:2");

    BelongsToV2 actual1BelongsToV2 = actual1.getRelatedTo().getBelongsToV2();
    assertEquals(actual1BelongsToV2.getDestination().getString(), owner.toString());
  }

  /**
   * Old schema does not support multiple criteria in the same filter. Skipped in this test.
   */
  @Test
  public void testFindRelationshipsV4WithTwoOrCriterion() throws URISyntaxException {
    FooUrn owner = new FooUrn(1);
    FooUrn owner2 = new FooUrn(3);
    FooUrn car = new FooUrn(2);
    FooUrn car2 = new FooUrn(4);

    // Add car and owner into entity tables.
    _fooUrnEBeanLocalAccess.add(car, new AspectFoo().setValue("Car"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(car2, new AspectFoo().setValue("Car2"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(owner, new AspectFoo().setValue("Owner"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(owner2, new AspectFoo().setValue("Owner2"), AspectFoo.class, new AuditStamp(), null, false);

    // Add car belongs-to owner relationship
    BelongsToV2 carBelongsToOwner = new BelongsToV2();
    carBelongsToOwner.setDestination(BelongsToV2.Destination.create(owner.toString()));
    _localRelationshipWriterDAO.addRelationships(car, AspectFoo.class, Collections.singletonList(carBelongsToOwner), false);

    // Add car belongs-to owner2 relationship
    BelongsToV2 carBelongsToOwner2 = new BelongsToV2();
    carBelongsToOwner2.setDestination(BelongsToV2.Destination.create(owner2.toString()));
    _localRelationshipWriterDAO.addRelationships(car2, AspectFoo.class, Collections.singletonList(carBelongsToOwner2), false);

    // Find all belongs-to relationship for owner.
    // criterion 1: owner = "Owner"
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Owner"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
    LogicalExpressionLocalRelationshipCriterion logicalExpressionCriterion = wrapCriterionAsLogicalExpression(filterCriterion);

    // criterion 2: owner = "Owner2"
    LocalRelationshipCriterion filterCriterion2 = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Owner2"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
    LogicalExpressionLocalRelationshipCriterion logicalExpressionCriterion2 = wrapCriterionAsLogicalExpression(filterCriterion2);

    LogicalExpressionLocalRelationshipCriterionArray array = new LogicalExpressionLocalRelationshipCriterionArray();
    array.add(logicalExpressionCriterion);
    array.add(logicalExpressionCriterion2);

    // or criterion: (owner = "Owner" OR owner = "Owner2")
    LogicalExpressionLocalRelationshipCriterion localRelationshipCriterion = buildLogicalGroup(Operator.OR, array);

    // Filter: (owner = "Owner" OR owner = "Owner2")
    LocalRelationshipFilter destFilter = new LocalRelationshipFilter().setLogicalExpressionCriteria(localRelationshipCriterion);

    Map<String, Object> wrapOptions = new HashMap<>();
    wrapOptions.put(RELATIONSHIP_RETURN_TYPE, MG_INTERNAL_ASSET_RELATIONSHIP_TYPE);

    List<AssetRelationship> belongsToOwner = _localRelationshipQueryDAO.findRelationshipsV4(
        null, null, "foo", destFilter,
        BelongsToV2.class, new LocalRelationshipFilter().setLogicalExpressionCriteria(
            new LogicalExpressionLocalRelationshipCriterion()).setDirection(RelationshipDirection.UNDIRECTED),
        AssetRelationship.class, wrapOptions,
        -1, -1, new RelationshipLookUpContext());

    assertEquals(belongsToOwner.size(), 2);

    AssetRelationship actual1 = belongsToOwner.get(0);
    assertEquals(actual1.getSource(), "urn:li:foo:2");

    BelongsToV2 actual1BelongsToV2 = actual1.getRelatedTo().getBelongsToV2();
    assertEquals(actual1BelongsToV2.getDestination().getString(), owner.toString());

    AssetRelationship actual2 = belongsToOwner.get(1);
    assertEquals(actual2.getSource(), "urn:li:foo:4");

    BelongsToV2 actual2BelongsToV2 = actual2.getRelatedTo().getBelongsToV2();
    assertEquals(actual2BelongsToV2.getDestination().getString(), owner2.toString());
  }

  /**
   * Old schema does not support multiple criteria in the same filter. Skipped in this test.
   */
  @Test
  public void testFindRelationshipsV4WithTwoAndCriterion() throws URISyntaxException {
    FooUrn owner = new FooUrn(1);
    FooUrn owner2 = new FooUrn(3);
    FooUrn car = new FooUrn(2);
    FooUrn car2 = new FooUrn(4);

    // Add car and owner into entity tables.
    _fooUrnEBeanLocalAccess.add(car, new AspectFoo().setValue("Car"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(car, new AspectBar().setValue("Bike"), AspectBar.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(car2, new AspectFoo().setValue("Car"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(owner, new AspectFoo().setValue("Owner"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(owner2, new AspectFoo().setValue("Owner2"), AspectFoo.class, new AuditStamp(), null, false);

    // Add car belongs-to owner relationship
    BelongsToV2 carBelongsToOwner = new BelongsToV2();
    carBelongsToOwner.setDestination(BelongsToV2.Destination.create(owner.toString()));
    _localRelationshipWriterDAO.addRelationships(car, AspectFoo.class, Collections.singletonList(carBelongsToOwner), false);

    // Add car belongs-to owner2 relationship
    BelongsToV2 carBelongsToOwner2 = new BelongsToV2();
    carBelongsToOwner2.setDestination(BelongsToV2.Destination.create(owner2.toString()));
    _localRelationshipWriterDAO.addRelationships(car2, AspectFoo.class, Collections.singletonList(carBelongsToOwner2), false);

    // Find all belongs-to relationship for owner.
    // criterion 1: foo = "Car"
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Car"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
    LogicalExpressionLocalRelationshipCriterion logicalExpressionCriterion = wrapCriterionAsLogicalExpression(filterCriterion);

    // criterion 2: bar = "Bike"
    LocalRelationshipCriterion filterCriterion2 = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Bike"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectBar.class.getCanonicalName()).setPath("/value"));
    LogicalExpressionLocalRelationshipCriterion logicalExpressionCriterion2 = wrapCriterionAsLogicalExpression(filterCriterion2);

    LogicalExpressionLocalRelationshipCriterionArray array = new LogicalExpressionLocalRelationshipCriterionArray();
    array.add(logicalExpressionCriterion);
    array.add(logicalExpressionCriterion2);

    // and criterion: (foo = "Car" AND bar = "Bike")
    LogicalExpressionLocalRelationshipCriterion localRelationshipCriterion = buildLogicalGroup(Operator.AND, array);

    // Filter: (foo = "Car" AND bar = "Bike")
    LocalRelationshipFilter srcFilter = new LocalRelationshipFilter().setLogicalExpressionCriteria(localRelationshipCriterion);

    Map<String, Object> wrapOptions = new HashMap<>();
    wrapOptions.put(RELATIONSHIP_RETURN_TYPE, MG_INTERNAL_ASSET_RELATIONSHIP_TYPE);

    List<AssetRelationship> belongsToOwner = _localRelationshipQueryDAO.findRelationshipsV4(
        "foo", srcFilter, null, null,
        BelongsToV2.class, new LocalRelationshipFilter().setLogicalExpressionCriteria(
            new LogicalExpressionLocalRelationshipCriterion()).setDirection(RelationshipDirection.UNDIRECTED),
        AssetRelationship.class, wrapOptions,
        -1, -1, new RelationshipLookUpContext());

    assertEquals(belongsToOwner.size(), 1);

    AssetRelationship actual1 = belongsToOwner.get(0);
    assertEquals(actual1.getSource(), "urn:li:foo:2");

    BelongsToV2 actual1BelongsToV2 = actual1.getRelatedTo().getBelongsToV2();
    assertEquals(actual1BelongsToV2.getDestination().getString(), owner.toString());
  }

  /**
   * Old schema does not support multiple criteria in the same filter. Skipped in this test.
   */
  @Test
  public void testFindRelationshipsV4WithOneNotCriterion() throws URISyntaxException {
    FooUrn owner = new FooUrn(1);
    FooUrn owner2 = new FooUrn(3);
    FooUrn car = new FooUrn(2);
    FooUrn car2 = new FooUrn(4);

    // Add car and owner into entity tables.
    _fooUrnEBeanLocalAccess.add(car, new AspectFoo().setValue("Car"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(car2, new AspectFoo().setValue("Car2"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(owner, new AspectFoo().setValue("Owner"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(owner2, new AspectFoo().setValue("Owner2"), AspectFoo.class, new AuditStamp(), null, false);

    // Add car belongs-to owner relationship
    BelongsToV2 carBelongsToOwner = new BelongsToV2();
    carBelongsToOwner.setDestination(BelongsToV2.Destination.create(owner.toString()));
    _localRelationshipWriterDAO.addRelationships(car, AspectFoo.class, Collections.singletonList(carBelongsToOwner), false);

    // Add car2 belongs-to owner2 relationship
    BelongsToV2 carBelongsToOwner2 = new BelongsToV2();
    carBelongsToOwner2.setDestination(BelongsToV2.Destination.create(owner2.toString()));
    _localRelationshipWriterDAO.addRelationships(car2, AspectFoo.class, Collections.singletonList(carBelongsToOwner2), false);

    // Find all belongs-to relationship for owner.
    // criterion 1: foo = "Car"
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Car"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
    LogicalExpressionLocalRelationshipCriterion logicalExpressionCriterion = wrapCriterionAsLogicalExpression(filterCriterion);

    LogicalExpressionLocalRelationshipCriterionArray array = new LogicalExpressionLocalRelationshipCriterionArray();
    array.add(logicalExpressionCriterion);

    // not criterion: ( NOT foo = "Car")
    LogicalExpressionLocalRelationshipCriterion localRelationshipCriterion = buildLogicalGroup(Operator.NOT, array);

    // Filter: ( NOT foo = "Car")
    LocalRelationshipFilter srcFilter = new LocalRelationshipFilter().setLogicalExpressionCriteria(localRelationshipCriterion);

    Map<String, Object> wrapOptions = new HashMap<>();
    wrapOptions.put(RELATIONSHIP_RETURN_TYPE, MG_INTERNAL_ASSET_RELATIONSHIP_TYPE);

    List<AssetRelationship> belongsToOwner = _localRelationshipQueryDAO.findRelationshipsV4(
        "foo", srcFilter, null, null,
        BelongsToV2.class, new LocalRelationshipFilter().setLogicalExpressionCriteria(
            new LogicalExpressionLocalRelationshipCriterion()).setDirection(RelationshipDirection.UNDIRECTED),
        AssetRelationship.class, wrapOptions,
        -1, -1, new RelationshipLookUpContext());

    assertEquals(belongsToOwner.size(), 1);

    AssetRelationship actual1 = belongsToOwner.get(0);
    assertEquals(actual1.getSource(), "urn:li:foo:4");

    BelongsToV2 actual1BelongsToV2 = actual1.getRelatedTo().getBelongsToV2();
    assertEquals(actual1BelongsToV2.getDestination().getString(), owner2.toString());
  }

  /**
   * Old schema does not support multiple criteria in the same filter. Skipped in this test.
   */
  @Test
  public void testFindRelationshipsV4WithOneNotCriterionInNested() throws URISyntaxException {
    FooUrn owner = new FooUrn(1);
    FooUrn owner2 = new FooUrn(3);
    FooUrn car = new FooUrn(2);
    FooUrn car2 = new FooUrn(4);

    // Add car and owner into entity tables.
    _fooUrnEBeanLocalAccess.add(car, new AspectFoo().setValue("Car"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(car, new AspectBar().setValue("Bike"), AspectBar.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(car2, new AspectFoo().setValue("Car"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(car2, new AspectBar().setValue("Bike2"), AspectBar.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(owner, new AspectFoo().setValue("Owner"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(owner2, new AspectFoo().setValue("Owner2"), AspectFoo.class, new AuditStamp(), null, false);

    // Add car belongs-to owner relationship
    BelongsToV2 carBelongsToOwner = new BelongsToV2();
    carBelongsToOwner.setDestination(BelongsToV2.Destination.create(owner.toString()));
    _localRelationshipWriterDAO.addRelationships(car, AspectFoo.class, Collections.singletonList(carBelongsToOwner), false);

    // Add car2 belongs-to owner2 relationship
    BelongsToV2 carBelongsToOwner2 = new BelongsToV2();
    carBelongsToOwner2.setDestination(BelongsToV2.Destination.create(owner2.toString()));
    _localRelationshipWriterDAO.addRelationships(car2, AspectFoo.class, Collections.singletonList(carBelongsToOwner2), false);

    // Find all belongs-to relationship for owner.
    // criterion 1: foo = "Car"
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Car"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
    LogicalExpressionLocalRelationshipCriterion logicalExpressionCriterion = wrapCriterionAsLogicalExpression(filterCriterion);

    // criterion 2: bar = "Bike"
    LocalRelationshipCriterion filterCriterion2 = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Bike"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectBar.class.getCanonicalName()).setPath("/value"));
    LogicalExpressionLocalRelationshipCriterion logicalExpressionCriterion2 = wrapCriterionAsLogicalExpression(filterCriterion2);

    LogicalExpressionLocalRelationshipCriterionArray array = new LogicalExpressionLocalRelationshipCriterionArray();
    array.add(logicalExpressionCriterion2);

    // not criterion: (NOT bar = "Bike")
    LogicalExpressionLocalRelationshipCriterion localRelationshipCriterion = buildLogicalGroup(Operator.NOT, array);

    LogicalExpressionLocalRelationshipCriterionArray array2 = new LogicalExpressionLocalRelationshipCriterionArray();
    array2.add(localRelationshipCriterion);
    array2.add(logicalExpressionCriterion);

    // and criterion: (foo = "Car" AND (NOT bar = "Bike"))
    LogicalExpressionLocalRelationshipCriterion localRelationshipCriterion2 = buildLogicalGroup(Operator.AND, array2);

    // Filter: (foo = "Car" AND (NOT bar = "Bike"))
    LocalRelationshipFilter srcFilter = new LocalRelationshipFilter().setLogicalExpressionCriteria(localRelationshipCriterion2);


    Map<String, Object> wrapOptions = new HashMap<>();
    wrapOptions.put(RELATIONSHIP_RETURN_TYPE, MG_INTERNAL_ASSET_RELATIONSHIP_TYPE);

    List<AssetRelationship> belongsToOwner = _localRelationshipQueryDAO.findRelationshipsV4(
        "foo", srcFilter, null, null,
        BelongsToV2.class, new LocalRelationshipFilter().setLogicalExpressionCriteria(
            new LogicalExpressionLocalRelationshipCriterion()).setDirection(RelationshipDirection.UNDIRECTED),
        AssetRelationship.class, wrapOptions,
        -1, -1, new RelationshipLookUpContext());

    assertEquals(belongsToOwner.size(), 1);

    AssetRelationship actual1 = belongsToOwner.get(0);
    assertEquals(actual1.getSource(), "urn:li:foo:4");

    BelongsToV2 actual1BelongsToV2 = actual1.getRelatedTo().getBelongsToV2();
    assertEquals(actual1BelongsToV2.getDestination().getString(), owner2.toString());
  }

  /**
   * Old schema does not support multiple criteria in the same filter. Skipped in this test.
   */
  @Test
  public void testFindRelationshipsV4WithNestedCriterion() throws URISyntaxException {
    FooUrn owner = new FooUrn(1);
    FooUrn owner2 = new FooUrn(3);
    FooUrn owner3 = new FooUrn(5);
    FooUrn car = new FooUrn(2);
    FooUrn car2 = new FooUrn(4);
    FooUrn car3 = new FooUrn(6);

    // Add car and owner into entity tables.
    _fooUrnEBeanLocalAccess.add(car, new AspectFoo().setValue("Car"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(car, new AspectBar().setValue("Bike"), AspectBar.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(car2, new AspectFoo().setValue("Car2"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(car3, new AspectFoo().setValue("Car3"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(owner, new AspectFoo().setValue("Owner"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(owner2, new AspectFoo().setValue("Owner2"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(owner3, new AspectFoo().setValue("Owner3"), AspectFoo.class, new AuditStamp(), null, false);

    // Add car belongs-to owner relationship
    BelongsToV2 carBelongsToOwner = new BelongsToV2();
    carBelongsToOwner.setDestination(BelongsToV2.Destination.create(owner.toString()));
    _localRelationshipWriterDAO.addRelationships(car, AspectFoo.class, Collections.singletonList(carBelongsToOwner), false);

    // Add car2 belongs-to owner2 relationship
    BelongsToV2 carBelongsToOwner2 = new BelongsToV2();
    carBelongsToOwner2.setDestination(BelongsToV2.Destination.create(owner2.toString()));
    _localRelationshipWriterDAO.addRelationships(car2, AspectFoo.class, Collections.singletonList(carBelongsToOwner2), false);

    // Add car3 belongs-to owner3 relationship
    BelongsToV2 carBelongsToOwner3 = new BelongsToV2();
    carBelongsToOwner3.setDestination(BelongsToV2.Destination.create(owner3.toString()));
    _localRelationshipWriterDAO.addRelationships(car3, AspectFoo.class, Collections.singletonList(carBelongsToOwner3), false);

    // Find all belongs-to relationship for owner.
    // criterion 1: foo = "Car"
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Car"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
    LogicalExpressionLocalRelationshipCriterion logicalExpressionCriterion = wrapCriterionAsLogicalExpression(filterCriterion);

    // criterion 2: bar = "Bike"
    LocalRelationshipCriterion filterCriterion2 = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Bike"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectBar.class.getCanonicalName()).setPath("/value"));
    LogicalExpressionLocalRelationshipCriterion logicalExpressionCriterion2 = wrapCriterionAsLogicalExpression(filterCriterion2);

    LogicalExpressionLocalRelationshipCriterionArray array = new LogicalExpressionLocalRelationshipCriterionArray();
    array.add(logicalExpressionCriterion);
    array.add(logicalExpressionCriterion2);

    // and criterion: (foo = "Car" AND bar = "Bike")
    LogicalExpressionLocalRelationshipCriterion andCriterion = buildLogicalGroup(Operator.AND, array);

    // criterion 3: foo = "Car2"
    LocalRelationshipCriterion filterCriterion3 = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Car2"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
    LogicalExpressionLocalRelationshipCriterion logicalExpressionCriterion3 = wrapCriterionAsLogicalExpression(filterCriterion3);

    LogicalExpressionLocalRelationshipCriterionArray array2 = new LogicalExpressionLocalRelationshipCriterionArray();
    array2.add(andCriterion);
    array2.add(logicalExpressionCriterion3);

    // or criterion: ((foo = "Car" AND bar = "Bike") OR (foo = "Car2"))
    LogicalExpressionLocalRelationshipCriterion orCriterion = buildLogicalGroup(Operator.OR, array2);

    // Filter: (foo = "Car" AND bar = "Bike") OR (foo = "Car2")
    LocalRelationshipFilter srcFilter = new LocalRelationshipFilter().setLogicalExpressionCriteria(orCriterion);

    Map<String, Object> wrapOptions = new HashMap<>();
    wrapOptions.put(RELATIONSHIP_RETURN_TYPE, MG_INTERNAL_ASSET_RELATIONSHIP_TYPE);

    List<AssetRelationship> belongsToOwner = _localRelationshipQueryDAO.findRelationshipsV4(
        "foo", srcFilter, null, null,
        BelongsToV2.class, new LocalRelationshipFilter().setLogicalExpressionCriteria(
            new LogicalExpressionLocalRelationshipCriterion()).setDirection(RelationshipDirection.UNDIRECTED),
        AssetRelationship.class, wrapOptions,
        -1, -1, new RelationshipLookUpContext());

    assertEquals(belongsToOwner.size(), 2);

    AssetRelationship actual1 = belongsToOwner.get(0);
    assertEquals(actual1.getSource(), "urn:li:foo:2");

    BelongsToV2 actual1BelongsToV2 = actual1.getRelatedTo().getBelongsToV2();
    assertEquals(actual1BelongsToV2.getDestination().getString(), owner.toString());

    AssetRelationship actual2 = belongsToOwner.get(1);
    assertEquals(actual2.getSource(), "urn:li:foo:4");

    BelongsToV2 actual2BelongsToV2 = actual2.getRelatedTo().getBelongsToV2();
    assertEquals(actual2BelongsToV2.getDestination().getString(), owner2.toString());
  }

  @Test
  public void testFindRelationshipsV4WithUnsetOp() {
    // Find all belongs-to relationship for owner.
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Owner"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
    LogicalExpressionLocalRelationshipCriterion logicalExpressionCriterion = wrapCriterionAsLogicalExpression(filterCriterion);

    LocalRelationshipCriterion filterCriterion2 = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Owner2"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
    LogicalExpressionLocalRelationshipCriterion logicalExpressionCriterion2 = wrapCriterionAsLogicalExpression(filterCriterion2);

    LogicalExpressionLocalRelationshipCriterionArray array = new LogicalExpressionLocalRelationshipCriterionArray();
    array.add(logicalExpressionCriterion);
    array.add(logicalExpressionCriterion2);

    // unset operator
    LogicalOperation operation = new LogicalOperation();
    operation.setExpressions(array);

    LogicalExpressionLocalRelationshipCriterion.Expr expr = new LogicalExpressionLocalRelationshipCriterion.Expr();
    expr.setLogical(operation);

    LogicalExpressionLocalRelationshipCriterion localRelationshipCriterion = new LogicalExpressionLocalRelationshipCriterion()
        .setExpr(expr);

    LocalRelationshipFilter destFilter = new LocalRelationshipFilter().setLogicalExpressionCriteria(localRelationshipCriterion);

    Map<String, Object> wrapOptions = new HashMap<>();
    wrapOptions.put(RELATIONSHIP_RETURN_TYPE, MG_INTERNAL_ASSET_RELATIONSHIP_TYPE);

    // illegalArgumentException is expected here because the filter contains the unset operator.
    assertThrows(IllegalArgumentException.class, () -> {
      _localRelationshipQueryDAO.findRelationshipsV4(
          null, null, "foo", destFilter,
          BelongsToV2.class, new LocalRelationshipFilter().setLogicalExpressionCriteria(
              new LogicalExpressionLocalRelationshipCriterion()).setDirection(RelationshipDirection.UNDIRECTED),
          AssetRelationship.class, wrapOptions,
          -1, -1, new RelationshipLookUpContext());
    });
  }

  @Test
  public void testFindRelationshipsV4WithUnknownOp() {
    // Find all belongs-to relationship for owner.
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Owner"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
    LogicalExpressionLocalRelationshipCriterion logicalExpressionCriterion = wrapCriterionAsLogicalExpression(filterCriterion);

    LocalRelationshipCriterion filterCriterion2 = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Owner2"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
    LogicalExpressionLocalRelationshipCriterion logicalExpressionCriterion2 = wrapCriterionAsLogicalExpression(filterCriterion2);

    LogicalExpressionLocalRelationshipCriterionArray array = new LogicalExpressionLocalRelationshipCriterionArray();
    array.add(logicalExpressionCriterion);
    array.add(logicalExpressionCriterion2);

    // unknown operator
    LogicalExpressionLocalRelationshipCriterion localRelationshipCriterion = buildLogicalGroup(Operator.UNKNOWN, array);

    LocalRelationshipFilter destFilter = new LocalRelationshipFilter().setLogicalExpressionCriteria(localRelationshipCriterion);

    Map<String, Object> wrapOptions = new HashMap<>();
    wrapOptions.put(RELATIONSHIP_RETURN_TYPE, MG_INTERNAL_ASSET_RELATIONSHIP_TYPE);

    // illegalArgumentException is expected here because the filter contains the unknown operator.
    assertThrows(IllegalArgumentException.class, () -> {
      _localRelationshipQueryDAO.findRelationshipsV4(
          null, null, "foo", destFilter,
          BelongsToV2.class, new LocalRelationshipFilter().setLogicalExpressionCriteria(
              new LogicalExpressionLocalRelationshipCriterion()).setDirection(RelationshipDirection.UNDIRECTED),
          AssetRelationship.class, wrapOptions,
          -1, -1, new RelationshipLookUpContext());
    });

    // $unknown operator
    LogicalExpressionLocalRelationshipCriterion localRelationshipCriterion1 = buildLogicalGroup(Operator.$UNKNOWN, array);

    LocalRelationshipFilter destFilter1 = new LocalRelationshipFilter().setLogicalExpressionCriteria(localRelationshipCriterion1);

    // illegalArgumentException is expected here because the filter contains the unknown operator.
    assertThrows(IllegalArgumentException.class, () -> {
      _localRelationshipQueryDAO.findRelationshipsV4(
          null, null, "foo", destFilter,
          BelongsToV2.class, new LocalRelationshipFilter().setLogicalExpressionCriteria(
              new LogicalExpressionLocalRelationshipCriterion()).setDirection(RelationshipDirection.UNDIRECTED),
          AssetRelationship.class, wrapOptions,
          -1, -1, new RelationshipLookUpContext());
    });
  }

  @Test(dataProvider = "schemaConfig")
  public void testFindRelationshipsV4WithCriteriaField(EbeanLocalDAO.SchemaConfig schemaConfig) throws URISyntaxException {
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

    // illegalArgumentException is expected here because the filter contains the criteria field for v2/v3 api.
    assertThrows(IllegalArgumentException.class, () -> {
      _localRelationshipQueryDAO.findRelationshipsV4(
          null, null, "foo", destFilter,
          BelongsToV2.class, new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
          AssetRelationship.class, wrapOptions,
          -1, -1, new RelationshipLookUpContext());
    });
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
        "metadata_entity_foo", srcFilter, "destination_table_name", null,
        -1, -1, new RelationshipLookUpContext());

    assertEquals(sql,
        "SELECT rt.* FROM relationship_table_name rt INNER JOIN destination_table_name dt ON dt.urn=rt.destination "
            + "INNER JOIN metadata_entity_foo st ON st.urn=rt.source WHERE rt.deleted_ts is NULL AND st.i_aspectfoo"
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
        "source_table_name", null, "metadata_entity_bar", destFilter,
        -1, -1, new RelationshipLookUpContext());

    assertEquals(sql,
        "SELECT rt.* FROM relationship_table_name rt INNER JOIN metadata_entity_bar dt ON dt.urn=rt.destination "
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
        "metadata_entity_foo", srcFilter, "metadata_entity_bar", destFilter,
        -1, -1, new RelationshipLookUpContext());

    char virtualColumnDelimiter = _eBeanDAOConfig.isNonDollarVirtualColumnsEnabled() ? '0' : '$';
    assertEquals(sql,
        "SELECT rt.* FROM relationship_table_name rt INNER JOIN metadata_entity_bar dt ON dt.urn=rt.destination "
            + "INNER JOIN metadata_entity_foo st ON st.urn=rt.source WHERE rt.deleted_ts is NULL AND (dt.i_aspectfoo"
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
        "metadata_entity_foo", srcFilter, "destination_table_name", null,
        -1, -1, new RelationshipLookUpContext(true));

    assertEquals(sql,
        "SELECT * FROM ("
            + "SELECT rt.*, ROW_NUMBER() OVER (PARTITION BY rt.source, rt.destination ORDER BY rt.lastmodifiedon DESC) AS row_num "
            + "FROM relationship_table_name rt INNER JOIN destination_table_name dt ON dt.urn=rt.destination "
            + "INNER JOIN metadata_entity_foo st ON st.urn=rt.source  WHERE st.i_aspectfoo"
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
        "source_table_name", null, "metadata_entity_bar", destFilter,
        -1, -1, new RelationshipLookUpContext(true));

    assertEquals(sql,
        "SELECT * FROM ("
            + "SELECT rt.*, ROW_NUMBER() OVER (PARTITION BY rt.source, rt.destination ORDER BY rt.lastmodifiedon DESC) AS row_num "
            + "FROM relationship_table_name rt INNER JOIN metadata_entity_bar dt ON dt.urn=rt.destination "
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
        "metadata_entity_foo", srcFilter, "metadata_entity_bar", destFilter,
        -1, -1, new RelationshipLookUpContext(true));

    char virtualColumnDelimiter = _eBeanDAOConfig.isNonDollarVirtualColumnsEnabled() ? '0' : '$';

    assertEquals(sql,
        "SELECT * FROM ("
            + "SELECT rt.*, ROW_NUMBER() OVER (PARTITION BY rt.source, rt.destination ORDER BY rt.lastmodifiedon DESC) AS row_num "
            + "FROM relationship_table_name rt INNER JOIN metadata_entity_bar dt ON dt.urn=rt.destination "
            + "INNER JOIN metadata_entity_foo st ON st.urn=rt.source  WHERE (dt.i_aspectfoo" + virtualColumnDelimiter
            + "value='Bob') AND (st.urn='urn:li:foo:4')) ranked_rows WHERE row_num = 1");
  }

  @Test
  public void testBuildFindRelationshipSQLWithLogicalExpression() {
    LocalRelationshipFilter srcFilter = createLocalRelationshipFilterWithAndLogicalExpression();

    String sql = _localRelationshipQueryDAO.buildFindRelationshipSQL("relationship_table_name",
        new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
        "source_table_name", srcFilter, "destination_table_name", null,
        -1, -1, new RelationshipLookUpContext());

    assertEquals(sql,
        "SELECT rt.* FROM relationship_table_name rt INNER JOIN destination_table_name dt ON dt.urn=rt.destination "
            + "INNER JOIN source_table_name st ON st.urn=rt.source WHERE rt.deleted_ts is NULL AND (st.urn='urn:li:foo:4' AND st.urn='urn:li:foo:5')");
  }

  private static LocalRelationshipFilter createLocalRelationshipFilterWithAndLogicalExpression() {
    LocalRelationshipCriterion urnCriterion1 = EBeanDAOUtils.buildRelationshipFieldCriterion(
        LocalRelationshipValue.create("urn:li:foo:4"),
        Condition.EQUAL,
        new UrnField());
    LogicalExpressionLocalRelationshipCriterion logical1 =
        wrapCriterionAsLogicalExpression(urnCriterion1);

    LocalRelationshipCriterion urnCriterion2 = EBeanDAOUtils.buildRelationshipFieldCriterion(
        LocalRelationshipValue.create("urn:li:foo:5"),
        Condition.EQUAL,
        new UrnField());
    LogicalExpressionLocalRelationshipCriterion logical2 =
        wrapCriterionAsLogicalExpression(urnCriterion2);

    LogicalExpressionLocalRelationshipCriterionArray andArray = new LogicalExpressionLocalRelationshipCriterionArray();
    andArray.add(logical1);
    andArray.add(logical2);

    LogicalExpressionLocalRelationshipCriterion andCriterion = buildLogicalGroup(Operator.AND, andArray);

    LocalRelationshipFilter srcFilter = new LocalRelationshipFilter();
    srcFilter.setLogicalExpressionCriteria(andCriterion);
    return srcFilter;
  }

  @Test
  public void testBuildFindRelationshipSQLWithSingleQuote() {
    // The bug case raised in META-22917
    String problematicUrn = "urn:li:dataset:(urn:li:dataPlatform:hdfs,/jobs/dsotnt/lix_evaluations/premium_custom_button_acq_convoad_trex_eval_results',PROD)";

    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(
        LocalRelationshipValue.create(problematicUrn),
        Condition.EQUAL,
        new UrnField().setName("destination"));
    LogicalExpressionLocalRelationshipCriterion logicalExpressionCriterion = wrapCriterionAsLogicalExpression(filterCriterion);

    LocalRelationshipFilter filter = new LocalRelationshipFilter()
        .setLogicalExpressionCriteria(logicalExpressionCriterion)
        .setDirection(RelationshipDirection.OUTGOING);

    String sql = _localRelationshipQueryDAO.buildFindRelationshipSQL(
        "metadata_relationship_belongsto",
        filter,
        "metadata_entity_foo", null,
        null, null,
        -1, -1, new RelationshipLookUpContext());

    // Verify the SQL is valid and the special characters are properly escaped
    assertNotNull(sql);
    assertTrue(sql.contains("metadata_relationship_belongsto"));
    assertTrue(sql.contains("metadata_entity_foo"));
    // The problematic URN should be properly escaped in the WHERE clause
    assertTrue(sql.contains("rt.destination="));

    // Most importantly, execute the SQL to ensure it doesn't throw syntax errors
    try {
      _server.createSqlQuery(sql).findList();
    } catch (PersistenceException e) {
      fail("SQL query with special characters should not throw syntax errors: " + e.getMessage());
    }
  }


  @Test
  public void testFindRelationshipsWithSingleQuoteInUrn() throws Exception {
    // The original bug case raised in META-22917
    String problematicUrn = "urn:li:dataset:(urn:li:dataPlatform:hdfs,/jobs/dsotnt/lix_evaluations/premium_custom_button_acq_convoad_trex_eval_results',PROD)";

    FooUrn source = new FooUrn(1);

    // Add source entity
    _fooUrnEBeanLocalAccess.add(source, new AspectFoo().setValue("Source"), AspectFoo.class, new AuditStamp(), null, false);

    // Create relationship with problematic destination URN
    BelongsToV2 relationship = new BelongsToV2();
    relationship.setDestination(BelongsToV2.Destination.create(problematicUrn));
    _localRelationshipWriterDAO.addRelationships(source, AspectFoo.class, Collections.singletonList(relationship), false);

    // Query with the problematic URN
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(
        LocalRelationshipValue.create(problematicUrn),
        Condition.EQUAL,
        new UrnField().setName("destination"));
    LogicalExpressionLocalRelationshipCriterion logicalExpressionCriterion = wrapCriterionAsLogicalExpression(filterCriterion);

    LocalRelationshipFilter filter = new LocalRelationshipFilter()
        .setLogicalExpressionCriteria(logicalExpressionCriterion)
        .setDirection(RelationshipDirection.OUTGOING);

    Map<String, Object> wrapOptions = new HashMap<>();
    wrapOptions.put(RELATIONSHIP_RETURN_TYPE, MG_INTERNAL_ASSET_RELATIONSHIP_TYPE);

    // This should work without SQL exceptions
    List<AssetRelationship> results = _localRelationshipQueryDAO.findRelationshipsV4(
        "foo", null, null, null,  // No destination entity type/filter since it's non-MG
        BelongsToV2.class,
        filter,
        AssetRelationship.class, wrapOptions,
        -1, -1, new RelationshipLookUpContext());

    // Should find the relationship
    assertEquals(results.size(), 1);
    assertEquals(results.get(0).getRelatedTo().getBelongsToV2().getDestination().getString(), problematicUrn);
  }

  // -------------------------------------------------------------------------
  // Keyset (seek) pagination: typed API, model and baseline SQL builder
  // -------------------------------------------------------------------------

  private LocalRelationshipFilter emptyFilter() {
    return new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray());
  }

  private LocalRelationshipFilter outgoingEmptyFilter() {
    return new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray())
        .setDirection(RelationshipDirection.OUTGOING);
  }

  private List<FooUrn> addReportsToChain(int count, FooUrn destination) throws URISyntaxException {
    List<FooUrn> sources = new ArrayList<>();
    for (int i = 1; i <= count; i++) {
      FooUrn source = new FooUrn(100 + i);
      _localRelationshipWriterDAO.addRelationships(source, AspectFoo.class,
          Collections.singletonList(new ReportsTo().setSource(source).setDestination(destination)), false);
      sources.add(source);
    }
    return sources;
  }

  private RelationshipKeysetPage<ReportsTo> keysetPage(int pageSize, RelationshipKeysetCursor cursor) {
    return _localRelationshipQueryDAO.findRelationshipsByKeyset(
        null, emptyFilter(), null, emptyFilter(), ReportsTo.class, outgoingEmptyFilter(), pageSize, cursor);
  }

  private List<ReportsTo> drainKeyset(int pageSize) {
    List<ReportsTo> all = new ArrayList<>();
    RelationshipKeysetCursor cursor = null;
    do {
      RelationshipKeysetPage<ReportsTo> page = keysetPage(pageSize, cursor);
      all.addAll(page.getRelationships());
      cursor = page.getNextCursor();
    } while (cursor != null);
    return all;
  }

  @Test
  public void testBuildFindRelationshipKeysetSQL() {
    // Unlike the existing offset builder, the keyset builder adds `id > lastId`, `id <= maxId`,
    // `ORDER BY rt.id ASC` and a finite LIMIT.
    String sql = _localRelationshipQueryDAO.buildFindRelationshipKeysetSQL("relationship_table_name",
        new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
        "source_table_name", null, "destination_table_name", null,
        10, 5, 20);

    assertEquals(sql,
        "SELECT rt.* FROM relationship_table_name rt INNER JOIN destination_table_name dt ON dt.urn=rt.destination "
            + "INNER JOIN source_table_name st ON st.urn=rt.source WHERE rt.deleted_ts is NULL "
            + "AND rt.id > 5 AND rt.id <= 20 ORDER BY rt.id ASC LIMIT 10");
  }

  @Test
  public void testBuildFindRelationshipKeysetSQLWithSource() {
    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Alice"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
    LocalRelationshipFilter srcFilter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));

    String sql = _localRelationshipQueryDAO.buildFindRelationshipKeysetSQL("relationship_table_name",
        new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
        "metadata_entity_foo", srcFilter, "destination_table_name", null,
        10, 5, 20);

    assertEquals(sql,
        "SELECT rt.* FROM relationship_table_name rt INNER JOIN destination_table_name dt ON dt.urn=rt.destination "
            + "INNER JOIN metadata_entity_foo st ON st.urn=rt.source WHERE rt.deleted_ts is NULL AND st.i_aspectfoo"
            + (_eBeanDAOConfig.isNonDollarVirtualColumnsEnabled() ? "0" : "$") + "value='Alice' "
            + "AND rt.id > 5 AND rt.id <= 20 ORDER BY rt.id ASC LIMIT 10");
  }

  @Test
  public void testBuildFindRelationshipKeysetSQLRejectsInvalidPageSize() {
    // Zero and negative are rejected.
    expectThrows(IllegalArgumentException.class, () ->
        _localRelationshipQueryDAO.buildFindRelationshipKeysetSQL("relationship_table_name",
            new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
            null, null, null, null, 0, 0, 20));
    expectThrows(IllegalArgumentException.class, () ->
        _localRelationshipQueryDAO.buildFindRelationshipKeysetSQL("relationship_table_name",
            new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
            null, null, null, null, -1, 0, 20));
    // Above the hard upper bound of 1000 is rejected.
    expectThrows(IllegalArgumentException.class, () ->
        _localRelationshipQueryDAO.buildFindRelationshipKeysetSQL("relationship_table_name",
            new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
            null, null, null, null, 1001, 0, 20));
  }

  @Test
  public void testKeysetPaginationRejectedInOldSchemaMode() throws URISyntaxException {
    addReportsToChain(2, new FooUrn(1));
    _localRelationshipQueryDAO.setSchemaConfig(EbeanLocalDAO.SchemaConfig.OLD_SCHEMA_ONLY);

    // Typed API rejects OLD_SCHEMA_ONLY before building/executing any SQL.
    expectThrows(UnsupportedOperationException.class, () -> keysetPage(3, null));

    // The public SQL builder rejects OLD_SCHEMA_ONLY as well.
    expectThrows(UnsupportedOperationException.class, () ->
        _localRelationshipQueryDAO.buildFindRelationshipKeysetSQL("relationship_table_name",
            new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray()).setDirection(RelationshipDirection.UNDIRECTED),
            null, null, null, null, 10, 5, 20));
  }

  @Test
  public void testFindRelationshipsByKeysetEmptyTable() {
    RelationshipKeysetPage<ReportsTo> page = keysetPage(3, null);
    assertTrue(page.getRelationships().isEmpty());
    assertNull(page.getNextCursor());
    assertEquals(page.getHighWaterId(), 0L);
  }

  @Test
  public void testFindRelationshipsByKeysetRejectsInvalidPageSize() {
    expectThrows(IllegalArgumentException.class, () -> keysetPage(0, null));
    expectThrows(IllegalArgumentException.class, () -> keysetPage(-1, null));
    // Above the hard upper bound of 1000 is rejected.
    expectThrows(IllegalArgumentException.class, () -> keysetPage(1001, null));
  }

  @Test
  public void testFindRelationshipsByKeysetFewerThanPageSize() throws URISyntaxException {
    addReportsToChain(2, new FooUrn(1));
    RelationshipKeysetPage<ReportsTo> page = keysetPage(5, null);
    assertEquals(page.getRelationships().size(), 2);
    assertNull(page.getNextCursor());
    assertEquals(page.getHighWaterId(), 2L);
  }

  @Test
  public void testFindRelationshipsByKeysetExactlyPageSize() throws URISyntaxException {
    addReportsToChain(3, new FooUrn(1));

    // A full page whose last matching row equals maxId ends the scan: no next cursor.
    RelationshipKeysetPage<ReportsTo> first = keysetPage(3, null);
    assertEquals(first.getRelationships().size(), 3);
    assertNull(first.getNextCursor());
    assertEquals(first.getHighWaterId(), 3L);
  }

  @Test
  public void testFindRelationshipsByKeysetFullPageBelowMaxIdYieldsFinalEmptyQuery()
      throws URISyntaxException {
    FooUrn dest = new FooUrn(1);
    // Insert 3 rows (ids 1..3), then soft-delete the last two by re-pointing their sources.
    List<FooUrn> sources = addReportsToChain(3, dest);
    // Re-adding source #2 and #3 soft-deletes ids 2 and 3 and inserts ids 4 and 5.
    _localRelationshipWriterDAO.addRelationships(sources.get(1), AspectFoo.class,
        Collections.singletonList(new ReportsTo().setSource(sources.get(1)).setDestination(new FooUrn(2))), false);
    _localRelationshipWriterDAO.addRelationships(sources.get(2), AspectFoo.class,
        Collections.singletonList(new ReportsTo().setSource(sources.get(2)).setDestination(new FooUrn(2))), false);

    // Current rows toward dest #1: only id 1. High-water id is 5 because later nonmatching rows
    // (ids 4/5 pointing elsewhere) set maxId. A page size of 1 fills fully with id 1 (below maxId
    // 5), so a next cursor is produced; that continuation query is empty because ids 2 and 3 are
    // soft-deleted and 4/5 point elsewhere, so one final empty page is needed to end the scan.
    LocalRelationshipCriterion relCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(
        LocalRelationshipValue.create(dest.toString()), Condition.EQUAL, new UrnField().setName("destination"));
    LocalRelationshipFilter relationshipFilter = new LocalRelationshipFilter()
        .setCriteria(new LocalRelationshipCriterionArray(relCriterion))
        .setDirection(RelationshipDirection.OUTGOING);

    RelationshipKeysetPage<ReportsTo> first = _localRelationshipQueryDAO.findRelationshipsByKeyset(
        null, emptyFilter(), null, emptyFilter(), ReportsTo.class, relationshipFilter, 1, null);
    assertEquals(first.getRelationships().size(), 1);
    assertNotNull(first.getNextCursor());
    assertEquals(first.getNextCursor().getMaxId(), 5L);

    RelationshipKeysetPage<ReportsTo> second = _localRelationshipQueryDAO.findRelationshipsByKeyset(
        null, emptyFilter(), null, emptyFilter(), ReportsTo.class, relationshipFilter, 1, first.getNextCursor());
    assertTrue(second.getRelationships().isEmpty());
    assertNull(second.getNextCursor());
  }

  @Test
  public void testFindRelationshipsByKeysetMultiPageOrderedComplete() throws URISyntaxException {
    Set<FooUrn> expectedSources = new java.util.HashSet<>(addReportsToChain(7, new FooUrn(1)));

    List<ReportsTo> drained = drainKeyset(2);

    assertEquals(drained.size(), 7);
    Set<FooUrn> actualSources = drained.stream()
        .map(r -> makeFooUrn(r.getSource().toString()))
        .collect(Collectors.toSet());
    // No duplicates and all sources present.
    assertEquals(actualSources.size(), 7);
    assertEquals(actualSources, expectedSources);
  }

  @Test
  public void testFindRelationshipsByKeysetHighWaterExcludesLaterInsert() throws URISyntaxException {
    addReportsToChain(5, new FooUrn(1));

    // Capture the high-water id on the first page.
    RelationshipKeysetCursor cursor = null;
    List<ReportsTo> drained = new ArrayList<>();
    RelationshipKeysetPage<ReportsTo> page = keysetPage(2, null);
    drained.addAll(page.getRelationships());
    cursor = page.getNextCursor();
    assertEquals(page.getHighWaterId(), 5L);

    // Insert more rows after the scan started; they must not be observed. These later inserts are
    // deterministically excluded by the fixed maxId (best effort only covers updates/deletes of
    // existing ids). Use distinct sources (200+) so no existing relationship is superseded.
    for (int i = 1; i <= 3; i++) {
      FooUrn src = new FooUrn(200 + i);
      _localRelationshipWriterDAO.addRelationships(src, AspectFoo.class,
          Collections.singletonList(new ReportsTo().setSource(src).setDestination(new FooUrn(2))), false);
    }

    while (cursor != null) {
      page = keysetPage(2, cursor);
      drained.addAll(page.getRelationships());
      cursor = page.getNextCursor();
    }

    assertEquals(drained.size(), 5);
  }

  @Test
  public void testFindRelationshipsByKeysetSoftDeletedGaps() throws URISyntaxException {
    FooUrn dest = new FooUrn(1);
    FooUrn a = new FooUrn(101);
    FooUrn b = new FooUrn(102);
    FooUrn c = new FooUrn(103);
    _localRelationshipWriterDAO.addRelationships(a, AspectFoo.class,
        Collections.singletonList(new ReportsTo().setSource(a).setDestination(dest)), false);
    _localRelationshipWriterDAO.addRelationships(b, AspectFoo.class,
        Collections.singletonList(new ReportsTo().setSource(b).setDestination(dest)), false);
    _localRelationshipWriterDAO.addRelationships(c, AspectFoo.class,
        Collections.singletonList(new ReportsTo().setSource(c).setDestination(dest)), false);

    // Re-adding B soft-deletes its first row and inserts a new one, creating an id gap.
    _localRelationshipWriterDAO.addRelationships(b, AspectFoo.class,
        Collections.singletonList(new ReportsTo().setSource(b).setDestination(dest)), false);

    List<ReportsTo> drained = drainKeyset(2);

    // Verifies a soft-deleted id sitting between live rows does not stop or skip the later live
    // rows (distinct from the final-empty case: the gap is mid-scan, not a trailing empty page).
    // Only the three current (non-deleted) relationships are returned, gap skipped.
    assertEquals(drained.size(), 3);
    Set<FooUrn> actual = drained.stream()
        .map(r -> makeFooUrn(r.getSource().toString()))
        .collect(Collectors.toSet());
    assertEquals(actual, ImmutableSet.of(a, b, c));
  }

  @Test
  public void testFindRelationshipsByKeysetWithSourceFilter() throws URISyntaxException {
    FooUrn alice = new FooUrn(1);
    FooUrn bob = new FooUrn(2);
    _fooUrnEBeanLocalAccess.add(alice, new AspectFoo().setValue("Alice"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(bob, new AspectFoo().setValue("Bob"), AspectFoo.class, new AuditStamp(), null, false);

    FooUrn boss = new FooUrn(9);
    _localRelationshipWriterDAO.addRelationships(alice, AspectFoo.class,
        Collections.singletonList(new ReportsTo().setSource(alice).setDestination(boss)), false);
    _localRelationshipWriterDAO.addRelationships(bob, AspectFoo.class,
        Collections.singletonList(new ReportsTo().setSource(bob).setDestination(boss)), false);

    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Alice"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
    LocalRelationshipFilter srcFilter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));

    RelationshipKeysetPage<ReportsTo> page = _localRelationshipQueryDAO.findRelationshipsByKeyset(
        FooSnapshot.class, srcFilter, null, emptyFilter(), ReportsTo.class,
        outgoingEmptyFilter(), 5, null);

    assertEquals(page.getRelationships().size(), 1);
    assertEquals(makeFooUrn(page.getRelationships().get(0).getSource().toString()), alice);
  }

  @Test
  public void testFindRelationshipsByKeysetWithDestinationFilter() throws URISyntaxException {
    FooUrn worker = new FooUrn(1);
    FooUrn alice = new FooUrn(2);
    FooUrn bob = new FooUrn(3);
    _fooUrnEBeanLocalAccess.add(alice, new AspectFoo().setValue("Alice"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(bob, new AspectFoo().setValue("Bob"), AspectFoo.class, new AuditStamp(), null, false);

    _localRelationshipWriterDAO.addRelationships(worker, AspectFoo.class,
        Collections.singletonList(new ReportsTo().setSource(worker).setDestination(alice)), false);
    // A second source reporting to bob so the destination filter has something to exclude.
    _localRelationshipWriterDAO.addRelationships(new FooUrn(4), AspectFoo.class,
        Collections.singletonList(new ReportsTo().setSource(new FooUrn(4)).setDestination(bob)), false);

    LocalRelationshipCriterion filterCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(LocalRelationshipValue.create("Alice"),
        Condition.EQUAL,
        new AspectField().setAspect(AspectFoo.class.getCanonicalName()).setPath("/value"));
    LocalRelationshipFilter destFilter = new LocalRelationshipFilter().setCriteria(new LocalRelationshipCriterionArray(filterCriterion));

    RelationshipKeysetPage<ReportsTo> page = _localRelationshipQueryDAO.findRelationshipsByKeyset(
        null, emptyFilter(), FooSnapshot.class, destFilter, ReportsTo.class,
        outgoingEmptyFilter(), 5, null);

    assertEquals(page.getRelationships().size(), 1);
    assertEquals(makeFooUrn(page.getRelationships().get(0).getDestination().toString()), alice);
  }

  @Test
  public void testFindRelationshipsByKeysetWithRelationshipFilter() throws URISyntaxException {
    FooUrn worker = new FooUrn(1);
    FooUrn alice = new FooUrn(2);
    _fooUrnEBeanLocalAccess.add(worker, new AspectFoo().setValue("Worker"), AspectFoo.class, new AuditStamp(), null, false);
    _fooUrnEBeanLocalAccess.add(alice, new AspectFoo().setValue("Alice"), AspectFoo.class, new AuditStamp(), null, false);

    _localRelationshipWriterDAO.addRelationships(worker, AspectFoo.class,
        Collections.singletonList(new ReportsTo().setSource(worker).setDestination(alice)), false);
    _localRelationshipWriterDAO.addRelationships(new FooUrn(3), AspectFoo.class,
        Collections.singletonList(new ReportsTo().setSource(new FooUrn(3)).setDestination(new FooUrn(4))), false);

    LocalRelationshipCriterion relCriterion = EBeanDAOUtils.buildRelationshipFieldCriterion(
        LocalRelationshipValue.create(alice.toString()), Condition.EQUAL, new UrnField().setName("destination"));
    LocalRelationshipFilter relationshipFilter = new LocalRelationshipFilter()
        .setCriteria(new LocalRelationshipCriterionArray(relCriterion))
        .setDirection(RelationshipDirection.OUTGOING);

    RelationshipKeysetPage<ReportsTo> page = _localRelationshipQueryDAO.findRelationshipsByKeyset(
        null, emptyFilter(), null, emptyFilter(), ReportsTo.class, relationshipFilter, 5, null);

    assertEquals(page.getRelationships().size(), 1);
    assertEquals(makeFooUrn(page.getRelationships().get(0).getDestination().toString()), alice);
  }

  @Test
  public void testFindRelationshipsByKeysetBestEffortMembershipUnderConcurrentReplacement()
      throws URISyntaxException {
    FooUrn dest = new FooUrn(1);
    FooUrn a = new FooUrn(101);
    FooUrn b = new FooUrn(102);
    FooUrn c = new FooUrn(103);
    FooUrn d = new FooUrn(104);
    // Ids 1..4, all current, all pointing at dest.
    _localRelationshipWriterDAO.addRelationships(a, AspectFoo.class,
        Collections.singletonList(new ReportsTo().setSource(a).setDestination(dest)), false);
    _localRelationshipWriterDAO.addRelationships(b, AspectFoo.class,
        Collections.singletonList(new ReportsTo().setSource(b).setDestination(dest)), false);
    _localRelationshipWriterDAO.addRelationships(c, AspectFoo.class,
        Collections.singletonList(new ReportsTo().setSource(c).setDestination(dest)), false);
    _localRelationshipWriterDAO.addRelationships(d, AspectFoo.class,
        Collections.singletonList(new ReportsTo().setSource(d).setDestination(dest)), false);

    // Page 1 (size 2) captures the insertion high-water id maxId = 4 and returns ids 1,2 (a, b).
    RelationshipKeysetPage<ReportsTo> first = keysetPage(2, null);
    assertEquals(first.getHighWaterId(), 4L);
    assertNotNull(first.getNextCursor());
    List<ReportsTo> drained = new ArrayList<>(first.getRelationships());

    // Between pages, "concurrently" replace c: this soft-deletes id 3 (which is <= maxId) and
    // inserts a fresh current row id 5 (which is > maxId). The replacement row is excluded by the
    // fixed maxId bound and the original id 3 is soft-deleted, so c may disappear from the scan even
    // though it was current when the scan began. Because each page is a separate statement, maxId is
    // only an insertion high-water mark, not a stable/complete snapshot: this is the documented
    // best-effort membership behavior, chosen so the scan stays finite.
    _localRelationshipWriterDAO.addRelationships(c, AspectFoo.class,
        Collections.singletonList(new ReportsTo().setSource(c).setDestination(dest)), false);

    RelationshipKeysetCursor cursor = first.getNextCursor();
    while (cursor != null) {
      RelationshipKeysetPage<ReportsTo> page = keysetPage(2, cursor);
      drained.addAll(page.getRelationships());
      cursor = page.getNextCursor();
    }

    // c is missing: the scan reflects rows current when each page ran, not a snapshot of the
    // rows that were current when the scan started.
    Set<FooUrn> actual = drained.stream()
        .map(r -> makeFooUrn(r.getSource().toString()))
        .collect(Collectors.toSet());
    assertEquals(actual, ImmutableSet.of(a, b, d));
    assertFalse(actual.contains(c));
  }

  @Test
  public void testRelationshipKeysetCursorValidationAndGetters() {
    RelationshipKeysetCursor cursor = new RelationshipKeysetCursor(3, 10);
    assertEquals(cursor.getLastId(), 3L);
    assertEquals(cursor.getMaxId(), 10L);

    expectThrows(IllegalArgumentException.class, () -> new RelationshipKeysetCursor(-1, 10));
    expectThrows(IllegalArgumentException.class, () -> new RelationshipKeysetCursor(3, -1));
    expectThrows(IllegalArgumentException.class, () -> new RelationshipKeysetCursor(11, 10));
  }

  @Test
  public void testRelationshipKeysetPageDefensiveAndValidation() throws URISyntaxException {
    List<ReportsTo> source = new ArrayList<>();
    source.add(new ReportsTo().setSource(new FooUrn(1)).setDestination(new FooUrn(2)));
    RelationshipKeysetPage<ReportsTo> page =
        new RelationshipKeysetPage<>(source, 10, new RelationshipKeysetCursor(5, 10));

    // Defensive copy: mutating the input list cannot alter the returned page contents.
    source.clear();
    assertEquals(page.getRelationships().size(), 1);
    assertNotNull(page.getNextCursor());
    expectThrows(UnsupportedOperationException.class, () -> page.getRelationships().add(null));

    // next cursor maxId must match highWaterId.
    expectThrows(IllegalArgumentException.class, () ->
        new RelationshipKeysetPage<>(new ArrayList<ReportsTo>(), 10, new RelationshipKeysetCursor(5, 9)));
    expectThrows(IllegalArgumentException.class, () ->
        new RelationshipKeysetPage<ReportsTo>(null, 10, null));
  }

}
