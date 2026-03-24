package com.linkedin.metadata.dao;

import com.google.common.io.Resources;
import com.linkedin.common.AuditStamp;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.urnpath.EmptyPathExtractor;
import com.linkedin.metadata.dao.utils.EmbeddedMariaInstance;
import com.linkedin.metadata.dao.utils.FooUrnPathExtractor;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.dao.utils.SQLIndexFilterUtils;
import com.linkedin.metadata.dao.utils.SchemaValidatorUtil;
import com.linkedin.metadata.query.Condition;
import com.linkedin.metadata.query.IndexCriterion;
import com.linkedin.metadata.query.IndexCriterionArray;
import com.linkedin.metadata.query.IndexFilter;
import com.linkedin.metadata.query.IndexGroupByCriterion;
import com.linkedin.metadata.query.IndexSortCriterion;
import com.linkedin.metadata.query.IndexValue;
import com.linkedin.metadata.query.SortOrder;
import com.linkedin.testing.AspectBar;
import com.linkedin.testing.AspectBaz;
import com.linkedin.testing.AspectFoo;
import com.linkedin.testing.urn.BurgerUrn;
import com.linkedin.testing.urn.FooUrn;
import io.ebean.Ebean;
import io.ebean.EbeanServer;
import io.ebean.SqlRow;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import static com.linkedin.common.AuditStamps.*;
import static com.linkedin.testing.TestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

public class EbeanLocalAccessTest {
  private static EbeanServer _server;
  private static EbeanLocalAccess<FooUrn> _ebeanLocalAccessFoo;
  private static IEbeanLocalAccess<BurgerUrn> _ebeanLocalAccessBurger;
  private static long _now;
  private final EBeanDAOConfig _ebeanConfig = new EBeanDAOConfig();

  @Factory(dataProvider = "inputList")
  public EbeanLocalAccessTest(boolean nonDollarVirtualColumnsEnabled) {
    _ebeanConfig.setNonDollarVirtualColumnsEnabled(nonDollarVirtualColumnsEnabled);
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
    _server = EmbeddedMariaInstance.getServer(EbeanLocalAccessTest.class.getSimpleName());
    _ebeanLocalAccessFoo = new EbeanLocalAccess<>(_server, EmbeddedMariaInstance.SERVER_CONFIG_MAP.get(_server.getName()),
        FooUrn.class, new FooUrnPathExtractor(), _ebeanConfig.isNonDollarVirtualColumnsEnabled());
    _ebeanLocalAccessBurger = new EbeanLocalAccess<>(_server, EmbeddedMariaInstance.SERVER_CONFIG_MAP.get(_server.getName()),
        BurgerUrn.class, new EmptyPathExtractor<>(), _ebeanConfig.isNonDollarVirtualColumnsEnabled());
    _now = System.currentTimeMillis();
  }

  @BeforeMethod
  public void setupTest() throws IOException {
    if (!_ebeanConfig.isNonDollarVirtualColumnsEnabled()) {
      _server.execute(Ebean.createSqlUpdate(
          Resources.toString(Resources.getResource("ebean-local-access-create-all.sql"), StandardCharsets.UTF_8)));
    } else {
      _server.execute(Ebean.createSqlUpdate(Resources.toString(
          Resources.getResource("ebean-local-access-create-all-with-non-dollar-virtual-column-names.sql"),
          StandardCharsets.UTF_8)));
    }
    // initialize data with metadata_entity_foo table with fooUrns from 0 ~ 99
    int numOfRecords = 100;
    for (int i = 0; i < numOfRecords; i++) {
      FooUrn fooUrn = makeFooUrn(i);
      AspectFoo aspectFoo = new AspectFoo();
      aspectFoo.setValue(String.valueOf(i));
      AuditStamp auditStamp = makeAuditStamp("foo", System.currentTimeMillis());
      _ebeanLocalAccessFoo.add(fooUrn, aspectFoo, AspectFoo.class, auditStamp, null, false);
    }
  }

  @BeforeMethod
  public void resetValidatorInstance() throws Exception {
    Field validatorField = _ebeanLocalAccessFoo.getClass().getDeclaredField("validator");
    validatorField.setAccessible(true);
    SchemaValidatorUtil freshValidator = new SchemaValidatorUtil(_server);
    validatorField.set(_ebeanLocalAccessFoo, freshValidator);
  }

  @Test
  public void testGetAspect() {

    // Given: metadata_entity_foo table with fooUrns from 0 ~ 99
    FooUrn fooUrn = makeFooUrn(0);
    AspectKey<FooUrn, AspectFoo> aspectKey = new AspectKey(AspectFoo.class, fooUrn, 0L);

    // When get AspectFoo from urn:li:foo:0
    List<EbeanMetadataAspect> ebeanMetadataAspectList =
        _ebeanLocalAccessFoo.batchGetUnion(Collections.singletonList(aspectKey), 1000, 0, false, false);
    assertEquals(1, ebeanMetadataAspectList.size());

    EbeanMetadataAspect ebeanMetadataAspect = ebeanMetadataAspectList.get(0);

    // Expect: the content of aspect foo is returned
    assertEquals(AspectFoo.class.getCanonicalName(), ebeanMetadataAspect.getKey().getAspect());
    assertEquals(fooUrn.toString(), ebeanMetadataAspect.getKey().getUrn());
    assertEquals("{\"value\":\"0\"}", ebeanMetadataAspect.getMetadata());
    assertEquals("urn:li:testActor:foo", ebeanMetadataAspect.getCreatedBy());

    // Make sure json can be deserialized to Aspect.
    assertNotNull(RecordUtils.toRecordTemplate(AspectFoo.class, ebeanMetadataAspect.getMetadata()));

    // When get AspectFoo from urn:li:foo:9999 (does not exist)
    FooUrn nonExistFooUrn = makeFooUrn(9999);
    AspectKey<FooUrn, AspectFoo> nonExistKey = new AspectKey(AspectFoo.class, nonExistFooUrn, 0L);
    ebeanMetadataAspectList = _ebeanLocalAccessFoo.batchGetUnion(Collections.singletonList(nonExistKey), 1000, 0, false, false);

    // Expect: get AspectFoo from urn:li:foo:9999 returns empty result
    assertTrue(ebeanMetadataAspectList.isEmpty());
  }

  @Test
  public void testGetAspectWhenColumnMissing() throws Exception {
    // Given: a valid URN for which the aspect column does not exist
    FooUrn fooUrn = makeFooUrn(0);
    AspectKey<FooUrn, AspectBaz> missingAspectKey = new AspectKey<>(AspectBaz.class, fooUrn, 0L);

    List<EbeanMetadataAspect> result = _ebeanLocalAccessFoo.batchGetUnion(Collections.singletonList(missingAspectKey), 1000, 0, false, false);
    // Expect: the result is empty since the column does not exist
    // Then: expect it to be silently skipped (no exception) and return empty result
    assertNotNull(result);
    assertTrue("Expected empty result when aspect column is missing", result.isEmpty());
  }

  @Test
  public void testListUrnsWithOffset() {

    // Given: metadata_entity_foo table with fooUrns from 0 ~ 99
    // When: finding urns where ids >= 25 and id < 50 sorting by ASC

    IndexFilter indexFilter = new IndexFilter();
    IndexCriterionArray indexCriterionArray = new IndexCriterionArray();

    IndexCriterion indexCriterion1 =
        SQLIndexFilterUtils.createIndexCriterion(AspectFoo.class, "value", Condition.GREATER_THAN_OR_EQUAL_TO,
            IndexValue.create(25));
    IndexCriterion indexCriterion2 =
        SQLIndexFilterUtils.createIndexCriterion(AspectFoo.class, "value", Condition.LESS_THAN, IndexValue.create(50));

    indexCriterionArray.add(indexCriterion1);
    indexCriterionArray.add(indexCriterion2);
    indexFilter.setCriteria(indexCriterionArray);

    IndexSortCriterion indexSortCriterion =
        SQLIndexFilterUtils.createIndexSortCriterion(AspectFoo.class, "value", SortOrder.ASCENDING);

    // When: list out results with start = 5 and pageSize = 5

    ListResult<FooUrn> listUrns = _ebeanLocalAccessFoo.listUrns(indexFilter, indexSortCriterion, 5, 5);

    assertEquals(5, listUrns.getValues().size());
    assertEquals(5, listUrns.getPageSize());
    assertEquals(10, listUrns.getNextStart());
    assertEquals(25, listUrns.getTotalCount());
    assertEquals(5, listUrns.getTotalPageCount());
  }

  @Test
  public void testListUrnsWithLastUrn() throws URISyntaxException {

    // Given: metadata_entity_foo table with fooUrns from 0 ~ 99
    // When: finding urns where ids >= 25 and id < 50 sorting by ASC

    IndexFilter indexFilter = new IndexFilter();
    IndexCriterionArray indexCriterionArray = new IndexCriterionArray();

    IndexCriterion indexCriterion1 =
        SQLIndexFilterUtils.createIndexCriterion(AspectFoo.class, "value", Condition.GREATER_THAN_OR_EQUAL_TO,
            IndexValue.create(25));
    IndexCriterion indexCriterion2 =
        SQLIndexFilterUtils.createIndexCriterion(AspectFoo.class, "value", Condition.LESS_THAN, IndexValue.create(50));

    indexCriterionArray.add(indexCriterion1);
    indexCriterionArray.add(indexCriterion2);
    indexFilter.setCriteria(indexCriterionArray);

    IndexSortCriterion indexSortCriterion =
        SQLIndexFilterUtils.createIndexSortCriterion(AspectFoo.class, "value", SortOrder.ASCENDING);

    FooUrn lastUrn = new FooUrn(29);

    // When: list out results with lastUrn = 'urn:li:foo:29' and pageSize = 5
    List<FooUrn> result1 = _ebeanLocalAccessFoo.listUrns(indexFilter, indexSortCriterion, lastUrn, 5);

    // Expect: 5 rows are returns (30~34) and the first element is 'urn:li:foo:30'
    assertEquals(5, result1.size());
    assertEquals("30", result1.get(0).getId());

    lastUrn = result1.get(result1.size() - 1);

    // When: list out results with lastUrn = 'urn:li:foo:34' and pageSize = 5, but with only a filter on the aspect
    IndexCriterion indexCriterion3 = new IndexCriterion().setAspect(FooUrn.class.getCanonicalName());
    indexCriterionArray = new IndexCriterionArray(Collections.singleton(indexCriterion3));
    IndexFilter filter = new IndexFilter().setCriteria(indexCriterionArray);
    List<FooUrn> result2 = _ebeanLocalAccessFoo.listUrns(filter, indexSortCriterion, lastUrn, 5);

    // Expect: 5 rows are returns (35~39) and the first element is 'urn:li:foo:35'
    assertEquals(5, result2.size());
    assertEquals("35", result2.get(0).getId());

    // When: list urns with no filter, no sorting criterion, no last urn.
    List<FooUrn> result3 = _ebeanLocalAccessFoo.listUrns(null, null, null, 10);

    // 0, 1, 10, 11, 12, 13, 14, 15, 16, 17
    assertEquals(result3.size(), 10);
    assertEquals(result3.get(0).getId(), "0");
    assertEquals(result3.get(9).getId(), "17");

    // When: list urns with no filter, no sorting criterion
    List<FooUrn> result4 = _ebeanLocalAccessFoo.listUrns(null, null, new FooUrn(17), 10);

    // 18, 19, 2, 20, 21, 22, 23, 24, 25, 26
    assertEquals(result4.size(), 10);
    assertEquals(result4.get(0).getId(), "18");
    assertEquals(result4.get(9).getId(), "26");
  }

  @Test
  public void testExists() throws URISyntaxException {
    // Given: metadata_entity_foo table with fooUrns from 0 ~ 99

    // When: check whether urn:li:foo:0 exist
    FooUrn foo0 = new FooUrn(0);

    // Expect: urn:li:foo:0 exists
    assertTrue(_ebeanLocalAccessFoo.exists(foo0));

    // When: check whether urn:li:foo:9999 exist
    FooUrn foo9999 = new FooUrn(9999);

    // Expect: urn:li:foo:9999 does not exists
    assertFalse(_ebeanLocalAccessFoo.exists(foo9999));
  }

  @Test
  public void testListUrns() throws URISyntaxException {
    // Given: metadata_entity_foo table with fooUrns from 0 ~ 99

    // When: list urns from the 1st record, with 50 page size
    ListResult<FooUrn> fooUrnListResult = _ebeanLocalAccessFoo.listUrns(AspectFoo.class, 0, 50);

    // Expect: 50 results is returned and 100 total records
    assertEquals(50, fooUrnListResult.getValues().size());
    assertEquals(100, fooUrnListResult.getTotalCount());

    // When: list urns from the 55th record, with 50 page size
    fooUrnListResult = _ebeanLocalAccessFoo.listUrns(AspectFoo.class, 55, 50);

    // Expect: 45 results is returned and 100 total records
    assertEquals(45, fooUrnListResult.getValues().size());
    assertEquals(100, fooUrnListResult.getTotalCount());

    // When: list urns from the 101th record, with 50 page size
    fooUrnListResult = _ebeanLocalAccessFoo.listUrns(AspectFoo.class, 101, 50);

    // Expect: 0 results is returned and 100 total records
    assertEquals(0, fooUrnListResult.getValues().size());
    assertEquals(100, fooUrnListResult.getTotalCount());
  }

  @Test
  public void testCountAggregate() {
    // Given: metadata_entity_foo table with fooUrns from 0 ~ 99

    // When: count aggregate with filter value = 25
    IndexFilter indexFilter = new IndexFilter();
    IndexCriterionArray indexCriterionArray = new IndexCriterionArray();

    IndexCriterion indexCriterion1 =
        SQLIndexFilterUtils.createIndexCriterion(AspectFoo.class, "value", Condition.EQUAL, IndexValue.create(25));

    indexCriterionArray.add(indexCriterion1);
    indexFilter.setCriteria(indexCriterionArray);

    IndexGroupByCriterion indexGroupByCriterion = new IndexGroupByCriterion();
    indexGroupByCriterion.setPath("/value");
    indexGroupByCriterion.setAspect(AspectFoo.class.getCanonicalName());
    Map<String, Long> countMap = _ebeanLocalAccessFoo.countAggregate(indexFilter, indexGroupByCriterion);

    // Expect: there is 1 count for value 25
    assertEquals(countMap.get("25"), Long.valueOf(1));

    // When: change foo:26's value to be 25

    FooUrn fooUrn = makeFooUrn(26);
    AspectFoo aspectFoo = new AspectFoo();
    aspectFoo.setValue(String.valueOf(25));
    AuditStamp auditStamp = makeAuditStamp("foo", System.currentTimeMillis());
    _ebeanLocalAccessFoo.add(fooUrn, aspectFoo, AspectFoo.class, auditStamp, null, false);
    countMap = _ebeanLocalAccessFoo.countAggregate(indexFilter, indexGroupByCriterion);

    // Expect: there are 2 counts for value 25
    assertEquals(countMap.get("25"), Long.valueOf(2));
  }

  @Test
  public void testCountAggregateSkipsMissingColumn() throws Exception {
    // Given: metadata_entity_foo table with fooUrns from 0 ~ 99

    // Given: a valid group by criterion filter value = 25
    IndexFilter indexFilter = new IndexFilter();
    IndexCriterion indexCriterion =
        SQLIndexFilterUtils.createIndexCriterion(AspectFoo.class, "value", Condition.EQUAL, IndexValue.create(25));
    indexFilter.setCriteria(new IndexCriterionArray(indexCriterion));

    IndexGroupByCriterion groupByCriterion = new IndexGroupByCriterion();
    groupByCriterion.setPath("/value");
    groupByCriterion.setAspect(AspectFoo.class.getCanonicalName());

    // Spy on validator to simulate column missing
    SchemaValidatorUtil validatorSpy = spy(new SchemaValidatorUtil(_server));
    doReturn(false).when(validatorSpy).columnExists(anyString(), anyString());

    // Inject the spy into _ebeanLocalAccessFoo
    Field validatorField = _ebeanLocalAccessFoo.getClass().getDeclaredField("validator");
    validatorField.setAccessible(true);
    validatorField.set(_ebeanLocalAccessFoo, validatorSpy);

    // When: countAggregate is called
    Map<String, Long> result = _ebeanLocalAccessFoo.countAggregate(indexFilter, groupByCriterion);

    // Then: expect empty result
    assertNotNull(result, "Expected non-null result even when group-by column is missing");
    assertTrue("Expected empty map when group-by column is missing", result.isEmpty());
  }


  @Test
  public void testEscapeSpecialCharInUrn() {
    AspectFoo aspectFoo = new AspectFoo().setValue("test");
    AuditStamp auditStamp = makeAuditStamp("foo", System.currentTimeMillis());

    // Single quote is a special char in SQL.
    BurgerUrn johnsBurgerUrn1 = makeBurgerUrn("urn:li:burger:John's burger");
    _ebeanLocalAccessBurger.add(johnsBurgerUrn1, aspectFoo, AspectFoo.class, auditStamp, null, false);

    AspectKey aspectKey1 = new AspectKey(AspectFoo.class, johnsBurgerUrn1, 0L);
    List<EbeanMetadataAspect> ebeanMetadataAspectList = _ebeanLocalAccessFoo.batchGetUnion(Collections.singletonList(aspectKey1), 1, 0, false, false);
    assertEquals(ebeanMetadataAspectList.size(), 1);
    assertEquals(ebeanMetadataAspectList.get(0).getKey().getUrn(), johnsBurgerUrn1.toString());

    // Double quote is a special char in SQL.
    BurgerUrn johnsBurgerUrn2 = makeBurgerUrn("urn:li:burger:John\"s burger");
    _ebeanLocalAccessBurger.add(johnsBurgerUrn2, aspectFoo, AspectFoo.class, auditStamp, null, false);

    AspectKey aspectKey2 = new AspectKey(AspectFoo.class, johnsBurgerUrn2, 0L);
    ebeanMetadataAspectList = _ebeanLocalAccessFoo.batchGetUnion(Collections.singletonList(aspectKey2), 1, 0, false, false);
    assertEquals(ebeanMetadataAspectList.size(), 1);
    assertEquals(ebeanMetadataAspectList.get(0).getKey().getUrn(), johnsBurgerUrn2.toString());

    // Backslash is a special char in SQL.
    BurgerUrn johnsBurgerUrn3 = makeBurgerUrn("urn:li:burger:John\\s burger");
    _ebeanLocalAccessBurger.add(johnsBurgerUrn3, aspectFoo, AspectFoo.class, auditStamp, null, false);

    AspectKey aspectKey3 = new AspectKey(AspectFoo.class, johnsBurgerUrn3, 0L);
    ebeanMetadataAspectList = _ebeanLocalAccessFoo.batchGetUnion(Collections.singletonList(aspectKey3), 1, 0, false, false);
    assertEquals(ebeanMetadataAspectList.size(), 1);
    assertEquals(ebeanMetadataAspectList.get(0).getKey().getUrn(), johnsBurgerUrn3.toString());
  }

  @Test
  public void testUrnExtraction() {
    FooUrn urn1 = makeFooUrn(1);
    AspectFoo foo1 = new AspectFoo().setValue("foo");
    _ebeanLocalAccessFoo.add(urn1, foo1, AspectFoo.class, makeAuditStamp("actor", _now), null, false);

    List<SqlRow> results;
    // get content of virtual column
    if (_ebeanConfig.isNonDollarVirtualColumnsEnabled()) {
      results = _server.createSqlQuery("SELECT i_urn0fooId as id FROM metadata_entity_foo").findList();
    } else {
      results = _server.createSqlQuery("SELECT i_urn$fooId as id FROM metadata_entity_foo").findList();
    }
    assertEquals(100, results.size());

    // ensure content is as expected
    SqlRow firstResult = results.get(0);
    assertEquals("0", firstResult.getString("id"));
  }

  @Test
  public void test() throws URISyntaxException {
    FooUrn foo0 = new FooUrn(0);
    // Expect: urn:li:foo:0 exists
    assertTrue(_ebeanLocalAccessFoo.exists(foo0));
  }

  @Test
  public void testFindLatestMetadataAspect() throws URISyntaxException {
    // Given: metadata_aspect table has a record of foo0

    FooUrn foo0 = new FooUrn(0);
    AspectFoo f = new AspectFoo();
    f.setValue("foo");
    EbeanMetadataAspect ebeanMetadataAspect = new EbeanMetadataAspect();
    ebeanMetadataAspect.setKey(new EbeanMetadataAspect.PrimaryKey(foo0.toString(), f.getClass().getCanonicalName(), 0));
    ebeanMetadataAspect.setCreatedOn(new Timestamp(System.currentTimeMillis()));
    ebeanMetadataAspect.setMetadata(f.toString());
    ebeanMetadataAspect.setCreatedBy("yanyang");
    _server.save(ebeanMetadataAspect);

    // When: check whether urn:li:foo:0 exist
    // Expect: urn:li:foo:0 exists
    ebeanMetadataAspect = EbeanLocalAccess.findLatestMetadataAspect(_server, foo0, AspectFoo.class);
    assertNotNull(ebeanMetadataAspect);
    assertEquals(ebeanMetadataAspect.getKey().getUrn(), foo0.toString());

    // When: check whether urn:li:foo:9999 exist
    FooUrn foo9999 = new FooUrn(9999);

    // Expect: urn:li:foo:9999 does not exists
    assertNull(EbeanLocalAccess.findLatestMetadataAspect(_server, foo9999, AspectFoo.class));
  }

  @Test
  public void testGetAspectNoSoftDeleteCheck() {
    FooUrn fooUrn = makeFooUrn(0);
    _ebeanLocalAccessFoo.add(fooUrn, null, AspectFoo.class, makeAuditStamp("foo", System.currentTimeMillis()), null, false);
    AspectKey<FooUrn, AspectFoo> aspectKey = new AspectKey(AspectFoo.class, fooUrn, 0L);
    List<EbeanMetadataAspect> ebeanMetadataAspectList =
        _ebeanLocalAccessFoo.batchGetUnion(Collections.singletonList(aspectKey), 1000, 0, false, false);
    assertEquals(0, ebeanMetadataAspectList.size());

    ebeanMetadataAspectList =
        _ebeanLocalAccessFoo.batchGetUnion(Collections.singletonList(aspectKey), 1000, 0, true, false);
    assertFalse(ebeanMetadataAspectList.isEmpty());
    assertEquals(fooUrn.toString(), ebeanMetadataAspectList.get(0).getKey().getUrn());
  }

  @Test
  public void testCreateNewAspect() {
    FooUrn fooUrn = makeFooUrn(101);
    AspectFoo aspectFoo = new AspectFoo().setValue("foo");
    AuditStamp auditStamp = makeAuditStamp("actor", _now);
    List<RecordTemplate> aspectValues = new ArrayList<>();
    aspectValues.add(aspectFoo);
    List<BaseLocalDAO.AspectCreateLambda<? extends RecordTemplate>> aspectCreateLambdas = new ArrayList<>();
    aspectCreateLambdas.add(new BaseLocalDAO.AspectCreateLambda(aspectFoo));
    int result = _ebeanLocalAccessFoo.create(fooUrn, aspectValues, aspectCreateLambdas, auditStamp, null, false);
    assertEquals(result, 1);
  }

  @Test
  public void testCreateDuplicateAsset() {
    FooUrn fooUrn = makeFooUrn(102);
    AspectFoo aspectFoo = new AspectFoo().setValue("foo");
    AuditStamp auditStamp = makeAuditStamp("actor", _now);
    List<RecordTemplate> aspectValues = new ArrayList<>();
    aspectValues.add(aspectFoo);
    List<BaseLocalDAO.AspectCreateLambda<? extends RecordTemplate>> aspectCreateLambdas = new ArrayList<>();
    aspectCreateLambdas.add(new BaseLocalDAO.AspectCreateLambda(aspectFoo));
    _ebeanLocalAccessFoo.create(fooUrn, aspectValues, aspectCreateLambdas, auditStamp, null, false);
    try {
      _ebeanLocalAccessFoo.create(fooUrn, aspectValues, aspectCreateLambdas, auditStamp, null, false);
    } catch (Exception duplicateKeyException) {
      assert (duplicateKeyException.getMessage().contains("DuplicateKeyException"));
    }
  }

  @Test
  public void testCreateMultipleAspect() {
    FooUrn fooUrn = makeFooUrn(110);
    AspectFoo aspectFoo = new AspectFoo().setValue("foo");
    AspectBar aspectBar = new AspectBar().setValue("bar");
    AuditStamp auditStamp = makeAuditStamp("actor", _now);
    List<RecordTemplate> aspectValues = new ArrayList<>();
    aspectValues.add(aspectFoo);
    aspectValues.add(aspectBar);
    List<BaseLocalDAO.AspectCreateLambda<? extends RecordTemplate>> aspectCreateLambdas = new ArrayList<>();
    aspectCreateLambdas.add(new BaseLocalDAO.AspectCreateLambda(aspectFoo));
    aspectCreateLambdas.add(new BaseLocalDAO.AspectCreateLambda(aspectBar));
    int numRowsCreated = _ebeanLocalAccessFoo.create(fooUrn, aspectValues, aspectCreateLambdas, auditStamp, null, false);
    // Assert that 1 record is created for asset with FooUrn
    assertEquals(numRowsCreated, 1);
  }

  @Test
  public void testDeleteAll() {
    FooUrn fooUrn = makeFooUrn(201);
    AspectFoo aspectFoo = new AspectFoo().setValue("foo");
    AuditStamp auditStamp = makeAuditStamp("actor", _now);
    List<RecordTemplate> aspectValues = new ArrayList<>();
    aspectValues.add(aspectFoo);
    List<BaseLocalDAO.AspectCreateLambda<? extends RecordTemplate>> aspectCreateLambdas = new ArrayList<>();
    aspectCreateLambdas.add(new BaseLocalDAO.AspectCreateLambda(aspectFoo));
    int createResult = _ebeanLocalAccessFoo.create(fooUrn, aspectValues, aspectCreateLambdas, auditStamp, null, false);
    assertEquals(createResult, 1);
    int numRowsDeleted = _ebeanLocalAccessFoo.softDeleteAsset(fooUrn, false);
    assertEquals(numRowsDeleted, 1);
  }

  // ==================== readDeletionInfoBatch tests ====================

  /**
   * Helper to insert a row into metadata_entity_foo with specific a_status JSON via raw SQL.
   * Uses URN IDs starting at 500+ to avoid collisions with the 0-99 range from setupTest().
   */
  private void insertFooEntityWithStatus(int id, String statusJson, String deletedTs) {
    String urn = "urn:li:foo:" + id;
    String deletedTsClause = deletedTs != null ? "'" + deletedTs + "'" : "NULL";
    String statusClause = statusJson != null ? "'" + statusJson + "'" : "NULL";
    String sql = String.format(
        "INSERT INTO metadata_entity_foo (urn, lastmodifiedon, lastmodifiedby, a_status, deleted_ts) "
            + "VALUES ('%s', NOW(), 'testActor', %s, %s) "
            + "ON DUPLICATE KEY UPDATE a_status = %s, deleted_ts = %s",
        urn, statusClause, deletedTsClause, statusClause, deletedTsClause);
    _server.createSqlUpdate(sql).execute();
  }

  private static String makeStatusJson(boolean removed, String lastModifiedOn) {
    return String.format("{\"aspect\":{\"removed\":%s},\"lastmodifiedon\":\"%s\",\"lastmodifiedby\":\"urn:li:corpuser:testActor\"}",
        removed, lastModifiedOn);
  }

  @Test
  public void testReadDeletionInfoBatchHappyPath() {
    // Given: 3 URNs with known status
    String oldTimestamp = "2025-01-01 00:00:00.000";
    insertFooEntityWithStatus(500, makeStatusJson(true, oldTimestamp), null);
    insertFooEntityWithStatus(501, makeStatusJson(false, oldTimestamp), null);
    insertFooEntityWithStatus(502, makeStatusJson(true, oldTimestamp), null);

    List<FooUrn> urns = new ArrayList<>();
    urns.add(makeFooUrn(500));
    urns.add(makeFooUrn(501));
    urns.add(makeFooUrn(502));

    // When
    Map<FooUrn, EntityDeletionInfo> result = _ebeanLocalAccessFoo.readDeletionInfoBatch(urns, "a_status", false);

    // Then: all 3 returned with correct fields
    assertEquals(result.size(), 3);

    EntityDeletionInfo info500 = result.get(makeFooUrn(500));
    assertNotNull(info500);
    assertNull(info500.getDeletedTs());
    assertTrue(info500.isStatusRemoved());
    assertNotNull(info500.getAspectColumns());
    assertTrue(info500.getAspectColumns().containsKey("a_status"));

    EntityDeletionInfo info501 = result.get(makeFooUrn(501));
    assertNotNull(info501);
    assertFalse(info501.isStatusRemoved());
  }

  @Test
  public void testReadDeletionInfoBatchEmptyList() {
    Map<FooUrn, EntityDeletionInfo> result = _ebeanLocalAccessFoo.readDeletionInfoBatch(Collections.emptyList(), "a_status", false);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testReadDeletionInfoBatchNonExistentUrns() {
    List<FooUrn> urns = Collections.singletonList(makeFooUrn(9998));
    Map<FooUrn, EntityDeletionInfo> result = _ebeanLocalAccessFoo.readDeletionInfoBatch(urns, "a_status", false);
    // Non-existent URNs are simply absent from the map
    assertFalse(result.containsKey(makeFooUrn(9998)));
  }

  @Test
  public void testReadDeletionInfoBatchMixedExistAndNonExist() {
    String statusJson = makeStatusJson(true, "2025-01-01 00:00:00.000");
    insertFooEntityWithStatus(510, statusJson, null);

    List<FooUrn> urns = new ArrayList<>();
    urns.add(makeFooUrn(510));   // exists
    urns.add(makeFooUrn(9997));  // does not exist

    Map<FooUrn, EntityDeletionInfo> result = _ebeanLocalAccessFoo.readDeletionInfoBatch(urns, "a_status", false);

    assertEquals(result.size(), 1);
    assertTrue(result.containsKey(makeFooUrn(510)));
    assertFalse(result.containsKey(makeFooUrn(9997)));
  }

  @Test
  public void testReadDeletionInfoBatchAlreadySoftDeleted() {
    String statusJson = makeStatusJson(true, "2025-01-01 00:00:00.000");
    insertFooEntityWithStatus(520, statusJson, "2025-06-01 00:00:00.000");

    List<FooUrn> urns = Collections.singletonList(makeFooUrn(520));
    Map<FooUrn, EntityDeletionInfo> result = _ebeanLocalAccessFoo.readDeletionInfoBatch(urns, "a_status", false);

    assertEquals(result.size(), 1);
    EntityDeletionInfo info = result.get(makeFooUrn(520));
    assertNotNull(info);
    assertNotNull(info.getDeletedTs());
  }

  // ==================== batchSoftDeleteAssets tests ====================

  @Test
  public void testBatchSoftDeleteAssetsHappyPath() {
    // Given: URNs with Status.removed=true and old lastmodifiedon
    String oldTimestamp = "2025-01-01 00:00:00.000";
    insertFooEntityWithStatus(600, makeStatusJson(true, oldTimestamp), null);
    insertFooEntityWithStatus(601, makeStatusJson(true, oldTimestamp), null);

    List<FooUrn> urns = new ArrayList<>();
    urns.add(makeFooUrn(600));
    urns.add(makeFooUrn(601));

    // Cutoff is after the lastmodifiedon, so these should be eligible
    String cutoffTimestamp = "2026-01-01 00:00:00.000";

    // When
    int rowsAffected = _ebeanLocalAccessFoo.batchSoftDeleteAssets(urns, cutoffTimestamp, "a_status", false);

    // Then: both rows soft-deleted
    assertEquals(rowsAffected, 2);

    // Verify deleted_ts is set in DB
    SqlRow row600 = _server.createSqlQuery("SELECT deleted_ts FROM metadata_entity_foo WHERE urn = 'urn:li:foo:600'").findOne();
    assertNotNull(row600.getTimestamp("deleted_ts"));
    SqlRow row601 = _server.createSqlQuery("SELECT deleted_ts FROM metadata_entity_foo WHERE urn = 'urn:li:foo:601'").findOne();
    assertNotNull(row601.getTimestamp("deleted_ts"));
  }

  @Test
  public void testBatchSoftDeleteAssetsEmptyList() {
    int rowsAffected = _ebeanLocalAccessFoo.batchSoftDeleteAssets(Collections.emptyList(), "2026-01-01 00:00:00.000", "a_status", false);
    assertEquals(rowsAffected, 0);
  }

  @Test
  public void testBatchSoftDeleteAssetsGuardsStatusNotRemoved() {
    // Given: URN with Status.removed=false
    insertFooEntityWithStatus(610, makeStatusJson(false, "2025-01-01 00:00:00.000"), null);

    List<FooUrn> urns = Collections.singletonList(makeFooUrn(610));

    // When: cutoff is in the future (would pass retention check)
    int rowsAffected = _ebeanLocalAccessFoo.batchSoftDeleteAssets(urns, "2026-01-01 00:00:00.000", "a_status", false);

    // Then: guard clause prevents deletion
    assertEquals(rowsAffected, 0);
    SqlRow row = _server.createSqlQuery("SELECT deleted_ts FROM metadata_entity_foo WHERE urn = 'urn:li:foo:610'").findOne();
    assertNull(row.getTimestamp("deleted_ts"));
  }

  @Test
  public void testBatchSoftDeleteAssetsGuardsRetentionNotMet() {
    // Given: URN with Status.removed=true but recent lastmodifiedon
    String recentTimestamp = "2026-03-01 00:00:00.000";
    insertFooEntityWithStatus(620, makeStatusJson(true, recentTimestamp), null);

    List<FooUrn> urns = Collections.singletonList(makeFooUrn(620));

    // When: cutoff is BEFORE the lastmodifiedon (retention window not met)
    int rowsAffected = _ebeanLocalAccessFoo.batchSoftDeleteAssets(urns, "2026-02-01 00:00:00.000", "a_status", false);

    // Then: guard clause prevents deletion
    assertEquals(rowsAffected, 0);
  }

  @Test
  public void testBatchSoftDeleteAssetsGuardsAlreadyDeleted() {
    // Given: URN already soft-deleted
    insertFooEntityWithStatus(630, makeStatusJson(true, "2025-01-01 00:00:00.000"), "2025-06-01 00:00:00.000");

    List<FooUrn> urns = Collections.singletonList(makeFooUrn(630));

    // When
    int rowsAffected = _ebeanLocalAccessFoo.batchSoftDeleteAssets(urns, "2026-01-01 00:00:00.000", "a_status", false);

    // Then: guard clause prevents re-deletion
    assertEquals(rowsAffected, 0);
  }

  @Test
  public void testBatchSoftDeleteAssetsMixedEligibility() {
    // Given: mix of eligible and ineligible URNs
    String oldTimestamp = "2025-01-01 00:00:00.000";
    insertFooEntityWithStatus(640, makeStatusJson(true, oldTimestamp), null);   // eligible
    insertFooEntityWithStatus(641, makeStatusJson(false, oldTimestamp), null);  // ineligible: not removed
    insertFooEntityWithStatus(642, makeStatusJson(true, oldTimestamp), null);   // eligible

    List<FooUrn> urns = new ArrayList<>();
    urns.add(makeFooUrn(640));
    urns.add(makeFooUrn(641));
    urns.add(makeFooUrn(642));

    // When
    int rowsAffected = _ebeanLocalAccessFoo.batchSoftDeleteAssets(urns, "2026-01-01 00:00:00.000", "a_status", false);

    // Then: only 2 eligible rows deleted
    assertEquals(rowsAffected, 2);

    // Verify: 640 and 642 deleted, 641 not
    SqlRow row640 = _server.createSqlQuery("SELECT deleted_ts FROM metadata_entity_foo WHERE urn = 'urn:li:foo:640'").findOne();
    assertNotNull(row640.getTimestamp("deleted_ts"));
    SqlRow row641 = _server.createSqlQuery("SELECT deleted_ts FROM metadata_entity_foo WHERE urn = 'urn:li:foo:641'").findOne();
    assertNull(row641.getTimestamp("deleted_ts"));
    SqlRow row642 = _server.createSqlQuery("SELECT deleted_ts FROM metadata_entity_foo WHERE urn = 'urn:li:foo:642'").findOne();
    assertNotNull(row642.getTimestamp("deleted_ts"));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testBatchSoftDeleteAssetsRejectsInvalidTimestampFormat() {
    List<FooUrn> urns = Collections.singletonList(makeFooUrn(0));
    _ebeanLocalAccessFoo.batchSoftDeleteAssets(urns, "2026-01-01T00:00:00Z", "a_status", false);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testBatchSoftDeleteAssetsRejectsSqlInjection() {
    List<FooUrn> urns = Collections.singletonList(makeFooUrn(0));
    _ebeanLocalAccessFoo.batchSoftDeleteAssets(urns, "'; DROP TABLE metadata_entity_foo; --", "a_status", false);
  }
}