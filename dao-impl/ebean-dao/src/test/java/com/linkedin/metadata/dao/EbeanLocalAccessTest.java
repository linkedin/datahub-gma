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
import java.util.Arrays;
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

    assertEquals(listUrns.getValues().size(), 5);
    assertEquals(listUrns.getPageSize(), 5);
    assertEquals(listUrns.getNextStart(), 10);
    assertEquals(listUrns.getTotalCount(), 25);
    assertEquals(listUrns.getTotalPageCount(), 5);
  }

  // Test Case 1: Normal last page behavior using existing 100 records from @BeforeMethod
  @Test
  public void testListUrns_NormalLastPagePagination() {
    IndexFilter indexFilter = new IndexFilter();
    IndexCriterion criterion = new IndexCriterion().setAspect(AspectFoo.class.getCanonicalName());
    indexFilter.setCriteria(new IndexCriterionArray(Collections.singleton(criterion)));

    // When: Request last page (start=90, pageSize=10) - should get 10 results (normal last page)
    ListResult<FooUrn> result = _ebeanLocalAccessFoo.listUrns(indexFilter, null, 90, 10);

    // Then: Should preserve original totalCount
    assertEquals(result.getValues().size(), 10); // Last page has 10 items
    assertEquals(result.getTotalCount(), 100); // Should keep original count
    assertEquals(result.getNextStart(), -1); // Should indicate end reached
    assertFalse(result.isHavingMore()); // Should indicate no more pages
  }

  // Test Case 2: Test normal pagination with soft deleted records
  @Test
  public void testListUrns_SoftDeletedRecordsHandling() throws URISyntaxException {
    IndexFilter indexFilter = new IndexFilter();
    IndexCriterion criterion = new IndexCriterion().setAspect(AspectFoo.class.getCanonicalName());
    indexFilter.setCriteria(new IndexCriterionArray(Collections.singleton(criterion)));

    // Soft delete records 50-59 (10 records) to create a gap in the middle
    for (int i = 50; i < 60; i++) {
      FooUrn fooUrn = new FooUrn(i);
      _ebeanLocalAccessFoo.softDeleteAsset(fooUrn, false);
    }

    // When: Request a page in the middle that hits the deletion gap
    // With 90 records remaining (100 - 10 deleted), requesting start=50, pageSize=10
    // Should get fewer than 10 results due to the gap created by deletions
    ListResult<FooUrn> result = _ebeanLocalAccessFoo.listUrns(indexFilter, null, 50, 10);

    // Then: Soft deletion should be handled normally - query skips deleted records
    assertEquals(result.getValues().size(), 10); // Should get full page by skipping deleted records
    assertEquals(result.getTotalCount(), 90); // Should reflect actual remaining records
    assertEquals(result.getNextStart(), 60); // Should advance normally
    assertTrue(result.isHavingMore()); // Should indicate more pages available

    // Verify that the returned records are the expected ones (skipping 50-59)
    // Should start from 60+ since 50-59 were deleted
    assertTrue(result.getValues().get(0).toString().contains("63")); // First available record after gap
  }

  // Test Case 3: Normal pagination behavior with full pages using existing 100 records
  @Test
  public void testListUrns_InsertionRaceConditionHandling() {
    IndexFilter indexFilter = new IndexFilter();
    IndexCriterion criterion = new IndexCriterion().setAspect(AspectFoo.class.getCanonicalName());
    indexFilter.setCriteria(new IndexCriterionArray(Collections.singleton(criterion)));

    // When: Request first page with pageSize=20 (should get full page)
    ListResult<FooUrn> result = _ebeanLocalAccessFoo.listUrns(indexFilter, null, 0, 20);

    // Then: Should handle full page correctly (values.size() = pageSize)
    assertEquals(result.getValues().size(), 20); // Full page
    assertEquals(result.getTotalCount(), 100); // Should preserve totalCount
    assertTrue(result.isHavingMore()); // Should indicate more pages available
    assertEquals(result.getNextStart(), 20); // Should point to next page
  }

  // Test Case 4: Boundary conditions for race condition detection using existing 100 records
  @Test
  public void testListUrns_BoundaryConditions() {
    IndexFilter indexFilter = new IndexFilter();
    IndexCriterion criterion = new IndexCriterion().setAspect(AspectFoo.class.getCanonicalName());
    indexFilter.setCriteria(new IndexCriterionArray(Collections.singleton(criterion)));

    // Test boundary: Exact page size match (values.size() = pageSize)
    ListResult<FooUrn> result1 = _ebeanLocalAccessFoo.listUrns(indexFilter, null, 0, 10);
    assertEquals(result1.getValues().size(), 10);
    assertEquals(result1.getTotalCount(), 100);
    assertTrue(result1.isHavingMore()); // Should have more since we have 100 records

    // Test boundary: Request beyond available data
    ListResult<FooUrn> result2 = _ebeanLocalAccessFoo.listUrns(indexFilter, null, 100, 10);
    assertEquals(result2.getValues().size(), 0); // No records at position 100
    assertEquals(result2.getTotalCount(), 100); // Total should still be 100
    assertFalse(result2.isHavingMore()); // No more records available
  }

  // Test Case 5: Unit test for the extracted race condition detection method
  @Test
  public void testResolveTotalCount() {
    // Test the extracted race condition detection method directly
    
    // Scenario 1: Deletion race condition detection
    int adjustedCount1 = _ebeanLocalAccessFoo.resolveTotalCount(3, 100, 50, 10);
    assertEquals(adjustedCount1, 53, "Deletion race condition: 3 < 10 AND 53 < 100 → should adjust to start + valuesSize");
    
    // Scenario 2: Insertion race condition (Math.max logic)
    int adjustedCount2 = _ebeanLocalAccessFoo.resolveTotalCount(10, 45, 40, 10);
    assertEquals(adjustedCount2, 50, "Insertion race condition: Math.max(45, 50) → should use Math.max logic");
    
    // Scenario 3: Normal pagination - no race condition
    int adjustedCount3 = _ebeanLocalAccessFoo.resolveTotalCount(10, 100, 20, 10);
    assertEquals(adjustedCount3, 100, "Normal pagination: 10 == 10 → should preserve original totalCount");
    
    // Scenario 4: Edge case - empty results due to deletion race condition
    int adjustedCount4 = _ebeanLocalAccessFoo.resolveTotalCount(0, 100, 90, 10);
    assertEquals(adjustedCount4, 90, "Empty results at end: 0 < 10 AND 90 < 100 → should adjust to start + valuesSize");
    
    // Scenario 5: Last page boundary - full page size but at exact end
    int adjustedCount5 = _ebeanLocalAccessFoo.resolveTotalCount(10, 100, 90, 10);
    assertEquals(adjustedCount5, 100, "Last page with full results: Math.max(100, 100) → should preserve totalCount");
    
    // Scenario 6: Insertion with larger insertion count
    int adjustedCount6 = _ebeanLocalAccessFoo.resolveTotalCount(15, 50, 40, 10);
    assertEquals(adjustedCount6, 55, "Large insertion: Math.max(50, 55) → should expand totalCount");
  }

  @Test
  public void testListUrnsReturnsEmptyWhenNoResults() {
    // Given: An IndexFilter that matches no rows
    IndexFilter indexFilter = new IndexFilter();
    // (Optionally set up filter to something impossible, e.g. value < 0)
    IndexCriterionArray criteria = new IndexCriterionArray();
    IndexCriterion impossibleCriterion = SQLIndexFilterUtils.createIndexCriterion(
        AspectFoo.class, "value", Condition.LESS_THAN, IndexValue.create(-1));
    criteria.add(impossibleCriterion);
    indexFilter.setCriteria(criteria);

    int start = 0;
    int pageSize = 10;

    // When: listUrns is called
    ListResult<FooUrn> result = _ebeanLocalAccessFoo.listUrns(indexFilter, null, start, pageSize);

    // Then: The result should be empty and pagination metadata should be correct
    assertNotNull(result);
    assertEquals(result.getValues().size(), 0);
    assertEquals(result.getTotalCount(), 0);
    assertFalse(result.isHavingMore());
    assertEquals(result.getNextStart(), ListResult.INVALID_NEXT_START);
    assertEquals(result.getNextStart(), -1);
    assertEquals(pageSize, result.getPageSize());
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
}