package com.linkedin.metadata.dao.search;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.UrnArray;
import com.linkedin.data.DataList;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.dao.utils.QueryUtils;
import com.linkedin.metadata.query.AggregationMetadataArray;
import com.linkedin.metadata.query.Condition;
import com.linkedin.metadata.query.Criterion;
import com.linkedin.metadata.query.CriterionArray;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.MatchedField;
import com.linkedin.metadata.query.SearchResultMetadata;
import com.linkedin.metadata.query.SortCriterion;
import com.linkedin.metadata.query.SortOrder;
import com.linkedin.testing.EntityDocument;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.metadata.dao.utils.QueryUtils.EMPTY_FILTER;
import static com.linkedin.metadata.dao.utils.QueryUtils.newFilter;
import static com.linkedin.testing.TestUtils.makeUrn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class ESSearchDAOTest {

  private ESSearchDAO<EntityDocument> _searchDAO;
  private ESAutoCompleteQueryForHighCardinalityFields _esAutoCompleteQuery;
  private TestSearchConfig _testSearchConfig;

  private static String loadJsonFromResource(String resourceName) throws IOException {
    String jsonStr = IOUtils.toString(ClassLoader.getSystemResourceAsStream(resourceName), StandardCharsets.UTF_8);
    return jsonStr.replaceAll("\\s+", "");
  }

  @BeforeMethod
  public void setup() throws Exception {
    _testSearchConfig = new TestSearchConfig();
    _searchDAO = new ESSearchDAO(null, EntityDocument.class, _testSearchConfig);
    _esAutoCompleteQuery = new ESAutoCompleteQueryForHighCardinalityFields(_testSearchConfig);
  }

  @Test
  public void testDecoupleArrayToGetSubstringMatch() throws Exception {
    // Test empty fieldVal
    List<String> fieldValList = Collections.emptyList();
    String searchInput = "searchInput";
    List<String> searchResult =
        ESAutoCompleteQueryForHighCardinalityFields.decoupleArrayToGetSubstringMatch(fieldValList, searchInput);
    assertEquals(searchResult.size(), 0);

    // Test non-list fieldVal
    String fieldValString = "fieldVal";
    searchInput = "searchInput";
    searchResult =
        ESAutoCompleteQueryForHighCardinalityFields.decoupleArrayToGetSubstringMatch(fieldValString, searchInput);
    assertEquals(searchResult.size(), 1);

    // Test list fieldVal with no match
    fieldValList = Arrays.asList("fieldVal1", "fieldVal2", "fieldVal3");
    searchInput = "searchInput";
    searchResult =
        ESAutoCompleteQueryForHighCardinalityFields.decoupleArrayToGetSubstringMatch(fieldValList, searchInput);
    assertEquals(searchResult.size(), 0);

    // Test list fieldVal with single match
    searchInput = "val1";
    searchResult =
        ESAutoCompleteQueryForHighCardinalityFields.decoupleArrayToGetSubstringMatch(fieldValList, searchInput);
    assertEquals(searchResult.size(), 1);
    assertEquals(searchResult.get(0), "fieldVal1");

    // Test list fieldVal with multiple match
    searchInput = "val";
    searchResult =
        ESAutoCompleteQueryForHighCardinalityFields.decoupleArrayToGetSubstringMatch(fieldValList, searchInput);
    assertEquals(searchResult.size(), 3);
    assertTrue(searchResult.equals(fieldValList));
  }

  @Test
  public void testGetSuggestionList() throws Exception {
    SearchHits searchHits = mock(SearchHits.class);
    SearchHit hit1 = makeSearchHit(1);
    SearchHit hit2 = makeSearchHit(2);
    SearchHit hit3 = makeSearchHit(3);
    when(searchHits.getHits()).thenReturn(new SearchHit[]{hit1, hit2, hit3});
    when(searchHits.getTotalHits()).thenReturn(new TotalHits(10L, TotalHits.Relation.EQUAL_TO));
    SearchResponse searchResponse = mock(SearchResponse.class);
    when(searchResponse.getHits()).thenReturn(searchHits);

    StringArray res = _esAutoCompleteQuery.getSuggestionList(searchResponse, "name", "test", 2);

    assertEquals(res.size(), 2);
  }

  @Test
  public void testExtractSearchResultMetadata() throws Exception {
    // Test: no aggregations in search response
    SearchHits searchHits1 = mock(SearchHits.class);
    when(searchHits1.getTotalHits()).thenReturn(new TotalHits(10L, TotalHits.Relation.EQUAL_TO));
    SearchResponse searchResponse1 = mock(SearchResponse.class);
    when(searchResponse1.getHits()).thenReturn(searchHits1);
    assertEquals(_searchDAO.extractSearchResultMetadata(searchResponse1), getDefaultSearchResultMetadata());

    // Test: urn field exists in search document
    SearchHits searchHits2 = mock(SearchHits.class);
    SearchHit hit1 = makeSearchHit(1);
    SearchHit hit2 = makeSearchHit(2);
    when(searchHits2.getHits()).thenReturn(new SearchHit[]{hit1, hit2});
    SearchResponse searchResponse2 = mock(SearchResponse.class);
    when(searchResponse2.getHits()).thenReturn(searchHits2);
    UrnArray urns = new UrnArray(Arrays.asList(makeUrn(1), makeUrn(2)));
    assertEquals(_searchDAO.extractSearchResultMetadata(searchResponse2),
        getDefaultSearchResultMetadata().setUrns(urns));

    // Test: urn field does not exist in one search document, exists in another
    SearchHits searchHits3 = mock(SearchHits.class);
    SearchHit hit3 = mock(SearchHit.class);
    when(hit3.getFields().get("urn")).thenReturn(null);
    SearchHit hit4 = makeSearchHit(1);
    when(searchHits3.getHits()).thenReturn(new SearchHit[]{hit3, hit4});
    SearchResponse searchResponse3 = mock(SearchResponse.class);
    when(searchResponse3.getHits()).thenReturn(searchHits3);
    assertThrows(RuntimeException.class, () -> _searchDAO.extractSearchResultMetadata(searchResponse3));

    // Test: highlights exist in one search document, and doesn't exist in another
    SearchHits searchHits4 = mock(SearchHits.class);
    SearchHit hit5 = makeSearchHit(5);
    SearchHit hit6 = makeSearchHit(6, ImmutableMap.of("field1", ImmutableList.of("fieldValue1")));
    SearchHit hit7 = makeSearchHit(7, ImmutableMap.of("field1", ImmutableList.of("fieldValue1"), "field2",
        ImmutableList.of("fieldValue21", "fieldValue22")));
    SearchHit hit8 = makeSearchHit(8, ImmutableMap.of("field1", ImmutableList.of("fieldValue1"), "field1.delimited",
        ImmutableList.of("fieldValue1", "fieldValue2")));
    when(searchHits4.getHits()).thenReturn(new SearchHit[]{hit5, hit6, hit7, hit8});
    SearchResponse searchResponse4 = mock(SearchResponse.class);
    when(searchResponse4.getHits()).thenReturn(searchHits4);
    SearchResultMetadata searchResultMetadata = _searchDAO.extractSearchResultMetadata(searchResponse4);
    assertEquals(searchResultMetadata.getMatches().size(), 4);
    assertEquals(extractMatchedFields(searchResultMetadata, 0), ImmutableList.of());
    assertEquals(extractMatchedFields(searchResultMetadata, 1),
        ImmutableList.of(new MatchedField().setName("field1").setValue("fieldValue1")));
    List<MatchedField> matchesHit7 = extractMatchedFields(searchResultMetadata, 2);
    assertEquals(matchesHit7.size(), 3);
    assertEquals(matchesHit7.get(0), new MatchedField().setName("field1").setValue("fieldValue1"));
    // Note order of values are not deterministic
    assertEquals(matchesHit7.get(1).getName(), "field2");
    assertEquals(matchesHit7.get(2).getName(), "field2");
    List<MatchedField> matchesHit8 = extractMatchedFields(searchResultMetadata, 3);
    assertEquals(matchesHit8.size(), 2);
    // Note order of values are not deterministic
    assertEquals(matchesHit8.get(0).getName(), "field1");
    assertEquals(matchesHit8.get(1).getName(), "field1");
  }

  @Test
  public void testBuildDocumentsDataMap() {
    Map<String, Object> sourceData = new HashMap<>();
    sourceData.put("field1", "val1");
    sourceData.put("field2", null);
    ArrayList<String> arrayList = new ArrayList<>(Arrays.asList("foo", "bar"));
    sourceData.put("field3", arrayList);
    DataMap dataMap = new DataMap();
    dataMap.put("field1", "val1");
    dataMap.put("field3", new DataList(arrayList));
    assertEquals(_searchDAO.buildDocumentsDataMap(sourceData), dataMap);
  }

  @Test
  public void testFilteredQueryWithTermsFilter() throws IOException {
    int from = 0;
    int size = 10;
    Filter filter = newFilter(ImmutableMap.of("key1", "value1, value2 ", "key2", "value3", "key3", " "));
    SortCriterion sortCriterion = new SortCriterion().setOrder(SortOrder.ASCENDING).setField("urn");

    // Test 1: sort order provided
    SearchRequest searchRequest = _searchDAO.getFilteredSearchQuery(filter, sortCriterion, from, size);
    assertEquals(searchRequest.source().toString(), loadJsonFromResource("SortByUrnTermsFilterQuery.json"));
    assertEquals(searchRequest.indices(), new String[]{_testSearchConfig.getIndexName()});

    // Test 2: no sort order provided, default is used.
    searchRequest = _searchDAO.getFilteredSearchQuery(filter, null, from, size);
    assertEquals(searchRequest.source().toString(), loadJsonFromResource("DefaultSortTermsFilterQuery.json"));
    assertEquals(searchRequest.indices(), new String[]{_testSearchConfig.getIndexName()});

    // Test 3: empty request map provided
    searchRequest = _searchDAO.getFilteredSearchQuery(EMPTY_FILTER, sortCriterion, from, size);
    assertEquals(searchRequest.source().toString(), loadJsonFromResource("EmptyFilterQuery.json"));
    assertEquals(searchRequest.indices(), new String[]{_testSearchConfig.getIndexName()});
  }

  @Test
  public void testFilteredQueryWithUrnValue() throws IOException {
    int from = 0;
    int size = 10;
    Filter filter = newFilter(ImmutableMap.of("key1", "value1, value2 ", "key2", "urn:li:entity:(foo,bar,baz)"));
    SortCriterion sortCriterion = new SortCriterion().setOrder(SortOrder.ASCENDING).setField("urn");

    SearchRequest searchRequest = _searchDAO.getFilteredSearchQuery(filter, sortCriterion, from, size);
    assertEquals(searchRequest.source().toString(), loadJsonFromResource("UrnFilterQuery.json"));
    assertEquals(searchRequest.indices(), new String[]{_testSearchConfig.getIndexName()});
  }

  @Test
  public void testFilteredQueryWithRangeFilter() throws IOException {
    int from = 0;
    int size = 10;
    final Filter filter1 = new Filter().setCriteria(new CriterionArray(
        Arrays.asList(new Criterion().setField("field_gt").setValue("100").setCondition(Condition.GREATER_THAN),
            new Criterion().setField("field_gte").setValue("200").setCondition(Condition.GREATER_THAN_OR_EQUAL_TO),
            new Criterion().setField("field_lt").setValue("300").setCondition(Condition.LESS_THAN),
            new Criterion().setField("field_lte").setValue("400").setCondition(Condition.LESS_THAN_OR_EQUAL_TO))));
    SortCriterion sortCriterion = new SortCriterion().setOrder(SortOrder.ASCENDING).setField("urn");

    SearchRequest searchRequest = _searchDAO.getFilteredSearchQuery(filter1, sortCriterion, from, size);
    assertEquals(searchRequest.source().toString(), loadJsonFromResource("RangeFilterQuery.json"));
    assertEquals(searchRequest.indices(), new String[]{_testSearchConfig.getIndexName()});
  }

  @Test
  public void testFilteredQueryUnsupportedCondition() {
    int from = 0;
    int size = 10;
    final Filter filter2 = new Filter().setCriteria(new CriterionArray(Arrays.asList(
        new Criterion().setField("field_contain").setValue("value_contain").setCondition(Condition.START_WITH))));
    SortCriterion sortCriterion = new SortCriterion().setOrder(SortOrder.ASCENDING).setField("urn");
    assertThrows(UnsupportedOperationException.class,
        () -> _searchDAO.getFilteredSearchQuery(filter2, sortCriterion, from, size));
  }

  @Test
  public void testPreferenceInSearchQuery() {
    String input = "test";
    Map<String, String> requestMap = Collections.singletonMap("key", "value");
    Filter filter = QueryUtils.newFilter(requestMap);
    String preference = "urn:li:servicePrincipal:appName";
    SearchRequest searchRequest = _searchDAO.constructSearchQuery(input, filter, null, preference, 0, 10);
    assertEquals(searchRequest.preference(), preference);
  }

  @Test
  public void testDefaultMaxTermBucketSize() {
    String facetFieldName = "value";
    Filter filter = QueryUtils.newFilter(Collections.singletonMap(facetFieldName, "dummy"));
    SearchRequest searchRequest = _searchDAO.constructSearchQuery("dummy", filter, null, null, 0, 10);
    assertEquals(searchRequest.source().aggregations().getAggregatorFactories().iterator().next(),
        AggregationBuilders.terms(facetFieldName).field(facetFieldName).size(100));
  }

  @Test
  public void testSetMaxTermBucketSize() {
    String facetFieldName = "value";
    Filter filter = QueryUtils.newFilter(Collections.singletonMap(facetFieldName, "dummy"));
    _searchDAO.setMaxTermBucketSize(5);
    SearchRequest searchRequest = _searchDAO.constructSearchQuery("dummy", filter, null, null, 0, 10);
    assertEquals(searchRequest.source().aggregations().getAggregatorFactories().iterator().next(),
        AggregationBuilders.terms(facetFieldName).field(facetFieldName).size(5));
  }

  private static SearchHit makeSearchHit(int id) {
    SearchHit hit = mock(SearchHit.class);
    Map<String, Object> sourceMap = new HashMap<>();
    sourceMap.put("urn", makeUrn(id).toString());
    sourceMap.put("name", "test" + id);
    when(hit.getSourceAsMap()).thenReturn(sourceMap);
    return hit;
  }

  private static SearchHit makeSearchHit(int id, Map<String, List<String>> highlightedFields) {
    SearchHit hit = mock(SearchHit.class);
    Map<String, Object> sourceMap = new HashMap<>();
    sourceMap.put("urn", makeUrn(id).toString());
    sourceMap.put("name", "test" + id);
    when(hit.getSourceAsMap()).thenReturn(sourceMap);
    when(hit.getHighlightFields()).thenReturn(highlightedFields.entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> new HighlightField(entry.getKey(),
            entry.getValue().stream().map(Text::new).toArray(Text[]::new)))));
    return hit;
  }

  private static SearchResultMetadata getDefaultSearchResultMetadata() {
    return new SearchResultMetadata().setSearchResultMetadatas(new AggregationMetadataArray())
        .setUrns(new UrnArray());
  }

  private static List<MatchedField> extractMatchedFields(SearchResultMetadata searchResultMetadata, int index) {
    return new ArrayList<>(searchResultMetadata.getMatches().get(index).getMatchedFields());
  }
}
