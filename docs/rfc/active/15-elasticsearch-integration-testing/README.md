- Start Date: 2020-10-19
- RFC PR: https://github.com/linkedin/datahub-gma/pull/15
- Discussion Issue: n/a
- Implementation PR(s): TODO

# Elasticsearch Integration Testing

## Summary

GMA has an [Elasticsearch](https://www.elastic.co/) implementation of its search DAO.

Today, any changes made to settings / mappings / queries must be done using manual testing. This can lead to brittle
infrastructure and unintentional bugs. We should add a small framework to make it easy to write integration tests of GMA
with Elasticsearch.

## Basic example

In DataHub, we have several entities (Datasets, Corp Users, etc) that use Elasticsearch. It would be nice to have
integration tests for these to verify that the settings / mappings / queries are well structured and work as intended.

An imaginary test may look something like this:

```java
@ElasticsearchIntegrationTest
public class DatasetSearchIntegrationTest {
  @SearchIndexType(DatasetDocument.class)
  @SearchIndexSettings("datasetSettings.json")
  @SearchIndexMappings("datasetMappings.json")
  public SearchIndex<DatasetDocument> searchIndex;

  @Test
  public void canWriteData() {
    // given
    final DatasetDocument document = new DatasetDocument() /* more initialization of data */;

    // when
    searchIndex.getWriterDao().upsertDocument(document, "myid");
    searchIndex.flushAndSettle();

    // then
    assertThat(searchIndex).wroteDocuments("myid");
  }

  @Test
  public void canReadData() {
    // given
    final DatasetDocument document = new DatasetDocument() /* more initialization of data */;

    // when
    searchIndex.getWriterDao().upsertDocument(document, "myid");
    searchIndex.flushAndSettle();
    final List<DatasetDocument> allDocuments = searchIndex.readAllDocuments();

    // then
    assertThat(allDocuments).containsExactly(document);
  }

  /*
   * The above are simple sanity tests (may not hurt even to have some common code to create/run those tests!). Users
   * should also write more specific tests to verify their settings / mappings / queries / rankings, etc.
   */
}
```

## Motivation

To increase test coverage and reliability of GMA's search stack.

## Requirements

- Must be able to easily start an Elasticsearch instance for testing at test start up, and then clean it up at test tear
  down, where start up is before _all_ tests, and tear down is after _all_ tests. Starting/stopping can be expensive and
  so the number of times it is done should be minimized.
- Must be able to easily create indexes for testing on the cluster. These ideally are given unique names for each test
  run, and should be cleaned up when the test is over.
- Must be able to easily create search DAO objects for use with GMA.
- Easy to use assertions for common assertions.

### Extensibility

The design should be open enough so that the logic to start and stop the cluster is abstracted away.

We can ship a default implementation that uses [Testcontainers](https://www.testcontainers.org/) (which, in turn, uses
docker), however this may not work for everyone (e.g. internally at LinkedIn), so we should make it easy to provide some
other mechanism to control the cluster.

## Non-Requirements

Works with any test framework or assertion library.

Being opinionated is probably fine here, if it means it works well. JUnit 5 is very easy to write extensions for. While
we use TestNG for the rest of our tests, it is probably fine to mix and match here if it means cleaner code.

Similarly, we can use assertj for assertions.

## Detailed design

As stated above, we will:

- Use JUnit 5.
- Use assertj.
- Provide a _default_ implementation that uses the Testcontainers framework.

We can add two new modules to the project:

1. `testing/elasticsearch-dao-integ-testing`: Contains JUnit 5 extension and interfaces, as well as assertj assertions.
   Tries to load the implementation via reflection (can look for some specific annotation in a specific namespace).
2. `testing/elasticsearch-dao-integ-testing-testcontainers`: Contains the Testcontainers implementation.

Note that as the second module will depend on the first, users need only add the second module as a dependency, if they
plan to use the default implementation.

JUnit has an [extension model](https://junit.org/junit5/docs/current/user-guide/#extensions) we can use. It gives us
hooks into the test life cycle, which we can use to start/stop Elasticsearch, as well as create/delete indexes.

### `SearchIndex`

Most of the interactions with the index can be done via the new `SearchIndex` interface. This will be populated via
reflection on fields in test classes annotated with `@SearchIndexType(Class)` (required). The `Class` argument is the
document type, which will in turn be used to create DAOs.

We can also add the optional `@SearchIndexSettings` and `@SearchIndexMappings(String)` annotations, to create indexes
with the given settings/mappings. We can also provide setters for these on `SearchIndex`, so the annotations are
optional.

A stub for `SearchIndex`:

```java
class SearchIndex<DOCUMENT> {
  ESSearchDAO<DOCUMENT> createReaderDAO(BaseConfig<DOCUMENT> config);
  Collection<Document> readAllDocuments();
  ESBulkWriterDAO<DOCUMENT> getWriterDAO();
  BulkRequestsContainer getRequestContainer();
  ElasticsearchConnection getConnection();
  void setSettings(String settingsJson);
  void setMappings(String mappingsJson);
}
```

With helper class stubs:

```java
/**
 * Keeps track of all bulk requests made to Elasticsearch while testing.
 */
class BulkRequestsContainer {
  Colleciton<BulkRequest> getAllRequests();
  Colleciton<BulkRequest> getExecutingRequests();
  Map<BulkRequest, BulkRequest> getResponses();
  Map<BulkRequest, Throwable> getErrors();
}
```

```java
/**
 * Simple wrapper around Elasticsearch client objects.
 */
class ElasticsearchConnection {
  RestHighLevelClient getRestHighLevelClient();
  TransportClient getTransportClient();
}
```

### Common Assertions

Commonly used assertions that we should provide helpers for will include:

- Asserting the given bulk request(s) were successful.
- Asserting the index contains documents with the given id(s).
- Asserting the index contains documents with the given id(s) and content(s).

We _could_ also provide assertions to assert that a query for on a `ESSearchDAO` returns some results, however, it may
be clearer to just query the DAO and assert the results in the test. We should debate this before finalizing the list of
assertions we wish to provide.

```java
@Test
public void withAssertionLibrary() {
  // This might actually be less clear than without assertions below? At least code wise. We could provide a much
  // clearer error message in the event of a failure, however.
  assertThat(searchIndex)
    .usingConfig(config)
    .search("stuff")
    .containsExactly(someDocument);

  // Alternative to assert directly on the DAO, might be a bit cleaner?
  assertThat(searchIndex.createReaderDAO(config)).search("stuff").containsExactly(someDocument);
}

@Test
public void withoutAssertionLibrary() {
  // given
  final ESSearchDAO<Document> dao = searchIndex.createReaderDAO(config);

  // when
  final SearchResult<DOCUMENT> result = dao.search("stuff", null, null, 0, 1);

  // then
  // Code is somewhat clearer here, however the failure will be less clear, as it will not include the query that was
  // performed.
  assertThat(result.getDocumentList()).containsExactly(someDocument);
}
```

## How we teach this

We can add an extensive readme to the new modules, as well as create robust java doc documentation on the classes.

## Drawbacks

- Works only with JUnit 5.
- Assertions provided only with assertj.
- **Works only with Elasticsearch 5.** This is the biggest drawback here. This design has not considered an
  Elasticsearch 6+ (ideallly 7) design. But, the underlying DAOs are tightly coupled to Elasticsearch 5 anyway. If/when
  we move to Elasticsearch 6/7, we may just have to copy and paste this library.

## Alternatives

I considered just suggesting users use Testcontainers directly, however, there is a lot of other boilerplate with
respect to Elasticsearch and GMA that would also be nice to provide common functionality for in tests.

## Rollout / Adoption Strategy

This is a new testing feature, and we can start using it for the existing indexes in DataHub. New indexes can add tests
from the outset.

## Future Work

Basic "sanity" tests (example above) might be nice to also make common so users don't need to copy and paste.

## Unresolved questions

See detailed design on asserts. Open question of if we should provide assertions for querying.
