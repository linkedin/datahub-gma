# Elasticsearch Integration Testing

This module includes a framework to write integration tests against Elasticsearch using GMA, Junit 5, and assertj.

Status: **In Development**.

## Creating a new integration test

1. Add this module as a dependency, as well as an [implementation](#implementations).
2. Next, create a new Junit 5 test class and add the `@ElasticsearchIntegrationTest` annotation. Adding this annotation
   should start and stop an Elasticsearch instance for you during your test.
3. Add fields to your test of type `SearchIndex`, and annotate them with `@SearchIndexType`. Static `SearchIndex` fields
   will reuse the index across the entire class; instance fields will create a new index per test.
4. Set the settings / mappings of your index, either with the `@SearchIndexSettings` and `@SearchIndexMappings`
   annotations, or via methods on `SearchIndex`.
5. Begin testing by data via `SearchIndex#getWriteDao` and asserting various queries.

```java
import com.linkedin.metadata.testing.ElasticsearchIntegrationTest;
import com.linkedin.metadata.testing.SearchIndex;
import com.linkedin.metadata.testing.annotations.SearchIndexMappings;
import com.linkedin.metadata.testing.annotations.SearchIndexSettings;
import com.linkedin.metadata.testing.annotations.SearchIndexType;
import org.junit.jupiter.api.Test;

@ElasticsearchIntegrationTest // 2
public class ExampleTest {
  @SearchIndexType(MySearchDocument.class) // 3
  @SearchIndexSettings("/settings.json") // 4
  @SearchIndexMappings("/mappings.json") // 4
  SearchIndex<MySearchDocument> index; // 3

  @Test
  public void example() {
    // 5
    // given
    final MySearchDocument mySearchDocument = new MySearchDocument();
    index.getWriteDao().upsertDocument(mySearchDocument, "myId");
    index.getRequestContainer().flushAndSettle();

    // TODO finish example once we've decided on how asserts look.
  }
}
```

## Implementations

This module does not ship with code to actually start and stop Elasticsearch. It looks for any class in the
`com.linkedin.metadata.testing` package annotated with `ElasticsearchContainerFactory.@Implementation`, and implements
`ElasticsearchContainerFactory`, to use as the implementation to start / stop Elasticsearch.

GMA ships with a default implementation in the `elasticsearch-dao-integ-testing-docker` module, which uses the
[Testcontainers](http://testcontainers.org) to start / stop Elasticsearch using docker. You are also free to write your
own implementation.
