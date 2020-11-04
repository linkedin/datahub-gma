package com.linkedin.metadata.testing;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.extension.ExtendWith;


/**
 * Junit 5 annotation to indicate that this test requires an instance of Elasticsearch to run against.
 *
 * <p>Test classes that have this annotation should also contain one or more public {@link SearchIndex} fields. These
 * can be static or instance variables to control the test life cycle of the index. The extension will populate these
 * fields for you.
 *
 * <p>The {@link ElasticsearchContainerFactory} implementation, which starts and stops the Elasticsearch instance, is
 * loaded via reflection. A class marked with {@link
 * com.linkedin.metadata.testing.ElasticsearchContainerFactory.Implementation} within the {@code
 * com.linkedin.metadata.testing} namespace will be used. See the {@code elasticsearch-dao-integ-testing-docker} module
 * for a good default implementation that uses the <a href="https://www.testcontainers.org/">Testcontainers</a>
 * framework.
 *
 * <pre>
 *   {@code
 * @ElasticsearchIntegrationTest
 * public class ExampleTest {
 *   // Index which is created before any test are run, and is cleaned up after all tests are done.
 *   @SearchIndexType(MySearchDocument.class)
 *   public static SearchIndex<MySearchDocument> perClassIndex;
 *
 *   // Index which is created before each test method and cleaned up after each test method.
 *   @SearchIndexType(MySearchDocument.class)
 *   public SearchIndex<MySearchDocument> perMethodIndex;
 *
 *   @BeforeEach
 *   public void setUpIndex() {
 *     perMethodIndex.setSettingsAndMappings(/* load json file * /);
 *   }
 *
 *   @Test
 *   public void example() {
 *     // given
 *     final BarSearchDocument searchDocument = new BarSearchDocument().setUrn(new BarUrn(42));
 *
 *     // when
 *     _searchIndex.getWriteDao().upsertDocument(searchDocument, "mydoc");
 *     _searchIndex.getRequestContainer().flushAndSettle();
 *
 *     // then
 *     assertThat(_searchIndex.getRequestContainer()).wroteOnlyDocuments("mydoc");
 *   }
 * }
 *   }
 * </pre>
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(ElasticsearchIntegrationTestExtension.class)
@Inherited
public @interface ElasticsearchIntegrationTest {
}
