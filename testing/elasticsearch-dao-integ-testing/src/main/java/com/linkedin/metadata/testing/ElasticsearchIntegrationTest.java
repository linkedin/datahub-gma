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
 * <p>See the README file in this module for more information.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(ElasticsearchIntegrationTestExtension.class)
@Inherited
public @interface ElasticsearchIntegrationTest {
}
