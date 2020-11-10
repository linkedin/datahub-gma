package com.linkedin.metadata.testing;

import com.linkedin.metadata.testing.annotations.SearchIndexMappings;
import com.linkedin.metadata.testing.annotations.SearchIndexSettings;
import com.linkedin.metadata.testing.annotations.SearchIndexType;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.util.ClassFilter;
import org.junit.platform.commons.util.ReflectionUtils;


/**
 * JUnit 5 extension to start an Elasticsearch instance and create indexes for testing with GMA.
 *
 * <p>See {@link ElasticsearchIntegrationTest}.
 */
final class ElasticsearchIntegrationTestExtension
    implements BeforeAllCallback, AfterAllCallback, BeforeEachCallback, AfterEachCallback {
  private static final ExtensionContext.Namespace NAMESPACE =
      ExtensionContext.Namespace.create(ElasticsearchIntegrationTestExtension.class);

  private final static String CONTAINER_FACTORY = "containerFactory";
  private final static String CONNECTION = "connection";
  private final static String STATIC_INDICIES = "staticIndicies";
  private final static String INDICIES = "indicies";

  @Override
  public void beforeAll(ExtensionContext context) throws Exception {
    final Class<?> testClass = context.getTestClass()
        .orElseThrow(() -> new ExtensionConfigurationException(
            "ElasticSearchIntegrationTestExtension is only supported for classes."));

    final ExtensionContext.Store store = context.getStore(NAMESPACE);

    final ElasticsearchContainerFactory factory = getContainerFactory();
    final ElasticsearchConnection connection = factory.start();

    store.put(CONTAINER_FACTORY, factory);
    store.put(CONNECTION, connection);

    final List<Field> fields = ReflectionUtils.findFields(testClass, field -> {
      return ReflectionUtils.isStatic(field) && ReflectionUtils.isPublic(field) && ReflectionUtils.isNotFinal(field)
          && SearchIndex.class.isAssignableFrom(field.getType());
    }, ReflectionUtils.HierarchyTraversalMode.TOP_DOWN);

    final SearchIndexFactory indexFactory = new SearchIndexFactory(connection);
    final List<SearchIndex<?>> indices = createIndices(indexFactory, context.getRequiredTestClass(), fields,
        fieldName -> String.format("%s_%s_%s", fieldName, testClass.getSimpleName(), System.currentTimeMillis()));
    store.put(STATIC_INDICIES, indices);
  }

  private List<SearchIndex<?>> createIndices(@Nonnull SearchIndexFactory indexFactory, @Nonnull Object testInstance,
      @Nonnull List<Field> fields, @Nonnull Function<String, String> nameFn) throws Exception {
    final List<SearchIndex<?>> indices = new ArrayList<>();

    for (Field field : fields) {
      final SearchIndexType searchIndexType = field.getAnnotation(SearchIndexType.class);

      if (searchIndexType == null) {
        throw new IllegalStateException(
            String.format("Field `%s` must be annotated with `SearchIndexType`.", field.getName()));
      }

      final String indexName = nameFn.apply(field.getName()).replaceAll("^_*", "").toLowerCase();

      final SearchIndexSettings settings = field.getAnnotation(SearchIndexSettings.class);
      final String settingsJson = settings == null ? null : loadResource(testInstance.getClass(), settings.value());

      final SearchIndexMappings mappings = field.getAnnotation(SearchIndexMappings.class);
      final String mappingsJson = mappings == null ? null : loadResource(testInstance.getClass(), mappings.value());

      final SearchIndex<?> index =
          indexFactory.createIndex(searchIndexType.value(), indexName, settingsJson, mappingsJson);
      field.set(testInstance, index);
      indices.add(index);
    }

    return indices;
  }

  private String loadResource(@Nonnull Class<?> testClass, @Nonnull String resource) throws IOException {
    final URL resourceUrl = testClass.getResource(resource);
    if (resourceUrl == null) {
      throw new IllegalArgumentException(String.format("Resource `%s` not found.", resource));
    }
    return IOUtils.toString(resourceUrl);
  }

  @Nonnull
  private Class<?> findContainerFactoryClass() {
    final List<Class<?>> classes = ReflectionUtils.findAllClassesInPackage("com.linkedin.metadata.testing",
        ClassFilter.of(clazz -> clazz.isAnnotationPresent(ElasticsearchContainerFactory.Implementation.class)));

    if (classes.size() == 0) {
      throw new IllegalStateException("Could not find any ElasticsearchContainerFactory implementations.");
    }

    if (classes.size() > 1) {
      throw new IllegalStateException(
          String.format("Found %s ElasticsearchContainerFactory implementations, expected 1. Found %s.", classes.size(),
              String.join(", ", classes.stream().map(Class::getSimpleName).collect(Collectors.toList()))));
    }

    return classes.get(0);
  }

  @Nonnull
  private ElasticsearchContainerFactory getContainerFactory() throws Exception {
    final Class<?> clazz = findContainerFactoryClass();

    if (!ElasticsearchContainerFactory.class.isAssignableFrom(clazz)) {
      throw new IllegalStateException(String.format(
          "Provided class `%s` to ElasticsearchIntegrationTest, but did not inherit from "
              + "ElasticsearchContainerFactory.", clazz.toString()));
    }

    Constructor<?> constructor;
    try {
      constructor = clazz.getConstructor();
    } catch (NoSuchMethodException e) {
      throw new NoSuchMethodException(String.format(
          "Expected ElasticsearchContainerFactory, `%s`, to have a default, public, constructor but found none.",
          clazz.toString()));
    }

    return (ElasticsearchContainerFactory) constructor.newInstance();
  }

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    final ExtensionContext.Store store = context.getStore(NAMESPACE);
    final ElasticsearchConnection connection = store.get(CONNECTION, ElasticsearchConnection.class);

    final List<Field> fields = ReflectionUtils.findFields(context.getRequiredTestClass(), field -> {
      return ReflectionUtils.isNotStatic(field) && ReflectionUtils.isPublic(field) && ReflectionUtils.isNotFinal(field)
          && SearchIndex.class.isAssignableFrom(field.getType());
    }, ReflectionUtils.HierarchyTraversalMode.TOP_DOWN);

    final SearchIndexFactory indexFactory = new SearchIndexFactory(connection);
    final List<SearchIndex<?>> indices = createIndices(indexFactory, context.getRequiredTestInstance(), fields,
        fieldName -> String.format("%s_%s_%s_%s", fieldName, context.getRequiredTestMethod().getName(),
            context.getRequiredTestClass().getSimpleName(), System.currentTimeMillis()));
    store.put(INDICIES, indices);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void afterAll(ExtensionContext context) throws Exception {
    final ExtensionContext.Store store = context.getStore(NAMESPACE);

    final List<SearchIndex<?>> indices = (List<SearchIndex<?>>) store.get(STATIC_INDICIES, List.class);
    final ElasticsearchConnection connection = store.get(CONNECTION, ElasticsearchConnection.class);

    cleanUp(connection, indices);

    // don't need to close the factory since it implements CloseableResource, junit will close it since it is in the
    // store
  }

  @SuppressWarnings("unchecked")
  @Override
  public void afterEach(ExtensionContext context) throws Exception {
    final ExtensionContext.Store store = context.getStore(NAMESPACE);

    final List<SearchIndex<?>> indices = (List<SearchIndex<?>>) store.get(INDICIES, List.class);
    final ElasticsearchConnection connection = store.get(CONNECTION, ElasticsearchConnection.class);

    if (indices != null) {
      cleanUp(connection, indices);
    }
  }

  private void cleanUp(@Nonnull ElasticsearchConnection connection, @Nonnull List<SearchIndex<?>> indices) {
    for (SearchIndex<?> i : indices) {
      connection.getTransportClient().admin().indices().prepareDelete(i.getName()).get();
    }

    indices.clear();
  }
}
