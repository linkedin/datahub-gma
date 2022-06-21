package com.linkedin.metadata.dao.utils;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.dao.EbeanMetadataAspect;
import com.linkedin.metadata.dao.ListResult;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.SoftDeletedAspect;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.functors.DefaultEquator;


/**
 * Miscellaneous utils used by metadata.dao package.
 */
@Slf4j
public class EBeanDAOUtils {

  public static final String DIFFERENT_RESULTS_TEMPLATE = "The results of %s from the new schema table and old schema table are not equal."
      + "Defaulting to using the value(s) from the old schema table.";
  // String stored in metadata_aspect table for soft deleted aspect
  private static final RecordTemplate DELETED_METADATA = new SoftDeletedAspect().setGma_deleted(true);
  public static final String DELETED_VALUE = RecordUtils.toJsonString(DELETED_METADATA);

  private EBeanDAOUtils() {

  }

  /**
   * Given urn class, return the entity type as string.
   * @param urnClass urn class that extends {@link Urn}
   * @param <URN> Urn type
   * @return entity type as string
   */
  public static <URN extends Urn> String getEntityType(Class<URN> urnClass) {
    try {
      Field field  = urnClass.getDeclaredField("ENTITY_TYPE");
      return (String) field.get(null);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new IllegalArgumentException("invalid Urn class: " + urnClass.getName());
    }
  }

  /**
   * Given urn string and Urn class, return Urn instance.
   * @param urn urn string
   * @param urnClass urn class
   * @param <URN> Urn instance
   * @return Urn instance
   */
  @Nonnull
  public static <URN> URN getUrn(@Nonnull String urn, Class<URN> urnClass) {
    try {
      final Method getUrn = urnClass.getMethod("createFromString", String.class);
      return urnClass.cast(getUrn.invoke(null, urn));
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      throw new IllegalArgumentException("URN conversion error for " + urn, e);
    }
  }

  /**
   * Calculate the counter part of floorDiv. E.g. ceilDiv(3, 2) = 2.
   * Reference: https://stackoverflow.com/questions/27643616/ceil-conterpart-for-math-floordiv-in-java
   * @param x x integer
   * @param y y integer
   * @return ceiling of x / y
   */
  public static int ceilDiv(int x, int y) {
    return -Math.floorDiv(-x, y);
  }

  /**
   * Compare lists, which should be results from reading the old and new schema tables. If different, log an error and
   * return false. Otherwise, return true.
   * @param resultOld Results from reading the old schema table
   * @param resultNew Results from reading the new schema table
   * @param methodName Name of method that called this function, for logging purposes
   * @return Boolean indicating equivalence
   */
  public static <T> boolean compareResults(List<T> resultOld, List<T> resultNew, String methodName) {
    // TODO: This needs testing
    // https://commons.apache.org/proper/commons-collections/javadocs/api-4.4/org/apache/commons/collections4/CollectionUtils.html#isEqualCollection-java.util.Collection-java.util.Collection-org.apache.commons.collections4.Equator-
    final DefaultEquator<EbeanMetadataAspect> equator = DefaultEquator.defaultEquator();
    if (!CollectionUtils.isEqualCollection(resultOld, resultNew, equator)) {
      log.error(String.format(DIFFERENT_RESULTS_TEMPLATE), methodName);
      return false;
    }
    return true;
  }

  /**
   * Compare lists, which should be results from reading the old and new schema tables. If different, log an error and
   * return false. Otherwise, return true.
   * @param resultOld Results from reading the old schema table
   * @param resultNew Results from reading the new schema table
   * @param methodName Name of method that called this function, for logging purposes
   * @return Boolean indicating equivalence
   */
  public static <T> boolean compareResults(ListResult<T> resultOld, ListResult<T> resultNew, String methodName) {
    if (!Objects.equals(resultOld.getValues(), resultNew.getValues())) {
      log.error(String.format(DIFFERENT_RESULTS_TEMPLATE), methodName);
      return false;
    }
    return true;
  }

  /**
   * Checks whether the aspect record has been soft deleted.
   *
   * @param aspect aspect value
   * @param aspectClass the type of the aspect
   * @return boolean representing whether the aspect record has been soft deleted
   */
  public static <ASPECT extends RecordTemplate> boolean isSoftDeletedAspect(@Nonnull EbeanMetadataAspect aspect,
      @Nonnull Class<ASPECT> aspectClass) {
    // Convert metadata string to record template object
    final RecordTemplate metadataRecord = RecordUtils.toRecordTemplate(aspectClass, aspect.getMetadata());
    return metadataRecord.equals(DELETED_METADATA);
  }
}
