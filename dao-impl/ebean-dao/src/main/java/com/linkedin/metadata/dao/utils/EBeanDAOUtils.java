package com.linkedin.metadata.dao.utils;

import com.linkedin.metadata.dao.EbeanMetadataAspect;
import com.linkedin.metadata.dao.ListResult;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.SoftDeletedAspect;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;


/**
 * Miscellaneous utils used by metadata.dao package.
 */
@Slf4j
public class EBeanDAOUtils {

  public static final String DIFFERENT_RESULTS_TEMPLATE = "The results of %s from the new schema table and old schema table are not equal. "
      + "Defaulting to using the value(s) from the old schema table.";
  // String stored in metadata_aspect table for soft deleted aspect
  private static final RecordTemplate DELETED_METADATA = new SoftDeletedAspect().setGma_deleted(true);
  public static final String DELETED_VALUE = RecordUtils.toJsonString(DELETED_METADATA);

  private EBeanDAOUtils() {
    // Utils class
  }

  /**
   * Given urn string and Urn class, return Urn instance.
   * @param urn urn string
   * @param urnClass urn class
   * @param <URN> Urn instance
   * @return Urn instance
   */
  @Nonnull
  public static <URN> URN getUrn(@Nonnull String urn, @Nonnull Class<URN> urnClass) {
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
  public static <T> boolean compareResults(@Nullable List<T> resultOld, @Nullable List<T> resultNew, @Nonnull String methodName) {
    if (resultOld == null && resultNew == null) {
      return true;
    }
    if (resultOld == null || resultNew == null) {
      log.error(String.format(DIFFERENT_RESULTS_TEMPLATE, methodName));
      return false;
    }
    if (resultOld.size() != resultNew.size()) {
      return false;
    }
    if (resultOld.containsAll(resultNew)) {
      return true;
    }
    log.error(String.format(DIFFERENT_RESULTS_TEMPLATE, methodName));
    return false;
  }

  /**
   * Compare lists, which should be results from reading the old and new schema tables. If different, log an error and
   * return false. Otherwise, return true.
   * @param resultOld Results from reading the old schema table
   * @param resultNew Results from reading the new schema table
   * @param methodName Name of method that called this function, for logging purposes
   * @return Boolean indicating equivalence
   */
  public static <T> boolean compareResults(@Nullable ListResult<T> resultOld, @Nullable ListResult<T> resultNew,
      @Nonnull String methodName) {
    if (resultOld == null && resultNew == null) {
      return true;
    }
    if (resultOld == null || resultNew == null) {
      log.error(String.format(DIFFERENT_RESULTS_TEMPLATE, methodName));
      return false;
    }
    if (resultOld.equals(resultNew)) {
      return true;
    }
    log.error(String.format(DIFFERENT_RESULTS_TEMPLATE, methodName));
    return false;
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
