package com.linkedin.metadata.dao.utils;

import com.linkedin.common.urn.Urn;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import javax.annotation.Nonnull;


/**
 * Miscellaneous utils used by metadata.dao package.
 */
public class EBeanDAOUtils {

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
}
