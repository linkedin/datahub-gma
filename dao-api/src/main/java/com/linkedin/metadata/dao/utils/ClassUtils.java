package com.linkedin.metadata.dao.utils;

import com.linkedin.metadata.dao.exception.ModelConversionException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class ClassUtils {
  private static final Map<String, Class> CACHED_CLASSES = new ConcurrentHashMap<>();

  private ClassUtils() {
    //Utils class
  }

  /**
   * Get the class associated with the canonical name with caching capability.
   * @param classCanonicalName Class canonical name
   * @return Class associated with the canonical name
   */
  public static Class loadClass(final String classCanonicalName) {
    return CACHED_CLASSES.computeIfAbsent(classCanonicalName, className -> {
      try {
        return Class.forName(classCanonicalName);
      } catch (ClassNotFoundException e) {
        throw new ModelConversionException("Unable to find class " + classCanonicalName);
      }
    });
  }
}
