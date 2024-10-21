package com.linkedin.metadata.dao.ingestion;

import com.linkedin.data.template.RecordTemplate;
import lombok.Data;


/**
 * A key class used in the AspectCallbackRegistry map to uniquely identify aspect callback routing clients.
 * The key is a combination of the aspect class and the entity type.
 */
@Data
public class AspectCallbackMapKey {
  private final Class<? extends RecordTemplate> aspectClass;
  private final String entityType;

  /**
   * Indicates whether some other object is "equal to" this one.
   * Two AspectCallbackMapKey objects are considered equal if their aspectClass and entityType are equal.
   *
   * @param o the reference object with which to compare
   * @return true if this object is the same as the obj argument; false otherwise
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AspectCallbackMapKey that = (AspectCallbackMapKey) o;
    return aspectClass.equals(that.aspectClass) && entityType.equals(that.entityType);
  }

  /**
   * Returns a hash code value for the object.
   * The hash code is computed based on the aspectClass and entityType.
   *
   * @return a hash code value for this object
   */
  @Override
  public int hashCode() {
    int result = aspectClass.hashCode();
    result = 31 * result + entityType.hashCode();
    return result;
  }
}


