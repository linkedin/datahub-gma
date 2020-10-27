package com.linkedin.common.urn;

import java.net.URISyntaxException;
import javax.annotation.Nonnull;


/**
 * Static utilities for {@link Urn}.
 */
public final class Urns {
  private Urns() {
  }

  /**
   * Create a Urn from an entity type and an encoded String key. The key is converted to a Tuple by parsing using {@link
   * TupleKey#fromString(String)}.
   *
   * <p>This differs from the {@link Urn#Urn(String, String)} (and {@link
   * Urn#createFromTypeSpecificString(String, String)}) in that this does not have a checked {@link
   * java.net.URISyntaxException}, and instead will throw an {@link IllegalArgumentException} if the {@code
   * typeSpecificString} fails to parse.
   *
   * <p>The ideal usage for this is when calling this method with compile time constant strings that are known to be
   * good. If using dynamic or user input strings, it may be wiser to handle the {@link URISyntaxException}.
   *
   * @param entityType - the entity type for the Urn
   * @param typeSpecificString - the encoded string representation of a TupleKey
   * @throws IllegalArgumentException if the typeSpecificString is not a valid encoding of a TupleKey
   */
  public static Urn createFromTypeSpecificString(@Nonnull String entityType, @Nonnull String typeSpecificString) {
    try {
      return new Urn(entityType, typeSpecificString);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Failed to create Urn.", e);
    }
  }
}
