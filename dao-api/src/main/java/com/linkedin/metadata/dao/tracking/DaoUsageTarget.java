package com.linkedin.metadata.dao.tracking;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.Value;


/**
 * A neutral, model-agnostic descriptor of one entity touched by a DAO operation.
 *
 * <p>Holds only strings so the kernel stays free of any Avro / event-schema dependency.
 * The concrete {@link BaseDaoUsageEmitter} implementation in the service layer maps these
 * fields onto the wire event.
 */
@Value
public class DaoUsageTarget {

  /**
   * String form of the entity URN that was read or written.
   */
  @Nonnull
  String urn;

  /**
   * Simple names of the aspects touched for this URN. May be empty -- e.g. a whole-entity
   * {@code DELETE_ALL}, or a {@code create} with no aspect values. Do NOT infer the operation
   * kind from an empty list; use the event's {@code operationType} as the authoritative
   * discriminator.
   */
  @Nonnull
  List<String> aspects;

  public DaoUsageTarget(@Nonnull String urn, @Nonnull List<String> aspects) {
    this.urn = urn;
    // Copy before wrapping: unmodifiableList is only a view, so without the copy the caller could
    // still mutate the list afterwards. Targets are handed to an asynchronous emitter, so they may
    // be read on another thread well after construction.
    this.aspects = Collections.unmodifiableList(new ArrayList<>(aspects));
  }
}
