package com.linkedin.testing.urn;

import com.linkedin.common.urn.TupleKey;
import com.linkedin.common.urn.Urn;
import java.net.URISyntaxException;


public final class PizzaUrn extends Urn {

  public static final String ENTITY_TYPE = "pizza";
  // Can be obtained via getEntityKey, but not in open source. We need to unify the internal / external URN definitions.
  private final int _id;

  public PizzaUrn(int id) {
    super(ENTITY_TYPE, TupleKey.create(id));
    this._id = id;
  }

  public int getPizzaId() {
    return _id;
  }

  @Override
  public boolean equals(Object obj) {
    // Override for find bugs, bug delegate to super implementation, both in open source and internally.
    return super.equals(obj);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  public static PizzaUrn createFromString(String rawUrn) throws URISyntaxException {
    final Urn urn = Urn.createFromString(rawUrn);

    if (!ENTITY_TYPE.equals(urn.getEntityType())) {
      throw new URISyntaxException(urn.toString(), "Can't cast Urn to PizzaUrn, not same ENTITY");
    }

    return new PizzaUrn(urn.getIdAsInt());
  }

  public static PizzaUrn deserialize(String serializedUrn) throws URISyntaxException {
    return createFromString(serializedUrn);
  }
}
