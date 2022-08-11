package com.linkedin.testing.urn;

import com.linkedin.common.urn.TupleKey;
import com.linkedin.common.urn.Urn;
import java.net.URISyntaxException;


public final class BurgerUrn extends Urn {

  public static final String ENTITY_TYPE = "burger";
  // Can be obtained via getEntityKey, but not in open source. We need to unify the internal / external URN definitions.
  private final String _burgerName;

  public BurgerUrn(String burgerName) {
    super(ENTITY_TYPE, TupleKey.create(burgerName));
    this._burgerName = burgerName;
  }

  public String getBurgerName() {
    return _burgerName;
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

  public static BurgerUrn createFromString(String rawUrn) throws URISyntaxException {
    final Urn urn = Urn.createFromString(rawUrn);

    if (!ENTITY_TYPE.equals(urn.getEntityType())) {
      throw new URISyntaxException(urn.toString(), "Can't cast Urn to BurgerUrn, not same ENTITY");
    }

    return new BurgerUrn(urn.getId());
  }
}
