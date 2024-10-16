package com.linkedin.testing.urn;

import com.linkedin.common.urn.Urn;
import java.net.URISyntaxException;


public final class BarUrn extends Urn {

  public static final String ENTITY_TYPE = "bar";

  public BarUrn(int id) throws URISyntaxException {
    super(ENTITY_TYPE, Integer.toString(id));
  }

  public static BarUrn createFromString(String rawUrn) throws URISyntaxException {
    return new BarUrn(Urn.createFromString(rawUrn).getIdAsInt());
  }

  public Integer getBarIdEntity() {
    return Integer.valueOf(getEntityKey().get(0));
  }
}
