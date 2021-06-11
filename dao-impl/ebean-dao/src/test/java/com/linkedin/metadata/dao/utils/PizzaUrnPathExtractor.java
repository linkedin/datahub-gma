package com.linkedin.metadata.dao.utils;

import com.linkedin.metadata.dao.scsi.EntityTypeProvider;
import com.linkedin.metadata.dao.scsi.UrnPathExtractor;
import com.linkedin.testing.urn.PizzaUrn;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;


public class PizzaUrnPathExtractor implements UrnPathExtractor<PizzaUrn>, EntityTypeProvider {
  @Override
  public Map<String, Object> extractPaths(@Nonnull PizzaUrn urn) {
    return Collections.unmodifiableMap(new HashMap<String, Integer>() {
      {
        put("/pizzaId", urn.getPizzaId());
      }
    });
  }

  @Override
  public String entityType() {
    return "PizzaEntity";
  }
}