package com.linkedin.metadata.dao.ingestion.preupdate;

import com.linkedin.data.template.RecordTemplate;
import lombok.Data;


@Data
public class PreUpdateRoutingAccessor {

  public PreUpdateRoutingClient<? extends RecordTemplate> preUpdateClient;

  public enum RoutingAction {
    PROCEED, SKIP
  }
}
