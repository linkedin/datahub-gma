package com.linkedin.metadata.dao.ingestion.preupdate;

import com.google.protobuf.Message;
import com.linkedin.data.template.RecordTemplate;
import lombok.Data;


@Data
public class PreRoutingAccessor {

  public PreUpdateRoutingClient<? extends RecordTemplate> preUpdateClient;

  public enum RoutingAction {
    PROCEED, SKIP
  }
}
